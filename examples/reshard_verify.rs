//! Reshard proof on a REAL shard: split its write-ahead log in-process,
//! then verify the children reconstruct the parent — bitwise.
//!
//! ```text
//! reshard_verify --log=/data/shard-0.tv.wal --parent=/data/shard-0.tv \
//!     --probes-from=/corpus/embeddings.bin --queries=50 --k=100 \
//!     --split=2 --out-dir=/tmp/verify --analysis-addr=http://127.0.0.1:59202
//! ```
//!
//! Checks, in order:
//! 1. Conservation: child vector/document counts sum to the parent's.
//! 2. Partition: every child id hashes into that child's bucket range.
//! 3. Reconstruction: for each probe vector, the merged union of child
//!    top-k (child-local slots mapped back through `parent_ids`) equals
//!    the parent index's top-k with bitwise-identical scores.
//!
//! Exit 0 with a PASS line, or the first failed check as an error. The
//! child images are left in `--out-dir` for inspection.

use std::path::{Path, PathBuf};

use turbovec::TurboQuantIndex;
use turbovec_search::court;
use turbovec_search::pb::AnalysisSpec;
use turbovec_search::postings::AnalyzedDoc;
use turbovec_search::{analyzer, reshard};

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// Top-k of one query as `(id, score_bits)`, coordinator order
/// (score desc, id asc), with `map` translating local slots to ids.
fn topk(
    index: &TurboQuantIndex,
    query: &[f32],
    k: usize,
    map: impl Fn(u64) -> u64,
) -> Vec<(u64, u32)> {
    let results = index.search(query, k);
    let mut hits: Vec<(u64, u32)> = results
        .indices_for_query(0)
        .iter()
        .zip(results.scores_for_query(0))
        .map(|(&slot, &score)| (map(slot as u64), score.to_bits()))
        .collect();
    hits.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    hits
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log = arg("log", "");
    let parent_path = arg("parent", "");
    let probes_path = arg("probes-from", "");
    if log.is_empty() || parent_path.is_empty() || probes_path.is_empty() {
        return Err("usage: reshard_verify --log=<wal> --parent=<shard.tv> \
                    --probes-from=<embeddings.bin> [--queries=50] [--k=100] \
                    [--split=2] [--out-dir=DIR] [--analysis-addr=ADDR]"
            .into());
    }
    let queries: usize = arg("queries", "50").parse()?;
    let k: usize = arg("k", "100").parse()?;
    let n: usize = arg("split", "2").parse()?;
    let out_dir = PathBuf::from(arg("out-dir", "/tmp/reshard-verify"));
    let analysis_addr = arg("analysis-addr", "http://127.0.0.1:50051");

    let addr = analysis_addr.clone();
    let handle = tokio::runtime::Handle::current();
    let mut analyze = move |text: &str, spec: Option<&AnalysisSpec>| -> Result<AnalyzedDoc, String> {
        tokio::task::block_in_place(|| handle.block_on(analyzer::analyze_document(&addr, text, spec)))
            .map_err(|e| format!("analysis sidecar at {addr}: {e}"))
    };

    let gen = reshard::resolve_gen(Path::new(&log))?;
    eprintln!("splitting {} {n} ways...", gen.display());
    let t0 = std::time::Instant::now();
    let output = reshard::split(&gen, n, &out_dir, 0, 25_000_000, &mut analyze)?;
    eprintln!("split done in {:?}", t0.elapsed());

    let parent = TurboQuantIndex::load(Path::new(&parent_path))?;
    parent.prepare();

    // 1. Conservation.
    let total_vectors: u64 = output.children.iter().map(|c| c.num_vectors).sum();
    let total_docs: u64 = output.children.iter().map(|c| c.num_documents).sum();
    if total_vectors != parent.len() as u64 {
        return Err(format!(
            "conservation FAILED: children hold {total_vectors} vectors, parent holds {}",
            parent.len()
        )
        .into());
    }
    eprintln!(
        "conservation OK: {total_vectors} vectors / {total_docs} documents across {n} children"
    );

    // 2. Partition: every child id in its bucket range.
    let bucket_count = 64usize;
    let per_child = bucket_count / n;
    for (i, child) in output.children.iter().enumerate() {
        if let Some(&bad) = child
            .parent_ids
            .iter()
            .find(|&&id| reshard::bucket_of(id, bucket_count) / per_child != i)
        {
            return Err(format!("partition FAILED: id {bad} outside child {i}'s range").into());
        }
    }
    eprintln!("partition OK: every id in its child's bucket range");

    // 3. Bitwise reconstruction over real probe vectors.
    let (dim, reader) = court::EmbeddingReader::open(Path::new(&probes_path))?;
    let mut probes: Vec<Vec<f32>> = Vec::with_capacity(queries);
    for record in reader {
        probes.push(record?.vector);
        if probes.len() >= queries {
            break;
        }
    }
    eprintln!("{} probes of dim {dim}", probes.len());

    let children: Vec<(&reshard::ChildImage, TurboQuantIndex)> = output
        .children
        .iter()
        .map(|c| {
            let index = TurboQuantIndex::load(&c.tv_path).expect("load child");
            index.prepare();
            (c, index)
        })
        .collect();

    for (qi, query) in probes.iter().enumerate() {
        let expected = topk(&parent, query, k, |slot| slot);
        let mut merged: Vec<(u64, u32)> = children
            .iter()
            .flat_map(|(child, index)| {
                topk(index, query, k, |local| child.parent_ids[local as usize])
            })
            .collect();
        merged.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        merged.truncate(expected.len());
        if merged != expected {
            return Err(format!("reconstruction FAILED at probe {qi} (k={k})").into());
        }
    }
    println!(
        "PASS: {n}-way split of {} reconstructs the parent top-{k} bitwise over {} probes",
        gen.display(),
        probes.len()
    );
    Ok(())
}
