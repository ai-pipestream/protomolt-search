//! Quantization recall at full corpus scale: exact fp32 brute-force
//! top-k over the embeddings file (ground truth) vs the quantized
//! cluster's top-k for the same probes. This isolates the 4-bit TQ+
//! encoding loss — the one tax the engine pays — from the distributed
//! protocol, which is proven lossless separately.
//!
//! Point --cluster at a layout whose global ids equal embeddings-file
//! record indexes (the monolithic layout, or any contiguous-slot
//! layout with matching stride).
//!
//! ```text
//! fp32_recall --cluster=127.0.0.1:59520 \
//!     --probes-from=/corpus/embeddings.bin --queries=20 --k=10,100
//! ```

use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::demo::court;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// Exact fp32 cosine top-k for every probe in one streaming pass over
/// the embeddings file (vectors are unit-normalized at embed time, so
/// the dot product is the cosine).
fn fp32_topk(
    path: &str,
    probes: &[Vec<f32>],
    k: usize,
) -> Result<Vec<Vec<(u64, f32)>>, Box<dyn std::error::Error>> {
    let (_, reader) = court::EmbeddingReader::open(std::path::Path::new(path))?;
    // One min-heap per probe as (score, id) with lazy sort at the end;
    // a simple threshold guard keeps heap churn negligible.
    let mut tops: Vec<Vec<(f32, u64)>> = vec![Vec::with_capacity(k + 1); probes.len()];
    let mut floors: Vec<f32> = vec![f32::NEG_INFINITY; probes.len()];
    for (idx, record) in reader.enumerate() {
        let v = record?.vector;
        for (p, probe) in probes.iter().enumerate() {
            let score: f32 = probe.iter().zip(&v).map(|(a, b)| a * b).sum();
            if score <= floors[p] {
                continue;
            }
            let top = &mut tops[p];
            top.push((score, idx as u64));
            if top.len() > k {
                // Drop the current minimum and raise the floor.
                let (mi, _) = top
                    .iter()
                    .enumerate()
                    .min_by(|a, b| a.1 .0.total_cmp(&b.1 .0))
                    .expect("nonempty");
                top.swap_remove(mi);
                floors[p] = top.iter().map(|&(s, _)| s).fold(f32::INFINITY, f32::min);
            }
        }
    }
    Ok(tops
        .into_iter()
        .map(|mut t| {
            t.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            t.into_iter().map(|(s, id)| (id, s)).collect()
        })
        .collect())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cluster: Vec<String> = arg("cluster", "127.0.0.1:59520")
        .split(',')
        .map(|s| format!("http://{}", s.trim()))
        .collect();
    let probes_path = arg("probes-from", "/work/court-corpus/embeddings-full.bin");
    let queries: usize = arg("queries", "20").parse()?;
    let ks: Vec<usize> = arg("k", "10,100")
        .split(',')
        .map(|s| s.trim().parse().expect("--k"))
        .collect();
    let kmax = *ks.iter().max().expect("at least one k");

    let (dim, reader) = court::EmbeddingReader::open(std::path::Path::new(&probes_path))?;
    let mut probes: Vec<Vec<f32>> = Vec::with_capacity(queries);
    for (i, record) in reader.enumerate() {
        if i % 4_000_037 == 0 {
            probes.push(record?.vector);
            if probes.len() >= queries {
                break;
            }
        } else {
            record?;
        }
    }
    eprintln!(
        "{} probes of dim {dim}; streaming fp32 ground truth (one pass)...",
        probes.len()
    );
    let t0 = std::time::Instant::now();
    let truth = fp32_topk(&probes_path, &probes, kmax)?;
    eprintln!("fp32 pass done in {:?}", t0.elapsed());

    let coordinator = CoordinatorServiceImpl::new(cluster);
    let mut quant: Vec<Vec<u64>> = Vec::with_capacity(probes.len());
    for (qi, vector) in probes.iter().enumerate() {
        let result = coordinator
            .fanout_search(&format!("recall-{qi}"), vector, kmax as u32, true, &Default::default())
            .await?;
        let mut hits: Vec<(u64, f32)> = result
            .shard_hits
            .iter()
            .flat_map(|(_, hits)| hits.iter().copied())
            .collect();
        hits.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        hits.truncate(kmax);
        quant.push(hits.into_iter().map(|(id, _)| id).collect());
    }

    for &k in &ks {
        let mut total = 0usize;
        for (t, q) in truth.iter().zip(&quant) {
            let want: std::collections::HashSet<u64> =
                t.iter().take(k).map(|&(id, _)| id).collect();
            total += q.iter().take(k).filter(|id| want.contains(id)).count();
        }
        let recall = total as f64 / (k * probes.len()) as f64;
        println!(
            "recall@{k}: {recall:.4}  ({total}/{} across {} probes)",
            k * probes.len(),
            probes.len()
        );
    }

    // Retrieve-then-rerank: score the quantized top-kmax pool with exact
    // fp32 (seek reads into the fixed-stride file), and measure recall of
    // the reranked top-k. This is the cheap recovery path the raw vectors
    // on disk make possible.
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(&probes_path)?;
    let rec = 12 + dim as u64 * 4;
    let mut fetch = |id: u64| -> Result<Vec<f32>, std::io::Error> {
        file.seek(SeekFrom::Start(12 + id * rec + 12))?;
        let mut buf = vec![0u8; dim as usize * 4];
        file.read_exact(&mut buf)?;
        Ok(buf
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    };
    let mut reranked: Vec<Vec<u64>> = Vec::with_capacity(probes.len());
    for (probe, pool) in probes.iter().zip(&quant) {
        let mut scored: Vec<(u64, f32)> = Vec::with_capacity(pool.len());
        for &id in pool {
            let v = fetch(id)?;
            scored.push((id, probe.iter().zip(&v).map(|(a, b)| a * b).sum()));
        }
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        reranked.push(scored.into_iter().map(|(id, _)| id).collect());
    }
    for &k in &ks {
        if k == kmax {
            continue; // reranking within the pool cannot change recall@kmax
        }
        let mut total = 0usize;
        for (t, r) in truth.iter().zip(&reranked) {
            let want: std::collections::HashSet<u64> =
                t.iter().take(k).map(|&(id, _)| id).collect();
            total += r.iter().take(k).filter(|id| want.contains(id)).count();
        }
        let recall = total as f64 / (k * probes.len()) as f64;
        println!(
            "rerank(fp32 over quantized top-{kmax}) recall@{k}: {recall:.4}  ({total}/{})",
            k * probes.len()
        );
    }
    Ok(())
}
