//! Cross-layout equivalence probe: the same queries against two cluster
//! layouts of the SAME corpus (e.g. 8-shard vs monolithic) must return
//! bitwise-identical top-k score multisets. Global ids differ by layout
//! (slot offsets), so scores are the comparable signature; ties at the
//! k boundary may legitimately pick different equal-scoring members.
//!
//! ```text
//! layout_equivalence --cluster-a=127.0.0.1:59300,... --cluster-b=127.0.0.1:59520 \
//!     --probes-from=/corpus/embeddings.bin --queries=20 --k=100
//! ```

use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::court;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn nodes(key: &str) -> Vec<String> {
    arg(key, "")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.starts_with("http://") {
                s.to_string()
            } else {
                format!("http://{s}")
            }
        })
        .collect()
}

/// Top-k score bits for one query through a coordinator, descending.
async fn scores(
    coordinator: &CoordinatorServiceImpl,
    tag: &str,
    vector: &[f32],
    k: u32,
) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    let result = coordinator.fanout_search(tag, vector, k, true).await?;
    let mut all: Vec<f32> = result
        .shard_hits
        .iter()
        .flat_map(|(_, hits)| hits.iter().map(|&(_, s)| s))
        .collect();
    all.sort_by(|a, b| b.total_cmp(a));
    all.truncate(k as usize);
    Ok(all.into_iter().map(f32::to_bits).collect())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = nodes("cluster-a");
    let b = nodes("cluster-b");
    let probes_path = arg("probes-from", "/work/court-corpus/embeddings-full.bin");
    let queries: usize = arg("queries", "20").parse()?;
    let k: u32 = arg("k", "100").parse()?;
    if a.is_empty() || b.is_empty() {
        return Err("--cluster-a and --cluster-b are required".into());
    }
    let coord_a = CoordinatorServiceImpl::new(a.clone());
    let coord_b = CoordinatorServiceImpl::new(b.clone());

    let (dim, reader) = court::EmbeddingReader::open(std::path::Path::new(&probes_path))?;
    // Spread probes through the corpus rather than taking a prefix.
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
        "{} probes of dim {dim}, k={k}: {}-node layout vs {}-node layout",
        probes.len(),
        a.len(),
        b.len()
    );

    for (qi, vector) in probes.iter().enumerate() {
        let sa = scores(&coord_a, &format!("eq-a-{qi}"), vector, k).await?;
        let sb = scores(&coord_b, &format!("eq-b-{qi}"), vector, k).await?;
        if sa != sb {
            let first = sa.iter().zip(&sb).position(|(x, y)| x != y);
            return Err(format!(
                "MISMATCH at probe {qi}: score lists diverge at rank {:?} \
                 (a has {} scores, b has {})",
                first,
                sa.len(),
                sb.len()
            )
            .into());
        }
    }
    println!(
        "PASS: {} probes, top-{k} score multisets bitwise-identical across layouts",
        probes.len()
    );
    Ok(())
}
