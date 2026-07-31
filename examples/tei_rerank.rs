//! The live-rerank experiment: does a REAL transformer (TEI,
//! all-MiniLM-L6-v2 — the teacher family of the static model2vec index
//! embeddings) reranking the quantized index's k=1000 pool recover the
//! loss from 4-bit quantization?
//!
//! Design: for each query text,
//!   1. embed with the sidecar's model2vec (the index's space) and take
//!      turbovec's k=1000 over the LIVE cluster (quantized pool);
//!   2. compute the EXACT fp32 model2vec top-1000 by streaming the full
//!      embeddings file (exact pool) — the no-quantization counterfactual;
//!   3. fetch both pools' chunk texts from the owning shards, re-embed
//!      them (and the query) with TEI, and rerank each pool by TEI cosine;
//!   4. recall@k = overlap of the two TEI-reranked top-k lists.
//!
//! A recall of 1.0 means the quantized index's pool, after the live TEI
//! rerank, ends in EXACTLY the results the lossless index would have
//! produced — the quantization loss is invisible behind the reranker.
//! Pool overlap@1000 is reported alongside as the pre-rerank baseline.
//!
//! ```text
//! tei_rerank --coordinator=http://127.0.0.1:59291 \
//!     --nodes=127.0.0.1:59300,...,127.0.0.1:59307 \
//!     --analysis=http://127.0.0.1:59202 --tei=http://127.0.0.1:8085
//! ```

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io::Read;
use std::time::Instant;

use turbovec_search::analyzer;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::tei::embed_client::EmbedClient;
use turbovec_search::pb::tei::EmbedRequest;
use turbovec_search::pb::{GetDocumentsRequest, HealthRequest, SearchRequest};

const DIM: usize = 256;
const RECORD_HEADER: usize = 12;
const SLOT_STRIDE: u64 = 25_000_000;
const QUERIES: &[&str] = &[
    "artificial intelligence copyright law",
    "habeas corpus ineffective assistance of counsel",
    "fourth amendment warrantless search of a vehicle",
    "patent claim construction doctrine of equivalents",
    "employment discrimination retaliation burden shifting",
    "eminent domain just compensation fair market value",
    "products liability failure to warn defective design",
    "securities fraud scienter pleading standard",
    "first amendment commercial speech regulation",
    "bankruptcy automatic stay relief from creditors",
    "child custody best interests of the child",
    "antitrust price fixing per se rule",
    "insurance bad faith denial of coverage",
    "medical malpractice standard of care expert testimony",
    "contract breach consequential damages foreseeability",
];

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

/// Min-heap entry so the heap root is the weakest of the kept top-k.
#[derive(PartialEq)]
struct Entry(f32, u64);
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .0
            .total_cmp(&self.0)
            .then_with(|| other.1.cmp(&self.1))
    }
}

/// Stream the whole embeddings file once, keeping the exact fp32 top-k
/// per query. Returns per-query `(file_index, score)` lists, best first.
fn exact_pools(path: &str, queries: &[Vec<f32>], k: usize) -> Vec<Vec<(u64, f32)>> {
    let file = std::fs::File::open(path).expect("open embeddings");
    let mut reader = std::io::BufReader::with_capacity(1 << 24, file);
    // File header: 8-byte magic + u32 dim, then the records.
    let mut header = [0u8; 12];
    reader.read_exact(&mut header).expect("embeddings header");
    assert_eq!(
        u32::from_le_bytes(header[8..12].try_into().expect("4 bytes")) as usize,
        DIM,
        "embeddings dim mismatch"
    );
    let record_bytes = RECORD_HEADER + DIM * 4;
    const BLOCK_ROWS: usize = 262_144;
    let mut block = vec![0u8; BLOCK_ROWS * record_bytes];
    let mut heaps: Vec<BinaryHeap<Entry>> = queries.iter().map(|_| BinaryHeap::new()).collect();
    let mut base_row: u64 = 0;
    let threads = std::thread::available_parallelism().map_or(16, |n| n.get());
    loop {
        let mut filled = 0;
        while filled < block.len() {
            match reader.read(&mut block[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => panic!("read embeddings: {e}"),
            }
        }
        if filled == 0 {
            break;
        }
        assert_eq!(filled % record_bytes, 0, "torn record at row {base_row}");
        let rows = filled / record_bytes;
        let data = &block[..filled];
        // Score the block in parallel slices; each slice returns its own
        // top-k per query, merged into the global heaps afterwards.
        let per_slice = rows.div_ceil(threads);
        let partials: Vec<Vec<Vec<(f32, u64)>>> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..threads)
                .map(|t| {
                    let start = t * per_slice;
                    let end = ((t + 1) * per_slice).min(rows);
                    let queries = &queries;
                    scope.spawn(move || {
                        let mut local: Vec<Vec<(f32, u64)>> =
                            queries.iter().map(|_| Vec::new()).collect();
                        for row in start..end {
                            let rec = &data[row * record_bytes..(row + 1) * record_bytes];
                            let vec_bytes = &rec[RECORD_HEADER..];
                            for (qi, q) in queries.iter().enumerate() {
                                let mut dot = 0.0f32;
                                for d in 0..DIM {
                                    let v = f32::from_le_bytes(
                                        vec_bytes[d * 4..d * 4 + 4].try_into().expect("4 bytes"),
                                    );
                                    dot += v * q[d];
                                }
                                local[qi].push((dot, base_row + row as u64));
                            }
                        }
                        for l in &mut local {
                            l.sort_by(|a, b| b.0.total_cmp(&a.0));
                            l.truncate(k);
                        }
                        local
                    })
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).collect()
        });
        for partial in partials {
            for (qi, list) in partial.into_iter().enumerate() {
                for (score, idx) in list {
                    if heaps[qi].len() < k {
                        heaps[qi].push(Entry(score, idx));
                    } else if score > heaps[qi].peek().expect("non-empty").0 {
                        heaps[qi].pop();
                        heaps[qi].push(Entry(score, idx));
                    }
                }
            }
        }
        base_row += rows as u64;
        if rows < BLOCK_ROWS {
            break;
        }
    }
    eprintln!("exact scan covered {base_row} rows");
    heaps
        .into_iter()
        .map(|h| {
            let mut v: Vec<(u64, f32)> = h.into_iter().map(|Entry(s, i)| (i, s)).collect();
            v.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            v
        })
        .collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        dot += f64::from(*x) * f64::from(*y);
        na += f64::from(*x) * f64::from(*x);
        nb += f64::from(*y) * f64::from(*y);
    }
    dot / (na.sqrt() * nb.sqrt())
}

fn overlap(a: &[u64], b: &[u64]) -> f64 {
    let set: HashSet<u64> = b.iter().copied().collect();
    a.iter().filter(|x| set.contains(x)).count() as f64 / b.len() as f64
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let coordinator = arg("coordinator", "http://127.0.0.1:59291");
    let nodes: Vec<String> = arg(
        "nodes",
        "127.0.0.1:59300,127.0.0.1:59301,127.0.0.1:59302,127.0.0.1:59303,127.0.0.1:59304,127.0.0.1:59305,127.0.0.1:59306,127.0.0.1:59307",
    )
    .split(',')
    .map(|s| format!("http://{}", s.trim_start_matches("http://")))
    .collect();
    let analysis = arg("analysis", "http://127.0.0.1:59202");
    let tei = arg("tei", "http://127.0.0.1:8085");
    let embeddings = arg("embeddings", "/work/court-corpus/embeddings-full.bin");
    let rerank_ks: Vec<usize> = vec![10, 100];
    let pool_k: usize = arg("pool-k", "1000").parse()?;

    // Per-shard vector counts -> contiguous file-range starts, for the
    // global-id <-> file-index mapping (shard i holds file rows
    // [start_i, start_i + n_i) as global ids i*25M + local).
    let mut counts = Vec::new();
    for node in &nodes {
        let mut client = NodeServiceClient::connect(node.clone()).await?;
        counts.push(client.health(HealthRequest {}).await?.into_inner().num_vectors);
    }
    let mut starts = vec![0u64; counts.len()];
    for i in 1..counts.len() {
        starts[i] = starts[i - 1] + counts[i - 1];
    }
    let file_to_gid = |file_index: u64| -> u64 {
        let shard = match starts.binary_search(&file_index) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        shard as u64 * SLOT_STRIDE + (file_index - starts[shard])
    };
    let gid_to_shard = |gid: u64| (gid / SLOT_STRIDE) as usize;

    // Model2vec query embeddings (the index's space).
    let mut m2v_queries = Vec::new();
    for q in QUERIES {
        m2v_queries.push(analyzer::embed_text(&analysis, q).await?);
    }

    // Quantized pools from the live cluster.
    let mut search = SearchServiceClient::connect(coordinator.clone()).await?;
    let mut quant_pools: Vec<Vec<u64>> = Vec::new();
    let t = Instant::now();
    for q in &m2v_queries {
        let hits = search
            .search(SearchRequest {
                request_id: String::new(),
                k: pool_k as u32,
                vector: q.clone(),
            })
            .await?
            .into_inner()
            .hits;
        quant_pools.push(hits.into_iter().map(|h| h.vector_id).collect());
    }
    eprintln!(
        "quantized pools: {} queries x k={pool_k} in {:?}",
        QUERIES.len(),
        t.elapsed()
    );

    // Exact fp32 pools from one streaming pass over the embeddings file.
    let t = Instant::now();
    let exact = exact_pools(&embeddings, &m2v_queries, pool_k);
    eprintln!("exact fp32 pools in {:?}", t.elapsed());
    let exact_pools_gid: Vec<Vec<u64>> = exact
        .iter()
        .map(|pool| pool.iter().map(|&(idx, _)| file_to_gid(idx)).collect())
        .collect();

    // Texts for the union of both pools, one GetDocuments per shard.
    let t = Instant::now();
    let mut wanted: HashSet<u64> = HashSet::new();
    for pool in quant_pools.iter().chain(exact_pools_gid.iter()) {
        wanted.extend(pool.iter().copied());
    }
    let mut by_shard: HashMap<usize, Vec<u64>> = HashMap::new();
    for gid in &wanted {
        by_shard.entry(gid_to_shard(*gid)).or_default().push(*gid);
    }
    let mut texts: HashMap<u64, String> = HashMap::new();
    for (shard, doc_ids) in by_shard {
        let mut client = NodeServiceClient::connect(nodes[shard].clone()).await?
            .max_decoding_message_size(turbovec_search::MAX_MESSAGE_BYTES);
        for ids in doc_ids.chunks(2000) {
            let found = client
                .get_documents(GetDocumentsRequest { doc_ids: ids.to_vec() })
                .await?
                .into_inner();
            for doc in found.documents {
                texts.insert(doc.doc_id, doc.text);
            }
        }
    }
    eprintln!(
        "fetched {} texts for {} wanted ids in {:?}",
        texts.len(),
        wanted.len(),
        t.elapsed()
    );

    // TEI embeddings for every pooled text + the queries, concurrently.
    let t = Instant::now();
    let ids: Vec<u64> = texts.keys().copied().collect();
    let concurrency = 32;
    // ONE h2 channel, cloned per call: tonic multiplexes concurrent
    // requests, and a connect-per-text at this volume reset the server
    // (the same lesson as the analysis sidecar's shared_channel).
    let tei_channel = tonic::transport::Endpoint::from_shared(tei.clone())?
        .connect()
        .await?;
    let tei_of: HashMap<u64, Vec<f32>> = {
        let mut out = HashMap::new();
        let mut inflight = tokio::task::JoinSet::new();
        let mut next = 0usize;
        while next < ids.len() || !inflight.is_empty() {
            while next < ids.len() && inflight.len() < concurrency {
                let id = ids[next];
                // MiniLM truncates at 256 tokens; 4000 chars covers that
                // with margin, and a multi-MB outlier chunk would tear
                // down the whole shared h2 connection.
                let text: String = texts[&id].chars().take(4000).collect();
                let mut client = EmbedClient::new(tei_channel.clone());
                let addr = tei.clone();
                inflight.spawn(async move {
                    let request = || EmbedRequest {
                        inputs: text.clone(),
                        truncate: true,
                        normalize: Some(true),
                        truncation_direction: 0,
                        prompt_name: None,
                        dimensions: None,
                    };
                    let mut attempt = 0;
                    loop {
                        match client.embed(request()).await {
                            Ok(r) => return (id, r.into_inner().embeddings),
                            Err(e) if attempt < 3 => {
                                attempt += 1;
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    200 * attempt,
                                ))
                                .await;
                                // Fresh connection: the shared channel may
                                // be the casualty of another task's failure.
                                client = EmbedClient::connect(addr.clone())
                                    .await
                                    .expect("reconnect TEI");
                            }
                            Err(e) => panic!("TEI embed after {attempt} retries: {e}"),
                        }
                    }
                });
                next += 1;
            }
            if let Some(done) = inflight.join_next().await {
                let (id, v) = done?;
                out.insert(id, v);
            }
        }
        out
    };
    let mut tei_queries = Vec::new();
    let mut tei_client = EmbedClient::connect(tei.clone()).await?;
    for q in QUERIES {
        let r = tei_client
            .embed(EmbedRequest {
                inputs: q.to_string(),
                truncate: true,
                normalize: Some(true),
                truncation_direction: 0,
                prompt_name: None,
                dimensions: None,
            })
            .await?
            .into_inner();
        tei_queries.push(r.embeddings);
    }
    let tei_ms = t.elapsed();
    eprintln!("TEI embedded {} texts in {:?}", tei_of.len(), tei_ms);

    // Falloff sweep: TEI-rank the FULL pools once per query; every
    // smaller pool k' is a prefix of the model2vec-ranked pool, so its
    // reranked list is the full reranked list filtered to prefix
    // members. recall@k = overlap of the two reranked top-k lists.
    let k_grid: Vec<usize> = vec![
        1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200, 300, 500, 700, 1000, 1500,
        2000, 3000, 5000, 7000, 10000, 15000, 20000,
    ];
    let pool_grid: Vec<usize> = vec![1000, 2000, 5000, 10000, 20000]
        .into_iter()
        .filter(|p| *p <= pool_k)
        .collect();
    let csv_path = arg("csv", "/tmp/tei_falloff.csv");
    let mut csv = String::from("pool_k,k,mean_recall,min_recall,max_recall\n");

    // Per query: full TEI-sorted lists for both pools.
    let tei_sort = |pool: &[u64], qi: usize| -> Vec<u64> {
        let mut scored: Vec<(u64, f64)> = pool
            .iter()
            .filter_map(|gid| tei_of.get(gid).map(|v| (*gid, cosine(v, &tei_queries[qi]))))
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored.into_iter().map(|(gid, _)| gid).collect()
    };
    let quant_ranked_full: Vec<Vec<u64>> = (0..QUERIES.len())
        .map(|qi| tei_sort(&quant_pools[qi], qi))
        .collect();
    let exact_ranked_full: Vec<Vec<u64>> = (0..QUERIES.len())
        .map(|qi| tei_sort(&exact_pools_gid[qi], qi))
        .collect();

    println!("\npool_k | trusted depth (mean recall >= 0.98) | recall@10 | recall@100 | recall@1000");
    for &pk in &pool_grid {
        // Reranked lists restricted to the k'-prefix pools.
        let per_query: Vec<(Vec<u64>, Vec<u64>)> = (0..QUERIES.len())
            .map(|qi| {
                let quant_members: HashSet<u64> =
                    quant_pools[qi][..pk.min(quant_pools[qi].len())].iter().copied().collect();
                let exact_members: HashSet<u64> =
                    exact_pools_gid[qi][..pk.min(exact_pools_gid[qi].len())].iter().copied().collect();
                (
                    quant_ranked_full[qi]
                        .iter()
                        .filter(|g| quant_members.contains(g))
                        .copied()
                        .collect(),
                    exact_ranked_full[qi]
                        .iter()
                        .filter(|g| exact_members.contains(g))
                        .copied()
                        .collect(),
                )
            })
            .collect();
        let mut trusted = 0usize;
        let mut at = HashMap::new();
        for &k in k_grid.iter().filter(|&&k| k <= pk) {
            let recalls: Vec<f64> = per_query
                .iter()
                .map(|(q, e)| overlap(&q[..k.min(q.len())], &e[..k.min(e.len())]))
                .collect();
            let mean = recalls.iter().sum::<f64>() / recalls.len() as f64;
            let min = recalls.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = recalls.iter().cloned().fold(0.0f64, f64::max);
            csv.push_str(&format!("{pk},{k},{mean:.4},{min:.4},{max:.4}\n"));
            if mean >= 0.98 {
                trusted = k;
            }
            at.insert(k, mean);
        }
        println!(
            "{pk} | {trusted} | {} | {} | {}",
            at.get(&10).map_or("-".into(), |r| format!("{r:.4}")),
            at.get(&100).map_or("-".into(), |r| format!("{r:.4}")),
            at.get(&1000).map_or("-".into(), |r| format!("{r:.4}")),
        );
    }
    std::fs::write(&csv_path, csv)?;
    eprintln!("falloff CSV written to {csv_path}");
    Ok(())
}
