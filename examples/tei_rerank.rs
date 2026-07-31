//! The live-rerank falloff experiment: rerank the quantized index's
//! k' pool with a REAL transformer (TEI, all-MiniLM-L6-v2 — the teacher
//! family of the static model2vec index embeddings) and measure, against
//! the same rerank over the EXACT fp32 pool (the no-quantization
//! counterfactual), how deep the reranked list can be trusted.
//!
//! Per query: (a) quantized pool = Search k' over the live cluster;
//! (b) exact pool = fp32 model2vec top-k' from one streaming pass over
//! the full embeddings file; (c) both pools' texts re-embedded through
//! TEI and reranked by cosine; (d) recall@k = overlap of the two
//! reranked top-k lists, for a dense k grid and every pool prefix
//! (smaller pools are prefixes of the largest, so one run yields the
//! whole (pool k', depth k) matrix).
//!
//! Queries: a fixed topical seed set plus deterministic span samples
//! drawn from random corpus chunks (real legal language, all unique),
//! up to `--queries` total. CSV per (pool, k): mean/min/max/p10 recall
//! over the query set.
//!
//! ```text
//! tei_rerank --queries=500 --pool-k=20000 --csv=/tmp/tei_falloff.csv
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
/// Topical seed queries; the rest are sampled from the corpus.
const SEED_QUERIES: &[&str] = &[
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

/// Deterministic LCG so query sampling is reproducible run to run.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
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

fn dot(vec_bytes: &[u8], q: &[f32]) -> f32 {
    // Four accumulators for ILP; the compiler cannot reassociate f32
    // adds on its own and this loop is the experiment's hot path.
    let (mut a0, mut a1, mut a2, mut a3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let mut d = 0;
    while d + 4 <= DIM {
        let f = |i: usize| {
            f32::from_le_bytes(vec_bytes[i * 4..i * 4 + 4].try_into().expect("4 bytes"))
        };
        a0 += f(d) * q[d];
        a1 += f(d + 1) * q[d + 1];
        a2 += f(d + 2) * q[d + 2];
        a3 += f(d + 3) * q[d + 3];
        d += 4;
    }
    (a0 + a1) + (a2 + a3)
}

/// Stream the whole embeddings file once, keeping the exact fp32 top-k
/// per query. Threads own QUERY SUBSETS (not row slices): with hundreds
/// of queries the dot products dominate, and per-thread global heaps
/// avoid any merge structure entirely.
fn exact_pools(path: &str, queries: &[Vec<f32>], k: usize) -> Vec<Vec<(u64, f32)>> {
    let file = std::fs::File::open(path).expect("open embeddings");
    let mut reader = std::io::BufReader::with_capacity(1 << 24, file);
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
    let threads = std::thread::available_parallelism().map_or(16, |n| n.get());
    let per_thread = queries.len().div_ceil(threads);
    let mut heaps: Vec<BinaryHeap<Entry>> = queries.iter().map(|_| BinaryHeap::new()).collect();
    let mut base_row: u64 = 0;
    let t0 = Instant::now();
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
        std::thread::scope(|scope| {
            for (heap_chunk, query_chunk) in
                heaps.chunks_mut(per_thread).zip(queries.chunks(per_thread))
            {
                scope.spawn(move || {
                    for row in 0..rows {
                        let vec_bytes = &data
                            [row * record_bytes + RECORD_HEADER..(row + 1) * record_bytes];
                        let gid = base_row + row as u64;
                        for (heap, q) in heap_chunk.iter_mut().zip(query_chunk) {
                            let score = dot(vec_bytes, q);
                            if heap.len() < k {
                                heap.push(Entry(score, gid));
                            } else if score > heap.peek().expect("non-empty").0 {
                                heap.pop();
                                heap.push(Entry(score, gid));
                            }
                        }
                    }
                });
            }
        });
        base_row += rows as u64;
        if base_row % (BLOCK_ROWS as u64 * 64) == 0 {
            eprintln!(
                "  exact scan: {base_row} rows in {:?}",
                t0.elapsed()
            );
        }
        if rows < BLOCK_ROWS {
            break;
        }
    }
    eprintln!("exact scan covered {base_row} rows in {:?}", t0.elapsed());
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
    let (mut d, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        d += f64::from(*x) * f64::from(*y);
        na += f64::from(*x) * f64::from(*x);
        nb += f64::from(*y) * f64::from(*y);
    }
    d / (na.sqrt() * nb.sqrt())
}

fn overlap(a: &[u64], b: &[u64]) -> f64 {
    let set: HashSet<u64> = b.iter().copied().collect();
    a.iter().filter(|x| set.contains(x)).count() as f64 / b.len() as f64
}

fn tei_request(text: String) -> EmbedRequest {
    EmbedRequest {
        inputs: text,
        truncate: true,
        normalize: Some(true),
        truncation_direction: 0,
        prompt_name: None,
        dimensions: None,
    }
}

/// Embed `texts` through TEI over one shared h2 channel with bounded
/// concurrency and per-call retry; inputs pre-truncated by the caller.
async fn tei_embed_batch(
    channel: &tonic::transport::Channel,
    tei_addr: &str,
    texts: Vec<(u64, String)>,
    concurrency: usize,
    out: &mut HashMap<u64, Vec<f32>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut inflight = tokio::task::JoinSet::new();
    let mut next = 0usize;
    while next < texts.len() || !inflight.is_empty() {
        while next < texts.len() && inflight.len() < concurrency {
            let (id, text) = texts[next].clone();
            let mut client = EmbedClient::new(channel.clone());
            let addr = tei_addr.to_string();
            inflight.spawn(async move {
                let mut attempt = 0;
                loop {
                    match client.embed(tei_request(text.clone())).await {
                        Ok(r) => return (id, r.into_inner().embeddings),
                        Err(_) if attempt < 3 => {
                            attempt += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(200 * attempt))
                                .await;
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
    Ok(())
}

/// A query-worthy span from a chunk: ~10 words from a third of the way
/// in. None when the text is too short to make one.
fn span_query(text: &str) -> Option<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < 8 {
        return None;
    }
    let start = words.len() / 3;
    let span: Vec<&str> = words[start..(start + 10).min(words.len())].to_vec();
    if span.len() < 6 {
        return None;
    }
    Some(span.join(" "))
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
    let pool_k: usize = arg("pool-k", "20000").parse()?;
    let n_queries: usize = arg("queries", "500").parse()?;
    let csv_path = arg("csv", "/tmp/tei_falloff.csv");
    let concurrency = 32;

    // Per-shard vector counts -> contiguous file-range starts for the
    // global-id <-> file-index mapping.
    let mut counts = Vec::new();
    for node in &nodes {
        let mut client = NodeServiceClient::connect(node.clone()).await?;
        counts.push(client.health(HealthRequest {}).await?.into_inner().num_vectors);
    }
    let total_docs: u64 = counts.iter().sum();
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

    // Query set: seeds + deterministic span samples from random chunks.
    let t = Instant::now();
    let mut queries: Vec<String> = SEED_QUERIES.iter().map(|s| s.to_string()).collect();
    let mut seen: HashSet<String> = queries.iter().map(|q| q.to_lowercase()).collect();
    let mut rng = Lcg(0x5eed_c0de_2026_0731);
    while queries.len() < n_queries {
        let mut batch: HashMap<usize, Vec<u64>> = HashMap::new();
        for _ in 0..256 {
            let r = rng.next() % total_docs;
            let shard = match starts.binary_search(&r) {
                Ok(i) => i,
                Err(i) => i - 1,
            };
            batch
                .entry(shard)
                .or_default()
                .push(shard as u64 * SLOT_STRIDE + (r - starts[shard]));
        }
        for (shard, doc_ids) in batch {
            let mut client = NodeServiceClient::connect(nodes[shard].clone()).await?;
            let found = client
                .get_documents(GetDocumentsRequest { doc_ids })
                .await?
                .into_inner();
            for doc in found.documents {
                if queries.len() >= n_queries {
                    break;
                }
                if let Some(span) = span_query(&doc.text) {
                    if seen.insert(span.to_lowercase()) {
                        queries.push(span);
                    }
                }
            }
        }
    }
    eprintln!(
        "query set: {} seeds + {} sampled spans in {:?}",
        SEED_QUERIES.len(),
        queries.len() - SEED_QUERIES.len(),
        t.elapsed()
    );

    // Model2vec query embeddings (the index's space), then the
    // quantized pools from the live cluster, bounded concurrency.
    let t = Instant::now();
    let mut m2v_queries = Vec::new();
    for q in &queries {
        m2v_queries.push(analyzer::embed_text(&analysis, q).await?);
    }
    eprintln!("model2vec embedded {} queries in {:?}", queries.len(), t.elapsed());

    let t = Instant::now();
    let quant_pools: Vec<Vec<u64>> = {
        let mut out: Vec<Option<Vec<u64>>> = vec![None; queries.len()];
        let mut inflight = tokio::task::JoinSet::new();
        let mut next = 0usize;
        while next < m2v_queries.len() || !inflight.is_empty() {
            while next < m2v_queries.len() && inflight.len() < 8 {
                let vector = m2v_queries[next].clone();
                let qi = next;
                let mut client = SearchServiceClient::connect(coordinator.clone())
                    .await?
                    .max_decoding_message_size(turbovec_search::MAX_MESSAGE_BYTES);
                inflight.spawn(async move {
                    let hits = client
                        .search(SearchRequest {
                            request_id: String::new(),
                            k: pool_k as u32,
                            vector,
                        })
                        .await
                        .expect("search")
                        .into_inner()
                        .hits;
                    (qi, hits.into_iter().map(|h| h.vector_id).collect::<Vec<u64>>())
                });
                next += 1;
            }
            if let Some(done) = inflight.join_next().await {
                let (qi, pool) = done?;
                out[qi] = Some(pool);
            }
        }
        out.into_iter().map(|p| p.expect("pool")).collect()
    };
    eprintln!(
        "quantized pools: {} queries x k={pool_k} in {:?}",
        queries.len(),
        t.elapsed()
    );

    let m2v_refs: Vec<Vec<f32>> = m2v_queries.clone();
    let exact = exact_pools(&embeddings, &m2v_refs, pool_k);
    let exact_pools_gid: Vec<Vec<u64>> = exact
        .iter()
        .map(|pool| pool.iter().map(|&(idx, _)| file_to_gid(idx)).collect())
        .collect();

    // Fetch + TEI-embed the union of all pools, streamed per shard so
    // raw text never accumulates: fetch a batch, embed it, keep only
    // the vector.
    let t = Instant::now();
    let mut wanted: HashSet<u64> = HashSet::new();
    for pool in quant_pools.iter().chain(exact_pools_gid.iter()) {
        wanted.extend(pool.iter().copied());
    }
    let mut by_shard: HashMap<usize, Vec<u64>> = HashMap::new();
    for gid in &wanted {
        by_shard.entry(gid_to_shard(*gid)).or_default().push(*gid);
    }
    let tei_channel = tonic::transport::Endpoint::from_shared(tei.clone())?
        .connect()
        .await?;
    let mut tei_of: HashMap<u64, Vec<f32>> = HashMap::with_capacity(wanted.len());
    let total_wanted = wanted.len();
    for (shard, doc_ids) in by_shard {
        let mut client = NodeServiceClient::connect(nodes[shard].clone())
            .await?
            .max_decoding_message_size(turbovec_search::MAX_MESSAGE_BYTES);
        for ids in doc_ids.chunks(2000) {
            let found = client
                .get_documents(GetDocumentsRequest { doc_ids: ids.to_vec() })
                .await?
                .into_inner();
            let batch: Vec<(u64, String)> = found
                .documents
                .into_iter()
                .map(|d| (d.doc_id, d.text.chars().take(4000).collect()))
                .collect();
            tei_embed_batch(&tei_channel, &tei, batch, concurrency, &mut tei_of).await?;
            if tei_of.len() % 500_000 < 2000 {
                eprintln!("  TEI: {}/{total_wanted} in {:?}", tei_of.len(), t.elapsed());
            }
        }
    }
    eprintln!("TEI embedded {}/{total_wanted} texts in {:?}", tei_of.len(), t.elapsed());

    let mut tei_queries = Vec::new();
    for q in &queries {
        let mut client = EmbedClient::new(tei_channel.clone());
        let r = client.embed(tei_request(q.clone())).await?.into_inner();
        tei_queries.push(r.embeddings);
    }

    // Falloff sweep over pool prefixes and the dense k grid.
    let k_grid: Vec<usize> = vec![
        1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200, 300, 500, 700, 1000, 1500,
        2000, 3000, 5000, 7000, 10000, 15000, 20000,
    ];
    let pool_grid: Vec<usize> = vec![1000, 2000, 5000, 10000, 20000]
        .into_iter()
        .filter(|p| *p <= pool_k)
        .collect();
    // Score regret: what the quantized pool SERVES at each rank vs what
    // the lossless pool serves, in TEI cosine points. Regret ~0 with low
    // id agreement means the disagreements are interchangeable ties;
    // material regret is real quantization loss.
    let mut csv =
        String::from("pool_k,k,mean_recall,min_recall,max_recall,p10_recall,mean_regret,p90_regret\n");

    let tei_sort = |pool: &[u64], qi: usize| -> Vec<(u64, f64)> {
        let mut scored: Vec<(u64, f64)> = pool
            .iter()
            .filter_map(|gid| tei_of.get(gid).map(|v| (*gid, cosine(v, &tei_queries[qi]))))
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored
    };
    let quant_ranked_full: Vec<Vec<(u64, f64)>> = (0..queries.len())
        .map(|qi| tei_sort(&quant_pools[qi], qi))
        .collect();
    let exact_ranked_full: Vec<Vec<(u64, f64)>> = (0..queries.len())
        .map(|qi| tei_sort(&exact_pools_gid[qi], qi))
        .collect();

    println!("\npool_k | trusted depth (mean >= 0.98) | mean@10 | p10@10 | mean@100 | p10@100");
    for &pk in &pool_grid {
        let per_query: Vec<(Vec<(u64, f64)>, Vec<(u64, f64)>)> = (0..queries.len())
            .map(|qi| {
                let quant_members: HashSet<u64> = quant_pools[qi]
                    [..pk.min(quant_pools[qi].len())]
                    .iter()
                    .copied()
                    .collect();
                let exact_members: HashSet<u64> = exact_pools_gid[qi]
                    [..pk.min(exact_pools_gid[qi].len())]
                    .iter()
                    .copied()
                    .collect();
                (
                    quant_ranked_full[qi]
                        .iter()
                        .filter(|(g, _)| quant_members.contains(g))
                        .copied()
                        .collect::<Vec<(u64, f64)>>(),
                    exact_ranked_full[qi]
                        .iter()
                        .filter(|(g, _)| exact_members.contains(g))
                        .copied()
                        .collect::<Vec<(u64, f64)>>(),
                )
            })
            .collect();
        let mut trusted = 0usize;
        let mut stopped = false;
        let mut at: HashMap<usize, (f64, f64, f64, f64)> = HashMap::new();
        for &k in k_grid.iter().filter(|&&k| k <= pk) {
            let mut recalls: Vec<f64> = Vec::with_capacity(per_query.len());
            let mut regrets: Vec<f64> = Vec::with_capacity(per_query.len());
            for (q, e) in &per_query {
                let kq = k.min(q.len());
                let ke = k.min(e.len());
                let q_ids: Vec<u64> = q[..kq].iter().map(|(g, _)| *g).collect();
                let e_ids: Vec<u64> = e[..ke].iter().map(|(g, _)| *g).collect();
                recalls.push(overlap(&q_ids, &e_ids));
                // Per-rank served-score gap, lossless minus quantized.
                let n = kq.min(ke);
                if n > 0 {
                    regrets.push(
                        (0..n).map(|i| e[i].1 - q[i].1).sum::<f64>() / n as f64,
                    );
                }
            }
            recalls.sort_by(f64::total_cmp);
            regrets.sort_by(f64::total_cmp);
            let mean = recalls.iter().sum::<f64>() / recalls.len() as f64;
            let min = recalls[0];
            let max = recalls[recalls.len() - 1];
            let p10 = recalls[recalls.len() / 10];
            let mean_regret = regrets.iter().sum::<f64>() / regrets.len() as f64;
            let p90_regret = regrets[(regrets.len() * 9) / 10];
            csv.push_str(&format!(
                "{pk},{k},{mean:.4},{min:.4},{max:.4},{p10:.4},{mean_regret:.5},{p90_regret:.5}\n"
            ));
            if mean >= 0.98 && !stopped {
                trusted = k;
            } else {
                stopped = true;
            }
            at.insert(k, (mean, p10, mean_regret, p90_regret));
        }
        let cell = |k: usize, which: usize| {
            at.get(&k).map_or("-".to_string(), |v| match which {
                0 => format!("{:.4}", v.0),
                1 => format!("{:.4}", v.1),
                2 => format!("{:.5}", v.2),
                _ => format!("{:.5}", v.3),
            })
        };
        println!(
            "{pk} | {trusted} | {} | {} | {} | {} | regret@10 {} (p90 {}) | regret@100 {} (p90 {})",
            cell(10, 0),
            cell(10, 1),
            cell(100, 0),
            cell(100, 1),
            cell(10, 2),
            cell(10, 3),
            cell(100, 2),
            cell(100, 3),
        );
    }
    std::fs::write(&csv_path, csv)?;
    eprintln!("falloff CSV written to {csv_path}");
    Ok(())
}
