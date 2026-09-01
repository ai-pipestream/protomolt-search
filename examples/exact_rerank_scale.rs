//! Scalable TurboQuant candidate-expansion and public FP32-rerank benchmark.
//!
//! The ordinary OpenSearch challenge deliberately uses human-readable JSONL.
//! This harness keeps million-row vector experiments in the product's binary
//! provider image and exact-vector sidecar instead. It measures two separate
//! facts:
//!
//! 1. the exact TurboQuant candidate depth needed to retain an FP32 top-k;
//! 2. the latency of the public `Query` FP32-rerank mode at fixed depths.
//!
//! Synthetic input preserves the topic-shaped distribution used by the
//! OpenSearch challenge. `court` input reads the real fixed-stride production
//! embedding artifact and records its declared dimension.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Instant;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::exact_vectors::ExactVectorStore;
use pipestream_search::harness::{fit_calibration, nodelay_incoming, seeded_index};
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    search_query, selection_query, DenseQuery, DenseScoreMode, QueryRequest, SearchQuery,
    SelectionQuery,
};
use pipestream_search::vector::{VectorIndex, VectorSearchOptions, VectorStreamControl};
use pipestream_search::{H2_CONN_WINDOW, H2_STREAM_WINDOW, MAX_MESSAGE_BYTES};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tonic::transport::Server;

type Error = Box<dyn std::error::Error + Send + Sync>;

const FORMAT: &str = "protomolt-exact-rerank-scale-v2";
const CORPUS_SEED: u64 = 0xC0DE_51A7;
const RECALL_TARGETS: [f64; 4] = [0.95, 0.99, 0.999, 1.0];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SourceKind {
    Synthetic,
    Court,
}

#[derive(Clone, Debug)]
struct Args {
    out: PathBuf,
    source: SourceKind,
    input: Option<PathBuf>,
    vectors: usize,
    dimensions: Option<usize>,
    topics: usize,
    queries: usize,
    ks: Vec<usize>,
    public_depths: Vec<usize>,
    public_warmup: usize,
    public_iterations: usize,
    run_public: bool,
    rebuild: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArtifactManifest {
    format: String,
    source: SourceKind,
    source_path: Option<PathBuf>,
    source_bytes: Option<u64>,
    vectors: usize,
    dimensions: usize,
    topics: usize,
    corpus_seed: Option<u64>,
    query_seed: Option<u64>,
    queries: Vec<Vec<f32>>,
    query_source_rows: Vec<Option<u64>>,
    index_file: String,
    exact_file: String,
    index_bytes: u64,
    exact_bytes: u64,
    build_ms: f64,
}

#[derive(Clone, Copy, Debug)]
struct Ranked {
    id: usize,
    score: f32,
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for Ranked {}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.id.cmp(&self.id))
    }
}

#[derive(Debug)]
struct QueryAnalysis {
    row: Value,
    quantized_ids: Vec<usize>,
}

struct CourtCorpus {
    dimensions: usize,
    values: Vec<f32>,
    queries: Vec<Vec<f32>>,
    query_source_rows: Vec<Option<u64>>,
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let unit = ((self.0 >> 40) as u32) as f32 / ((1u32 << 24) - 1) as f32;
        unit * 2.0 - 1.0
    }
}

fn option(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args()
        .skip(1)
        .find_map(|value| value.strip_prefix(&prefix).map(str::to_string))
}

fn parsed<T>(key: &str, default: T) -> Result<T, Error>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    option(key).map_or(Ok(default), |value| {
        value
            .parse::<T>()
            .map_err(|error| format!("invalid --{key}={value:?}: {error}").into())
    })
}

fn parse_list(key: &str, default: &str) -> Result<Vec<usize>, Error> {
    let value = option(key).unwrap_or_else(|| default.to_string());
    let mut parsed = value
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<usize>()
                .map_err(|error| format!("invalid --{key} entry {part:?}: {error}").into())
        })
        .collect::<Result<Vec<_>, Error>>()?;
    parsed.sort_unstable();
    parsed.dedup();
    if parsed.is_empty() || parsed[0] == 0 {
        return Err(format!("--{key} must contain positive integers").into());
    }
    Ok(parsed)
}

fn parse_bool(key: &str, default: bool) -> Result<bool, Error> {
    match option(key).as_deref() {
        None => Ok(default),
        Some("true" | "1" | "yes") => Ok(true),
        Some("false" | "0" | "no") => Ok(false),
        Some(value) => Err(format!("invalid --{key}={value:?}: expected true or false").into()),
    }
}

fn parse_args() -> Result<Args, Error> {
    let out = option("out")
        .map(PathBuf::from)
        .ok_or("--out is required")?;
    let source = match option("source").as_deref().unwrap_or("synthetic") {
        "synthetic" => SourceKind::Synthetic,
        "court" => SourceKind::Court,
        value => return Err(format!("unknown --source={value:?}; use synthetic or court").into()),
    };
    let dimensions = option("dimensions")
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| format!("invalid --dimensions={value:?}: {error}"))
        })
        .transpose()?;
    let args = Args {
        out,
        source,
        input: option("input").map(PathBuf::from),
        vectors: parsed("vectors", 1_000_000)?,
        dimensions,
        topics: parsed("topics", 16)?,
        queries: parsed("queries", 16)?,
        ks: parse_list("k", "10,100,1000,10000")?,
        public_depths: parse_list("public-depths", "10000,35777")?,
        public_warmup: parsed("public-warmup", 1)?,
        public_iterations: parsed("public-iterations", 3)?,
        run_public: parse_bool("public", true)?,
        rebuild: parse_bool("rebuild", false)?,
    };
    if args.vectors == 0 || args.queries == 0 || args.topics == 0 {
        return Err("vectors, queries, and topics must be positive".into());
    }
    if *args.ks.last().expect("validated non-empty") > args.vectors {
        return Err("largest k exceeds the corpus size".into());
    }
    match args.source {
        SourceKind::Synthetic => {
            let dim = args.dimensions.unwrap_or(64);
            if dim < args.topics {
                return Err("synthetic dimensions must be at least topics".into());
            }
            if args.input.is_some() {
                return Err("--input is only valid with --source=court".into());
            }
        }
        SourceKind::Court => {
            if args.input.is_none() {
                return Err("--source=court requires --input".into());
            }
        }
    }
    Ok(args)
}

fn normalize(vector: &mut [f32]) {
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt() as f32;
    for value in vector {
        *value /= norm;
    }
}

fn synthetic_corpus_and_queries(
    count: usize,
    dim: usize,
    topics: usize,
    query_count: usize,
) -> (Vec<f32>, Vec<Vec<f32>>) {
    let mut corpus_rng = Lcg(CORPUS_SEED);
    let mut corpus = vec![0.0f32; count * dim];
    for (id, vector) in corpus.chunks_mut(dim).enumerate() {
        for value in vector.iter_mut() {
            *value = corpus_rng.next() * 0.075;
        }
        let topic = id % topics;
        vector[topic] += 1.0;
        vector[(topic * 7 + 19) % dim] += 0.25;
        normalize(vector);
    }

    let queries = (0..query_count)
        .map(|query_index| {
            let mut vector: Vec<f32> = (0..dim).map(|_| corpus_rng.next() * 0.015).collect();
            let topic = query_index % topics;
            vector[topic] += 1.0;
            vector[(topic * 7 + 19) % dim] += 0.25;
            normalize(&mut vector);
            vector
        })
        .collect();
    (corpus, queries)
}

fn court_corpus_and_queries(
    path: &Path,
    count: usize,
    query_count: usize,
) -> Result<CourtCorpus, Error> {
    let (declared_dim, reader) = pipestream_search::demo::court::EmbeddingReader::open(path)?;
    let dim = declared_dim as usize;
    if dim == 0 {
        return Err("court embedding header declares dimension zero".into());
    }
    let query_rows: Vec<usize> = (0..query_count)
        .map(|index| (index + 1) * count / (query_count + 1))
        .collect();
    let query_set: HashSet<usize> = query_rows.iter().copied().collect();
    let mut corpus = Vec::with_capacity(count * dim);
    let mut queries = Vec::with_capacity(query_count);
    let mut source_rows = Vec::with_capacity(query_count);
    for (row, record) in reader.take(count).enumerate() {
        let record = record?;
        if record.vector.len() != dim {
            return Err(format!(
                "court embedding row {row} has dimension {}, header says {dim}",
                record.vector.len()
            )
            .into());
        }
        if query_set.contains(&row) {
            queries.push(record.vector.clone());
            source_rows.push(Some(row as u64));
        }
        corpus.extend_from_slice(&record.vector);
    }
    if corpus.len() != count * dim {
        return Err(format!(
            "court embedding file ended after {} rows; requested {count}",
            corpus.len() / dim
        )
        .into());
    }
    if queries.len() != query_count {
        return Err(format!(
            "captured {} court queries, expected {query_count}",
            queries.len()
        )
        .into());
    }
    Ok(CourtCorpus {
        dimensions: dim,
        values: corpus,
        queries,
        query_source_rows: source_rows,
    })
}

fn manifest_path(out: &Path) -> PathBuf {
    out.join("artifacts.json")
}

fn manifest_matches(args: &Args, manifest: &ArtifactManifest) -> Result<bool, Error> {
    if manifest.format != FORMAT
        || manifest.source != args.source
        || manifest.vectors != args.vectors
        || manifest.topics != args.topics
        || manifest.queries.len() != args.queries
    {
        return Ok(false);
    }
    if args
        .dimensions
        .is_some_and(|dimension| dimension != manifest.dimensions)
    {
        return Ok(false);
    }
    let index = args.out.join(&manifest.index_file);
    let exact = args.out.join(&manifest.exact_file);
    if !index.is_file() || !exact.is_file() {
        return Ok(false);
    }
    if std::fs::metadata(&index)?.len() != manifest.index_bytes
        || std::fs::metadata(&exact)?.len() != manifest.exact_bytes
    {
        return Ok(false);
    }
    if args.source == SourceKind::Court {
        let input = args.input.as_ref().expect("validated court input");
        if manifest.source_path.as_deref() != Some(input.as_path())
            || manifest.source_bytes != Some(std::fs::metadata(input)?.len())
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn load_or_build(args: &Args) -> Result<ArtifactManifest, Error> {
    std::fs::create_dir_all(&args.out)?;
    let path = manifest_path(&args.out);
    if !args.rebuild && path.is_file() {
        let manifest: ArtifactManifest = serde_json::from_slice(&std::fs::read(&path)?)?;
        if manifest_matches(args, &manifest)? {
            eprintln!("artifacts: reusing {}", args.out.display());
            return Ok(manifest);
        }
        return Err(format!(
            "{} does not match this run; use a new --out or pass --rebuild=true",
            path.display()
        )
        .into());
    }

    let started = Instant::now();
    eprintln!(
        "artifacts: loading/generating {} rows from {:?}",
        args.vectors, args.source
    );
    let (dim, corpus, queries, query_source_rows, source_path, source_bytes) = match args.source {
        SourceKind::Synthetic => {
            let dim = args.dimensions.unwrap_or(64);
            let (corpus, queries) =
                synthetic_corpus_and_queries(args.vectors, dim, args.topics, args.queries);
            (dim, corpus, queries, vec![None; args.queries], None, None)
        }
        SourceKind::Court => {
            let input = args.input.as_ref().expect("validated court input");
            let bytes = std::fs::metadata(input)?.len();
            let corpus = court_corpus_and_queries(input, args.vectors, args.queries)?;
            let dim = corpus.dimensions;
            if args.dimensions.is_some_and(|expected| expected != dim) {
                return Err(format!(
                    "--dimensions={} disagrees with {} header dimension {dim}",
                    args.dimensions.expect("checked Some"),
                    input.display()
                )
                .into());
            }
            (
                dim,
                corpus.values,
                corpus.queries,
                corpus.query_source_rows,
                Some(input.clone()),
                Some(bytes),
            )
        }
    };
    eprintln!(
        "artifacts: fitting calibration and encoding {}x{} vectors",
        args.vectors, dim
    );
    let sample_rows = if args.source == SourceKind::Synthetic {
        args.vectors
    } else {
        8192.min(args.vectors)
    };
    let (shift, scale) = fit_calibration(dim, 4, &corpus[..sample_rows * dim]);
    let mut index = seeded_index(dim, 4, &shift, &scale);
    index.add(&corpus, dim)?;
    index.prepare()?;
    let index_file = "vectors.tv".to_string();
    let exact_file = "vectors.exact".to_string();
    let index_path = args.out.join(&index_file);
    let exact_path = args.out.join(&exact_file);
    index.write(&index_path)?;
    drop(index);
    ExactVectorStore::from_values(dim, corpus)?.write(&exact_path)?;
    let manifest = ArtifactManifest {
        format: FORMAT.to_string(),
        source: args.source.clone(),
        source_path,
        source_bytes,
        vectors: args.vectors,
        dimensions: dim,
        topics: args.topics,
        corpus_seed: (args.source == SourceKind::Synthetic).then_some(CORPUS_SEED),
        query_seed: None,
        queries,
        query_source_rows,
        index_file,
        exact_file,
        index_bytes: std::fs::metadata(&index_path)?.len(),
        exact_bytes: std::fs::metadata(&exact_path)?.len(),
        build_ms: started.elapsed().as_secs_f64() * 1e3,
    };
    std::fs::write(&path, serde_json::to_vec_pretty(&manifest)?)?;
    eprintln!("artifacts: built in {:.3}s", manifest.build_ms / 1e3);
    Ok(manifest)
}

fn top_k(scored: Vec<(usize, f32)>, k: usize) -> Vec<Ranked> {
    let mut heap: BinaryHeap<Reverse<Ranked>> = BinaryHeap::with_capacity(k + 1);
    for (id, score) in scored {
        let candidate = Ranked { id, score };
        if heap.len() < k {
            heap.push(Reverse(candidate));
        } else if candidate > heap.peek().expect("full heap").0 {
            heap.pop();
            heap.push(Reverse(candidate));
        }
    }
    let mut ranked: Vec<Ranked> = heap.into_iter().map(|entry| entry.0).collect();
    ranked.sort_unstable_by(|left, right| right.cmp(left));
    ranked
}

fn quantized_ranking(index: &VectorIndex, query: &[f32]) -> Result<Vec<Ranked>, Error> {
    let mut ranked = Vec::with_capacity(index.len());
    let mut invalid_slot = None;
    let summary = index.try_search_streaming_controlled(
        query,
        VectorSearchOptions::new(),
        |batch| {
            for (id, score) in batch
                .slots
                .iter()
                .copied()
                .zip(batch.scores.iter().copied())
            {
                match usize::try_from(id) {
                    Ok(id) => ranked.push(Ranked { id, score }),
                    Err(_) => invalid_slot = Some(id),
                }
            }
            VectorStreamControl::Continue
        },
        || VectorStreamControl::Continue,
    )?;
    if let Some(id) = invalid_slot {
        return Err(format!("provider streamed invalid negative slot {id}").into());
    }
    if !summary.completed || summary.query_count != 1 || ranked.len() != index.len() {
        return Err(format!(
            "provider stream is incomplete: completed={}, queries={}, emitted={}, rows={}",
            summary.completed,
            summary.query_count,
            ranked.len(),
            index.len()
        )
        .into());
    }
    ranked.sort_unstable_by(|left, right| right.cmp(left));
    Ok(ranked)
}

fn rank_needed(sorted_relevant_ranks: &[usize], recall_target: f64) -> usize {
    let needed = ((sorted_relevant_ranks.len() as f64 * recall_target).ceil() as usize)
        .clamp(1, sorted_relevant_ranks.len());
    sorted_relevant_ranks[needed - 1]
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
}

fn analyze_query(
    query_index: usize,
    query: &[f32],
    index: &VectorIndex,
    exact: &ExactVectorStore,
    slots: &[usize],
    ks: &[usize],
) -> Result<QueryAnalysis, Error> {
    let max_k = *ks.last().expect("validated non-empty");
    let exact_started = Instant::now();
    let exact_top = top_k(exact.score_slots(query, slots)?, max_k);
    let exact_ms = exact_started.elapsed().as_secs_f64() * 1e3;
    let quantized_started = Instant::now();
    let quantized = quantized_ranking(index, query)?;
    let quantized_ms = quantized_started.elapsed().as_secs_f64() * 1e3;
    let mut rank_by_id = vec![0usize; index.len()];
    for (rank, hit) in quantized.iter().enumerate() {
        rank_by_id[hit.id] = rank + 1;
    }

    let mut by_k = Vec::with_capacity(ks.len());
    for &k in ks {
        let exact_ids: HashSet<usize> = exact_top[..k].iter().map(|hit| hit.id).collect();
        let raw_hits = quantized[..k]
            .iter()
            .filter(|hit| exact_ids.contains(&hit.id))
            .count();
        let mut relevant_ranks: Vec<usize> = exact_top[..k]
            .iter()
            .map(|hit| rank_by_id[hit.id])
            .collect();
        relevant_ranks.sort_unstable();
        let thresholds: Vec<Value> = RECALL_TARGETS
            .iter()
            .map(|&target| {
                let depth = rank_needed(&relevant_ranks, target);
                json!({
                    "recall_target": target,
                    "candidate_depth": depth,
                    "expansion_factor": depth as f64 / k as f64,
                })
            })
            .collect();
        by_k.push(json!({
            "k": k,
            "raw_recall": raw_hits as f64 / k as f64,
            "required_depths": thresholds,
            "relevant_native_ranks": relevant_ranks,
        }));
    }

    Ok(QueryAnalysis {
        row: json!({
            "query_index": query_index,
            "exact_oracle_ms": exact_ms,
            "provider_full_ranking_ms": quantized_ms,
            "by_k": by_k,
        }),
        quantized_ids: quantized.into_iter().map(|hit| hit.id).collect(),
    })
}

fn aggregate_thresholds(per_query: &[Value], ks: &[usize], corpus_size: usize) -> Value {
    let by_k: Vec<Value> = ks
        .iter()
        .enumerate()
        .map(|(k_index, &k)| {
            let raw_recalls: Vec<f64> = per_query
                .iter()
                .map(|query| query["by_k"][k_index]["raw_recall"].as_f64().unwrap())
                .collect();
            let required_depths: Vec<Value> = RECALL_TARGETS
                .iter()
                .enumerate()
                .map(|(target_index, &target)| {
                    let depths: Vec<usize> = per_query
                        .iter()
                        .map(|query| {
                            query["by_k"][k_index]["required_depths"][target_index]
                                ["candidate_depth"]
                                .as_u64()
                                .unwrap() as usize
                        })
                        .collect();
                    let every_query = *depths.iter().max().unwrap();
                    let mean = depths.iter().sum::<usize>() as f64 / depths.len() as f64;
                    json!({
                        "recall_target": target,
                        "mean_query_depth": mean,
                        "mean_query_expansion_factor": mean / k as f64,
                        "every_query_depth": every_query,
                        "every_query_expansion_factor": every_query as f64 / k as f64,
                    })
                })
                .collect();
            json!({
                "k": k,
                "corpus_fraction": k as f64 / corpus_size as f64,
                "mean_raw_recall": raw_recalls.iter().sum::<f64>() / raw_recalls.len() as f64,
                "minimum_raw_recall": raw_recalls.iter().copied().fold(1.0, f64::min),
                "required_depths": required_depths,
            })
        })
        .collect();
    json!({"by_k": by_k})
}

fn exact_rerank_ids(
    exact: &ExactVectorStore,
    query: &[f32],
    candidate_ids: &[usize],
    k: usize,
) -> Result<Vec<u64>, Error> {
    let mut scored: Vec<Ranked> = exact
        .score_slots(query, candidate_ids)?
        .into_iter()
        .map(|(id, score)| Ranked { id, score })
        .collect();
    scored.sort_unstable_by(|left, right| right.cmp(left));
    Ok(scored
        .into_iter()
        .take(k)
        .map(|hit| hit.id as u64)
        .collect())
}

async fn start_public_cluster(
    index_path: &Path,
    exact_path: &Path,
    max_k: usize,
) -> Result<
    (
        String,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ),
    Error,
> {
    let index = VectorIndex::load("embedded-turbovec", index_path)?;
    let exact = ExactVectorStore::open(exact_path)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let node_addr: SocketAddr = listener.local_addr()?;
    let node = NodeServiceImpl::new(
        Some(index),
        NodeConfig {
            chunk_blocks: 8192,
            coalesce: false,
            ..Default::default()
        },
    )
    .with_exact_vectors(Some(exact))?;
    node.spawn_floor_listener(node_addr);
    let node_handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(H2_STREAM_WINDOW)
            .initial_connection_window_size(H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, MAX_MESSAGE_BYTES))
            .serve_with_incoming(nodelay_incoming(listener)),
    );

    let coord_listener = TcpListener::bind("127.0.0.1:0").await?;
    let coord_addr = coord_listener.local_addr()?;
    let coordinator = CoordinatorServiceImpl::new(vec![format!("http://{node_addr}")])
        .with_max_k(u32::try_from(max_k)?)
        .with_stream_search(true);
    let coord_handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(H2_STREAM_WINDOW)
            .initial_connection_window_size(H2_CONN_WINDOW)
            .add_service(CoordinatorServiceImpl::into_server(
                coordinator,
                MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(nodelay_incoming(coord_listener)),
    );
    Ok((format!("http://{coord_addr}"), node_handle, coord_handle))
}

fn fp32_request(query: &[f32], k: usize, depth: usize, sequence: usize) -> QueryRequest {
    QueryRequest {
        request_id: format!("exact-rerank-scale-{depth}-{sequence}"),
        k: k as u32,
        selection_k: depth as u32,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "dense".to_string(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: query.to_vec(),
                    score_mode: DenseScoreMode::Fp32Rerank as i32,
                })),
            })),
        }),
        profile: true,
        ..Default::default()
    }
}

async fn benchmark_public(
    args: &Args,
    manifest: &ArtifactManifest,
    candidate_ids: &[usize],
    dynamic_depth: usize,
) -> Result<Value, Error> {
    let k = *args.ks.last().expect("validated non-empty");
    let mut depths = args.public_depths.clone();
    depths.push(dynamic_depth);
    depths.retain(|depth| *depth >= k && *depth <= manifest.vectors);
    depths.sort_unstable();
    depths.dedup();
    if depths.is_empty() {
        return Err("no public candidate depth survives k/corpus validation".into());
    }
    let max_depth = *depths.last().unwrap();
    let index_path = args.out.join(&manifest.index_file);
    let exact_path = args.out.join(&manifest.exact_file);
    let oracle = ExactVectorStore::open(&exact_path)?;
    let query = &manifest.queries[0];
    let all_slots: Vec<usize> = (0..manifest.vectors).collect();
    let global_exact_ids: Vec<u64> = top_k(oracle.score_slots(query, &all_slots)?, k)
        .into_iter()
        .map(|hit| hit.id as u64)
        .collect();
    let expected: Vec<(usize, Vec<u64>)> = depths
        .iter()
        .map(|&depth| {
            Ok((
                depth,
                exact_rerank_ids(&oracle, query, &candidate_ids[..depth], k)?,
            ))
        })
        .collect::<Result<_, Error>>()?;
    drop(oracle);

    let (address, node_handle, coord_handle) =
        start_public_cluster(&index_path, &exact_path, max_depth).await?;
    let mut client = SearchServiceClient::connect(address).await?;
    let mut rows = Vec::with_capacity(depths.len());
    for (depth, expected_ids) in expected {
        eprintln!("public: k={k}, candidate depth={depth}");
        let global_exactness_verified = depth == dynamic_depth;
        if global_exactness_verified && expected_ids != global_exact_ids {
            return Err(format!(
                "the measured full-recall depth {depth} does not reproduce global exact top-{k}"
            )
            .into());
        }
        let mut samples = Vec::new();
        for sequence in 0..args.public_warmup + args.public_iterations {
            let started = Instant::now();
            let response = client
                .query(fp32_request(query, k, depth, sequence))
                .await?
                .into_inner();
            let wall_ms = started.elapsed().as_secs_f64() * 1e3;
            let ids: Vec<u64> = response.hits.iter().map(|hit| hit.doc_id).collect();
            if ids != expected_ids {
                return Err(format!(
                    "public FP32 rerank differs from the exact fixed-pool order at depth {depth}"
                )
                .into());
            }
            if sequence >= args.public_warmup {
                let profile = response
                    .profile
                    .ok_or("profile missing from public Query")?;
                samples.push(json!({
                    "wall_ms": wall_ms,
                    "selection_ms": profile.selection_ms,
                    "rerank_ms": profile.rerank_ms,
                    "total_ms": profile.total_ms,
                }));
            }
        }
        let mut walls: Vec<f64> = samples
            .iter()
            .map(|sample| sample["wall_ms"].as_f64().unwrap())
            .collect();
        let mut reranks: Vec<f64> = samples
            .iter()
            .map(|sample| sample["rerank_ms"].as_f64().unwrap())
            .collect();
        walls.sort_by(f64::total_cmp);
        reranks.sort_by(f64::total_cmp);
        let payload_bytes = depth * manifest.dimensions * std::mem::size_of::<f32>();
        let median_rerank_ms = percentile(&reranks, 0.5);
        rows.push(json!({
            "k": k,
            "candidate_depth": depth,
            "payload_bytes": payload_bytes,
            "payload_mib": payload_bytes as f64 / (1024.0 * 1024.0),
            "median_wall_ms": percentile(&walls, 0.5),
            "p90_wall_ms": percentile(&walls, 0.9),
            "median_rerank_ms": median_rerank_ms,
            "p90_rerank_ms": percentile(&reranks, 0.9),
            "effective_payload_gb_s": payload_bytes as f64 / 1.0e9 / (median_rerank_ms / 1.0e3),
            "samples": samples,
            "fixed_pool_exactness_verified": true,
            "global_exactness_verified": global_exactness_verified,
        }));
    }
    node_handle.abort();
    coord_handle.abort();
    Ok(json!({
        "query_index": 0,
        "warmup": args.public_warmup,
        "iterations": args.public_iterations,
        "rows": rows,
    }))
}

fn every_query_depth(aggregate: &Value, k: usize, target: f64) -> Result<usize, Error> {
    let row = aggregate["by_k"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["k"].as_u64() == Some(k as u64)))
        .ok_or_else(|| format!("aggregate has no k={k}"))?;
    row["required_depths"]
        .as_array()
        .and_then(|rows| {
            rows.iter().find(|row| {
                row["recall_target"]
                    .as_f64()
                    .is_some_and(|value| value == target)
            })
        })
        .and_then(|row| row["every_query_depth"].as_u64())
        .map(|depth| depth as usize)
        .ok_or_else(|| format!("aggregate has no target {target} at k={k}").into())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let manifest = load_or_build(&args)?;
    if manifest
        .queries
        .iter()
        .any(|query| query.len() != manifest.dimensions)
    {
        return Err("artifact query dimension does not match its manifest".into());
    }
    let index_path = args.out.join(&manifest.index_file);
    let exact_path = args.out.join(&manifest.exact_file);
    let index = VectorIndex::load("embedded-turbovec", &index_path)?;
    let exact = ExactVectorStore::open(&exact_path)?;
    exact.verify_payload()?;
    if index.len() != manifest.vectors
        || index.dimension() != Some(manifest.dimensions)
        || exact.len() != manifest.vectors
        || exact.dim() != Some(manifest.dimensions)
    {
        return Err("provider/exact artifact shape disagrees with the manifest".into());
    }
    let slots: Vec<usize> = (0..manifest.vectors).collect();
    let mut rows = Vec::with_capacity(manifest.queries.len());
    let mut first_quantized_ids = Vec::new();
    for (query_index, query) in manifest.queries.iter().enumerate() {
        eprintln!(
            "analyze: query {}/{} over {}x{}",
            query_index + 1,
            manifest.queries.len(),
            manifest.vectors,
            manifest.dimensions
        );
        let analysis = analyze_query(query_index, query, &index, &exact, &slots, &args.ks)?;
        if query_index == 0 {
            first_quantized_ids = analysis.quantized_ids;
        }
        rows.push(analysis.row);
    }
    drop(exact);
    drop(index);
    let aggregate = aggregate_thresholds(&rows, &args.ks, manifest.vectors);
    let max_k = *args.ks.last().expect("validated non-empty");
    let dynamic_depth = every_query_depth(&aggregate, max_k, 1.0)?;
    let public = if args.run_public {
        benchmark_public(&args, &manifest, &first_quantized_ids, dynamic_depth).await?
    } else {
        Value::Null
    };
    let output = json!({
        "format": FORMAT,
        "source": manifest.source,
        "source_path": manifest.source_path,
        "source_bytes": manifest.source_bytes,
        "vectors": manifest.vectors,
        "dimensions": manifest.dimensions,
        "queries": manifest.queries.len(),
        "ks": args.ks,
        "artifact_manifest": manifest_path(&args.out),
        "index_bytes": manifest.index_bytes,
        "exact_bytes": manifest.exact_bytes,
        "build_ms": manifest.build_ms,
        "aggregate": aggregate,
        "per_query": rows,
        "public_fp32_rerank": public,
    });
    let report_path = args.out.join("report.json");
    std::fs::write(&report_path, serde_json::to_vec_pretty(&output)?)?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    eprintln!("report: {}", report_path.display());
    Ok(())
}
