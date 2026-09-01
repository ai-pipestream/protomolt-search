use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};
use std::time::Instant;

use pipestream_search::demo::court::EmbeddingReader;
use pipestream_search::harness::{fit_calibration, seeded_index};
use pipestream_search::vector::{
    QualityContract, ScoreDirection, VectorBackendConfig, VectorBackendDescriptor,
    VectorCapability, VectorError, VectorIndex, VectorProvider, VectorSearchOptions,
    VectorSearchResults, VectorStreamBatch, VectorStreamControl, VectorStreamSummary,
    EMBEDDED_TURBOVEC,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use turbovec_ivf::ivf::IvfIndex;

type Error = Box<dyn std::error::Error + Send + Sync>;

const FORMAT: &str = "protomolt-ivf-eval-v1";
const IVF_REVISION: &str = "1452b6e8f1eee9d071c22bd8f850cd9ada2acf7a";
const SYNTHETIC_SEED: u64 = 0xC0DE_51A7;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SourceKind {
    Synthetic,
    Court,
}

#[derive(Debug)]
struct Args {
    source: SourceKind,
    input: Option<PathBuf>,
    out: PathBuf,
    vectors: usize,
    dimensions: Option<usize>,
    topics: usize,
    queries: usize,
    ks: Vec<usize>,
    nprobes: Vec<Probe>,
    warmup: usize,
    iterations: usize,
    filter_modulus: usize,
    bit_width: usize,
    fit_threshold: usize,
    product_revision: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probe {
    Count(usize),
    All,
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
struct Corpus {
    values: Vec<f32>,
    queries: Vec<f32>,
    dimensions: usize,
    query_provenance: String,
}

#[derive(Clone)]
struct IvfControl {
    nprobe: Arc<AtomicUsize>,
}

impl IvfControl {
    fn set_nprobe(&self, nprobe: usize) {
        self.nprobe.store(nprobe, AtomicOrdering::Relaxed);
    }
}

struct ExperimentalIvf {
    index: IvfIndex,
    dimension: usize,
    bit_width: usize,
    nlist: usize,
    fit_threshold: usize,
    nprobe: Arc<AtomicUsize>,
}

impl ExperimentalIvf {
    fn new(
        dimension: usize,
        bit_width: usize,
        nlist: usize,
        fit_threshold: usize,
    ) -> Result<(Self, IvfControl), VectorError> {
        let index = IvfIndex::new(dimension, bit_width, nlist)
            .map_err(|error| VectorError::new(error.to_string()))?
            .with_fit_threshold(fit_threshold);
        let nprobe = Arc::new(AtomicUsize::new(nlist));
        Ok((
            Self {
                index,
                dimension,
                bit_width,
                nlist,
                fit_threshold,
                nprobe: Arc::clone(&nprobe),
            },
            IvfControl { nprobe },
        ))
    }

    fn config_payload(&self) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "bit_width": self.bit_width,
            "nlist": self.nlist,
            "fit_threshold": self.fit_threshold,
            "upstream_revision": IVF_REVISION,
        }))
        .expect("static IVF config is serializable")
    }
}

impl VectorProvider for ExperimentalIvf {
    fn descriptor(&self) -> VectorBackendDescriptor {
        VectorBackendDescriptor {
            backend_kind: "experimental-turbovec-ivf".into(),
            backend_version: format!("0.0.0+{IVF_REVISION}"),
            dimension: Some(self.dimension),
            bits_per_dimension: Some(self.bit_width as u32),
            metric: "inner_product".into(),
            score_direction: ScoreDirection::HigherIsBetter,
            scoring_fingerprint: format!(
                "experimental-turbovec-ivf:{IVF_REVISION}:d{}:b{}:nlist{}:fit{}",
                self.dimension, self.bit_width, self.nlist, self.fit_threshold
            ),
            quality_contract: QualityContract::ConfiguredAnn,
            capabilities: vec![VectorCapability::BatchQuery, VectorCapability::Append],
        }
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        Ok(VectorBackendConfig {
            backend_kind: "experimental-turbovec-ivf".into(),
            config_format: "application/vnd.ai.pipestream.experimental-turbovec-ivf+json;version=1"
                .into(),
            payload: self.config_payload(),
        })
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn dimension(&self) -> Option<usize> {
        Some(self.dimension)
    }

    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        if dimension != self.dimension {
            return Err(VectorError::new(format!(
                "experimental IVF dimension is {}, add supplied {dimension}",
                self.dimension
            )));
        }
        if vectors.len() % dimension != 0 {
            return Err(VectorError::new(format!(
                "vector buffer length {} is not a multiple of dimension {dimension}",
                vectors.len()
            )));
        }
        self.index.add(vectors);
        Ok(())
    }

    fn prepare(&mut self) -> Result<(), VectorError> {
        self.index.fit();
        Ok(())
    }

    fn write(&self, path: &Path) -> Result<(), VectorError> {
        Err(VectorError::new(format!(
            "experimental IVF revision {IVF_REVISION} has no persistence surface; refusing write {}",
            path.display()
        )))
    }

    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        if options.allow.is_some() {
            return Err(VectorError::new(
                "experimental IVF has no dense-mask search; post-filtering an ANN top-k cannot certify the allowed top-k",
            ));
        }
        if queries.len() % self.dimension != 0 {
            return Err(VectorError::new(format!(
                "query buffer length {} is not a multiple of dimension {}",
                queries.len(),
                self.dimension
            )));
        }
        let query_count = queries.len() / self.dimension;
        let nprobe = self.nprobe.load(AtomicOrdering::Relaxed).min(self.nlist);
        let mut scores = Vec::with_capacity(query_count * k);
        let mut slots = Vec::with_capacity(query_count * k);

        // Preserve the prototype's optimized batch path whenever it returns
        // the documented nq x k rectangle. Its flattened return has no row
        // offsets when a probe set contains fewer than k unique vectors, so a
        // short batch is rerun row-by-row and padded explicitly.
        let (batch_scores, batch_ids) = self.index.search(queries, k, nprobe);
        let rectangular =
            batch_scores.len() == query_count * k && batch_ids.len() == query_count * k;
        for (query_index, query) in queries.chunks_exact(self.dimension).enumerate() {
            let row_start = scores.len();
            let (owned_scores, owned_ids);
            let (row_scores, row_ids): (&[f32], &[u64]) = if rectangular {
                (
                    &batch_scores[query_index * k..(query_index + 1) * k],
                    &batch_ids[query_index * k..(query_index + 1) * k],
                )
            } else {
                (owned_scores, owned_ids) = self.index.search(query, k, nprobe);
                (&owned_scores, &owned_ids)
            };
            for (&score, &id) in row_scores.iter().zip(row_ids).take(k) {
                if options.minimum_score.is_some_and(|minimum| score < minimum) {
                    continue;
                }
                let id = i64::try_from(id)
                    .map_err(|_| VectorError::new(format!("IVF id {id} exceeds i64")))?;
                scores.push(score);
                slots.push(id);
            }
            while scores.len() - row_start < k {
                scores.push(f32::NEG_INFINITY);
                slots.push(-1);
            }
        }
        Ok(VectorSearchResults {
            scores,
            slots,
            query_count,
            result_count: k,
        })
    }

    fn search_streaming_controlled(
        &self,
        _queries: &[f32],
        _options: VectorSearchOptions<'_>,
        _sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        _control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        Err(VectorError::new(
            "experimental IVF has no candidate stream or exhaustive completion certificate",
        ))
    }
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

fn normalize(vector: &mut [f32]) {
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    let inverse = 1.0 / norm.max(f64::EPSILON);
    for value in vector {
        *value = (f64::from(*value) * inverse) as f32;
    }
}

fn synthetic_corpus(args: &Args, dimension: usize) -> Corpus {
    let mut rng = Lcg(SYNTHETIC_SEED);
    let mut values = vec![0.0f32; args.vectors * dimension];
    for (id, vector) in values.chunks_mut(dimension).enumerate() {
        for value in vector.iter_mut() {
            *value = rng.next() * 0.075;
        }
        let topic = id % args.topics;
        vector[topic] += 1.0;
        vector[(topic * 7 + 19) % dimension] += 0.25;
        normalize(vector);
    }
    let mut queries = vec![0.0f32; args.queries * dimension];
    for (query_index, vector) in queries.chunks_mut(dimension).enumerate() {
        for value in vector.iter_mut() {
            *value = rng.next() * 0.015;
        }
        let topic = query_index % args.topics;
        vector[topic] += 1.0;
        vector[(topic * 7 + 19) % dimension] += 0.25;
        normalize(vector);
    }
    Corpus {
        values,
        queries,
        dimensions: dimension,
        query_provenance: "independent deterministic topic-shaped queries".into(),
    }
}

fn court_corpus(args: &Args, input: &Path) -> Result<Corpus, Error> {
    let (declared_dimension, reader) = EmbeddingReader::open(input)?;
    let dimension = declared_dimension as usize;
    if args
        .dimensions
        .is_some_and(|expected| expected != dimension)
    {
        return Err(format!(
            "--dimensions={} disagrees with {} header dimension {dimension}",
            args.dimensions.expect("checked Some"),
            input.display()
        )
        .into());
    }
    let mut values = Vec::with_capacity(args.vectors * dimension);
    let mut queries = Vec::with_capacity(args.queries * dimension);
    for (row, record) in reader.take(args.vectors + args.queries).enumerate() {
        let record = record?;
        if record.vector.len() != dimension {
            return Err(format!(
                "court row {row} has dimension {}, header says {dimension}",
                record.vector.len()
            )
            .into());
        }
        if row < args.vectors {
            values.extend_from_slice(&record.vector);
        } else {
            queries.extend_from_slice(&record.vector);
        }
    }
    if values.len() != args.vectors * dimension || queries.len() != args.queries * dimension {
        return Err(format!(
            "{} ended before {} corpus rows plus {} query rows",
            input.display(),
            args.vectors,
            args.queries
        )
        .into());
    }
    Ok(Corpus {
        values,
        queries,
        dimensions: dimension,
        query_provenance: format!(
            "CourtListener embedding rows {}..{} outside the indexed prefix",
            args.vectors,
            args.vectors + args.queries
        ),
    })
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
    let text = option(key).unwrap_or_else(|| default.into());
    let mut values = text
        .split(',')
        .map(|part| {
            part.parse::<usize>()
                .map_err(|error| format!("invalid --{key} entry {part:?}: {error}").into())
        })
        .collect::<Result<Vec<_>, Error>>()?;
    values.sort_unstable();
    values.dedup();
    if values.is_empty() || values[0] == 0 {
        return Err(format!("--{key} must contain positive integers").into());
    }
    Ok(values)
}

fn parse_nprobes() -> Result<Vec<Probe>, Error> {
    let text = option("nprobe").unwrap_or_else(|| "8,16,32,64,128,all".into());
    let mut probes = Vec::new();
    for part in text.split(',') {
        if part == "all" {
            probes.push(Probe::All);
        } else {
            let value = part
                .parse::<usize>()
                .map_err(|error| format!("invalid --nprobe entry {part:?}: {error}"))?;
            if value == 0 {
                return Err("--nprobe values must be positive".into());
            }
            probes.push(Probe::Count(value));
        }
    }
    if probes.is_empty() {
        return Err("--nprobe must not be empty".into());
    }
    Ok(probes)
}

fn parse_args() -> Result<Args, Error> {
    let source = match option("source").as_deref().unwrap_or("synthetic") {
        "synthetic" => SourceKind::Synthetic,
        "court" => SourceKind::Court,
        other => return Err(format!("unknown --source={other:?}; use synthetic or court").into()),
    };
    let input = option("input").map(PathBuf::from);
    if source == SourceKind::Court && input.is_none() {
        return Err("--input is required with --source=court".into());
    }
    let dimensions = option("dimensions")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|error| format!("invalid --dimensions: {error}"))?;
    let out = option("out")
        .map(PathBuf::from)
        .ok_or("--out is required")?;
    let args = Args {
        source,
        input,
        out,
        vectors: parsed("vectors", 100_000)?,
        dimensions,
        topics: parsed("topics", 16)?,
        queries: parsed("queries", 16)?,
        ks: parse_list("k", "10,100,10000")?,
        nprobes: parse_nprobes()?,
        warmup: parsed("warmup", 2)?,
        iterations: parsed("iterations", 5)?,
        filter_modulus: parsed("filter-modulus", 10)?,
        bit_width: parsed("bit-width", 4)?,
        fit_threshold: parsed("fit-threshold", 40_000)?,
        product_revision: option("product-revision").unwrap_or_else(|| "unknown".into()),
    };
    if args.vectors == 0
        || args.queries == 0
        || args.topics == 0
        || args.iterations == 0
        || args.filter_modulus == 0
    {
        return Err(
            "vectors, queries, topics, iterations, and filter-modulus must be positive".into(),
        );
    }
    if args.ks.last().is_some_and(|k| *k > args.vectors) {
        return Err("every k must be no larger than vectors".into());
    }
    if args.source == SourceKind::Synthetic {
        let dimension = args.dimensions.unwrap_or(64);
        if dimension < args.topics || dimension < 20 {
            return Err("synthetic dimensions must be at least max(topics, 20)".into());
        }
    }
    if args.out.exists() {
        return Err(format!(
            "refusing to overwrite existing output {}",
            args.out.display()
        )
        .into());
    }
    Ok(args)
}

fn top_k(values: impl Iterator<Item = Ranked>, k: usize) -> Vec<Ranked> {
    let mut heap: BinaryHeap<Reverse<Ranked>> = BinaryHeap::with_capacity(k + 1);
    for value in values {
        if heap.len() < k {
            heap.push(Reverse(value));
        } else if value > heap.peek().expect("full heap").0 {
            heap.pop();
            heap.push(Reverse(value));
        }
    }
    let mut ranked: Vec<_> = heap.into_iter().map(|entry| entry.0).collect();
    ranked.sort_unstable_by(|left, right| right.cmp(left));
    ranked
}

fn exact_truth(corpus: &Corpus, max_k: usize, allow: Option<&[bool]>) -> Vec<Vec<Ranked>> {
    corpus
        .queries
        .par_chunks(corpus.dimensions)
        .map(|query| {
            top_k(
                corpus
                    .values
                    .chunks(corpus.dimensions)
                    .enumerate()
                    .filter(|(id, _)| allow.is_none_or(|allowed| allowed[*id]))
                    .map(|(id, vector)| Ranked {
                        id,
                        score: query
                            .iter()
                            .zip(vector)
                            .map(|(left, right)| left * right)
                            .sum(),
                    }),
                max_k,
            )
        })
        .collect()
}

fn percentiles(mut samples: Vec<f64>) -> Value {
    samples.sort_by(f64::total_cmp);
    let at = |fraction: f64| samples[((samples.len() - 1) as f64 * fraction).round() as usize];
    json!({
        "p50_ms": at(0.50),
        "p95_ms": at(0.95),
        "p99_ms": at(0.99),
        "samples": samples.len(),
    })
}

fn recall_rows(results: &VectorSearchResults, truth: &[Vec<Ranked>], k: usize) -> (f64, f64, bool) {
    let mut recalls = Vec::with_capacity(results.query_count);
    let mut complete = true;
    for (query, wanted) in truth.iter().enumerate() {
        let got: HashSet<usize> = results
            .slots_for_query(query)
            .iter()
            .take(k)
            .filter_map(|slot| usize::try_from(*slot).ok())
            .collect();
        complete &= got.len() == k;
        let matches = wanted
            .iter()
            .take(k)
            .filter(|hit| got.contains(&hit.id))
            .count();
        recalls.push(matches as f64 / k as f64);
    }
    (
        recalls.iter().sum::<f64>() / recalls.len() as f64,
        recalls.into_iter().fold(1.0, f64::min),
        complete,
    )
}

fn memory_value(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix(field)?.trim();
        let kib = rest.strip_suffix(" kB")?.trim().parse::<u64>().ok()?;
        Some(kib * 1024)
    })
}

fn benchmark_cell(
    engine: &str,
    index: &VectorIndex,
    queries: &[f32],
    dimension: usize,
    k: usize,
    truth: &[Vec<Ranked>],
    warmup: usize,
    iterations: usize,
) -> Result<Value, Error> {
    for _ in 0..warmup {
        index.try_search(queries, k, VectorSearchOptions::new())?;
    }
    let mut batches = Vec::with_capacity(iterations);
    let mut last = None;
    for _ in 0..iterations {
        let started = Instant::now();
        let result = index.try_search(queries, k, VectorSearchOptions::new())?;
        batches.push(started.elapsed().as_secs_f64());
        last = Some(result);
    }
    let mut singles = Vec::with_capacity(iterations * truth.len());
    for _ in 0..iterations {
        for query in queries.chunks_exact(dimension) {
            let started = Instant::now();
            index.try_search(query, k, VectorSearchOptions::new())?;
            singles.push(started.elapsed().as_secs_f64() * 1e3);
        }
    }
    let result = last.expect("positive iterations");
    let (mean_recall, worst_recall, complete) = recall_rows(&result, truth, k);
    let batch_qps: Vec<f64> = batches
        .into_iter()
        .map(|seconds| truth.len() as f64 / seconds)
        .collect();
    let qps = percentiles(batch_qps);
    Ok(json!({
        "engine": engine,
        "k": k,
        "mean_recall": mean_recall,
        "worst_query_recall": worst_recall,
        "complete_rows": complete,
        "batch_qps": {
            "p50": qps["p50_ms"],
            "p95": qps["p95_ms"],
            "p99": qps["p99_ms"],
            "samples": qps["samples"],
        },
        "single_latency": percentiles(singles),
    }))
}

fn decision_gate(
    cells: &[Value],
    ks: &[usize],
    flat_build_ms: f64,
    ivf_build_ms: f64,
    flat_memory: u64,
    ivf_memory: u64,
) -> Value {
    let mut reasons = Vec::new();
    let build_ratio = ivf_build_ms / flat_build_ms.max(f64::EPSILON);
    if build_ratio > 2.0 {
        reasons.push(format!("IVF build ratio {build_ratio:.3} exceeds 2.0"));
    }
    let memory_ratio = if flat_memory == 0 {
        None
    } else {
        Some(ivf_memory as f64 / flat_memory as f64)
    };
    if memory_ratio.is_some_and(|ratio| ratio > 2.0) {
        reasons.push(format!(
            "IVF retained-memory ratio {:.3} exceeds 2.0",
            memory_ratio.expect("checked Some")
        ));
    }

    let mut per_k = Vec::with_capacity(ks.len());
    for &k in ks {
        let flat = cells
            .iter()
            .find(|cell| cell["engine"] == EMBEDDED_TURBOVEC && cell["k"] == k)
            .expect("one flat cell per k");
        let flat_mean = flat["mean_recall"].as_f64().expect("numeric recall");
        let flat_worst = flat["worst_query_recall"].as_f64().expect("numeric recall");
        let flat_qps = flat["batch_qps"]["p50"].as_f64().expect("numeric qps");
        let flat_p95 = flat["single_latency"]["p95_ms"]
            .as_f64()
            .expect("numeric latency");
        let candidates: Vec<&Value> = cells
            .iter()
            .filter(|cell| {
                cell["engine"] == "experimental-turbovec-ivf"
                    && cell["k"] == k
                    && cell["all_cells"] == false
                    && cell["complete_rows"] == true
                    && cell["mean_recall"].as_f64().unwrap_or(0.0) + 1e-12 >= flat_mean
                    && cell["worst_query_recall"].as_f64().unwrap_or(0.0) + 1e-12 >= flat_worst
                    && cell["batch_qps"]["p50"].as_f64().unwrap_or(0.0) > flat_qps
                    && cell["single_latency"]["p95_ms"]
                        .as_f64()
                        .unwrap_or(f64::INFINITY)
                        < flat_p95
            })
            .collect();
        let best = candidates.into_iter().max_by(|left, right| {
            left["batch_qps"]["p50"]
                .as_f64()
                .unwrap_or(0.0)
                .total_cmp(&right["batch_qps"]["p50"].as_f64().unwrap_or(0.0))
        });
        if best.is_none() {
            reasons.push(format!(
                "k={k} has no complete ANN point matching flat mean/worst recall while improving QPS and p95"
            ));
        }
        let all_cells = cells
            .iter()
            .find(|cell| {
                cell["engine"] == "experimental-turbovec-ivf"
                    && cell["k"] == k
                    && cell["all_cells"] == true
            })
            .expect("one all-cell result per k");
        let ceiling_passed = all_cells["complete_rows"] == true
            && all_cells["mean_recall"].as_f64().unwrap_or(0.0) + 1e-12 >= flat_mean
            && all_cells["worst_query_recall"].as_f64().unwrap_or(0.0) + 1e-12 >= flat_worst;
        if !ceiling_passed {
            reasons.push(format!(
                "k={k} all-cell ceiling is incomplete or below flat recall"
            ));
        }
        per_k.push(json!({
            "k": k,
            "flat": {
                "mean_recall": flat_mean,
                "worst_query_recall": flat_worst,
                "batch_qps_p50": flat_qps,
                "single_latency_p95_ms": flat_p95,
            },
            "qualifying_ann_nprobe": best.and_then(|cell| cell["nprobe"].as_u64()),
            "all_cell_ceiling_passed": ceiling_passed,
        }));
    }
    json!({
        "passed": reasons.is_empty(),
        "reasons": reasons,
        "build_ratio": build_ratio,
        "retained_memory_ratio": memory_ratio,
        "per_k": per_k,
        "scope": "workload-local authorization for lifecycle implementation, not production enablement",
    })
}

fn cpu_model() -> Option<String> {
    let text = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    text.lines().find_map(|line| {
        line.strip_prefix("model name")
            .and_then(|rest| rest.split_once(':'))
            .map(|(_, value)| value.trim().to_string())
    })
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let load_started = Instant::now();
    let corpus = match args.source {
        SourceKind::Synthetic => synthetic_corpus(&args, args.dimensions.unwrap_or(64)),
        SourceKind::Court => {
            court_corpus(&args, args.input.as_deref().expect("validated court input"))?
        }
    };
    let load_ms = load_started.elapsed().as_secs_f64() * 1e3;
    let max_k = *args.ks.last().expect("validated k");
    eprintln!(
        "ivf-eval: exact truth for {} queries over {}x{} rows",
        args.queries, args.vectors, corpus.dimensions
    );
    let truth_started = Instant::now();
    let truth = exact_truth(&corpus, max_k, None);
    let allow: Vec<bool> = (0..args.vectors)
        .map(|slot| slot % args.filter_modulus == 0)
        .collect();
    let filtered_truth = exact_truth(
        &corpus,
        max_k.min(allow.iter().filter(|v| **v).count()),
        Some(&allow),
    );
    let truth_ms = truth_started.elapsed().as_secs_f64() * 1e3;

    let mut cells = Vec::new();
    let flat_rss_before = memory_value("VmRSS:").unwrap_or(0);
    let flat_started = Instant::now();
    let sample_rows = args.vectors.min(8192);
    let (shift, scale) = fit_calibration(
        corpus.dimensions,
        args.bit_width,
        &corpus.values[..sample_rows * corpus.dimensions],
    );
    let mut flat = seeded_index(corpus.dimensions, args.bit_width, &shift, &scale);
    flat.add(&corpus.values, corpus.dimensions)?;
    flat.prepare()?;
    let flat_build_ms = flat_started.elapsed().as_secs_f64() * 1e3;
    let flat_rss_after = memory_value("VmRSS:").unwrap_or(0);
    eprintln!("ivf-eval: flat built in {:.3}s", flat_build_ms / 1e3);
    for &k in &args.ks {
        cells.push(benchmark_cell(
            EMBEDDED_TURBOVEC,
            &flat,
            &corpus.queries,
            corpus.dimensions,
            k,
            &truth,
            args.warmup,
            args.iterations,
        )?);
    }
    let filtered_k = max_k.min(filtered_truth[0].len());
    let filtered_started = Instant::now();
    let filtered = flat.try_search(
        &corpus.queries,
        filtered_k,
        VectorSearchOptions::new().with_allowlist(&allow),
    )?;
    let filtered_ms = filtered_started.elapsed().as_secs_f64() * 1e3;
    let (filtered_mean, filtered_worst, filtered_complete) =
        recall_rows(&filtered, &filtered_truth, filtered_k);
    let flat_filtered = json!({
        "supported": true,
        "allow_fraction": 1.0 / args.filter_modulus as f64,
        "k": filtered_k,
        "wall_ms": filtered_ms,
        "mean_recall": filtered_mean,
        "worst_query_recall": filtered_worst,
        "complete_rows": filtered_complete,
    });

    let nlist = (args.vectors as f64).sqrt().floor().max(1.0) as usize;
    let ivf_rss_before = memory_value("VmRSS:").unwrap_or(0);
    let ivf_started = Instant::now();
    let (provider, control) = ExperimentalIvf::new(
        corpus.dimensions,
        args.bit_width,
        nlist,
        args.fit_threshold.min(args.vectors),
    )?;
    let mut ivf = VectorIndex::from_provider(provider);
    ivf.add(&corpus.values, corpus.dimensions)?;
    ivf.prepare()?;
    let ivf_build_ms = ivf_started.elapsed().as_secs_f64() * 1e3;
    let ivf_rss_after = memory_value("VmRSS:").unwrap_or(0);
    eprintln!("ivf-eval: IVF built in {:.3}s", ivf_build_ms / 1e3);

    for probe in &args.nprobes {
        let nprobe = match probe {
            Probe::Count(value) => (*value).min(nlist),
            Probe::All => nlist,
        };
        control.set_nprobe(nprobe);
        for &k in &args.ks {
            let mut cell = benchmark_cell(
                "experimental-turbovec-ivf",
                &ivf,
                &corpus.queries,
                corpus.dimensions,
                k,
                &truth,
                args.warmup,
                args.iterations,
            )?;
            cell["nprobe"] = json!(nprobe);
            cell["all_cells"] = json!(nprobe == nlist);
            cells.push(cell);
        }
    }
    let ivf_filtered_error = ivf
        .try_search(
            &corpus.queries[..corpus.dimensions],
            args.ks[0],
            VectorSearchOptions::new().with_allowlist(&allow),
        )
        .expect_err("experimental IVF must refuse filters")
        .to_string();

    let flat_memory = flat_rss_after.saturating_sub(flat_rss_before);
    let ivf_memory = ivf_rss_after.saturating_sub(ivf_rss_before);
    let gate = decision_gate(
        &cells,
        &args.ks,
        flat_build_ms,
        ivf_build_ms,
        flat_memory,
        ivf_memory,
    );

    let output = json!({
        "format": FORMAT,
        "product_revision": args.product_revision,
        "source": args.source,
        "source_path": args.input,
        "source_bytes": args.input.as_ref().and_then(|path| std::fs::metadata(path).ok()).map(|m| m.len()),
        "vectors": args.vectors,
        "dimensions": corpus.dimensions,
        "queries": args.queries,
        "query_provenance": corpus.query_provenance,
        "ks": args.ks,
        "bit_width": args.bit_width,
        "nlist": nlist,
        "fit_threshold": args.fit_threshold.min(args.vectors),
        "warmup": args.warmup,
        "iterations": args.iterations,
        "synthetic_seed": (args.source == SourceKind::Synthetic).then_some(SYNTHETIC_SEED),
        "upstream_ivf_revision": IVF_REVISION,
        "host": {
            "cpu_model": cpu_model(),
            "logical_parallelism": std::thread::available_parallelism().ok().map(|value| value.get()),
            "rayon_threads": rayon::current_num_threads(),
        },
        "flat_descriptor": flat.descriptor(),
        "ivf_descriptor": ivf.descriptor(),
        "load_ms": load_ms,
        "exact_truth_ms": truth_ms,
        "builds": {
            "flat": {
                "build_ms": flat_build_ms,
                "rss_before_build_bytes": flat_rss_before,
                "rss_after_build_bytes": flat_rss_after,
                "retained_rss_increment_bytes": flat_rss_after.saturating_sub(flat_rss_before),
            },
            "ivf": {
                "build_ms": ivf_build_ms,
                "rss_before_build_bytes": ivf_rss_before,
                "rss_after_build_bytes": ivf_rss_after,
                "retained_rss_increment_bytes": ivf_rss_after.saturating_sub(ivf_rss_before),
            },
        },
        "peak_rss_bytes": memory_value("VmHWM:"),
        "filtered_search": {
            "flat": flat_filtered,
            "ivf": {
                "supported": false,
                "error": ivf_filtered_error,
            },
        },
        "decision_gate": gate,
        "cells": cells,
    });
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, serde_json::to_vec_pretty(&output)?)?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_declares_ann_and_refuses_unsupported_surfaces() {
        let (provider, _) = ExperimentalIvf::new(32, 4, 4, 8).unwrap();
        let mut index = VectorIndex::from_provider(provider);
        let corpus = synthetic_corpus(
            &Args {
                source: SourceKind::Synthetic,
                input: None,
                out: PathBuf::from("unused"),
                vectors: 32,
                dimensions: Some(32),
                topics: 4,
                queries: 2,
                ks: vec![4],
                nprobes: vec![Probe::All],
                warmup: 0,
                iterations: 1,
                filter_modulus: 2,
                bit_width: 4,
                fit_threshold: 8,
                product_revision: "test".into(),
            },
            32,
        );
        index.add(&corpus.values, 32).unwrap();
        index.prepare().unwrap();
        let descriptor = index.descriptor();
        assert_eq!(descriptor.quality_contract, QualityContract::ConfiguredAnn);
        assert!(!descriptor
            .capabilities
            .contains(&VectorCapability::ExhaustiveCompletion));
        let allow = vec![true; index.len()];
        assert!(index
            .try_search(
                &corpus.queries[..32],
                4,
                VectorSearchOptions::new().with_allowlist(&allow),
            )
            .unwrap_err()
            .to_string()
            .contains("no dense-mask"));
        assert!(index
            .try_search_streaming_controlled(
                &corpus.queries[..32],
                VectorSearchOptions::new(),
                |_| VectorStreamControl::Continue,
                || VectorStreamControl::Continue,
            )
            .unwrap_err()
            .to_string()
            .contains("no candidate stream"));
    }

    #[test]
    fn short_probe_rows_are_padded_to_the_provider_shape() {
        let (provider, control) = ExperimentalIvf::new(32, 4, 16, 16).unwrap();
        let mut index = VectorIndex::from_provider(provider);
        let args = Args {
            source: SourceKind::Synthetic,
            input: None,
            out: PathBuf::from("unused"),
            vectors: 64,
            dimensions: Some(32),
            topics: 4,
            queries: 2,
            ks: vec![32],
            nprobes: vec![Probe::Count(1)],
            warmup: 0,
            iterations: 1,
            filter_modulus: 2,
            bit_width: 4,
            fit_threshold: 16,
            product_revision: "test".into(),
        };
        let corpus = synthetic_corpus(&args, 32);
        index.add(&corpus.values, 32).unwrap();
        index.prepare().unwrap();
        control.set_nprobe(1);
        let results = index
            .try_search(&corpus.queries, 32, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(results.query_count, 2);
        assert_eq!(results.result_count, 32);
        assert_eq!(results.slots.len(), 64);
        assert!(results.slots.contains(&-1));

        let filtered = index
            .try_search(
                &corpus.queries,
                32,
                VectorSearchOptions::new().with_minimum_score(f32::MAX),
            )
            .unwrap();
        assert_eq!(filtered.slots.len(), 64);
        assert!(filtered.slots.iter().all(|slot| *slot == -1));
    }
}
