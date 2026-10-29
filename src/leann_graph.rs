//! LEANN-style graph vector provider (`leann-graph`): an optional,
//! approximate engine style beside the exhaustive embedded TurboVec default.
//!
//! This is a native port of the two techniques of LEANN (Wang et al.,
//! "LEANN: A Low-Storage Vector Index", arXiv:2506.08276; reference
//! implementation StarTrail-org/LEANN, MIT licensed, see NOTICE). Nothing
//! from LEANN is linked or vendored: no Python package, no FAISS or DiskANN
//! fork. Both algorithms are implemented from the paper, because the
//! reference implementation ships the degree pruning disabled and runs the
//! approximate-distance-guided search only in its DiskANN backend.
//!
//! - **High-degree preserving graph pruning** (paper Algorithm 3). A full
//!   graph at degree `hub_degree` (M) is built first, only to measure node
//!   degrees. The top `hub_ratio_ppm` of nodes by degree become hubs. The
//!   serving graph is then rebuilt by insertion: a hub selects up to M
//!   neighbors, every other node up to `base_degree` (m), edges are added
//!   both ways and a node whose list grows past M is shrunk back with the
//!   same neighbor heuristic. Navigability survives through the hubs at a
//!   fraction of the edges.
//! - **Two-level search with selective recomputation** (paper Algorithm 2).
//!   Traversal ranks newly discovered neighbors by an approximate score from
//!   compact scalar codes and computes the exact FP32 score only for the
//!   best `1 - prune_ratio` of them per hop, from one global approximate
//!   queue. Exact embeddings are either kept in the image
//!   ([`EmbeddingStorage::Stored`]) or dropped after the build and
//!   recomputed on demand from a product-side [`RecomputeSource`]
//!   ([`EmbeddingStorage::Recomputed`]), which is LEANN's storage saving.
//!
//! The provider advertises [`QualityContract::ConfiguredAnn`]. It is never
//! selected implicitly: `EXACT` and the legacy unspecified mode refuse it,
//! and `AUTO` admits it only through a measured dense execution policy
//! (`docs/dense-execution-policy.md`). Reported scores are exact FP32 inner
//! products, so they are comparable across shards that share a config.
//! `docs/leann-graph.md` is the operator-facing description.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use crate::vector::{
    first_invalid_coordinate, QualityContract, RecomputeSource, ScoreDirection,
    VectorBackendConfig, VectorBackendDescriptor, VectorCapability, VectorError, VectorIndex,
    VectorProvider, VectorSearchOptions, VectorSearchResults, VectorStreamBatch,
    VectorStreamControl, VectorStreamSummary,
};

/// Stable identifier of the graph backend, as `vector_backend` names it.
pub const LEANN_GRAPH: &str = "leann-graph";
const BACKEND_VERSION: &str = "1.0.0";
/// The backend config's format: a fixed 32-byte little-endian record.
pub const CONFIG_FORMAT: &str = "application/vnd.ai-pipestream.leann-graph-config;version=1";
const CONFIG_BYTES: usize = 32;
const IMAGE_MAGIC: &[u8; 8] = b"PMLEANN1";
const IMAGE_FORMAT: u32 = 1;
const PPM: u64 = 1_000_000;
const MAX_DEGREE: u32 = 512;
const MAX_BEAM: u32 = 1 << 16;
/// The unindexed tail is folded into the graph once it reaches this many
/// rows or the indexed row count, whichever is larger: a geometric schedule,
/// so appends rebuild O(log n) times.
const REBUILD_MIN_TAIL: usize = 1024;
/// Rows recomputed when a recompute source is attached (the determinism gate).
const GATE_SAMPLE_ROWS: usize = 32;
/// Rows materialized per recompute call during a rebuild.
const REBUILD_CHUNK_ROWS: usize = 4096;

/// Where the exact embeddings of indexed rows come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingStorage {
    /// The image keeps every FP32 row; nothing is recomputed.
    Stored,
    /// The image keeps no FP32 row for indexed rows (only the unindexed
    /// tail); exact scores are recomputed through an attached
    /// [`RecomputeSource`] and checked against the indexed row's
    /// fingerprint.
    Recomputed,
}

/// Graph construction and traversal parameters. All of them are part of the
/// backend config and so of the scoring fingerprint: a dense execution
/// policy measured at one setting does not qualify another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeannGraphConfig {
    /// Bits per dimension of the approximate scalar codes: 4 or 8.
    pub bits_per_dimension: u32,
    /// M: the neighbor cap of hubs, and the shrink cap of every node.
    pub hub_degree: u32,
    /// m: the neighbor count a non-hub node selects on insertion.
    pub base_degree: u32,
    /// Share of nodes, by degree in the full graph, kept as hubs.
    pub hub_ratio_ppm: u32,
    /// Beam width of insertion searches (`ef_construction`).
    pub build_beam: u32,
    /// Minimum beam width of a query (`ef`); a query asks for `max(k, ef)`.
    pub search_beam: u32,
    /// Share of the neighbors discovered in one hop that are ranked by
    /// approximate score only and not recomputed in that hop. At least one
    /// discovered neighbor is evaluated exactly per hop.
    pub prune_ratio_ppm: u32,
    pub storage: EmbeddingStorage,
}

impl Default for LeannGraphConfig {
    /// The paper's shape: M = 32 (LEANN's default graph degree), m = 8,
    /// 2% hubs; every discovered neighbor evaluated exactly.
    fn default() -> Self {
        Self {
            bits_per_dimension: 8,
            hub_degree: 32,
            base_degree: 8,
            hub_ratio_ppm: 20_000,
            build_beam: 64,
            search_beam: 64,
            prune_ratio_ppm: 0,
            storage: EmbeddingStorage::Stored,
        }
    }
}

impl LeannGraphConfig {
    /// The default parameters at `bits_per_dimension`.
    pub fn with_bits(bits_per_dimension: usize) -> Self {
        Self {
            bits_per_dimension: u32::try_from(bits_per_dimension).unwrap_or(u32::MAX),
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<(), VectorError> {
        let refuse = |what: String| Err(VectorError::new(format!("{LEANN_GRAPH} config: {what}")));
        if !matches!(self.bits_per_dimension, 4 | 8) {
            return refuse(format!(
                "bits_per_dimension must be 4 or 8, not {}",
                self.bits_per_dimension
            ));
        }
        if self.base_degree < 2 || self.base_degree > self.hub_degree {
            return refuse(format!(
                "base_degree must be at least 2 and at most hub_degree ({}), not {}",
                self.hub_degree, self.base_degree
            ));
        }
        if self.hub_degree > MAX_DEGREE {
            return refuse(format!(
                "hub_degree must be at most {MAX_DEGREE}, not {}",
                self.hub_degree
            ));
        }
        if u64::from(self.hub_ratio_ppm) > PPM {
            return refuse(format!(
                "hub_ratio_ppm must be at most {PPM}, not {}",
                self.hub_ratio_ppm
            ));
        }
        for (name, beam) in [
            ("build_beam", self.build_beam),
            ("search_beam", self.search_beam),
        ] {
            if beam == 0 || beam > MAX_BEAM {
                return refuse(format!("{name} must be in 1..={MAX_BEAM}, not {beam}"));
            }
        }
        if u64::from(self.prune_ratio_ppm) >= PPM {
            return refuse(format!(
                "prune_ratio_ppm must be below {PPM}, not {}",
                self.prune_ratio_ppm
            ));
        }
        Ok(())
    }

    /// The canonical 32-byte record: eight little-endian u32 fields.
    pub fn encode(&self) -> [u8; CONFIG_BYTES] {
        let storage = match self.storage {
            EmbeddingStorage::Stored => 0u32,
            EmbeddingStorage::Recomputed => 1,
        };
        let mut out = [0u8; CONFIG_BYTES];
        for (i, value) in [
            self.bits_per_dimension,
            self.hub_degree,
            self.base_degree,
            self.hub_ratio_ppm,
            self.build_beam,
            self.search_beam,
            self.prune_ratio_ppm,
            storage,
        ]
        .into_iter()
        .enumerate()
        {
            out[i * 4..i * 4 + 4].copy_from_slice(&value.to_le_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, VectorError> {
        if bytes.len() != CONFIG_BYTES {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} config must be {CONFIG_BYTES} bytes, not {}",
                bytes.len()
            )));
        }
        let field = |i: usize| u32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap());
        let storage = match field(7) {
            0 => EmbeddingStorage::Stored,
            1 => EmbeddingStorage::Recomputed,
            other => {
                return Err(VectorError::new(format!(
                    "{LEANN_GRAPH} config names unknown embedding storage {other}"
                )))
            }
        };
        let config = Self {
            bits_per_dimension: field(0),
            hub_degree: field(1),
            base_degree: field(2),
            hub_ratio_ppm: field(3),
            build_beam: field(4),
            search_beam: field(5),
            prune_ratio_ppm: field(6),
            storage,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.validate()?;
        Ok(VectorBackendConfig {
            backend_kind: LEANN_GRAPH.to_string(),
            config_format: CONFIG_FORMAT.to_string(),
            payload: self.encode().to_vec(),
        })
    }

    pub fn from_backend_config(config: &VectorBackendConfig) -> Result<Self, VectorError> {
        if config.backend_kind != LEANN_GRAPH {
            return Err(VectorError::new(format!(
                "backend config names {:?}, not {LEANN_GRAPH}",
                config.backend_kind
            )));
        }
        if config.config_format != CONFIG_FORMAT {
            return Err(VectorError::new(format!(
                "unsupported {LEANN_GRAPH} config format {:?}",
                config.config_format
            )));
        }
        Self::decode(&config.payload)
    }
}

/// The determinism fingerprint of one FP32 row: FNV-1a 64 over the
/// little-endian bit patterns. It detects a recompute that disagrees with
/// the indexed vector (another model, a nondeterministic kernel); it is not
/// an integrity hash against an adversary — the image trailer is SHA-256.
pub fn row_fingerprint(row: &[f32]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for value in row {
        for byte in value.to_bits().to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// A row and its score. Ordered by score (total order), then by lower row
/// first, so heaps and sorts are deterministic under ties.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Scored {
    score: f32,
    row: u32,
}

impl Eq for Scored {}

impl Ord for Scored {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.row.cmp(&self.row))
    }
}

impl PartialOrd for Scored {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-dimension scalar quantization of the indexed rows: the approximate
/// distance of the two-level search (the paper uses PQ; scalar codes need
/// no training and keep the build deterministic).
#[derive(Debug, Clone, Default, PartialEq)]
struct Codes {
    bits: u32,
    dim: usize,
    min: Vec<f32>,
    step: Vec<f32>,
    bytes: Vec<u8>,
}

impl Codes {
    fn stride(bits: u32, dim: usize) -> usize {
        (dim * bits as usize).div_ceil(8)
    }

    fn fit(bits: u32, dim: usize, vectors: &[f32]) -> Self {
        let mut min = vec![f32::INFINITY; dim];
        let mut max = vec![f32::NEG_INFINITY; dim];
        for row in vectors.chunks_exact(dim) {
            for (i, &v) in row.iter().enumerate() {
                min[i] = min[i].min(v);
                max[i] = max[i].max(v);
            }
        }
        let levels = ((1u32 << bits) - 1) as f32;
        let step: Vec<f32> = min
            .iter()
            .zip(&max)
            .map(|(&lo, &hi)| if hi > lo { (hi - lo) / levels } else { 0.0 })
            .collect();
        for value in &mut min {
            if !value.is_finite() {
                *value = 0.0;
            }
        }
        let mut codes = Self {
            bits,
            dim,
            min,
            step,
            bytes: Vec::with_capacity(vectors.len() / dim.max(1) * Self::stride(bits, dim)),
        };
        for row in vectors.chunks_exact(dim) {
            codes.push_row(row);
        }
        codes
    }

    fn push_row(&mut self, row: &[f32]) {
        let levels = (1u32 << self.bits) - 1;
        let start = self.bytes.len();
        self.bytes
            .resize(start + Self::stride(self.bits, self.dim), 0);
        for (i, &v) in row.iter().enumerate() {
            let level = if self.step[i] > 0.0 {
                (((v - self.min[i]) / self.step[i]).round().max(0.0) as u32).min(levels)
            } else {
                0
            };
            if self.bits == 8 {
                self.bytes[start + i] = level as u8;
            } else {
                self.bytes[start + i / 2] |= (level as u8) << ((i % 2) * 4);
            }
        }
    }

    fn row(&self, row: u32) -> &[u8] {
        let stride = Self::stride(self.bits, self.dim);
        &self.bytes[row as usize * stride..(row as usize + 1) * stride]
    }

    /// Fold the query into per-dimension weights: the approximate inner
    /// product is `base + sum(weight[i] * level[i])`.
    fn query(&self, query: &[f32]) -> (f32, Vec<f32>) {
        let base = dot(query, &self.min);
        let weights = query.iter().zip(&self.step).map(|(q, s)| q * s).collect();
        (base, weights)
    }

    fn approx(&self, base: f32, weights: &[f32], row: u32) -> f32 {
        let code = self.row(row);
        let mut sum = base;
        if self.bits == 8 {
            for (w, &c) in weights.iter().zip(code) {
                sum += w * f32::from(c);
            }
        } else {
            for (i, w) in weights.iter().enumerate() {
                sum += w * f32::from((code[i / 2] >> ((i % 2) * 4)) & 0x0f);
            }
        }
        sum
    }
}

/// A visited set reused across insertion searches.
struct Visited {
    stamp: Vec<u32>,
    epoch: u32,
}

impl Visited {
    fn new(rows: usize) -> Self {
        Self {
            stamp: vec![0; rows],
            epoch: 0,
        }
    }

    fn reset(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamp.fill(0);
            self.epoch = 1;
        }
    }

    /// Mark `row`; true when it was not yet visited in this epoch.
    fn visit(&mut self, row: u32) -> bool {
        let slot = &mut self.stamp[row as usize];
        if *slot == self.epoch {
            false
        } else {
            *slot = self.epoch;
            true
        }
    }
}

/// Insertion-based graph construction over exact vectors held in memory.
struct Builder<'a> {
    vectors: &'a [f32],
    dim: usize,
    beam: usize,
    adjacency: Vec<Vec<u32>>,
    entry: Option<u32>,
    visited: Visited,
}

impl<'a> Builder<'a> {
    fn new(vectors: &'a [f32], dim: usize, beam: usize) -> Self {
        let rows = vectors.len() / dim;
        Self {
            vectors,
            dim,
            beam,
            adjacency: vec![Vec::new(); rows],
            entry: None,
            visited: Visited::new(rows),
        }
    }

    fn row(&self, row: u32) -> &'a [f32] {
        let start = row as usize * self.dim;
        &self.vectors[start..start + self.dim]
    }

    /// The `beam` best inserted nodes for `query` by exact inner product,
    /// best first (the standard single-layer HNSW search).
    fn search(&mut self, query: &[f32]) -> Vec<Scored> {
        let Some(entry) = self.entry else {
            return Vec::new();
        };
        self.visited.reset();
        self.visited.visit(entry);
        let first = Scored {
            score: dot(query, self.row(entry)),
            row: entry,
        };
        let mut frontier = BinaryHeap::from([first]);
        let mut best = BinaryHeap::from([Reverse(first)]);
        while let Some(candidate) = frontier.pop() {
            if best.len() >= self.beam && candidate < best.peek().unwrap().0 {
                break;
            }
            for i in 0..self.adjacency[candidate.row as usize].len() {
                let neighbor = self.adjacency[candidate.row as usize][i];
                if !self.visited.visit(neighbor) {
                    continue;
                }
                let scored = Scored {
                    score: dot(query, self.row(neighbor)),
                    row: neighbor,
                };
                if best.len() < self.beam || scored > best.peek().unwrap().0 {
                    frontier.push(scored);
                    best.push(Reverse(scored));
                    if best.len() > self.beam {
                        best.pop();
                    }
                }
            }
        }
        let mut out: Vec<Scored> = best.into_iter().map(|Reverse(s)| s).collect();
        out.sort_unstable_by(|a, b| b.cmp(a));
        out
    }

    /// The HNSW neighbor heuristic: walk candidates best first and keep one
    /// only when it is closer to `base` than to every neighbor already kept,
    /// then top up from the skipped ones so the cap is used.
    fn select(&self, base: u32, mut candidates: Vec<Scored>, cap: usize) -> Vec<u32> {
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        candidates.dedup_by_key(|c| c.row);
        let mut kept: Vec<u32> = Vec::with_capacity(cap);
        let mut skipped = Vec::new();
        for candidate in candidates {
            if candidate.row == base {
                continue;
            }
            if kept.len() >= cap {
                break;
            }
            let row = self.row(candidate.row);
            if kept
                .iter()
                .all(|&k| dot(row, self.row(k)) < candidate.score)
            {
                kept.push(candidate.row);
            } else {
                skipped.push(candidate.row);
            }
        }
        for row in skipped {
            if kept.len() >= cap {
                break;
            }
            kept.push(row);
        }
        kept
    }

    /// Insert `node` selecting up to `out_cap` neighbors, link back, and
    /// shrink any neighbor list that grows past `shrink_cap`.
    fn insert(&mut self, node: u32, out_cap: usize, shrink_cap: usize) {
        if self.entry.is_none() {
            self.entry = Some(node);
            return;
        }
        let candidates = self.search(self.row(node));
        let neighbors = self.select(node, candidates, out_cap);
        for &neighbor in &neighbors {
            let list = &mut self.adjacency[neighbor as usize];
            if !list.contains(&node) {
                list.push(node);
            }
            if list.len() > shrink_cap {
                let base = self.row(neighbor);
                let scored = self.adjacency[neighbor as usize]
                    .iter()
                    .map(|&row| Scored {
                        score: dot(base, self.row(row)),
                        row,
                    })
                    .collect();
                self.adjacency[neighbor as usize] = self.select(neighbor, scored, shrink_cap);
            }
        }
        self.adjacency[node as usize] = neighbors;
    }
}

/// The serving graph over rows `0..adjacency.len()`.
#[derive(Debug, Clone, Default, PartialEq)]
struct Graph {
    adjacency: Vec<Vec<u32>>,
    hubs: Vec<u32>,
    entry: u32,
}

/// Build the pruned graph (paper Algorithm 3) over `vectors`.
fn build_graph(vectors: &[f32], dim: usize, config: &LeannGraphConfig) -> Graph {
    let rows = vectors.len() / dim;
    if rows == 0 {
        return Graph::default();
    }
    let hub_degree = config.hub_degree as usize;
    let base_degree = config.base_degree as usize;
    let beam = config.build_beam as usize;

    // The original graph at full degree, built only to measure degrees.
    let mut full = Builder::new(vectors, dim, beam);
    for row in 0..rows as u32 {
        full.insert(row, hub_degree, hub_degree);
    }
    let mut degree: Vec<usize> = full.adjacency.iter().map(Vec::len).collect();
    for list in &full.adjacency {
        for &neighbor in list {
            degree[neighbor as usize] += 1;
        }
    }
    drop(full);

    let hub_count = (rows as u64 * u64::from(config.hub_ratio_ppm)).div_ceil(PPM) as usize;
    let mut by_degree: Vec<u32> = (0..rows as u32).collect();
    by_degree.sort_unstable_by(|&a, &b| {
        degree[b as usize]
            .cmp(&degree[a as usize])
            .then_with(|| a.cmp(&b))
    });
    let mut hubs: Vec<u32> = by_degree[..hub_count.min(rows)].to_vec();
    let mut is_hub = vec![false; rows];
    for &hub in &hubs {
        is_hub[hub as usize] = true;
    }

    // The pruned graph: hubs keep M, every other node selects m; reverse
    // links may grow any list up to M before it is shrunk.
    let mut pruned = Builder::new(vectors, dim, beam);
    for row in 0..rows as u32 {
        let out_cap = if is_hub[row as usize] {
            hub_degree
        } else {
            base_degree
        };
        pruned.insert(row, out_cap, hub_degree);
    }
    // Enter through the best-connected hub of the pruned graph.
    let entry = hubs
        .iter()
        .copied()
        .max_by(|&a, &b| {
            pruned.adjacency[a as usize]
                .len()
                .cmp(&pruned.adjacency[b as usize].len())
                .then_with(|| b.cmp(&a))
        })
        .unwrap_or(0);
    hubs.sort_unstable();
    Graph {
        adjacency: pruned.adjacency,
        hubs,
        entry,
    }
}

/// Counters of one query's traversal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraversalStats {
    /// Expansion steps of the two-level search.
    pub hops: u64,
    /// Indexed rows reached (approximate score computed).
    pub visited: u64,
    /// Indexed rows whose exact score was computed.
    pub exact_evaluations: u64,
    /// Of those, rows whose embedding was recomputed through the source.
    pub recomputed: u64,
    /// Unindexed tail rows scored exhaustively.
    pub tail_scanned: u64,
}

/// Shape of the current image.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphStats {
    pub rows: usize,
    pub indexed_rows: usize,
    pub edges: usize,
    pub hubs: usize,
    pub max_degree: usize,
    /// Rows whose FP32 embedding the image holds.
    pub stored_rows: usize,
}

/// The `leann-graph` provider.
///
/// Rows `0..indexed` are in the graph; later rows form an unindexed tail
/// scored exhaustively until the next rebuild. The image holds the FP32
/// rows `stored_base..rows`: all of them under [`EmbeddingStorage::Stored`],
/// only the tail under [`EmbeddingStorage::Recomputed`].
pub struct LeannGraph {
    config: LeannGraphConfig,
    dim: Option<usize>,
    rows: usize,
    indexed: usize,
    stored_base: usize,
    stored: Vec<f32>,
    fingerprints: Vec<u64>,
    codes: Codes,
    graph: Graph,
    source: Option<Arc<dyn RecomputeSource>>,
}

impl std::fmt::Debug for LeannGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeannGraph")
            .field("config", &self.config)
            .field("stats", &self.graph_stats())
            .field("recompute_source", &self.source.is_some())
            .finish()
    }
}

impl LeannGraph {
    /// An empty provider; `dimension` is fixed by the first `add` when absent.
    pub fn new(dimension: Option<usize>, config: LeannGraphConfig) -> Result<Self, VectorError> {
        config.validate()?;
        if dimension == Some(0) {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} dimension must be positive"
            )));
        }
        Ok(Self {
            codes: Codes {
                bits: config.bits_per_dimension,
                dim: dimension.unwrap_or(0),
                ..Codes::default()
            },
            config,
            dim: dimension,
            rows: 0,
            indexed: 0,
            stored_base: 0,
            stored: Vec::new(),
            fingerprints: Vec::new(),
            graph: Graph::default(),
            source: None,
        })
    }

    pub fn into_index(self) -> VectorIndex {
        VectorIndex::from_provider(self)
    }

    pub fn config(&self) -> &LeannGraphConfig {
        &self.config
    }

    pub fn graph_stats(&self) -> GraphStats {
        GraphStats {
            rows: self.rows,
            indexed_rows: self.indexed,
            edges: self.graph.adjacency.iter().map(Vec::len).sum(),
            hubs: self.graph.hubs.len(),
            max_degree: self.graph.adjacency.iter().map(Vec::len).max().unwrap_or(0),
            stored_rows: self.rows - self.stored_base,
        }
    }

    /// Attach the product-side embedding source, after the determinism gate:
    /// up to [`GATE_SAMPLE_ROWS`] indexed rows, evenly spaced, are recomputed
    /// and each must reproduce its indexed fingerprint bit for bit. A source
    /// that fails is not attached.
    pub fn attach_recompute_source(
        &mut self,
        source: Arc<dyn RecomputeSource>,
    ) -> Result<(), VectorError> {
        let dim = self.dim.unwrap_or(0);
        if self.indexed > 0 && dim > 0 {
            let sample = GATE_SAMPLE_ROWS.min(self.indexed);
            let rows: Vec<u32> = (0..sample)
                .map(|i| (i * self.indexed / sample) as u32)
                .collect();
            let mut out = vec![0f32; rows.len() * dim];
            source.embed_rows(&rows, dim, &mut out)?;
            self.verify(&rows, &out, dim)?;
        }
        self.source = Some(source);
        Ok(())
    }

    fn verify(&self, rows: &[u32], vectors: &[f32], dim: usize) -> Result<(), VectorError> {
        for (row, vector) in rows.iter().zip(vectors.chunks_exact(dim)) {
            let expected = self.fingerprints[*row as usize];
            let actual = row_fingerprint(vector);
            if actual != expected {
                return Err(VectorError::new(format!(
                    "recomputed embedding of row {row} does not reproduce the indexed vector \
                     (fingerprint {actual:016x}, indexed {expected:016x}); the recompute source \
                     is not the model the image was built with, or is not deterministic"
                )));
            }
        }
        Ok(())
    }

    /// Exact FP32 rows for `rows`, in order: held rows are copied, the rest
    /// recomputed through the source and checked against their fingerprints.
    fn exact_rows(
        &self,
        rows: &[u32],
        out: &mut Vec<f32>,
        stats: &mut TraversalStats,
    ) -> Result<(), VectorError> {
        let dim = self.dim.unwrap_or(0);
        out.clear();
        out.resize(rows.len() * dim, 0.0);
        let mut missing = Vec::new();
        for (i, &row) in rows.iter().enumerate() {
            let row = row as usize;
            if row >= self.stored_base {
                let start = (row - self.stored_base) * dim;
                out[i * dim..(i + 1) * dim].copy_from_slice(&self.stored[start..start + dim]);
            } else {
                missing.push(i);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let source = self.source.as_ref().ok_or_else(|| {
            VectorError::new(format!(
                "{LEANN_GRAPH} image keeps no embeddings for rows below {} (recompute mode); \
                 attach a recompute source before searching it",
                self.stored_base
            ))
        })?;
        let ids: Vec<u32> = missing.iter().map(|&i| rows[i]).collect();
        let mut buffer = vec![0f32; ids.len() * dim];
        source.embed_rows(&ids, dim, &mut buffer)?;
        self.verify(&ids, &buffer, dim)?;
        for (j, &i) in missing.iter().enumerate() {
            out[i * dim..(i + 1) * dim].copy_from_slice(&buffer[j * dim..(j + 1) * dim]);
        }
        stats.recomputed += ids.len() as u64;
        Ok(())
    }

    /// Whether every indexed row's embedding can be produced for a rebuild.
    fn can_rebuild(&self) -> bool {
        self.stored_base == 0 || self.source.is_some()
    }

    /// Rebuild the graph over every row, folding in the tail.
    fn rebuild(&mut self) -> Result<(), VectorError> {
        let Some(dim) = self.dim else {
            return Ok(());
        };
        let mut vectors = Vec::with_capacity(self.rows * dim);
        let mut stats = TraversalStats::default();
        let mut chunk = Vec::new();
        let mut start = 0usize;
        while start < self.stored_base {
            let end = self.stored_base.min(start + REBUILD_CHUNK_ROWS);
            let rows: Vec<u32> = (start as u32..end as u32).collect();
            self.exact_rows(&rows, &mut chunk, &mut stats)?;
            vectors.extend_from_slice(&chunk);
            start = end;
        }
        vectors.extend_from_slice(&self.stored);
        self.graph = build_graph(&vectors, dim, &self.config);
        self.codes = Codes::fit(self.config.bits_per_dimension, dim, &vectors);
        self.indexed = self.rows;
        match self.config.storage {
            EmbeddingStorage::Stored => {
                self.stored = vectors;
                self.stored_base = 0;
            }
            EmbeddingStorage::Recomputed => {
                self.stored = Vec::new();
                self.stored_base = self.rows;
            }
        }
        Ok(())
    }

    fn check_allow(&self, allow: Option<&[bool]>) -> Result<(), VectorError> {
        match allow {
            Some(mask) if mask.len() != self.rows => Err(VectorError::new(format!(
                "allow mask has {} entries for {} rows",
                mask.len(),
                self.rows
            ))),
            _ => Ok(()),
        }
    }

    /// One query's top `k`, best first, as `(score, row)`: the graph's
    /// two-level traversal at beam `max(k, search_beam)` merged with the
    /// exhaustively scored tail. Scores are exact FP32 inner products; rows
    /// below `minimum_score` and rows the mask excludes are never returned.
    pub fn search_with_stats(
        &self,
        query: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<(Vec<(f32, u32)>, TraversalStats), VectorError> {
        let dim = self
            .dim
            .ok_or_else(|| VectorError::new(format!("{LEANN_GRAPH} index has no dimension yet")))?;
        if query.len() != dim {
            return Err(VectorError::new(format!(
                "query has dimension {}, index expects {dim}",
                query.len()
            )));
        }
        self.check_allow(options.allow)?;
        let floor = options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        let beam = k.max(self.config.search_beam as usize);
        let mut stats = TraversalStats::default();
        let mut hits = self.traverse(query, beam, options.allow, floor, &mut stats)?;
        let allowed = |row: usize| options.allow.is_none_or(|mask| mask[row]);
        for row in self.indexed..self.rows {
            if !allowed(row) {
                continue;
            }
            let start = (row - self.stored_base) * dim;
            hits.push(Scored {
                score: dot(query, &self.stored[start..start + dim]),
                row: row as u32,
            });
            stats.tail_scanned += 1;
        }
        hits.retain(|hit| hit.score >= floor);
        hits.sort_unstable_by(|a, b| b.cmp(a));
        hits.truncate(k);
        Ok((hits.into_iter().map(|h| (h.score, h.row)).collect(), stats))
    }

    /// The two-level search (paper Algorithm 2) over the indexed rows.
    fn traverse(
        &self,
        query: &[f32],
        beam: usize,
        allow: Option<&[bool]>,
        floor: f32,
        stats: &mut TraversalStats,
    ) -> Result<Vec<Scored>, VectorError> {
        if self.indexed == 0 || beam == 0 {
            return Ok(Vec::new());
        }
        let allowed = |row: u32| allow.is_none_or(|mask| mask[row as usize]);
        let (base, weights) = self.codes.query(query);
        let mut visited = vec![0u64; self.indexed.div_ceil(64)];
        let mut visit = |row: u32| {
            let (word, bit) = (row as usize / 64, 1u64 << (row % 64));
            let fresh = visited[word] & bit == 0;
            visited[word] |= bit;
            fresh
        };
        let mut results: BinaryHeap<Reverse<Scored>> = BinaryHeap::new();
        let mut frontier: BinaryHeap<Scored> = BinaryHeap::new();
        let mut pending: BinaryHeap<Scored> = BinaryHeap::new();
        let mut exact = Vec::new();
        let mut batch: Vec<u32> = vec![self.graph.entry];
        visit(self.graph.entry);
        stats.visited += 1;
        let keep = |discovered: usize| -> usize {
            if discovered == 0 {
                return 0;
            }
            let pruned =
                (discovered as u64 * u64::from(self.config.prune_ratio_ppm) / PPM) as usize;
            (discovered - pruned).max(1)
        };
        loop {
            // Exact scores for this hop's batch: the recompute step.
            if !batch.is_empty() {
                self.exact_rows(&batch, &mut exact, stats)?;
                stats.exact_evaluations += batch.len() as u64;
                for (&row, vector) in batch.iter().zip(exact.chunks_exact(query.len())) {
                    let scored = Scored {
                        score: dot(query, vector),
                        row,
                    };
                    frontier.push(scored);
                    if allowed(row) {
                        results.push(Reverse(scored));
                        if results.len() > beam {
                            results.pop();
                        }
                    }
                }
                batch.clear();
            }
            let worst = (results.len() >= beam).then(|| results.peek().unwrap().0.score);
            let take = if let Some(candidate) = frontier.pop() {
                if worst.is_some_and(|w| candidate.score < w) {
                    break;
                }
                // Floor-aware stop: nothing reachable from here is known to
                // clear the shared floor. A heuristic on a graph, which is
                // why the provider is ANN-only.
                if candidate.score < floor && pending.peek().is_none_or(|p| p.score < floor) {
                    break;
                }
                stats.hops += 1;
                let mut discovered = 0;
                for &neighbor in &self.graph.adjacency[candidate.row as usize] {
                    if visit(neighbor) {
                        pending.push(Scored {
                            score: self.codes.approx(base, &weights, neighbor),
                            row: neighbor,
                        });
                        discovered += 1;
                    }
                }
                stats.visited += discovered as u64;
                keep(discovered)
            } else {
                match pending.peek() {
                    None => break,
                    Some(best) if worst.is_some_and(|w| best.score < w) || best.score < floor => {
                        break
                    }
                    Some(_) => 1,
                }
            };
            for _ in 0..take {
                match pending.pop() {
                    Some(next) => batch.push(next.row),
                    None => break,
                }
            }
        }
        Ok(results.into_iter().map(|Reverse(s)| s).collect())
    }

    fn encode_image(&self) -> Vec<u8> {
        let dim = self.dim.unwrap_or(0);
        let mut out = Vec::new();
        out.extend_from_slice(IMAGE_MAGIC);
        out.extend_from_slice(&IMAGE_FORMAT.to_le_bytes());
        out.extend_from_slice(&self.config.encode());
        for value in [dim, self.rows, self.indexed, self.stored_base] {
            out.extend_from_slice(&(value as u64).to_le_bytes());
        }
        out.extend_from_slice(&self.graph.entry.to_le_bytes());
        for fingerprint in &self.fingerprints {
            out.extend_from_slice(&fingerprint.to_le_bytes());
        }
        if self.indexed > 0 {
            for value in self.codes.min.iter().chain(&self.codes.step) {
                out.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            out.extend_from_slice(&self.codes.bytes);
        }
        out.extend_from_slice(&(self.graph.hubs.len() as u64).to_le_bytes());
        for hub in &self.graph.hubs {
            out.extend_from_slice(&hub.to_le_bytes());
        }
        // CSR: offsets, then the concatenated neighbor lists.
        let mut offset = 0u64;
        out.extend_from_slice(&offset.to_le_bytes());
        for list in &self.graph.adjacency {
            offset += list.len() as u64;
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for list in &self.graph.adjacency {
            for neighbor in list {
                out.extend_from_slice(&neighbor.to_le_bytes());
            }
        }
        for value in &self.stored {
            out.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        let digest = crate::sha256::digest(&out);
        out.extend_from_slice(&digest);
        out
    }

    /// Read an image written by [`VectorProvider::write`]. Every structural
    /// field is checked; a truncated, extended or corrupted image refuses.
    pub fn load(path: &Path) -> Result<Self, VectorError> {
        let bytes = fs::read(path)
            .map_err(|e| VectorError::new(format!("load {}: {e}", path.display())))?;
        Self::decode_image(&bytes)
            .map_err(|e| VectorError::new(format!("load {}: {e}", path.display())))
    }

    fn decode_image(bytes: &[u8]) -> Result<Self, VectorError> {
        if bytes.len() < IMAGE_MAGIC.len() + 32 || &bytes[..8] != IMAGE_MAGIC {
            return Err(VectorError::new(format!("not a {LEANN_GRAPH} image")));
        }
        let (body, trailer) = bytes.split_at(bytes.len() - 32);
        if crate::sha256::digest(body).as_slice() != trailer {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image checksum does not match its contents"
            )));
        }
        let mut reader = Reader {
            bytes: body,
            at: IMAGE_MAGIC.len(),
        };
        let format = reader.u32()?;
        if format != IMAGE_FORMAT {
            return Err(VectorError::new(format!(
                "unsupported {LEANN_GRAPH} image format {format}"
            )));
        }
        let config = LeannGraphConfig::decode(reader.take(CONFIG_BYTES)?)?;
        let dim = reader.usize()?;
        let rows = reader.usize()?;
        let indexed = reader.usize()?;
        let stored_base = reader.usize()?;
        let entry = reader.u32()?;
        let expected_base = match config.storage {
            EmbeddingStorage::Stored => 0,
            EmbeddingStorage::Recomputed => indexed,
        };
        if indexed > rows || stored_base != expected_base || (rows > 0 && dim == 0) {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image row layout is inconsistent: {rows} rows, {indexed} \
                 indexed, embeddings held from row {stored_base}, dimension {dim}"
            )));
        }
        if rows > u32::MAX as usize || (indexed > 0 && entry as usize >= indexed) {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image entry {entry} or row count {rows} is out of range"
            )));
        }
        let fingerprints = (0..rows)
            .map(|_| reader.u64())
            .collect::<Result<Vec<_>, _>>()?;
        let mut codes = Codes {
            bits: config.bits_per_dimension,
            dim,
            ..Codes::default()
        };
        if indexed > 0 {
            codes.min = (0..dim).map(|_| reader.f32()).collect::<Result<_, _>>()?;
            codes.step = (0..dim).map(|_| reader.f32()).collect::<Result<_, _>>()?;
            let length = Codes::stride(codes.bits, dim)
                .checked_mul(indexed)
                .ok_or_else(|| VectorError::new("code length overflow"))?;
            codes.bytes = reader.take(length)?.to_vec();
        }
        let hub_count = reader.usize()?;
        if hub_count > indexed {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image names {hub_count} hubs for {indexed} indexed rows"
            )));
        }
        let hubs = (0..hub_count)
            .map(|_| reader.u32())
            .collect::<Result<Vec<_>, _>>()?;
        let offsets = (0..=indexed)
            .map(|_| reader.usize())
            .collect::<Result<Vec<_>, _>>()?;
        if offsets[0] != 0 || offsets.windows(2).any(|w| w[1] < w[0]) {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image adjacency offsets are not monotonic"
            )));
        }
        let mut adjacency = Vec::with_capacity(indexed);
        for window in offsets.windows(2) {
            let list = (window[0]..window[1])
                .map(|_| reader.u32())
                .collect::<Result<Vec<_>, _>>()?;
            if list.iter().any(|&n| n as usize >= indexed) || list.len() > MAX_DEGREE as usize {
                return Err(VectorError::new(format!(
                    "{LEANN_GRAPH} image adjacency names a row outside the graph"
                )));
            }
            adjacency.push(list);
        }
        if hubs.iter().any(|&h| h as usize >= indexed) {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image names a hub outside the graph"
            )));
        }
        let stored = ((stored_base * dim)..(rows * dim))
            .map(|_| reader.f32())
            .collect::<Result<Vec<_>, _>>()?;
        if reader.at != body.len() {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image has {} trailing bytes",
                body.len() - reader.at
            )));
        }
        Ok(Self {
            config,
            dim: (dim > 0).then_some(dim),
            rows,
            indexed,
            stored_base,
            stored,
            fingerprints,
            codes,
            graph: Graph {
                adjacency,
                hubs,
                entry,
            },
            source: None,
        })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], VectorError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| VectorError::new(format!("{LEANN_GRAPH} image is truncated")))?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32, VectorError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, VectorError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn usize(&mut self) -> Result<usize, VectorError> {
        usize::try_from(self.u64()?).map_err(|_| VectorError::new("image field exceeds usize"))
    }

    fn f32(&mut self) -> Result<f32, VectorError> {
        Ok(f32::from_bits(self.u32()?))
    }
}

impl VectorProvider for LeannGraph {
    fn descriptor(&self) -> VectorBackendDescriptor {
        let dimension = (self.dim.unwrap_or(0) as u64).to_le_bytes();
        let payload = self.config.encode();
        let mut capabilities = vec![
            VectorCapability::BatchQuery,
            VectorCapability::CandidateStream,
            VectorCapability::LiveBoundInput,
            VectorCapability::DenseMask,
            VectorCapability::Append,
            VectorCapability::Flush,
            VectorCapability::SnapshotInstall,
        ];
        if self.config.storage == EmbeddingStorage::Stored {
            capabilities.push(VectorCapability::RawVectorRebuild);
        }
        VectorBackendDescriptor {
            backend_kind: LEANN_GRAPH.to_string(),
            backend_version: BACKEND_VERSION.to_string(),
            dimension: self.dim,
            bits_per_dimension: Some(self.config.bits_per_dimension),
            metric: "inner_product".to_string(),
            score_direction: ScoreDirection::HigherIsBetter,
            scoring_fingerprint: crate::sha256::hex_digest(
                [
                    LEANN_GRAPH.as_bytes(),
                    BACKEND_VERSION.as_bytes(),
                    dimension.as_slice(),
                    CONFIG_FORMAT.as_bytes(),
                    payload.as_slice(),
                ]
                .concat()
                .as_slice(),
            ),
            quality_contract: QualityContract::ConfiguredAnn,
            capabilities,
        }
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.config.backend_config()
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn dimension(&self) -> Option<usize> {
        self.dim
    }

    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        if dimension == 0 || !vectors.len().is_multiple_of(dimension) {
            return Err(VectorError::new(format!(
                "{} values do not form rows of dimension {dimension}",
                vectors.len()
            )));
        }
        match self.dim {
            Some(dim) if dim != dimension => {
                return Err(VectorError::new(format!(
                    "vectors have dimension {dimension}, index expects {dim}"
                )))
            }
            _ => {}
        }
        if let Some((row, coord, value)) = first_invalid_coordinate(vectors, dimension) {
            return Err(VectorError::new(format!(
                "row {row} coordinate {coord} is invalid: {value}"
            )));
        }
        let added = vectors.len() / dimension;
        if self.rows + added > u32::MAX as usize {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} addresses at most {} rows",
                u32::MAX
            )));
        }
        self.dim = Some(dimension);
        self.codes.dim = dimension;
        self.fingerprints
            .extend(vectors.chunks_exact(dimension).map(row_fingerprint));
        self.stored.extend_from_slice(vectors);
        self.rows += added;
        let tail = self.rows - self.indexed;
        if tail >= REBUILD_MIN_TAIL.max(self.indexed) && self.can_rebuild() {
            self.rebuild()?;
        }
        Ok(())
    }

    /// Fold the unindexed tail into the graph. Under recompute storage this
    /// is also where the build's embeddings are dropped.
    fn prepare(&mut self) -> Result<(), VectorError> {
        if self.indexed == self.rows {
            return Ok(());
        }
        if !self.can_rebuild() {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} cannot rebuild a recompute-mode image without a recompute source"
            )));
        }
        self.rebuild()
    }

    fn write(&self, path: &Path) -> Result<(), VectorError> {
        let image = self.encode_image();
        let mut file = fs::File::create(path)
            .map_err(|e| VectorError::new(format!("write {}: {e}", path.display())))?;
        file.write_all(&image)
            .and_then(|()| file.sync_all())
            .map_err(|e| VectorError::new(format!("write {}: {e}", path.display())))
    }

    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        let dim = self
            .dim
            .ok_or_else(|| VectorError::new(format!("{LEANN_GRAPH} index has no dimension yet")))?;
        if !queries.len().is_multiple_of(dim) {
            return Err(VectorError::new(format!(
                "{} query values do not form rows of dimension {dim}",
                queries.len()
            )));
        }
        let query_count = queries.len() / dim;
        let mut scores = Vec::with_capacity(query_count * k);
        let mut slots = Vec::with_capacity(query_count * k);
        for query in queries.chunks_exact(dim) {
            let (hits, _) = self.search_with_stats(query, k, options)?;
            for i in 0..k {
                match hits.get(i) {
                    Some(&(score, row)) => {
                        scores.push(score);
                        slots.push(i64::from(row));
                    }
                    None => {
                        scores.push(f32::NEG_INFINITY);
                        slots.push(-1);
                    }
                }
            }
        }
        Ok(VectorSearchResults {
            scores,
            slots,
            query_count,
            result_count: k,
        })
    }

    /// One traversal per query at the configured beam, emitted best first as
    /// a single batch. `completed` means the traversal finished, never that
    /// every row was evaluated: the descriptor carries no exhaustive
    /// completion. A floor raised before a query starts gates that query.
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        let dim = self
            .dim
            .ok_or_else(|| VectorError::new(format!("{LEANN_GRAPH} index has no dimension yet")))?;
        if !queries.len().is_multiple_of(dim) {
            return Err(VectorError::new(format!(
                "{} query values do not form rows of dimension {dim}",
                queries.len()
            )));
        }
        let query_count = queries.len() / dim;
        let mut floor = options.minimum_score;
        let mut emitted = 0usize;
        let mut units = 0usize;
        for (query_index, query) in queries.chunks_exact(dim).enumerate() {
            match control() {
                VectorStreamControl::Stop => {
                    return Ok(VectorStreamSummary {
                        query_count,
                        emitted,
                        units_scanned: units,
                        completed: false,
                    })
                }
                VectorStreamControl::RaiseFloor(raised) => {
                    floor = Some(floor.map_or(raised, |f| f.max(raised)));
                }
                VectorStreamControl::Continue => {}
            }
            let mut query_options = options;
            query_options.minimum_score = floor;
            let (hits, stats) =
                self.search_with_stats(query, self.config.search_beam as usize, query_options)?;
            units += (stats.exact_evaluations + stats.tail_scanned) as usize;
            if hits.is_empty() {
                continue;
            }
            let scores: Vec<f32> = hits.iter().map(|h| h.0).collect();
            let slots: Vec<i64> = hits.iter().map(|h| i64::from(h.1)).collect();
            emitted += hits.len();
            let control = sink(&VectorStreamBatch {
                query_index,
                block_base: 0,
                scores: &scores,
                slots: &slots,
            });
            if let VectorStreamControl::Stop = control {
                return Ok(VectorStreamSummary {
                    query_count,
                    emitted,
                    units_scanned: units,
                    completed: false,
                });
            }
        }
        Ok(VectorStreamSummary {
            query_count,
            emitted,
            units_scanned: units,
            completed: true,
        })
    }

    /// A row's transcript is its identity under this backend's score: the
    /// fingerprint of the FP32 row every reported score is computed from.
    /// The approximate codes only steer traversal and are refit at every
    /// rebuild, so they are deliberately not part of it.
    fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        if rows.start > rows.end || rows.end > self.rows {
            return Err(VectorError::new(format!(
                "rows {rows:?} are outside the {} stored vectors",
                self.rows
            )));
        }
        let dim = self.dim.unwrap_or(0) as u64;
        let mut out = Vec::with_capacity(20);
        for row in rows {
            out.clear();
            out.extend_from_slice(b"LEAN");
            out.extend_from_slice(&dim.to_le_bytes());
            out.extend_from_slice(&self.fingerprints[row].to_le_bytes());
            sink(row, &out);
        }
        Ok(())
    }

    fn attach_recompute_source(
        &mut self,
        source: Arc<dyn RecomputeSource>,
    ) -> Result<(), VectorError> {
        LeannGraph::attach_recompute_source(self, source)
    }
}

pub(crate) fn create(
    dimension: usize,
    bits_per_dimension: usize,
) -> Result<VectorIndex, VectorError> {
    LeannGraph::new(
        Some(dimension),
        LeannGraphConfig::with_bits(bits_per_dimension),
    )
    .map(LeannGraph::into_index)
}

pub(crate) fn from_config(
    dimension: usize,
    config: &VectorBackendConfig,
) -> Result<VectorIndex, VectorError> {
    LeannGraph::new(
        Some(dimension),
        LeannGraphConfig::from_backend_config(config)?,
    )
    .map(LeannGraph::into_index)
}

pub(crate) fn load(path: &Path) -> Result<VectorIndex, VectorError> {
    LeannGraph::load(path).map(LeannGraph::into_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random unit vectors in a few clusters.
    fn corpus(rows: usize, dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f32 / (1u64 << 53) as f32 - 0.5
        };
        let centers: Vec<f32> = (0..8 * dim).map(|_| next()).collect();
        let mut out = Vec::with_capacity(rows * dim);
        for r in 0..rows {
            let center = &centers[(r % 8) * dim..(r % 8 + 1) * dim];
            let row: Vec<f32> = center.iter().map(|c| c + 0.6 * next()).collect();
            let norm = dot(&row, &row).sqrt();
            out.extend(row.iter().map(|v| v / norm));
        }
        out
    }

    fn exact_top(vectors: &[f32], dim: usize, query: &[f32], k: usize) -> Vec<u32> {
        let mut all: Vec<Scored> = vectors
            .chunks_exact(dim)
            .enumerate()
            .map(|(row, v)| Scored {
                score: dot(query, v),
                row: row as u32,
            })
            .collect();
        all.sort_unstable_by(|a, b| b.cmp(a));
        all.truncate(k);
        all.into_iter().map(|s| s.row).collect()
    }

    fn recall(graph: &LeannGraph, vectors: &[f32], dim: usize, queries: usize, k: usize) -> f64 {
        let mut found = 0usize;
        for q in 0..queries {
            let query = &vectors[q * 7 * dim..(q * 7 + 1) * dim];
            let truth = exact_top(vectors, dim, query, k);
            let (hits, _) = graph
                .search_with_stats(query, k, VectorSearchOptions::new())
                .unwrap();
            found += hits.iter().filter(|(_, row)| truth.contains(row)).count();
        }
        found as f64 / (queries * k) as f64
    }

    struct FromCopy {
        vectors: Vec<f32>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl RecomputeSource for FromCopy {
        fn embed_rows(&self, rows: &[u32], dim: usize, out: &mut [f32]) -> Result<(), VectorError> {
            self.calls
                .fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
            for (i, &row) in rows.iter().enumerate() {
                let start = row as usize * dim;
                out[i * dim..(i + 1) * dim].copy_from_slice(&self.vectors[start..start + dim]);
            }
            Ok(())
        }
    }

    #[test]
    fn config_round_trips_and_refuses_by_name() {
        let config = LeannGraphConfig {
            prune_ratio_ppm: 500_000,
            storage: EmbeddingStorage::Recomputed,
            ..LeannGraphConfig::default()
        };
        let backend = config.backend_config().unwrap();
        assert_eq!(
            LeannGraphConfig::from_backend_config(&backend).unwrap(),
            config
        );
        for (bad, needle) in [
            (
                LeannGraphConfig {
                    bits_per_dimension: 3,
                    ..config.clone()
                },
                "bits_per_dimension",
            ),
            (
                LeannGraphConfig {
                    base_degree: 64,
                    ..config.clone()
                },
                "base_degree",
            ),
            (
                LeannGraphConfig {
                    search_beam: 0,
                    ..config.clone()
                },
                "search_beam",
            ),
            (
                LeannGraphConfig {
                    prune_ratio_ppm: 1_000_000,
                    ..config.clone()
                },
                "prune_ratio_ppm",
            ),
            (
                LeannGraphConfig {
                    hub_ratio_ppm: 1_000_001,
                    ..config.clone()
                },
                "hub_ratio_ppm",
            ),
        ] {
            let error = bad.validate().unwrap_err().to_string();
            assert!(error.contains(needle), "{error}");
        }
        let mut payload = config.encode();
        payload[28] = 7;
        assert!(LeannGraphConfig::decode(&payload).is_err());
        assert!(LeannGraphConfig::decode(&payload[..31]).is_err());
    }

    #[test]
    fn pruning_keeps_hubs_dense_and_everything_else_sparse() {
        let dim = 16;
        let vectors = corpus(1500, dim, 3);
        let config = LeannGraphConfig::default();
        let pruned = build_graph(&vectors, dim, &config);
        let unpruned = build_graph(
            &vectors,
            dim,
            &LeannGraphConfig {
                base_degree: config.hub_degree,
                ..config.clone()
            },
        );
        let edges = |g: &Graph| g.adjacency.iter().map(Vec::len).sum::<usize>();
        assert_eq!(pruned.hubs.len(), 30, "2% of 1500 rows");
        assert!(
            edges(&pruned) * 2 < edges(&unpruned),
            "pruned {} edges, unpruned {}",
            edges(&pruned),
            edges(&unpruned)
        );
        let hub_mean = pruned
            .hubs
            .iter()
            .map(|&h| pruned.adjacency[h as usize].len())
            .sum::<usize>() as f64
            / pruned.hubs.len() as f64;
        let mean = edges(&pruned) as f64 / pruned.adjacency.len() as f64;
        assert!(hub_mean > 2.0 * mean, "hubs {hub_mean:.1}, all {mean:.1}");
        assert!(pruned
            .adjacency
            .iter()
            .all(|list| list.len() <= config.hub_degree as usize));
    }

    #[test]
    fn pruned_graph_keeps_recall_and_selective_recompute_cuts_exact_work() {
        let dim = 16;
        let rows = 2000;
        let vectors = corpus(rows, dim, 11);
        let mut graph = LeannGraph::new(Some(dim), LeannGraphConfig::default()).unwrap();
        graph.add(&vectors, dim).unwrap();
        graph.prepare().unwrap();
        let full = recall(&graph, &vectors, dim, 40, 10);
        assert!(full >= 0.9, "recall@10 {full}");

        let mut pruning = LeannGraph::new(
            Some(dim),
            LeannGraphConfig {
                prune_ratio_ppm: 600_000,
                ..LeannGraphConfig::default()
            },
        )
        .unwrap();
        pruning.add(&vectors, dim).unwrap();
        pruning.prepare().unwrap();
        let query = &vectors[5 * dim..6 * dim];
        let (_, all) = graph
            .search_with_stats(query, 10, VectorSearchOptions::new())
            .unwrap();
        let (_, selective) = pruning
            .search_with_stats(query, 10, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(all.exact_evaluations, all.visited);
        assert!(
            selective.exact_evaluations < selective.visited,
            "{selective:?}"
        );
        assert!(recall(&pruning, &vectors, dim, 40, 10) >= 0.8);
    }

    #[test]
    fn recompute_mode_drops_embeddings_and_gates_the_source() {
        let dim = 16;
        let vectors = corpus(1200, dim, 5);
        let config = LeannGraphConfig {
            storage: EmbeddingStorage::Recomputed,
            prune_ratio_ppm: 500_000,
            ..LeannGraphConfig::default()
        };
        let mut graph = LeannGraph::new(Some(dim), config).unwrap();
        graph.add(&vectors, dim).unwrap();
        graph.prepare().unwrap();
        assert_eq!(graph.graph_stats().stored_rows, 0);

        let query = &vectors[..dim];
        let error = graph
            .search_with_stats(query, 5, VectorSearchOptions::new())
            .unwrap_err();
        assert!(
            error.to_string().contains("attach a recompute source"),
            "{error}"
        );

        let mut wrong = vectors.clone();
        wrong[0] += 1.0e-3;
        let error = graph
            .attach_recompute_source(Arc::new(FromCopy {
                vectors: wrong,
                calls: Default::default(),
            }))
            .unwrap_err();
        assert!(error.to_string().contains("row 0"), "{error}");

        let source = Arc::new(FromCopy {
            vectors: vectors.clone(),
            calls: Default::default(),
        });
        graph.attach_recompute_source(source.clone()).unwrap();
        let (hits, stats) = graph
            .search_with_stats(query, 5, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(hits[0].1, 0);
        assert_eq!(hits[0].0, dot(query, query));
        assert_eq!(stats.recomputed, stats.exact_evaluations);
        assert!(stats.recomputed < stats.visited);

        // Stored mode answers identically without recomputing anything.
        let mut stored = LeannGraph::new(
            Some(dim),
            LeannGraphConfig {
                storage: EmbeddingStorage::Stored,
                ..graph.config().clone()
            },
        )
        .unwrap();
        stored.add(&vectors, dim).unwrap();
        stored.prepare().unwrap();
        let (same, none) = stored
            .search_with_stats(query, 5, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(same, hits);
        assert_eq!(none.recomputed, 0);
    }

    #[test]
    fn tail_is_exact_until_folded_and_masks_and_floors_hold() {
        let dim = 8;
        let vectors = corpus(600, dim, 9);
        let mut graph = LeannGraph::new(Some(dim), LeannGraphConfig::default()).unwrap();
        graph.add(&vectors[..400 * dim], dim).unwrap();
        graph.prepare().unwrap();
        graph.add(&vectors[400 * dim..], dim).unwrap();
        assert_eq!(graph.graph_stats().indexed_rows, 400);
        let query = &vectors[500 * dim..501 * dim];
        let (hits, stats) = graph
            .search_with_stats(query, 3, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(hits[0].1, 500);
        assert_eq!(stats.tail_scanned, 200);

        let mut mask = vec![false; 600];
        for row in (0..600).step_by(3) {
            mask[row] = true;
        }
        let (hits, _) = graph
            .search_with_stats(query, 20, VectorSearchOptions::new().with_mask(&mask))
            .unwrap();
        assert!(hits.iter().all(|(_, row)| mask[*row as usize]));
        let floor = hits[4].0;
        let (above, _) = graph
            .search_with_stats(
                query,
                20,
                VectorSearchOptions::new().with_minimum_score(floor),
            )
            .unwrap();
        assert!(above.iter().all(|(score, _)| *score >= floor));
        assert!(graph
            .search_with_stats(query, 3, VectorSearchOptions::new().with_mask(&mask[..5]))
            .is_err());
    }

    #[test]
    fn image_round_trips_and_refuses_corruption() {
        let dim = 8;
        let vectors = corpus(700, dim, 2);
        for storage in [EmbeddingStorage::Stored, EmbeddingStorage::Recomputed] {
            let mut graph = LeannGraph::new(
                Some(dim),
                LeannGraphConfig {
                    storage,
                    bits_per_dimension: 4,
                    ..LeannGraphConfig::default()
                },
            )
            .unwrap();
            graph.add(&vectors[..600 * dim], dim).unwrap();
            graph.prepare().unwrap();
            graph.add(&vectors[600 * dim..], dim).unwrap();
            let image = graph.encode_image();
            let mut loaded = LeannGraph::decode_image(&image).unwrap();
            assert_eq!(loaded.graph, graph.graph);
            assert_eq!(loaded.codes, graph.codes);
            assert_eq!(loaded.fingerprints, graph.fingerprints);
            assert_eq!(loaded.graph_stats(), graph.graph_stats());
            let source = Arc::new(FromCopy {
                vectors: vectors.clone(),
                calls: Default::default(),
            });
            if storage == EmbeddingStorage::Recomputed {
                assert_eq!(loaded.graph_stats().stored_rows, 100);
                graph.attach_recompute_source(source.clone()).unwrap();
                loaded.attach_recompute_source(source).unwrap();
            }
            let query = &vectors[3 * dim..4 * dim];
            assert_eq!(
                loaded
                    .search_with_stats(query, 10, VectorSearchOptions::new())
                    .unwrap(),
                graph
                    .search_with_stats(query, 10, VectorSearchOptions::new())
                    .unwrap()
            );

            let mut flipped = image.clone();
            flipped[60] ^= 1;
            assert!(LeannGraph::decode_image(&flipped).is_err());
            assert!(LeannGraph::decode_image(&image[..image.len() - 1]).is_err());
        }
    }

    #[test]
    fn provider_contract_is_configured_ann_and_dispatches_by_kind() {
        let dim = 8;
        let vectors = corpus(300, dim, 4);
        let mut index = VectorIndex::create(LEANN_GRAPH, dim, 8).unwrap();
        index.add(&vectors, dim).unwrap();
        index.prepare().unwrap();
        let descriptor = index.descriptor();
        assert_eq!(descriptor.backend_kind, LEANN_GRAPH);
        assert_eq!(descriptor.quality_contract, QualityContract::ConfiguredAnn);
        assert!(!descriptor
            .capabilities
            .contains(&VectorCapability::ExhaustiveCompletion));
        let rebuilt =
            VectorIndex::from_backend_config(dim, &index.backend_config().unwrap()).unwrap();
        assert_eq!(
            rebuilt.descriptor(),
            VectorIndex::create(LEANN_GRAPH, dim, 8)
                .unwrap()
                .descriptor()
        );

        let results = index.search(&vectors[..2 * dim], 4, VectorSearchOptions::new());
        assert_eq!(results.query_count, 2);
        assert_eq!(results.slots_for_query(0)[0], 0);
        assert_eq!(results.slots_for_query(1)[0], 1);

        let mut batches = 0;
        let summary = index
            .try_search_streaming_controlled(
                &vectors[..dim],
                VectorSearchOptions::new(),
                |batch| {
                    batches += 1;
                    assert_eq!(batch.slots[0], 0);
                    VectorStreamControl::Continue
                },
                || VectorStreamControl::Continue,
            )
            .unwrap();
        assert_eq!(batches, 1);
        assert!(summary.completed);
        assert_eq!(index.row_transcript(7).unwrap().len(), 20);

        let dir = std::env::temp_dir().join(format!("leann-graph-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("image");
        index.write(&path).unwrap();
        let loaded = VectorIndex::load(LEANN_GRAPH, &path).unwrap();
        let mapped = VectorIndex::load_mapped(LEANN_GRAPH, &path).unwrap();
        assert_eq!(loaded.descriptor(), descriptor);
        assert_eq!(
            mapped.search(&vectors[..dim], 4, VectorSearchOptions::new()),
            index.search(&vectors[..dim], 4, VectorSearchOptions::new())
        );
        fs::remove_dir_all(&dir).unwrap();
    }
}
