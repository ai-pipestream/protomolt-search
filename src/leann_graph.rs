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
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;

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
/// so appends rebuild O(log n) times. Below it an image that is never
/// `prepare`d stays a plain exact scan, which at that size is cheaper than
/// a graph and returns the exact top k.
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

/// Bytes kept per row as its FP32 identity.
pub const ROW_FINGERPRINT_BYTES: usize = 16;

/// The identity of one FP32 row: the first 128 bits of the SHA-256 of its
/// little-endian bit patterns. Every reported score is computed from that
/// row, so it is the row's transcript; the determinism gate and every
/// recompute check a recomputed embedding against it.
pub fn row_fingerprint(row: &[f32]) -> [u8; ROW_FINGERPRINT_BYTES] {
    let mut hash = crate::sha256::Sha256::new();
    for value in row {
        hash.update(&value.to_bits().to_le_bytes());
    }
    let digest = hash.finalize();
    let mut out = [0u8; ROW_FINGERPRINT_BYTES];
    out.copy_from_slice(&digest[..ROW_FINGERPRINT_BYTES]);
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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

    /// The largest gap between [`Self::approx`] and the exact inner product
    /// for any indexed row. Codes are fit on exactly those rows, so every
    /// coordinate lies within its range and rounds to the nearest level:
    /// off by at most half a step, `|q[i]| * step[i] / 2` per dimension.
    /// The relative slack covers f32 accumulation.
    fn error_bound(&self, weights: &[f32]) -> f32 {
        let bound: f32 = weights.iter().map(|w| w.abs()).sum::<f32>() * 0.5;
        bound * (1.0 + 1.0e-3) + 1.0e-6
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
    /// L2 norms, floored above zero, for the direction-only heuristic.
    norms: Vec<f32>,
}

impl<'a> Builder<'a> {
    fn new(vectors: &'a [f32], dim: usize, beam: usize) -> Self {
        let rows = vectors.len() / dim;
        Self {
            vectors,
            dim,
            beam,
            norms: vectors
                .chunks_exact(dim)
                .map(|row| dot(row, row).sqrt().max(f32::MIN_POSITIVE))
                .collect(),
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

    /// The HNSW neighbor heuristic: walk candidates best first (by inner
    /// product with `base`) and keep one only when its direction is closer
    /// to `base` than to every neighbor already kept, then top up from the
    /// skipped ones so the cap is used. Directions (cosine), not raw inner
    /// products, decide diversity, so a long kept vector cannot crowd out a
    /// candidate that sits right next to `base`.
    fn select(&self, base: u32, mut candidates: Vec<Scored>, cap: usize) -> Vec<u32> {
        // cos(e, k) < cos(e, base), with |e| cancelled from both sides.
        let base_norm = self.norms[base as usize];
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
            let toward_base = candidate.score / base_norm;
            if kept
                .iter()
                .all(|&k| dot(row, self.row(k)) / self.norms[k as usize] < toward_base)
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
    // The pruned graph is then rebuilt by insertion, as the paper's
    // Algorithm 3 does, rather than by trimming these lists: insertion
    // searches the sparser graph being built, so each node's links are
    // chosen against the neighborhood that will actually serve.
    let mut full = Builder::new(vectors, dim, beam);
    for row in 0..rows as u32 {
        full.insert(row, hub_degree, hub_degree);
    }
    // Undirected degree: distinct neighbors over out- and in-links. Links
    // are mostly reciprocal, so an in-link counts only when the reverse
    // out-link was shrunk away.
    let mut degree: Vec<usize> = full.adjacency.iter().map(Vec::len).collect();
    for (node, list) in full.adjacency.iter().enumerate() {
        for &neighbor in list {
            if !full.adjacency[neighbor as usize].contains(&(node as u32)) {
                degree[neighbor as usize] += 1;
            }
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

/// Pending rows evaluated together once the frontier is behind the beam.
const PENDING_BATCH_ROWS: usize = 16;

/// One query's hits, best first, after the floor.
struct Run {
    hits: Vec<Scored>,
    stats: TraversalStats,
    completed: bool,
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
    /// Whether a background rebuild is running.
    pub building: bool,
}

/// The graph over rows `0..indexed` and the codes that steer its walk:
/// the part a rebuild replaces, swapped in whole.
#[derive(Debug, Clone, Default, PartialEq)]
struct Index {
    indexed: usize,
    codes: Codes,
    graph: Graph,
}

/// Everything a rebuild needs, copied out so it runs with no lock held.
struct BuildJob {
    config: LeannGraphConfig,
    dim: usize,
    /// Rows `stored_base..rows` as held.
    held: Vec<f32>,
    stored_base: usize,
    /// Fingerprints of rows `0..stored_base`, which the source recomputes.
    fingerprints: Vec<[u8; ROW_FINGERPRINT_BYTES]>,
    source: Option<Arc<dyn RecomputeSource>>,
}

impl BuildJob {
    /// The rows' embeddings in order (recomputing those not held, in chunks,
    /// each checked against its fingerprint), then the pruned graph and the
    /// codes over all of them.
    fn run(self) -> Result<Index, VectorError> {
        let dim = self.dim;
        let rows = self.stored_base + self.held.len() / dim;
        let mut vectors = Vec::with_capacity(rows * dim);
        let mut start = 0usize;
        while start < self.stored_base {
            let source = self.source.as_ref().ok_or_else(|| {
                VectorError::new(format!(
                    "{LEANN_GRAPH} cannot rebuild a recompute-mode image without a recompute source"
                ))
            })?;
            let end = self.stored_base.min(start + REBUILD_CHUNK_ROWS);
            let ids: Vec<u32> = (start as u32..end as u32).collect();
            let mut chunk = vec![0f32; ids.len() * dim];
            source.embed_rows(&ids, dim, &mut chunk)?;
            verify(&self.fingerprints, &ids, &chunk, dim)?;
            vectors.extend_from_slice(&chunk);
            start = end;
        }
        vectors.extend_from_slice(&self.held);
        Ok(Index {
            indexed: rows,
            graph: build_graph(&vectors, dim, &self.config),
            codes: Codes::fit(self.config.bits_per_dimension, dim, &vectors),
        })
    }
}

fn verify(
    fingerprints: &[[u8; ROW_FINGERPRINT_BYTES]],
    rows: &[u32],
    vectors: &[f32],
    dim: usize,
) -> Result<(), VectorError> {
    for (row, vector) in rows.iter().zip(vectors.chunks_exact(dim)) {
        let expected = fingerprints[*row as usize];
        let actual = row_fingerprint(vector);
        if actual != expected {
            return Err(VectorError::new(format!(
                "recomputed embedding of row {row} does not reproduce the indexed vector \
                 (fingerprint {}, indexed {}); the recompute source \
                 is not the model the image was built with, or is not deterministic",
                hex(&actual),
                hex(&expected)
            )));
        }
    }
    Ok(())
}

/// The `leann-graph` provider.
///
/// Rows `0..indexed` are in the graph; later rows form an unindexed tail
/// scored exhaustively until the next rebuild. The provider holds the FP32
/// rows `stored_base..rows`: all of them under [`EmbeddingStorage::Stored`];
/// under [`EmbeddingStorage::Recomputed`] the tail, plus rows a background
/// build indexed since the last append (dropped at the next `add` or
/// `prepare`, and never written to an image).
///
/// Rebuilds run on a background thread over a copy of their inputs, so
/// neither the caller of `add` nor any reader waits for one. A finished
/// build is swapped in whole the next time any method looks at the index;
/// until then its rows are scored exactly as tail.
pub struct LeannGraph {
    config: LeannGraphConfig,
    dim: Option<usize>,
    rows: usize,
    stored_base: usize,
    stored: Vec<f32>,
    fingerprints: Vec<[u8; ROW_FINGERPRINT_BYTES]>,
    index: RwLock<Arc<Index>>,
    build: Mutex<Option<JoinHandle<Result<Index, VectorError>>>>,
    build_error: Mutex<Option<VectorError>>,
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
            index: RwLock::new(Arc::new(Index {
                codes: Codes {
                    bits: config.bits_per_dimension,
                    dim: dimension.unwrap_or(0),
                    ..Codes::default()
                },
                ..Index::default()
            })),
            config,
            dim: dimension,
            rows: 0,
            stored_base: 0,
            stored: Vec::new(),
            fingerprints: Vec::new(),
            build: Mutex::new(None),
            build_error: Mutex::new(None),
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
        let index = self.index();
        GraphStats {
            rows: self.rows,
            indexed_rows: index.indexed,
            edges: index.graph.adjacency.iter().map(Vec::len).sum(),
            hubs: index.graph.hubs.len(),
            max_degree: index
                .graph
                .adjacency
                .iter()
                .map(Vec::len)
                .max()
                .unwrap_or(0),
            stored_rows: self.rows - self.held_from(&index),
            building: self.build.lock().unwrap().is_some(),
        }
    }

    /// The current index, after swapping in a finished background build.
    fn index(&self) -> Arc<Index> {
        let finished = {
            let mut build = self.build.lock().unwrap();
            match build.as_ref() {
                Some(handle) if handle.is_finished() => build.take(),
                _ => None,
            }
        };
        if let Some(handle) = finished {
            self.install(handle);
        }
        self.index.read().unwrap().clone()
    }

    fn install(&self, handle: JoinHandle<Result<Index, VectorError>>) {
        let outcome = handle.join().unwrap_or_else(|_| {
            Err(VectorError::new(format!(
                "{LEANN_GRAPH} background build panicked"
            )))
        });
        match outcome {
            Ok(built) => {
                let mut index = self.index.write().unwrap();
                if built.indexed > index.indexed {
                    *index = Arc::new(built);
                }
            }
            Err(error) => *self.build_error.lock().unwrap() = Some(error),
        }
    }

    /// Wait for a background build, swap it in, and report its failure if
    /// it failed (once; the next append schedules a fresh build).
    pub fn wait_for_build(&self) -> Result<(), VectorError> {
        let pending = self.build.lock().unwrap().take();
        if let Some(handle) = pending {
            self.install(handle);
        }
        match self.build_error.lock().unwrap().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// The first row an image of `index` holds as FP32.
    fn held_from(&self, index: &Index) -> usize {
        match self.config.storage {
            EmbeddingStorage::Stored => 0,
            EmbeddingStorage::Recomputed => index.indexed.max(self.stored_base),
        }
    }

    /// Under recompute storage, drop the held rows a build has indexed.
    fn trim_indexed_rows(&mut self) {
        let held_from = self.held_from(&self.index());
        if let (Some(dim), true) = (self.dim, held_from > self.stored_base) {
            self.stored.drain(..(held_from - self.stored_base) * dim);
            self.stored_base = held_from;
        }
    }

    fn build_job(&self) -> Option<BuildJob> {
        Some(BuildJob {
            config: self.config.clone(),
            dim: self.dim?,
            held: self.stored.clone(),
            stored_base: self.stored_base,
            fingerprints: self.fingerprints[..self.stored_base].to_vec(),
            source: self.source.clone(),
        })
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
        let indexed = self.index().indexed;
        if indexed > 0 && dim > 0 {
            let sample = GATE_SAMPLE_ROWS.min(indexed);
            let rows: Vec<u32> = (0..sample).map(|i| (i * indexed / sample) as u32).collect();
            let mut out = vec![0f32; rows.len() * dim];
            source.embed_rows(&rows, dim, &mut out)?;
            verify(&self.fingerprints, &rows, &out, dim)?;
        }
        self.source = Some(source);
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
        verify(&self.fingerprints, &ids, &buffer, dim)?;
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
        let beam = k.max(self.config.search_beam as usize);
        let mut run = self.run(query, beam, options, false, &mut || {
            VectorStreamControl::Continue
        })?;
        run.hits.truncate(k);
        Ok((
            run.hits.into_iter().map(|h| (h.score, h.row)).collect(),
            run.stats,
        ))
    }

    /// Traverse the graph and score the tail. With `every_evaluated` the
    /// hits are every allowed row whose exact score was computed, not only
    /// the beam's best; `control` is polled after every hop.
    fn run(
        &self,
        query: &[f32],
        beam: usize,
        options: VectorSearchOptions<'_>,
        every_evaluated: bool,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<Run, VectorError> {
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
        let mut floor = options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        let mut stats = TraversalStats::default();
        let index = self.index();
        let (best, evaluated, stopped) = self.traverse(
            &index,
            query,
            beam,
            options.allow,
            &mut floor,
            &mut stats,
            control,
        )?;
        let mut hits = if every_evaluated { evaluated } else { best };
        if !stopped {
            let allowed = |row: usize| options.allow.is_none_or(|mask| mask[row]);
            for row in index.indexed..self.rows {
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
        }
        hits.retain(|hit| hit.score >= floor);
        hits.sort_unstable_by(|a, b| b.cmp(a));
        Ok(Run {
            hits,
            stats,
            completed: !stopped,
        })
    }

    /// The two-level search (paper Algorithm 2) over the indexed rows.
    /// Returns the beam's best allowed rows, every allowed row scored
    /// exactly, and whether `control` stopped the traversal.
    ///
    /// Approximate scores never decide a stop on their own: a pending row
    /// is passed over only when its approximate score plus the codes' proven
    /// error bound cannot reach the beam's worst exact score. The floor only
    /// filters results; a node below it can still lead to rows above it.
    #[allow(clippy::too_many_arguments)]
    fn traverse(
        &self,
        index: &Index,
        query: &[f32],
        beam: usize,
        allow: Option<&[bool]>,
        floor: &mut f32,
        stats: &mut TraversalStats,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<(Vec<Scored>, Vec<Scored>, bool), VectorError> {
        if index.indexed == 0 || beam == 0 {
            return Ok((Vec::new(), Vec::new(), false));
        }
        let allowed = |row: u32| allow.is_none_or(|mask| mask[row as usize]);
        let (base, weights) = index.codes.query(query);
        let bound = index.codes.error_bound(&weights);
        let mut visited = vec![0u64; index.indexed.div_ceil(64)];
        let mut visit = |row: u32| {
            let (word, bit) = (row as usize / 64, 1u64 << (row % 64));
            let fresh = visited[word] & bit == 0;
            visited[word] |= bit;
            fresh
        };
        let mut results: BinaryHeap<Reverse<Scored>> = BinaryHeap::new();
        let mut evaluated: Vec<Scored> = Vec::new();
        let mut frontier: BinaryHeap<Scored> = BinaryHeap::new();
        let mut pending: BinaryHeap<Scored> = BinaryHeap::new();
        let mut exact = Vec::new();
        let mut batch: Vec<u32> = vec![index.graph.entry];
        visit(index.graph.entry);
        stats.visited += 1;
        let keep = |discovered: usize| -> usize {
            if discovered == 0 {
                return 0;
            }
            let pruned =
                (discovered as u64 * u64::from(self.config.prune_ratio_ppm) / PPM) as usize;
            (discovered - pruned).max(1)
        };
        let mut stopped = false;
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
                        evaluated.push(scored);
                        results.push(Reverse(scored));
                        if results.len() > beam {
                            results.pop();
                        }
                    }
                }
                batch.clear();
            }
            match control() {
                VectorStreamControl::Stop => {
                    stopped = true;
                    break;
                }
                VectorStreamControl::RaiseFloor(raised) => *floor = floor.max(raised),
                VectorStreamControl::Continue => {}
            }
            let worst = (results.len() >= beam).then(|| results.peek().unwrap().0.score);
            let competitive = |score: f32| worst.is_none_or(|w| score >= w);
            if let Some(candidate) = frontier.peek().copied().filter(|c| competitive(c.score)) {
                frontier.pop();
                stats.hops += 1;
                let mut discovered = 0;
                for &neighbor in &index.graph.adjacency[candidate.row as usize] {
                    if visit(neighbor) {
                        pending.push(Scored {
                            score: index.codes.approx(base, &weights, neighbor),
                            row: neighbor,
                        });
                        discovered += 1;
                    }
                }
                stats.visited += discovered as u64;
                for _ in 0..keep(discovered) {
                    match pending.pop() {
                        Some(next) => batch.push(next.row),
                        None => break,
                    }
                }
            }
            // The frontier is exhausted or behind the beam: evaluate the
            // pending rows that could still enter it, best first.
            if batch.is_empty() {
                while batch.len() < PENDING_BATCH_ROWS {
                    match pending.peek() {
                        Some(next) if competitive(next.score + bound) => {
                            batch.push(next.row);
                            pending.pop();
                        }
                        _ => break,
                    }
                }
                if batch.is_empty() {
                    break;
                }
            }
        }
        let best = results.into_iter().map(|Reverse(s)| s).collect();
        Ok((best, evaluated, stopped))
    }

    fn encode_image(&self) -> Vec<u8> {
        let dim = self.dim.unwrap_or(0);
        let index = self.index();
        let held_from = self.held_from(&index);
        let mut out = Vec::new();
        out.extend_from_slice(IMAGE_MAGIC);
        out.extend_from_slice(&IMAGE_FORMAT.to_le_bytes());
        out.extend_from_slice(&self.config.encode());
        for value in [dim, self.rows, index.indexed, held_from] {
            out.extend_from_slice(&(value as u64).to_le_bytes());
        }
        out.extend_from_slice(&index.graph.entry.to_le_bytes());
        for fingerprint in &self.fingerprints {
            out.extend_from_slice(fingerprint);
        }
        if index.indexed > 0 {
            for value in index.codes.min.iter().chain(&index.codes.step) {
                out.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            out.extend_from_slice(&index.codes.bytes);
        }
        out.extend_from_slice(&(index.graph.hubs.len() as u64).to_le_bytes());
        for hub in &index.graph.hubs {
            out.extend_from_slice(&hub.to_le_bytes());
        }
        // CSR: offsets, then the concatenated neighbor lists.
        let mut offset = 0u64;
        out.extend_from_slice(&offset.to_le_bytes());
        for list in &index.graph.adjacency {
            offset += list.len() as u64;
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for list in &index.graph.adjacency {
            for neighbor in list {
                out.extend_from_slice(&neighbor.to_le_bytes());
            }
        }
        for value in &self.stored[(held_from - self.stored_base) * dim..] {
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
        // Every array is length-checked against the bytes that remain before
        // anything is allocated for it, with overflow-checked sizes.
        let fingerprints = reader
            .array(rows, ROW_FINGERPRINT_BYTES)?
            .chunks_exact(ROW_FINGERPRINT_BYTES)
            .map(|chunk| chunk.try_into().unwrap())
            .collect();
        let mut codes = Codes {
            bits: config.bits_per_dimension,
            dim,
            ..Codes::default()
        };
        if indexed > 0 {
            codes.min = reader.f32s(dim)?;
            codes.step = reader.f32s(dim)?;
            let stride = dim
                .checked_mul(codes.bits as usize)
                .map(|bits| bits.div_ceil(8))
                .ok_or_else(|| VectorError::new("code length overflow"))?;
            codes.bytes = reader.array(indexed, stride)?.to_vec();
        }
        let hub_count = reader.usize()?;
        if hub_count > indexed {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image names {hub_count} hubs for {indexed} indexed rows"
            )));
        }
        let hubs = reader.u32s(hub_count)?;
        let offsets = reader
            .array(indexed + 1, 8)?
            .chunks_exact(8)
            .map(|chunk| usize::try_from(u64::from_le_bytes(chunk.try_into().unwrap())))
            .collect::<Result<Vec<usize>, _>>()
            .map_err(|_| VectorError::new("image field exceeds usize"))?;
        if offsets[0] != 0 || offsets.windows(2).any(|w| w[1] < w[0]) {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} image adjacency offsets are not monotonic"
            )));
        }
        let mut adjacency = Vec::with_capacity(indexed);
        for window in offsets.windows(2) {
            if window[1] - window[0] > MAX_DEGREE as usize {
                return Err(VectorError::new(format!(
                    "{LEANN_GRAPH} image adjacency list exceeds {MAX_DEGREE} neighbors"
                )));
            }
            let list = reader.u32s(window[1] - window[0])?;
            if list.iter().any(|&n| n as usize >= indexed) {
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
        let held = (rows - stored_base)
            .checked_mul(dim)
            .ok_or_else(|| VectorError::new("held row length overflow"))?;
        let stored = reader.f32s(held)?;
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
            stored_base,
            stored,
            fingerprints,
            index: RwLock::new(Arc::new(Index {
                indexed,
                codes,
                graph: Graph {
                    adjacency,
                    hubs,
                    entry,
                },
            })),
            build: Mutex::new(None),
            build_error: Mutex::new(None),
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

    /// `count` items of `width` bytes, refused before any allocation when
    /// the size overflows or exceeds what remains.
    fn array(&mut self, count: usize, width: usize) -> Result<&'a [u8], VectorError> {
        let length = count
            .checked_mul(width)
            .ok_or_else(|| VectorError::new(format!("{LEANN_GRAPH} image array size overflows")))?;
        self.take(length)
    }

    fn u32s(&mut self, count: usize) -> Result<Vec<u32>, VectorError> {
        Ok(self
            .array(count, 4)?
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect())
    }

    fn f32s(&mut self, count: usize) -> Result<Vec<f32>, VectorError> {
        Ok(self.u32s(count)?.into_iter().map(f32::from_bits).collect())
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
        // A failed background build is reported here, before anything is
        // appended, so the caller sees it; the next append builds afresh.
        if let Some(error) = self.build_error.lock().unwrap().take() {
            return Err(error);
        }
        self.trim_indexed_rows();
        let indexed = self.index().indexed;
        let threshold = REBUILD_MIN_TAIL.max(indexed);
        if !self.can_rebuild() && self.rows + added - indexed >= threshold {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} recompute-mode image has {} unindexed rows and no recompute \
                 source to rebuild with; attach one before appending more",
                self.rows - indexed
            )));
        }
        if self.dim.is_none() {
            let mut index = self.index.write().unwrap();
            Arc::make_mut(&mut index).codes.dim = dimension;
        }
        self.dim = Some(dimension);
        self.fingerprints
            .extend(vectors.chunks_exact(dimension).map(row_fingerprint));
        self.stored.extend_from_slice(vectors);
        self.rows += added;
        let mut build = self.build.lock().unwrap();
        if build.is_none() && self.rows - indexed >= threshold {
            if let Some(job) = self.build_job() {
                let handle = std::thread::Builder::new()
                    .name("leann-graph-build".into())
                    .spawn(move || job.run())
                    .map_err(|e| {
                        VectorError::new(format!("{LEANN_GRAPH} cannot start a build: {e}"))
                    })?;
                *build = Some(handle);
            }
        }
        Ok(())
    }

    /// Fold the unindexed tail into the graph. Under recompute storage this
    /// is also where the build's embeddings are dropped.
    fn prepare(&mut self) -> Result<(), VectorError> {
        self.wait_for_build()?;
        self.trim_indexed_rows();
        if self.index().indexed == self.rows {
            return Ok(());
        }
        if !self.can_rebuild() {
            return Err(VectorError::new(format!(
                "{LEANN_GRAPH} cannot rebuild a recompute-mode image without a recompute source"
            )));
        }
        if let Some(job) = self.build_job() {
            let built = job.run()?;
            *self.index.get_mut().unwrap() = Arc::new(built);
            self.trim_indexed_rows();
        }
        Ok(())
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

    /// One traversal per query at the configured beam (the depth the
    /// scoring fingerprint binds), emitting every allowed row whose exact
    /// score was computed, plus the tail, best first as one batch.
    /// `control` is polled after every hop: `Stop` ends the stream
    /// uncompleted and a raised floor gates the rest of the query and every
    /// later one. `completed` means the traversal finished, never that every
    /// row was evaluated: the descriptor carries no exhaustive completion.
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
            let mut query_options = options;
            query_options.minimum_score = floor;
            // Polled after every hop; a raised floor carries to later queries.
            let mut poll = || {
                let decision = control();
                if let VectorStreamControl::RaiseFloor(raised) = decision {
                    floor = Some(floor.map_or(raised, |f| f.max(raised)));
                }
                decision
            };
            let run = self.run(
                query,
                self.config.search_beam as usize,
                query_options,
                true,
                &mut poll,
            )?;
            units += (run.stats.exact_evaluations + run.stats.tail_scanned) as usize;
            let mut stop = !run.completed;
            if !run.hits.is_empty() {
                let scores: Vec<f32> = run.hits.iter().map(|h| h.score).collect();
                let slots: Vec<i64> = run.hits.iter().map(|h| i64::from(h.row)).collect();
                emitted += run.hits.len();
                let decision = sink(&VectorStreamBatch {
                    query_index,
                    block_base: 0,
                    scores: &scores,
                    slots: &slots,
                });
                stop |= matches!(decision, VectorStreamControl::Stop);
            }
            if stop {
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
    /// 128-bit SHA-256 fingerprint of the FP32 row every reported score is
    /// computed from (the bytes the scorer reads, which a recompute-mode
    /// image does not keep). The approximate codes only steer traversal and
    /// are refit at every rebuild, so they are deliberately not part of it.
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
        let mut out = Vec::with_capacity(12 + ROW_FINGERPRINT_BYTES);
        for row in rows {
            out.clear();
            out.extend_from_slice(b"LEAN");
            out.extend_from_slice(&dim.to_le_bytes());
            out.extend_from_slice(&self.fingerprints[row]);
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

    fn indexed_len(&self) -> Option<usize> {
        Some(self.index().indexed)
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

/// Nodes cannot yet bind a vector field to the model that embedded it, so
/// nothing could attach a recompute source after compaction replays a
/// segment and `prepare` rebuilds it. Until that binding exists the node
/// paths refuse recompute storage; library callers that own a source use
/// [`LeannGraph`] directly.
fn refuse_recompute_on_nodes(config: &LeannGraphConfig) -> Result<(), VectorError> {
    match config.storage {
        EmbeddingStorage::Stored => Ok(()),
        EmbeddingStorage::Recomputed => Err(VectorError::new(format!(
            "{LEANN_GRAPH} recompute storage needs a recompute source, which nodes cannot \
             bind to a vector field yet; configure stored embeddings"
        ))),
    }
}

pub(crate) fn from_config(
    dimension: usize,
    config: &VectorBackendConfig,
) -> Result<VectorIndex, VectorError> {
    let config = LeannGraphConfig::from_backend_config(config)?;
    refuse_recompute_on_nodes(&config)?;
    LeannGraph::new(Some(dimension), config).map(LeannGraph::into_index)
}

pub(crate) fn load(path: &Path) -> Result<VectorIndex, VectorError> {
    let graph = LeannGraph::load(path)?;
    refuse_recompute_on_nodes(&graph.config)?;
    Ok(graph.into_index())
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
            assert_eq!(loaded.index().graph, graph.index().graph);
            assert_eq!(loaded.index().codes, graph.index().codes);
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
        assert_eq!(
            index.row_transcript(7).unwrap().len(),
            12 + ROW_FINGERPRINT_BYTES
        );

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
    #[test]
    fn appends_index_once_the_tail_reaches_the_threshold() {
        let dim = 8;
        let vectors = corpus(REBUILD_MIN_TAIL + 10, dim, 21);
        let mut graph = LeannGraph::new(Some(dim), LeannGraphConfig::default()).unwrap();
        graph
            .add(&vectors[..(REBUILD_MIN_TAIL - 1) * dim], dim)
            .unwrap();
        assert_eq!(graph.graph_stats().indexed_rows, 0);
        graph
            .add(&vectors[(REBUILD_MIN_TAIL - 1) * dim..], dim)
            .unwrap();
        // The append returned without building; until the build lands every
        // row is still answered, from the tail.
        let (hits, _) = graph
            .search_with_stats(&vectors[..dim], 5, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(hits[0].1, 0);
        graph.wait_for_build().unwrap();
        let stats = graph.graph_stats();
        assert_eq!(stats.indexed_rows, REBUILD_MIN_TAIL + 10);
        assert!(!stats.building);
        let (hits, stats) = graph
            .search_with_stats(&vectors[..dim], 5, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(stats.tail_scanned, 0);
        assert!(stats.hops > 0);
        assert_eq!(hits[0].1, 0);
    }

    /// Recompute storage with no source refuses the append that would need a
    /// rebuild instead of letting the tail grow unindexed, and nodes refuse
    /// recompute storage outright until they can bind a source.
    #[test]
    fn recompute_storage_refuses_what_it_cannot_rebuild() {
        let dim = 8;
        let config = LeannGraphConfig {
            storage: EmbeddingStorage::Recomputed,
            ..LeannGraphConfig::default()
        };
        let vectors = corpus(3 * REBUILD_MIN_TAIL, dim, 31);
        let mut graph = LeannGraph::new(Some(dim), config.clone()).unwrap();
        graph.add(&vectors[..REBUILD_MIN_TAIL * dim], dim).unwrap();
        graph.prepare().unwrap();
        assert_eq!(graph.graph_stats().stored_rows, 0);
        let more = &vectors[REBUILD_MIN_TAIL * dim..];
        let error = graph.add(more, dim).unwrap_err().to_string();
        assert!(error.contains("no recompute source"), "{error}");
        assert_eq!(graph.graph_stats().rows, REBUILD_MIN_TAIL);
        graph
            .attach_recompute_source(Arc::new(FromCopy {
                vectors: vectors.clone(),
                calls: Default::default(),
            }))
            .unwrap();
        graph.add(more, dim).unwrap();
        graph.prepare().unwrap();
        assert_eq!(graph.graph_stats().indexed_rows, 3 * REBUILD_MIN_TAIL);
        assert_eq!(graph.graph_stats().stored_rows, 0);

        let error = from_config(dim, &config.backend_config().unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("recompute storage"), "{error}");
        assert!(from_config(dim, &LeannGraphConfig::default().backend_config().unwrap()).is_ok());
    }

    /// 4-bit codes bound the approximate score loosely, so most pruned rows
    /// still clear the bound and are scored exactly: recall holds, and the
    /// saving `prune_ratio` buys is real at 8 bits, not at 4.
    #[test]
    fn four_bit_codes_keep_recall_under_pruning() {
        let dim = 32;
        let vectors = corpus(3000, dim, 41);
        let build = |bits_per_dimension| {
            let mut graph = LeannGraph::new(
                Some(dim),
                LeannGraphConfig {
                    bits_per_dimension,
                    prune_ratio_ppm: 800_000,
                    ..LeannGraphConfig::default()
                },
            )
            .unwrap();
            graph.add(&vectors, dim).unwrap();
            graph.prepare().unwrap();
            graph
        };
        let exact_rows = |graph: &LeannGraph| {
            (0..40)
                .map(|q| {
                    let query = &vectors[q * 7 * dim..(q * 7 + 1) * dim];
                    graph
                        .search_with_stats(query, 10, VectorSearchOptions::new())
                        .unwrap()
                        .1
                        .exact_evaluations
                })
                .sum::<u64>()
        };
        let four = build(4);
        let eight = build(8);
        assert!(recall(&four, &vectors, dim, 40, 10) >= 0.9);
        assert!(recall(&eight, &vectors, dim, 40, 10) >= 0.9);
        assert!(exact_rows(&eight) < exact_rows(&four));
    }

    /// A floor above the entry hub's own score must not stop the walk
    /// before it leaves the entry, and a heavy prune ratio must not strand
    /// competitive rows in the approximate queue.
    #[test]
    fn floors_and_pruning_do_not_cut_the_walk_short() {
        let dim = 16;
        let vectors = corpus(1500, dim, 13);
        let config = LeannGraphConfig {
            prune_ratio_ppm: 900_000,
            ..LeannGraphConfig::default()
        };
        let mut graph = LeannGraph::new(Some(dim), config).unwrap();
        graph.add(&vectors, dim).unwrap();
        graph.prepare().unwrap();
        let entry = graph.index().graph.entry as usize;
        let mut checked = 0;
        for q in 0..40 {
            let query = &vectors[q * 11 * dim..(q * 11 + 1) * dim];
            let truth = exact_top(&vectors, dim, query, 3);
            let third = dot(query, &vectors[truth[2] as usize * dim..][..dim]);
            if dot(query, &vectors[entry * dim..(entry + 1) * dim]) >= third {
                continue;
            }
            checked += 1;
            let (hits, _) = graph
                .search_with_stats(
                    query,
                    3,
                    VectorSearchOptions::new().with_minimum_score(third),
                )
                .unwrap();
            assert!(
                !hits.is_empty(),
                "query {q} returned nothing above its floor"
            );
            assert!(hits.iter().all(|(score, _)| *score >= third));
        }
        assert!(checked > 10);
        assert!(recall(&graph, &vectors, dim, 40, 10) >= 0.8);
    }

    #[test]
    fn heuristic_holds_recall_on_vectors_of_varied_length() {
        let dim = 16;
        let mut vectors = corpus(2000, dim, 17);
        for (r, row) in vectors.chunks_exact_mut(dim).enumerate() {
            let scale = 0.25 + (r * 7919 % 1000) as f32 / 250.0;
            row.iter_mut().for_each(|v| *v *= scale);
        }
        let mut graph = LeannGraph::new(Some(dim), LeannGraphConfig::default()).unwrap();
        graph.add(&vectors, dim).unwrap();
        graph.prepare().unwrap();
        let found = recall(&graph, &vectors, dim, 40, 10);
        assert!(found >= 0.9, "recall@10 {found}");
    }

    #[test]
    fn streaming_emits_every_exact_score_and_honors_control_between_hops() {
        let dim = 16;
        let vectors = corpus(1500, dim, 19);
        let mut graph = LeannGraph::new(Some(dim), LeannGraphConfig::default()).unwrap();
        graph.add(&vectors, dim).unwrap();
        graph.prepare().unwrap();
        let query = &vectors[..dim];
        let (_, stats) = graph
            .search_with_stats(query, 64, VectorSearchOptions::new())
            .unwrap();
        let mut emitted = Vec::new();
        let summary = graph
            .search_streaming_controlled(
                query,
                VectorSearchOptions::new(),
                &mut |batch| {
                    emitted.extend_from_slice(batch.slots);
                    VectorStreamControl::Continue
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        assert!(summary.completed);
        assert_eq!(emitted.len() as u64, stats.exact_evaluations);
        assert!(emitted.len() > 64, "{} rows", emitted.len());

        let mut polls = 0;
        let summary = graph
            .search_streaming_controlled(
                query,
                VectorSearchOptions::new(),
                &mut |_| VectorStreamControl::Continue,
                &mut || {
                    polls += 1;
                    if polls == 3 {
                        VectorStreamControl::Stop
                    } else {
                        VectorStreamControl::Continue
                    }
                },
            )
            .unwrap();
        assert!(!summary.completed);
        assert_eq!(polls, 3);

        let floor = dot(query, query) - 1.0e-3;
        let mut low = 0;
        graph
            .search_streaming_controlled(
                query,
                VectorSearchOptions::new(),
                &mut |batch| {
                    low += batch.scores.iter().filter(|&&s| s < floor).count();
                    VectorStreamControl::Continue
                },
                &mut || VectorStreamControl::RaiseFloor(floor),
            )
            .unwrap();
        assert_eq!(low, 0);
    }

    /// Header counts larger than the image are refused before anything is
    /// allocated for them, and sizes that overflow refuse instead of
    /// wrapping.
    #[test]
    fn hostile_headers_refuse_before_allocating() {
        let mut body = Vec::new();
        body.extend_from_slice(IMAGE_MAGIC);
        body.extend_from_slice(&IMAGE_FORMAT.to_le_bytes());
        body.extend_from_slice(&LeannGraphConfig::default().encode());
        let header_end = body.len();
        for (dim, rows) in [(8u64, u32::MAX as u64), (u64::MAX / 2, 4)] {
            body.truncate(header_end);
            for value in [dim, rows, 0, 0] {
                body.extend_from_slice(&value.to_le_bytes());
            }
            body.extend_from_slice(&0u32.to_le_bytes());
            let mut image = body.clone();
            image.extend_from_slice(&crate::sha256::digest(&body));
            let error = LeannGraph::decode_image(&image).unwrap_err().to_string();
            assert!(
                error.contains("truncated") || error.contains("overflow"),
                "{error}"
            );
        }
    }
}
