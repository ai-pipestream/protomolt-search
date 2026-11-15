//! Unit tests of the `hnsw` provider. Everything is seeded.

mod bulk_walk;
mod config_build;
mod filter_aware;
mod floor_prune;
mod images;
mod locality;
mod locality_identity;
mod masks;
mod merge;
mod patience;
mod provider;
mod regimes;
mod repair;
mod streaming;
mod unlinked;
mod visit_budget;

use std::path::PathBuf;

use super::config::{BuildMode, HnswConfig, CONFIG_FORMAT};
use super::graph::{level_for, MAX_LEVEL};
use super::{HnswIndex, HNSW};
use crate::boolean_bits::Bits;
use crate::vector::{
    FilterRegime, FilterRouting, QualityContract, VectorBackendConfig, VectorCapability,
    VectorIndex, VectorProvider, VectorSearchOptions, VectorStreamControl, EMBEDDED_TURBOVEC,
};

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Approximately normal: the sum of twelve uniforms, centered.
    fn normal(&mut self) -> f32 {
        ((0..12).map(|_| self.unit()).sum::<f64>() - 6.0) as f32
    }
}

/// `rows + queries` unit vectors of `dim` values drawn around sixteen
/// seeded topic centers (intra-topic cosine about 0.9, as tight as the
/// challenge fixture's topics), the queries from the same centers, so they
/// have the neighborhoods a real query has.
fn corpus_with_queries(rows: usize, queries: usize, dim: usize, seed: u64) -> (Vec<f32>, Vec<f32>) {
    let mut rng = Rng(seed);
    let topics: Vec<Vec<f32>> = (0..16)
        .map(|_| (0..dim).map(|_| rng.normal()).collect())
        .collect();
    let mut draw = |count: usize| {
        let mut out = Vec::with_capacity(count * dim);
        for _ in 0..count {
            let topic = &topics[(rng.next_u64() % 16) as usize];
            let mut row: Vec<f32> = topic.iter().map(|t| t + 0.3 * rng.normal()).collect();
            let norm = row.iter().map(|x| x * x).sum::<f32>().sqrt();
            for x in &mut row {
                *x /= norm;
            }
            out.extend_from_slice(&row);
        }
        out
    };
    let base = draw(rows);
    let queries = draw(queries);
    (base, queries)
}

/// Rows only; see [`corpus_with_queries`].
fn corpus(rows: usize, dim: usize, seed: u64) -> Vec<f32> {
    corpus_with_queries(rows, 0, dim, seed).0
}

fn exact_top(vectors: &[f32], dim: usize, query: &[f32], k: usize) -> Vec<u32> {
    let mut all: Vec<(f32, u32)> = vectors
        .chunks_exact(dim)
        .enumerate()
        .map(|(i, row)| (row.iter().zip(query).map(|(a, b)| a * b).sum(), i as u32))
        .collect();
    all.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    all.truncate(k);
    all.into_iter().map(|(_, i)| i).collect()
}

fn recall(
    index: &HnswIndex,
    vectors: &[f32],
    queries: &[f32],
    dim: usize,
    k: usize,
    beam: usize,
) -> f64 {
    let mut found = 0usize;
    let mut total = 0usize;
    for query in queries.chunks_exact(dim) {
        let truth = exact_top(vectors, dim, query, k);
        let (hits, _) = index
            .search_with_stats(query, k, beam, VectorSearchOptions::new())
            .unwrap();
        found += hits.iter().filter(|(row, _)| truth.contains(row)).count();
        total += truth.len();
    }
    found as f64 / total as f64
}

fn config(mode: BuildMode) -> HnswConfig {
    HnswConfig {
        m: 16,
        ef_construction: 100,
        build_mode: mode,
        seed: 7,
        locality_order: super::config::LocalityOrder::None,
    }
}

fn built(vectors: &[f32], dim: usize, mode: BuildMode) -> HnswIndex {
    let mut index = HnswIndex::new(dim, config(mode)).unwrap();
    index.add(vectors, dim).unwrap();
    index.prepare().unwrap();
    index
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("hnsw-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
/// The exact top `k` of the allowed rows, by the scalar scorer: row and
/// score, best first, ties by row.
fn exact_top_masked(
    vectors: &[f32],
    dim: usize,
    query: &[f32],
    allow: &[bool],
    k: usize,
) -> Vec<(u32, f32)> {
    let mut all: Vec<(u32, f32)> = vectors
        .chunks_exact(dim)
        .enumerate()
        .filter(|(i, _)| allow[*i])
        .map(|(i, row)| (i as u32, crate::exact_vectors::dot(row, query)))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    all.truncate(k);
    all
}

/// The `percent` of rows whose direction is closest to `anchor`: a filter
/// correlated with a region of the space.
fn region_mask(vectors: &[f32], dim: usize, anchor: &[f32], percent: usize) -> Vec<bool> {
    let n = vectors.len() / dim;
    let keep = exact_top(vectors, dim, anchor, n * percent / 100);
    let mut allow = vec![false; n];
    for row in keep {
        allow[row as usize] = true;
    }
    allow
}
/// A built index together with the corpus and queries it was built from.
struct Fixture {
    dim: usize,
    vectors: Vec<f32>,
    queries: Vec<f32>,
    index: HnswIndex,
}

impl Fixture {
    fn new(rows: usize, queries: usize, dim: usize, seed: u64, mode: BuildMode) -> Self {
        Self::with_config(rows, queries, dim, seed, config(mode))
    }

    fn with_config(rows: usize, queries: usize, dim: usize, seed: u64, config: HnswConfig) -> Self {
        let (vectors, queries) = corpus_with_queries(rows, queries, dim, seed);
        let mut index = HnswIndex::new(dim, config).unwrap();
        index.add(&vectors, dim).unwrap();
        index.prepare().unwrap();
        Self {
            dim,
            vectors,
            queries,
            index,
        }
    }

    fn rows(&self) -> usize {
        self.vectors.len() / self.dim
    }
}
