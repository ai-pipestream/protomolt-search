//! Fixtures: rows, held-out queries, filter columns, filter cells and the
//! exact FP32 truth, written once to a directory that every runner (Rust,
//! FAISS, Lucene) reads. Nothing about a fixture is engine specific.
//!
//! Directory layout (all little endian):
//!
//! ```text
//! manifest.json        rows, dim, queries, cells, filters, provenance, digests
//! base.f32             rows x dim
//! queries.f32          queries x dim
//! parents.u64          rows; the row's parent document id
//! bucket.u8            rows; a hash of the row id mod 100 (uncorrelated filters)
//! cell.u16             rows; the row's k-means cell (correlated filters)
//! filters/<name>.json  {"column", "shared"|"per_query": allowed values}
//! truth/none.u32       queries x k_truth_none exact FP32 top rows, unfiltered
//! truth/<name>.u32     queries x k_truth exact FP32 top rows under the filter
//! placements/<placement>-<shards>.u16
//!                      rows; the shard each row lives on in the sharded shape
//! ```

use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::util::{
    arg, dot, list_usize, normalize, par_map, read_f32, read_json, read_u32, write_f32, write_json,
    write_u32, Bitset, Error, TopK,
};

pub const FORMAT: &str = "protomolt-ann-fixture-v2";
pub const PLACEMENTS: [&str; 2] = ["hash", "skewed"];
pub const SELECTIVITIES: [usize; 3] = [50, 10, 1];
pub const SETS: [&str; 3] = ["uncorrelated", "near", "far"];

pub struct Rows {
    pub base: Vec<f32>,
    pub queries: Vec<f32>,
    pub parents: Vec<u64>,
    pub dim: usize,
    pub source: String,
    pub provenance: String,
}

impl Rows {
    pub fn rows(&self) -> usize {
        self.base.len() / self.dim
    }
    pub fn query_count(&self) -> usize {
        self.queries.len() / self.dim
    }
    pub fn row(&self, i: usize) -> &[f32] {
        &self.base[i * self.dim..(i + 1) * self.dim]
    }
    pub fn query(&self, i: usize) -> &[f32] {
        &self.queries[i * self.dim..(i + 1) * self.dim]
    }
}

/// The challenge driver's generator (`examples/opensearch_challenge.rs`).
#[derive(Clone, Copy)]
struct ChallengeLcg(u64);

impl ChallengeLcg {
    fn next(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let unit = ((self.0 >> 40) as u32) as f32 / ((1u32 << 24) - 1) as f32;
        unit * 2.0 - 1.0
    }
}

fn normalize_f64(vector: &mut [f32]) {
    let norm = vector
        .iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt() as f32;
    for v in vector {
        *v /= norm;
    }
}

/// The OpenSearch challenge fixture, regenerated bit for bit: 100,000 unit
/// vectors in 64 dimensions over 16 topics, seed 0xC0DE51A7. Its first 16
/// queries are the challenge's 16 vector workloads; queries past 16 continue
/// the same generator with the same recipe (topic = query mod 16), so they
/// are held out from the corpus and drawn from the same distribution.
pub fn challenge(queries: usize) -> Rows {
    let (count, dim, topics, seed) = (100_000usize, 64usize, 16usize, 0xC0DE_51A7_u64);
    let mut rng = ChallengeLcg(seed);
    let mut base = Vec::with_capacity(count * dim);
    let mut parents = Vec::with_capacity(count);
    for id in 0..count {
        let topic = id % topics;
        let mut v: Vec<f32> = (0..dim).map(|_| rng.next() * 0.075).collect();
        v[topic] += 1.0;
        v[(topic * 7 + 19) % dim] += 0.25;
        normalize_f64(&mut v);
        base.extend_from_slice(&v);
        parents.push((id / 3 + 1) as u64);
    }
    let mut q = Vec::with_capacity(queries * dim);
    for i in 0..queries {
        let topic = i % topics;
        let mut v: Vec<f32> = (0..dim).map(|_| rng.next() * 0.015).collect();
        v[topic] += 1.0;
        v[(topic * 7 + 19) % dim] += 0.25;
        normalize_f64(&mut v);
        q.extend_from_slice(&v);
    }
    Rows {
        base,
        queries: q,
        parents,
        dim,
        source: "challenge".into(),
        provenance: format!(
            "OpenSearch challenge fixture: 100000 x 64 unit vectors, 16 topics, seed 0xC0DE51A7 \
             (examples/opensearch_challenge.rs generate); {queries} queries, the first 16 are the \
             challenge's vector workloads"
        ),
    }
}

/// CourtListener chunk embeddings: rows `0..rows` indexed, the next
/// `queries` rows held out as queries. Each row keeps its opinion id as
/// its parent.
pub fn court(path: &Path, rows: usize, queries: usize) -> Result<Rows, Error> {
    use pipestream_search::demo::court::EmbeddingReader;
    let (dim, reader) = EmbeddingReader::open(path)?;
    let dim = dim as usize;
    let mut base = Vec::with_capacity(rows * dim);
    let mut held_out = Vec::with_capacity(queries * dim);
    let mut parents = Vec::with_capacity(rows);
    for (row, record) in reader.take(rows + queries).enumerate() {
        let record = record?;
        if row < rows {
            base.extend_from_slice(&record.vector);
            parents.push(record.opinion_id);
        } else {
            held_out.extend_from_slice(&record.vector);
        }
    }
    if base.len() != rows * dim || held_out.len() != queries * dim {
        return Err(format!(
            "{} ended before {rows} rows plus {queries} queries",
            path.display()
        )
        .into());
    }
    Ok(Rows {
        base,
        queries: held_out,
        parents,
        dim,
        source: "court".into(),
        provenance: format!(
            "CourtListener chunk embeddings {} (dim {dim}): rows 0..{rows}, queries {rows}..{}",
            path.display(),
            rows + queries
        ),
    })
}

/// Topic-shaped unit vectors from a fixed generator, for smoke runs.
pub fn synthetic(rows: usize, dim: usize, queries: usize) -> Rows {
    let mut state = 0x5EED_A11E_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((state >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let topics: Vec<Vec<f32>> = (0..64)
        .map(|_| {
            let mut t: Vec<f32> = (0..dim).map(|_| next()).collect();
            normalize(&mut t);
            t
        })
        .collect();
    let mut draw = |n: usize| {
        let mut out = Vec::with_capacity(n * dim);
        for _ in 0..n {
            let topic = &topics[((next() + 1.0) * 32.0) as usize % topics.len()];
            let mut row: Vec<f32> = topic.iter().map(|t| t + 0.35 * next()).collect();
            normalize(&mut row);
            out.extend_from_slice(&row);
        }
        out
    };
    let base = draw(rows);
    let q = draw(queries);
    Rows {
        base,
        queries: q,
        parents: (0..rows as u64).map(|r| r / 4).collect(),
        dim,
        source: "synthetic".into(),
        provenance: format!("synthetic {rows}x{dim}, 64 topics, seed 0x5EEDA11E"),
    }
}

/// splitmix64 of the row id: a hash uncorrelated with the vectors.
pub fn row_hash(row: u64) -> u64 {
    let mut h = row.wrapping_add(0x9E37_79B9_7F4A_7C15);
    h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
}

/// The row hash mod 100: a column uncorrelated with the vectors.
pub fn bucket(row: u64) -> u8 {
    (row_hash(row) % 100) as u8
}

/// Hash placement: the row hash mod the shard count, so every shard holds a
/// uniform sample of the rows and a query's true neighbors spread evenly.
/// The bucket column is not reused because 100 buckets do not divide evenly
/// over every shard count (16 shards would get 7 or 6 buckets).
pub fn hash_placement(rows: usize, shards: usize) -> Vec<u16> {
    (0..rows as u64)
        .map(|r| (row_hash(r) % shards as u64) as u16)
        .collect()
}

/// Skewed placement: whole k-means cells per shard. Cells are taken in cell
/// id order and a shard is closed once it holds its share of the rows not
/// yet placed (those rows over the shards still open), so shard sizes stay
/// close while every cell, and with it most of a query's true neighbors,
/// lives on one shard. Refuses a split that leaves a shard empty (fewer
/// populated cells than shards).
pub fn skewed_placement(cell: &[u16], cells: usize, shards: usize) -> Result<Vec<u16>, Error> {
    let mut sizes = vec![0usize; cells];
    for c in cell {
        sizes[*c as usize] += 1;
    }
    let mut target = cell.len().div_ceil(shards);
    let mut shard_of_cell = vec![0u16; cells];
    let mut shard = 0usize;
    let mut held = vec![0usize; shards];
    let mut placed = 0usize;
    for (c, size) in sizes.iter().enumerate() {
        shard_of_cell[c] = shard as u16;
        held[shard] += size;
        placed += size;
        if held[shard] >= target && shard + 1 < shards {
            shard += 1;
            // The rows still to place, spread over the shards still open,
            // so the last shard is not left with a remainder.
            target = (cell.len() - placed).div_ceil(shards - shard);
        }
    }
    if let Some(empty) = held.iter().position(|h| *h == 0) {
        return Err(format!(
            "skewed placement over {shards} shards leaves shard {empty} empty ({cells} cells)"
        )
        .into());
    }
    Ok(cell.iter().map(|c| shard_of_cell[*c as usize]).collect())
}

/// The placement file of a shard count, as `ann_bench fixture` writes it.
pub fn placement_name(placement: &str, shards: usize) -> String {
    format!("{placement}-{shards}")
}

struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Spherical k-means: Lloyd iterations on a sample, then every row assigned
/// to its most similar centroid. Returns (centroids, cell per row).
pub fn kmeans(
    rows: &Rows,
    cells: usize,
    iterations: usize,
    threads: usize,
) -> (Vec<f32>, Vec<u16>) {
    let n = rows.rows();
    let dim = rows.dim;
    let mut rng = Rng(0xCE11_5EED);
    let sample_n = n.min(cells * 256).max(cells);
    let sample: Vec<usize> = if sample_n >= n {
        (0..n).collect()
    } else {
        let mut picked: Vec<usize> = (0..sample_n).map(|_| rng.below(n)).collect();
        picked.sort_unstable();
        picked.dedup();
        picked
    };
    let mut centroids = Vec::with_capacity(cells * dim);
    for c in 0..cells {
        let r = sample[(c * sample.len()) / cells];
        centroids.extend_from_slice(rows.row(r));
    }
    let assign = |centroids: &[f32], row: &[f32]| -> u16 {
        let mut best = (0usize, f32::NEG_INFINITY);
        for c in 0..cells {
            let s = dot(row, &centroids[c * dim..(c + 1) * dim]);
            if s > best.1 {
                best = (c, s);
            }
        }
        best.0 as u16
    };
    for _ in 0..iterations {
        let labels = par_map(sample.len(), threads, |i| {
            assign(&centroids, rows.row(sample[i]))
        });
        let mut sums = vec![0f64; cells * dim];
        let mut counts = vec![0usize; cells];
        for (i, &label) in labels.iter().enumerate() {
            let c = label as usize;
            counts[c] += 1;
            for (s, v) in sums[c * dim..(c + 1) * dim]
                .iter_mut()
                .zip(rows.row(sample[i]))
            {
                *s += f64::from(*v);
            }
        }
        for c in 0..cells {
            if counts[c] == 0 {
                // Reseed an empty cell from a random sample row.
                let r = sample[rng.below(sample.len())];
                centroids[c * dim..(c + 1) * dim].copy_from_slice(rows.row(r));
                continue;
            }
            let mut v: Vec<f32> = sums[c * dim..(c + 1) * dim]
                .iter()
                .map(|s| *s as f32)
                .collect();
            normalize(&mut v);
            centroids[c * dim..(c + 1) * dim].copy_from_slice(&v);
        }
    }
    let labels = par_map(n.div_ceil(4096), threads, |chunk| {
        let start = chunk * 4096;
        let end = (start + 4096).min(n);
        (start..end)
            .map(|r| assign(&centroids, rows.row(r)))
            .collect::<Vec<_>>()
    })
    .into_iter()
    .flatten()
    .collect();
    (centroids, labels)
}

/// Which column a filter reads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Column {
    Bucket,
    Cell,
}

impl Column {
    pub fn name(self) -> &'static str {
        match self {
            Column::Bucket => "bucket",
            Column::Cell => "cell",
        }
    }
}

/// One filter cell: a column and, per query, the allowed values. The CEL
/// predicate it stands for is `column in [values]`.
pub struct Filter {
    pub name: String,
    pub set: String,
    pub target_percent: usize,
    pub column: Column,
    /// Shared by every query (uncorrelated) or one list per query.
    pub per_query: Vec<Vec<u16>>,
    pub shared: bool,
}

impl Filter {
    pub fn values(&self, query: usize) -> &[u16] {
        if self.shared {
            &self.per_query[0]
        } else {
            &self.per_query[query]
        }
    }
}

/// Column postings: value -> ascending rows. This is the column index the
/// product's filter path builds its allow bitset from (#55 prerequisite),
/// so building the mask costs allowed rows plus n/64 words, not n rows.
pub struct ColumnIndex {
    pub rows: usize,
    pub bucket: Vec<Vec<u32>>,
    pub cell: Vec<Vec<u32>>,
}

impl ColumnIndex {
    pub fn new(bucket_col: &[u8], cell_col: &[u16], cells: usize) -> Self {
        let mut bucket = vec![Vec::new(); 100];
        let mut cell = vec![Vec::new(); cells];
        for (r, b) in bucket_col.iter().enumerate() {
            bucket[*b as usize].push(r as u32);
        }
        for (r, c) in cell_col.iter().enumerate() {
            cell[*c as usize].push(r as u32);
        }
        Self {
            rows: bucket_col.len(),
            bucket,
            cell,
        }
    }

    /// The allow bitset of `column in values`.
    pub fn mask(&self, column: Column, values: &[u16]) -> Bitset {
        let postings = match column {
            Column::Bucket => &self.bucket,
            Column::Cell => &self.cell,
        };
        let mut bits = Bitset::new(self.rows);
        for v in values {
            for r in &postings[*v as usize] {
                bits.set(*r as usize);
            }
        }
        bits
    }
}

/// Exact FP32 top-`k` rows of `query` among the allowed rows.
pub fn exact_top(rows: &Rows, query: &[f32], k: usize, allow: Option<&Bitset>) -> Vec<(u32, f32)> {
    let mut top = TopK::new(k);
    match allow {
        None => {
            for r in 0..rows.rows() {
                let s = dot(query, rows.row(r));
                if s > top.threshold() {
                    top.push(r as u32, s);
                }
            }
        }
        Some(bits) => {
            for r in bits.ones_in(0, rows.rows()) {
                let s = dot(query, rows.row(r));
                if s > top.threshold() {
                    top.push(r as u32, s);
                }
            }
        }
    }
    top.into_sorted()
}

/// A fully built fixture in memory.
pub struct Fixture {
    pub dir: PathBuf,
    pub manifest: Value,
    pub rows: Rows,
    /// Truth depth of every filter cell.
    pub k_truth: usize,
    /// Truth depth unfiltered, deep enough for the sharded shape's large k.
    pub k_truth_none: usize,
    /// "none" first, then every filter cell.
    pub filters: Vec<Filter>,
    /// Truth per entry of `filter_names()`: queries x `truth_depth(filter)`.
    pub truth: Vec<Vec<u32>>,
    pub index: ColumnIndex,
}

impl Fixture {
    pub fn filter_names(&self) -> Vec<String> {
        std::iter::once("none".to_string())
            .chain(self.filters.iter().map(|f| f.name.clone()))
            .collect()
    }

    /// How many exact top rows the truth holds per query for a filter
    /// (index 0 is unfiltered). A recall at a larger k cannot be judged.
    pub fn truth_depth(&self, filter: usize) -> usize {
        if filter == 0 {
            self.k_truth_none
        } else {
            self.k_truth
        }
    }

    pub fn truth_for(&self, filter: usize, query: usize) -> &[u32] {
        let depth = self.truth_depth(filter);
        &self.truth[filter][query * depth..(query + 1) * depth]
    }

    pub fn name(&self) -> String {
        self.manifest["name"].as_str().unwrap_or("").to_string()
    }

    /// The shard of every row under a placement the fixture wrote. Refuses
    /// a placement or shard count the fixture was not built with.
    pub fn placement(&self, placement: &str, shards: usize) -> Result<Vec<u16>, Error> {
        let name = placement_name(placement, shards);
        let listed = self.manifest["placements"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|p| p["name"] == name.as_str());
        if !listed {
            return Err(format!(
                "{}: no placement {name}; rebuild the fixture with --shard-counts listing {shards}",
                self.dir.display()
            )
            .into());
        }
        let bytes = std::fs::read(self.dir.join(format!("placements/{name}.u16")))?;
        let shard: Vec<u16> = bytes
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        if shard.len() != self.rows.rows() || shard.iter().any(|s| *s as usize >= shards) {
            return Err(format!(
                "{}: placements/{name}.u16 does not hold one shard below {shards} per row",
                self.dir.display()
            )
            .into());
        }
        Ok(shard)
    }
}

/// `ann_bench fixture`: build and write a fixture directory.
pub fn build_command() -> Result<(), Error> {
    let source = arg("source", "challenge");
    let queries: usize = arg("queries", "1000").parse()?;
    let out = PathBuf::from(arg("out", ""));
    if out.as_os_str().is_empty() {
        return Err("--out=DIR is required".into());
    }
    let threads = crate::util::host_threads();
    let rows = match source.as_str() {
        "challenge" => challenge(queries),
        "court" => court(
            Path::new(&arg("input", "/work/court-corpus/embeddings-full.bin")),
            arg("vectors", "1000000").parse()?,
            queries,
        )?,
        s if s.starts_with("synthetic") => {
            let spec = s.strip_prefix("synthetic:").unwrap_or("50000:64");
            let (r, d) = spec.split_once(':').ok_or("--source=synthetic:ROWS:DIM")?;
            synthetic(r.parse()?, d.parse()?, queries)
        }
        other => return Err(format!("unknown --source {other:?}").into()),
    };
    let n = rows.rows();
    let default_cells = if n >= 500_000 { 400 } else { 200 };
    let cells: usize = arg("cells", &default_cells.to_string()).parse()?;
    let k_truth: usize = arg("k-truth", "100").parse()?;
    let k_truth_none: usize = arg("k-truth-none", "10000").parse()?;
    if k_truth == 0 || k_truth_none < k_truth {
        return Err(format!(
            "--k-truth={k_truth} must be at least 1 and --k-truth-none={k_truth_none} at least --k-truth"
        )
        .into());
    }
    if k_truth_none > n {
        return Err(format!("--k-truth-none={k_truth_none} exceeds the fixture's {n} rows").into());
    }
    let shard_counts = list_usize("shard-counts", "4,16")?;
    if let Some(bad) = shard_counts
        .iter()
        .find(|s| **s < 2 || **s > u16::MAX as usize)
    {
        return Err(format!("--shard-counts: {bad} is not a shard count of at least 2").into());
    }
    let name = arg("name", &format!("{}-{}x{}", rows.source, n, rows.dim));
    eprintln!(
        "fixture: {} ({n} rows, {} queries)",
        rows.provenance,
        rows.query_count()
    );

    let started = std::time::Instant::now();
    let (centroids, cell) = kmeans(&rows, cells, 12, threads);
    eprintln!(
        "fixture: k-means {cells} cells in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let bucket: Vec<u8> = (0..n as u64).map(bucket).collect();
    let index = ColumnIndex::new(&bucket, &cell, cells);

    // Unfiltered truth first: correlated filters are defined from it.
    let qn = rows.query_count();
    let unfiltered: Vec<Vec<(u32, f32)>> = par_map(qn, threads, |q| {
        exact_top(&rows, rows.query(q), k_truth_none.max(100), None)
    });

    let mut filters = Vec::new();
    for set in SETS {
        for pct in SELECTIVITIES {
            let want = (n * pct).div_ceil(100);
            let (column, per_query, shared) = match set {
                "uncorrelated" => (
                    Column::Bucket,
                    vec![(0..pct as u16).collect::<Vec<_>>()],
                    true,
                ),
                "near" => {
                    let lists = (0..qn)
                        .map(|q| {
                            let query = rows.query(q);
                            let mut votes = vec![0usize; cells];
                            for (r, _) in unfiltered[q].iter().take(10) {
                                votes[cell[*r as usize] as usize] += 1;
                            }
                            let home = (0..cells)
                                .max_by_key(|c| (votes[*c], usize::MAX - c))
                                .unwrap();
                            let mut order: Vec<(usize, f32)> = (0..cells)
                                .filter(|c| *c != home)
                                .map(|c| {
                                    (c, dot(query, &centroids[c * rows.dim..(c + 1) * rows.dim]))
                                })
                                .collect();
                            order.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                            let mut chosen = vec![home as u16];
                            let mut have = index.cell[home].len();
                            for (c, _) in order {
                                if have >= want {
                                    break;
                                }
                                chosen.push(c as u16);
                                have += index.cell[c].len();
                            }
                            chosen.sort_unstable();
                            chosen
                        })
                        .collect();
                    (Column::Cell, lists, false)
                }
                "far" => {
                    let lists = (0..qn)
                        .map(|q| {
                            let mut banned = vec![false; cells];
                            for (r, _) in unfiltered[q].iter().take(100) {
                                banned[cell[*r as usize] as usize] = true;
                            }
                            let mut order: Vec<usize> =
                                (0..cells).filter(|c| !banned[*c]).collect();
                            let mut rng = Rng(0xFA12_0000 ^ ((q as u64) << 8) ^ pct as u64);
                            for i in (1..order.len()).rev() {
                                order.swap(i, rng.below(i + 1));
                            }
                            let mut chosen = Vec::new();
                            let mut have = 0;
                            for c in order {
                                if have >= want {
                                    break;
                                }
                                chosen.push(c as u16);
                                have += index.cell[c].len();
                            }
                            chosen.sort_unstable();
                            chosen
                        })
                        .collect();
                    (Column::Cell, lists, false)
                }
                _ => unreachable!(),
            };
            filters.push(Filter {
                name: format!("{set}-{pct}"),
                set: set.to_string(),
                target_percent: pct,
                column,
                per_query,
                shared,
            });
        }
    }

    std::fs::create_dir_all(out.join("filters"))?;
    std::fs::create_dir_all(out.join("truth"))?;
    let mut truth_none = Vec::with_capacity(qn * k_truth_none);
    for hits in &unfiltered {
        truth_none.extend(hits.iter().take(k_truth_none).map(|(r, _)| *r));
    }
    write_u32(&out.join("truth/none.u32"), &truth_none)?;

    let mut filter_meta = Vec::new();
    for f in &filters {
        let started = std::time::Instant::now();
        let per: Vec<(Vec<u32>, f64)> = par_map(qn, threads, |q| {
            let bits = index.mask(f.column, f.values(q));
            let actual = bits.count() as f64 / n as f64;
            let top = exact_top(&rows, rows.query(q), k_truth, Some(&bits));
            let mut ids: Vec<u32> = top.iter().map(|(r, _)| *r).collect();
            // A filter smaller than k_truth pads with u32::MAX (never matches).
            ids.resize(k_truth, u32::MAX);
            (ids, actual)
        });
        let mean_actual = per.iter().map(|p| p.1).sum::<f64>() / qn as f64 * 100.0;
        let truth: Vec<u32> = per.into_iter().flat_map(|p| p.0).collect();
        write_u32(&out.join(format!("truth/{}.u32", f.name)), &truth)?;
        let body = if f.shared {
            json!({ "column": f.column.name(), "shared": f.per_query[0] })
        } else {
            json!({ "column": f.column.name(), "per_query": f.per_query })
        };
        write_json(&out.join(format!("filters/{}.json", f.name)), &body)?;
        filter_meta.push(json!({
            "name": f.name, "set": f.set, "target_percent": f.target_percent,
            "actual_mean_percent": mean_actual, "column": f.column.name(),
        }));
        eprintln!(
            "fixture: {} truth in {:.1}s (allows {mean_actual:.2}% on average)",
            f.name,
            started.elapsed().as_secs_f64()
        );
    }

    write_f32(&out.join("base.f32"), &rows.base)?;
    write_f32(&out.join("queries.f32"), &rows.queries)?;
    let parent_bytes: Vec<u8> = rows.parents.iter().flat_map(|p| p.to_le_bytes()).collect();
    std::fs::write(out.join("parents.u64"), parent_bytes)?;
    std::fs::write(out.join("bucket.u8"), &bucket)?;
    let cell_bytes: Vec<u8> = cell.iter().flat_map(|c| c.to_le_bytes()).collect();
    std::fs::write(out.join("cell.u16"), cell_bytes)?;
    std::fs::create_dir_all(out.join("placements"))?;
    let mut placements = Vec::new();
    for &shards in &shard_counts {
        for placement in PLACEMENTS {
            let shard = match placement {
                "hash" => hash_placement(n, shards),
                _ => skewed_placement(&cell, cells, shards)?,
            };
            let name = placement_name(placement, shards);
            let mut rows_per_shard = vec![0usize; shards];
            for s in &shard {
                rows_per_shard[*s as usize] += 1;
            }
            let bytes: Vec<u8> = shard.iter().flat_map(|s| s.to_le_bytes()).collect();
            std::fs::write(out.join(format!("placements/{name}.u16")), bytes)?;
            placements.push(json!({
                "name": name, "placement": placement, "shards": shards,
                "rows_per_shard": rows_per_shard,
            }));
        }
    }
    let base_bytes: Vec<u8> = rows.base.iter().flat_map(|v| v.to_le_bytes()).collect();
    let manifest = json!({
        "format": FORMAT,
        "name": name,
        "source": rows.source,
        "provenance": rows.provenance,
        "rows": n,
        "dim": rows.dim,
        "queries": qn,
        "metric": "inner product over unit vectors (equivalently L2)",
        "cells": cells,
        "k_truth": k_truth,
        "k_truth_none": k_truth_none,
        "base_sha256": pipestream_search::sha256::hex_digest(&base_bytes),
        "filters": filter_meta,
        "masks": "uncorrelated: bucket in 0..p (bucket = splitmix64(row) mod 100). \
                  near: the k-means cell holding most of the query's unfiltered top-10, then cells by \
                  centroid similarity to the query until p% of rows. far: cells holding none of the \
                  query's unfiltered top-100, in a seeded random order, until p% of rows.",
        "placements": placements,
        "placement_rules": "hash: splitmix64(row) mod shards. skewed: whole k-means cells in cell id \
                  order, a shard closed once it holds its share of the rows not yet placed.",
    });
    write_json(&out.join("manifest.json"), &manifest)?;
    eprintln!("fixture: wrote {}", out.display());
    Ok(())
}

/// Load a fixture directory written by `build_command`.
pub fn load(dir: &Path) -> Result<Fixture, Error> {
    let manifest = read_json(&dir.join("manifest.json"))?;
    if manifest["format"] != FORMAT {
        return Err(format!(
            "{}: not a {FORMAT} fixture (format {}); rebuild it with ann_bench fixture",
            dir.display(),
            manifest["format"]
        )
        .into());
    }
    let dim = manifest["dim"].as_u64().ok_or("manifest dim")? as usize;
    let k_truth = manifest["k_truth"].as_u64().ok_or("manifest k_truth")? as usize;
    let k_truth_none = manifest["k_truth_none"]
        .as_u64()
        .ok_or("manifest k_truth_none")? as usize;
    let cells = manifest["cells"].as_u64().ok_or("manifest cells")? as usize;
    let base = read_f32(&dir.join("base.f32"))?;
    let queries = read_f32(&dir.join("queries.f32"))?;
    let parents: Vec<u64> = std::fs::read(dir.join("parents.u64"))?
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| u64::from_le_bytes(*c))
        .collect();
    let bucket = std::fs::read(dir.join("bucket.u8"))?;
    let cell: Vec<u16> = std::fs::read(dir.join("cell.u16"))?
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| u16::from_le_bytes(*c))
        .collect();
    let rows = Rows {
        base,
        queries,
        parents,
        dim,
        source: manifest["source"].as_str().unwrap_or("").to_string(),
        provenance: manifest["provenance"].as_str().unwrap_or("").to_string(),
    };
    let mut filters = Vec::new();
    let mut truth = vec![read_u32(&dir.join("truth/none.u32"))?];
    for meta in manifest["filters"].as_array().ok_or("manifest filters")? {
        let name = meta["name"].as_str().ok_or("filter name")?.to_string();
        let body = read_json(&dir.join(format!("filters/{name}.json")))?;
        let column = match body["column"].as_str() {
            Some("bucket") => Column::Bucket,
            Some("cell") => Column::Cell,
            other => return Err(format!("filter {name}: column {other:?}").into()),
        };
        let to_list = |v: &Value| -> Vec<u16> {
            v.as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_u64())
                        .map(|x| x as u16)
                        .collect()
                })
                .unwrap_or_default()
        };
        let (per_query, shared) = if let Some(s) = body.get("shared") {
            (vec![to_list(s)], true)
        } else {
            (
                body["per_query"]
                    .as_array()
                    .ok_or("per_query")?
                    .iter()
                    .map(to_list)
                    .collect(),
                false,
            )
        };
        filters.push(Filter {
            name: name.clone(),
            set: meta["set"].as_str().unwrap_or("").to_string(),
            target_percent: meta["target_percent"].as_u64().unwrap_or(0) as usize,
            column,
            per_query,
            shared,
        });
        truth.push(read_u32(&dir.join(format!("truth/{name}.u32")))?);
    }
    let index = ColumnIndex::new(&bucket, &cell, cells);
    let fixture = Fixture {
        dir: dir.to_path_buf(),
        manifest,
        rows,
        k_truth,
        k_truth_none,
        filters,
        truth,
        index,
    };
    let queries = fixture.rows.query_count();
    for (filter, name) in fixture.filter_names().iter().enumerate() {
        let want = queries * fixture.truth_depth(filter);
        if fixture.truth[filter].len() != want {
            return Err(format!(
                "{}: truth/{name}.u32 holds {} rows, the manifest implies {want}",
                dir.display(),
                fixture.truth[filter].len()
            )
            .into());
        }
    }
    Ok(fixture)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_placement_is_fixed_and_fills_every_shard() {
        let shard = hash_placement(10_000, 16);
        assert_eq!(shard, hash_placement(10_000, 16));
        let mut sizes = [0usize; 16];
        for s in &shard {
            sizes[*s as usize] += 1;
        }
        // A uniform hash puts 625 rows on each shard; allow a wide band.
        assert!(sizes.iter().all(|s| (500..750).contains(s)), "{sizes:?}");
    }

    #[test]
    fn skewed_placement_keeps_whole_cells_and_closes_shards_by_size() {
        // Six cells of 10 rows, rows interleaved by cell.
        let cell: Vec<u16> = (0..60u16).map(|r| r % 6).collect();
        let shard = skewed_placement(&cell, 6, 3).unwrap();
        for (row, s) in shard.iter().enumerate() {
            let c = cell[row];
            assert_eq!(*s, shard[c as usize], "cell {c} split across shards");
        }
        // Target 20 rows per shard: cells 0-1, 2-3, 4-5.
        assert_eq!(&shard[..6], &[0, 0, 1, 1, 2, 2]);
        // Two cells cannot fill three shards.
        let two: Vec<u16> = (0..20u16).map(|r| r % 2).collect();
        assert!(skewed_placement(&two, 2, 3).is_err());
    }
}
