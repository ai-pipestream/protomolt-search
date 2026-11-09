//! In-process engines behind one seam. Every engine indexes a contiguous
//! run of fixture rows (a whole fixture, or one segment of it) and returns
//! `(local row, similarity)` best first, where similarity is the inner
//! product (or a monotone function of the engine's own distance on unit
//! vectors, mapped back to the inner product) so segment results merge.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};

use crate::util::{dot, Bitset, Error, TopK};

/// One query's allow mask, built inside the timed region. `bools` exists
/// only for engines that take a `&[bool]` (the product's VectorIndex seam).
pub struct Allow {
    pub bits: Bitset,
    pub bools: Option<Vec<bool>>,
}

#[derive(Default, Clone, Copy)]
pub struct QueryStats {
    pub hops: u64,
    pub exact: u64,
}

/// Build parameters of one sweep point.
#[derive(Clone, Debug)]
pub struct BuildParams {
    pub m: usize,
    pub ef_construction: usize,
    /// Code bits for the exhaustive engine; 0 otherwise.
    pub bits: usize,
}

impl BuildParams {
    pub fn to_json(&self) -> Value {
        json!({ "m": self.m, "ef_construction": self.ef_construction, "bits": self.bits })
    }
}

/// Contiguous rows an index covers.
#[derive(Clone)]
pub struct Rows {
    pub base: Arc<Vec<f32>>,
    pub dim: usize,
    pub offset: usize,
    pub len: usize,
}

impl Rows {
    #[inline]
    pub fn row(&self, local: usize) -> &[f32] {
        let at = (self.offset + local) * self.dim;
        &self.base[at..at + self.dim]
    }
    pub fn slice(&self) -> &[f32] {
        &self.base[self.offset * self.dim..(self.offset + self.len) * self.dim]
    }
}

pub trait Index: Send + Sync {
    /// Up to `k` `(local row, inner product)` best first. `ef` is the
    /// engine's query-time breadth: beam, efSearch, expansion, or rerank
    /// depth for the exhaustive path.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        allow: Option<&Allow>,
        stats: &mut QueryStats,
    ) -> Vec<(u32, f32)>;

    /// Engines whose breadth is index state rather than a query argument.
    fn set_ef(&mut self, _ef: usize) {}

    /// Serialized size when the engine can serialize.
    fn disk_bytes(&self) -> Option<u64> {
        None
    }
}

pub struct EngineInfo {
    pub name: &'static str,
    pub library: &'static str,
    pub version: &'static str,
    pub license: &'static str,
    pub filter: &'static str,
    pub wants_bools: bool,
    pub supports_filter: bool,
    /// Whether `ef` is meaningful (false: one point per cell).
    pub sweeps_ef: bool,
}

pub const ENGINES: &[&str] = &[
    "enumerate",
    "exhaustive-tq",
    "hnsw",
    "hnsw_rs",
    "rust-cv-hnsw",
    "corenn-hnswlib",
    "usearch",
];

pub fn info(name: &str) -> Result<EngineInfo, Error> {
    Ok(match name {
        "enumerate" => EngineInfo {
            name: "enumerate",
            library: "ours: exact FP32 scan over the allow mask's set bits",
            version: env!("CARGO_PKG_VERSION"),
            license: "ours",
            filter: "allow bitset, set bits iterated",
            wants_bools: false,
            supports_filter: true,
            sweeps_ef: false,
        },
        "exhaustive-tq" => EngineInfo {
            name: "exhaustive-tq",
            library: "ours: embedded TurboVec (TurboQuant) scan + FP32 rerank of the top ef",
            version: env!("CARGO_PKG_VERSION"),
            license: "ours",
            filter: "Vec<bool> mask (VectorSearchOptions::with_mask)",
            wants_bools: true,
            supports_filter: true,
            sweeps_ef: true,
        },
        "hnsw" => EngineInfo {
            name: "hnsw",
            library: "ours: src/hnsw/ (HNSW over FP32 rows, parallel build)",
            version: env!("CARGO_PKG_VERSION"),
            license: "ours",
            filter: "Vec<bool> mask (VectorSearchOptions::with_mask)",
            wants_bools: true,
            supports_filter: true,
            sweeps_ef: true,
        },
        "hnsw_rs" => EngineInfo {
            name: "hnsw_rs",
            library: "hnsw_rs (jean-pierreBoth), DistL2, simdeez_f",
            version: "0.3.5",
            license: "MIT OR Apache-2.0",
            filter: "FilterT closure over the allow bitset",
            wants_bools: false,
            supports_filter: true,
            sweeps_ef: true,
        },
        "rust-cv-hnsw" => EngineInfo {
            name: "rust-cv-hnsw",
            library: "hnsw (rust-cv), L2 as ordered u32 bits",
            version: "0.11.0",
            license: "MIT",
            filter: "none: the library has no predicate filter, filtered cells are skipped",
            wants_bools: false,
            supports_filter: false,
            sweeps_ef: true,
        },
        "corenn-hnswlib" => EngineInfo {
            name: "corenn-hnswlib",
            library: "hnswlib-rs (CoreNN), InnerProduct, borrowed-slice VectorStore",
            version: "0.10.0",
            license: "Apache-2.0",
            filter: "Fn(&key) closure over the allow bitset",
            wants_bools: false,
            supports_filter: true,
            sweeps_ef: true,
        },
        "usearch" => EngineInfo {
            name: "usearch",
            library: "usearch Rust bindings, MetricKind::IP, f32",
            version: "2.26.6",
            license: "Apache-2.0",
            filter: "filtered_search closure over the allow bitset",
            wants_bools: false,
            supports_filter: true,
            sweeps_ef: true,
        },
        other => return Err(format!("unknown engine {other:?}; known: {ENGINES:?}").into()),
    })
}

/// The build sweep for an engine.
pub fn build_sweep(name: &str, ms: &[usize], efcs: &[usize], bits: &[usize]) -> Vec<BuildParams> {
    match name {
        "enumerate" => vec![BuildParams {
            m: 0,
            ef_construction: 0,
            bits: 0,
        }],
        "exhaustive-tq" => bits
            .iter()
            .map(|b| BuildParams {
                m: 0,
                ef_construction: 0,
                bits: *b,
            })
            .collect(),
        _ => {
            let mut out = Vec::new();
            for &m in ms {
                for &efc in efcs {
                    out.push(BuildParams {
                        m,
                        ef_construction: efc,
                        bits: 0,
                    });
                }
            }
            out
        }
    }
}

pub fn build(
    name: &str,
    rows: Rows,
    p: &BuildParams,
    threads: usize,
) -> Result<Box<dyn Index>, Error> {
    Ok(match name {
        "enumerate" => Box::new(Enumerate { rows }),
        "exhaustive-tq" => Box::new(ExhaustiveTq::build(rows, p.bits)?),
        "hnsw" => Box::new(Hnsw::build(rows, p)?),
        "hnsw_rs" => Box::new(HnswRs::build(rows, p)),
        "rust-cv-hnsw" => rust_cv::build(rows, p)?,
        "corenn-hnswlib" => Box::new(CoreNn::build(rows, p, threads)?),
        "usearch" => Box::new(USearch::build(rows, p, threads)?),
        other => return Err(format!("unknown engine {other:?}").into()),
    })
}

// ---------------------------------------------------------------- enumerate

/// The exact fallback (#55 "Enumerate"): score every allowed row from FP32.
/// Unfiltered, it is the flat scan.
pub struct Enumerate {
    rows: Rows,
}

impl Index for Enumerate {
    fn search(
        &self,
        q: &[f32],
        k: usize,
        _ef: usize,
        allow: Option<&Allow>,
        stats: &mut QueryStats,
    ) -> Vec<(u32, f32)> {
        let mut top = TopK::new(k);
        let mut scored = 0u64;
        match allow {
            None => {
                for r in 0..self.rows.len {
                    let s = dot(q, self.rows.row(r));
                    if s > top.threshold() {
                        top.push(r as u32, s);
                    }
                }
                scored = self.rows.len as u64;
            }
            Some(a) => {
                let start = self.rows.offset;
                for g in a.bits.ones_in(start, start + self.rows.len) {
                    let r = g - start;
                    let s = dot(q, self.rows.row(r));
                    scored += 1;
                    if s > top.threshold() {
                        top.push(r as u32, s);
                    }
                }
            }
        }
        stats.exact += scored;
        top.into_sorted()
    }

    fn disk_bytes(&self) -> Option<u64> {
        Some((self.rows.len * self.rows.dim * 4) as u64)
    }
}

// ------------------------------------------------------------ exhaustive-tq

/// The product's default path: embedded TurboVec over every row, then an
/// FP32 rerank of the top `ef` from the exact copy (the `.exact` sidecar in
/// the product; the fixture's rows here). The rerank is inside the timing.
pub struct ExhaustiveTq {
    rows: Rows,
    index: pipestream_search::vector::VectorIndex,
    disk: Option<u64>,
}

impl ExhaustiveTq {
    fn build(rows: Rows, bits: usize) -> Result<Self, Error> {
        use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};
        let sample = rows.len.min(8192) * rows.dim;
        let config = VectorIndex::fit_backend_config(
            EMBEDDED_TURBOVEC,
            rows.dim,
            bits,
            &rows.slice()[..sample],
        )?;
        let mut index = VectorIndex::from_backend_config(rows.dim, &config)?;
        index.add(rows.slice(), rows.dim)?;
        index.prepare()?;
        let disk = disk_size(|p| index.write(p).map_err(Into::into));
        Ok(Self { rows, index, disk })
    }
}

impl Index for ExhaustiveTq {
    fn search(
        &self,
        q: &[f32],
        k: usize,
        ef: usize,
        allow: Option<&Allow>,
        stats: &mut QueryStats,
    ) -> Vec<(u32, f32)> {
        use pipestream_search::vector::VectorSearchOptions;
        let depth = ef.max(k).min(self.rows.len);
        let options = match allow.and_then(|a| a.bools.as_deref()) {
            Some(mask) => VectorSearchOptions::new()
                .with_mask(&mask[self.rows.offset..self.rows.offset + self.rows.len]),
            None => VectorSearchOptions::new(),
        };
        let result = self
            .index
            .try_search(q, depth, options)
            .expect("turbovec search");
        let mut top = TopK::new(k);
        for slot in result.slots_for_query(0) {
            let Ok(r) = usize::try_from(*slot) else {
                continue;
            };
            let s = dot(q, self.rows.row(r));
            stats.exact += 1;
            top.push(r as u32, s);
        }
        top.into_sorted()
    }

    fn disk_bytes(&self) -> Option<u64> {
        self.disk
    }
}

fn disk_size(write: impl FnOnce(&std::path::Path) -> Result<(), Error>) -> Option<u64> {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "ann-bench-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).ok()?;
    let path = dir.join("image");
    let size = write(&path).ok().and_then(|_| dir_size(&dir));
    let _ = std::fs::remove_dir_all(&dir);
    size
}

fn dir_size(dir: &std::path::Path) -> Option<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).ok()? {
        let entry = entry.ok()?;
        let meta = entry.metadata().ok()?;
        total += if meta.is_dir() {
            dir_size(&entry.path())?
        } else {
            meta.len()
        };
    }
    Some(total)
}

// --------------------------------------------------------------------- hnsw

/// The product's graph provider (`src/hnsw/`), built in parallel mode. The
/// query beam is the requested depth, so asking for `k` at beam `ef` is an
/// HNSW search at `ef`. Scores are exact FP32 inner products.
pub struct Hnsw {
    rows: Rows,
    index: pipestream_search::hnsw::HnswIndex,
    disk: Option<u64>,
}

impl Hnsw {
    fn build(rows: Rows, p: &BuildParams) -> Result<Self, Error> {
        use pipestream_search::hnsw::{BuildMode, HnswConfig, HnswIndex};
        use pipestream_search::vector::VectorProvider;
        let config = HnswConfig {
            m: p.m as u32,
            ef_construction: p.ef_construction as u32,
            build_mode: BuildMode::Parallel,
            seed: 0x5EED_0A11,
        };
        let mut index = HnswIndex::new(rows.dim, config)?;
        index.add(rows.slice(), rows.dim)?;
        index.prepare()?;
        let disk = disk_size(|path| index.write(path).map_err(Into::into));
        Ok(Self { rows, index, disk })
    }
}

impl Index for Hnsw {
    fn search(
        &self,
        q: &[f32],
        k: usize,
        ef: usize,
        allow: Option<&Allow>,
        stats: &mut QueryStats,
    ) -> Vec<(u32, f32)> {
        use pipestream_search::vector::VectorSearchOptions;
        let options = match allow.and_then(|a| a.bools.as_deref()) {
            Some(mask) => VectorSearchOptions::new()
                .with_mask(&mask[self.rows.offset..self.rows.offset + self.rows.len]),
            None => VectorSearchOptions::new(),
        };
        let (hits, s) = self
            .index
            .search_with_stats(q, k, ef, options)
            .expect("hnsw search");
        stats.hops += s.hops;
        stats.exact += s.scored;
        hits
    }

    fn disk_bytes(&self) -> Option<u64> {
        self.disk
    }
}

// ------------------------------------------------------------------ hnsw_rs

pub struct HnswRs {
    rows: Rows,
    hnsw: hnsw_rs::hnsw::Hnsw<'static, f32, hnsw_rs::anndists::dist::DistL2>,
}

impl HnswRs {
    fn build(rows: Rows, p: &BuildParams) -> Self {
        use hnsw_rs::anndists::dist::DistL2;
        use hnsw_rs::hnsw::Hnsw;
        let nb_layer = 16.min((rows.len as f32).ln().trunc() as usize).max(1);
        let mut hnsw =
            Hnsw::<f32, DistL2>::new(p.m, rows.len, nb_layer, p.ef_construction, DistL2 {});
        let data: Vec<(&[f32], usize)> = (0..rows.len).map(|r| (rows.row(r), r)).collect();
        hnsw.parallel_insert_slice(&data);
        hnsw.set_searching_mode(true);
        Self { rows, hnsw }
    }
}

impl Index for HnswRs {
    fn search(
        &self,
        q: &[f32],
        k: usize,
        ef: usize,
        allow: Option<&Allow>,
        _stats: &mut QueryStats,
    ) -> Vec<(u32, f32)> {
        let offset = self.rows.offset;
        let hits = match allow {
            None => self.hnsw.search(q, k, ef),
            Some(a) => {
                let f = |id: &usize| a.bits.get(offset + *id);
                self.hnsw.search_filter(q, k, ef, Some(&f))
            }
        };
        // DistL2 is the Euclidean distance; on unit vectors dot = 1 - d^2/2.
        hits.into_iter()
            .map(|n| (n.d_id as u32, 1.0 - n.distance * n.distance / 2.0))
            .collect()
    }
}

// ------------------------------------------------------------- rust-cv/hnsw

mod rust_cv {
    use super::*;
    use hnsw::{Hnsw, Searcher};
    use rand_pcg::Pcg64;
    use space::{Metric, Neighbor};

    /// L2 on unit vectors, as the f32 bit pattern (monotone for d >= 0).
    #[derive(Clone, Copy, Default)]
    pub struct L2Bits;

    impl Metric<Vec<f32>> for L2Bits {
        type Unit = u32;
        fn distance(&self, a: &Vec<f32>, b: &Vec<f32>) -> u32 {
            crate::util::l2sq(a, b).max(0.0).to_bits()
        }
    }

    thread_local! {
        static SEARCHER: RefCell<Searcher<u32>> = RefCell::new(Searcher::default());
    }

    pub struct RustCv<const M: usize, const M0: usize> {
        rows: Rows,
        hnsw: Hnsw<L2Bits, Vec<f32>, Pcg64, M, M0>,
    }

    impl<const M: usize, const M0: usize> Index for RustCv<M, M0> {
        fn search(
            &self,
            q: &[f32],
            k: usize,
            ef: usize,
            allow: Option<&Allow>,
            _stats: &mut QueryStats,
        ) -> Vec<(u32, f32)> {
            debug_assert!(allow.is_none());
            let _ = &self.rows;
            let query = q.to_vec();
            let mut dest = vec![
                Neighbor {
                    index: !0,
                    distance: !0
                };
                k
            ];
            SEARCHER.with(|s| {
                let mut searcher = s.borrow_mut();
                let found = self
                    .hnsw
                    .nearest(&query, ef.max(k), &mut searcher, &mut dest);
                found
                    .iter()
                    .map(|n| (n.index as u32, 1.0 - f32::from_bits(n.distance) / 2.0))
                    .collect()
            })
        }
    }

    fn build_m<const M: usize, const M0: usize>(rows: Rows, efc: usize) -> Box<dyn Index> {
        let params = hnsw::Params::new().ef_construction(efc);
        let mut hnsw: Hnsw<L2Bits, Vec<f32>, Pcg64, M, M0> = Hnsw::new_params(L2Bits, params);
        let mut searcher = Searcher::default();
        for r in 0..rows.len {
            hnsw.insert(rows.row(r).to_vec(), &mut searcher);
        }
        Box::new(RustCv { rows, hnsw })
    }

    pub fn build(rows: Rows, p: &BuildParams) -> Result<Box<dyn Index>, Error> {
        Ok(match p.m {
            12 => build_m::<12, 24>(rows, p.ef_construction),
            16 => build_m::<16, 32>(rows, p.ef_construction),
            24 => build_m::<24, 48>(rows, p.ef_construction),
            32 => build_m::<32, 64>(rows, p.ef_construction),
            48 => build_m::<48, 96>(rows, p.ef_construction),
            64 => build_m::<64, 128>(rows, p.ef_construction),
            m => {
                return Err(format!(
                "rust-cv-hnsw: M is a const generic; {m} is not compiled in (12,16,24,32,48,64)"
            )
                .into())
            }
        })
    }
}

// ------------------------------------------------------- CoreNN hnswlib-rs

/// A VectorStore over the fixture rows: nodes map to rows; `set` records
/// which row a node holds (from the slice's address in the shared base).
struct SliceStore {
    rows: Rows,
    node_row: Vec<AtomicU32>,
}

impl hnswlib_rs::VectorStore for SliceStore {
    type Family = hnswlib_rs::Dense<f32>;
    type Vector<'a> = &'a [f32];
    fn dim(&self) -> usize {
        self.rows.dim
    }
    fn vector(&self, id: hnswlib_rs::NodeId) -> Option<&[f32]> {
        let row = self.node_row.get(id.as_usize())?.load(Ordering::Relaxed);
        (row != u32::MAX).then(|| self.rows.row(row as usize))
    }
}

impl hnswlib_rs::VectorStoreMut for SliceStore {
    fn set(&self, id: hnswlib_rs::NodeId, vector: &[f32]) -> hnswlib_rs::Result<()> {
        let base = self.rows.slice().as_ptr() as usize;
        let at = vector.as_ptr() as usize;
        let row = (at - base) / 4 / self.rows.dim;
        self.node_row[id.as_usize()].store(row as u32, Ordering::Relaxed);
        Ok(())
    }
}

pub struct CoreNn {
    store: SliceStore,
    hnsw: hnswlib_rs::Hnsw<u32, hnswlib_rs::InnerProduct<f32>>,
    disk: Option<u64>,
}

impl CoreNn {
    fn build(rows: Rows, p: &BuildParams, threads: usize) -> Result<Self, Error> {
        use hnswlib_rs::{Hnsw, HnswConfig, InnerProduct};
        let cfg = HnswConfig::new(rows.dim, rows.len)
            .m(p.m)
            .ef_construction(p.ef_construction)
            .ef_search(64)
            .seed(42);
        let hnsw = Hnsw::new(InnerProduct::new(), cfg);
        let store = SliceStore {
            node_row: (0..rows.len).map(|_| AtomicU32::new(u32::MAX)).collect(),
            rows: rows.clone(),
        };
        let next = std::sync::atomic::AtomicUsize::new(0);
        let failed = std::sync::Mutex::new(None);
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| loop {
                    let r = next.fetch_add(1, Ordering::Relaxed);
                    if r >= rows.len {
                        break;
                    }
                    if let Err(e) = hnsw.insert(&store, r as u32, rows.row(r)) {
                        *failed.lock().unwrap() = Some(e.to_string());
                        break;
                    }
                });
            }
        });
        if let Some(e) = failed.into_inner().unwrap() {
            return Err(format!("corenn-hnswlib insert: {e}").into());
        }
        let mut counter = CountingWriter(0);
        let disk = hnsw
            .save_to(&mut counter)
            .ok()
            .map(|_| counter.0 + (rows.len * rows.dim * 4) as u64);
        Ok(Self { store, hnsw, disk })
    }
}

struct CountingWriter(u64);
impl std::io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Index for CoreNn {
    fn search(
        &self,
        q: &[f32],
        k: usize,
        _ef: usize,
        allow: Option<&Allow>,
        _stats: &mut QueryStats,
    ) -> Vec<(u32, f32)> {
        let offset = self.store.rows.offset;
        let hits = match allow {
            None => self.hnsw.search(&self.store, q, k, None),
            Some(a) => {
                let f = |key: &u32| a.bits.get(offset + *key as usize);
                self.hnsw.search(&self.store, q, k, Some(&f))
            }
        }
        .expect("corenn search");
        // InnerProduct distance is 1 - dot.
        hits.into_iter()
            .map(|h| (h.key, 1.0 - h.distance))
            .collect()
    }

    fn set_ef(&mut self, ef: usize) {
        self.hnsw.set_ef_search(ef);
    }

    fn disk_bytes(&self) -> Option<u64> {
        self.disk
    }
}

// ------------------------------------------------------------------ USearch

pub struct USearch {
    rows: Rows,
    index: usearch::Index,
}

impl USearch {
    fn build(rows: Rows, p: &BuildParams, threads: usize) -> Result<Self, Error> {
        use usearch::{Index, IndexOptions, MetricKind, ScalarKind};
        let options = IndexOptions {
            dimensions: rows.dim,
            metric: MetricKind::IP,
            quantization: ScalarKind::F32,
            connectivity: p.m,
            expansion_add: p.ef_construction,
            expansion_search: 64,
            multi: false,
        };
        let index = Index::new(&options)?;
        index.reserve_capacity_and_threads(rows.len, threads)?;
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| loop {
                    let r = next.fetch_add(1, Ordering::Relaxed);
                    if r >= rows.len {
                        break;
                    }
                    index.add(r as u64, rows.row(r)).expect("usearch add");
                });
            }
        });
        Ok(Self { rows, index })
    }
}

impl Index for USearch {
    fn search(
        &self,
        q: &[f32],
        k: usize,
        _ef: usize,
        allow: Option<&Allow>,
        _stats: &mut QueryStats,
    ) -> Vec<(u32, f32)> {
        let offset = self.rows.offset;
        let m = match allow {
            None => self.index.search(q, k),
            Some(a) => self
                .index
                .filtered_search(q, k, |key| a.bits.get(offset + key as usize)),
        }
        .expect("usearch search");
        // MetricKind::IP distance is 1 - dot.
        m.keys
            .iter()
            .zip(&m.distances)
            .map(|(key, d)| (*key as u32, 1.0 - d))
            .collect()
    }

    fn set_ef(&mut self, ef: usize) {
        self.index.change_expansion_search(ef);
    }

    fn disk_bytes(&self) -> Option<u64> {
        Some(self.index.serialized_length() as u64)
    }
}
