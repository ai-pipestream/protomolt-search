//! The vocabulary index: streaming corpus statistics accumulated inline in
//! the ingest path, from the AnalyzeStream responses the node already
//! receives — no new gRPC channel, no extra analysis pass.
//!
//! Design: `grpc-opennlp-analysis/VOCABULARY-LISTENER.md` (the sidecar
//! repo). The split by role: Java calculates (term identity is counted
//! exactly as produced), Rust accumulates, stores, and aggregates. Every
//! analyzed document feeds two channels — TERMS (term-vector entries with
//! their frequencies, exactly the BM25 index identity) and TOKENS (raw
//! token surface forms) — each holding a HyperLogLog (cardinality), a
//! count-min sketch (point frequencies), a space-saving top-K (heavy
//! hitters), and counters. Memory is bounded by configuration, never by
//! corpus size (~11 MiB per channel per window).
//!
//! A window rolls over at [`DEFAULT_WINDOW_DOCS`] documents (or on an
//! explicit snapshot) and is sealed as a protobuf `VocabSnapshot` at
//! `<index path>.vocab/snapshot-<seq>-<millis>.pb` — the shared on-disk
//! contract with the Java listener, so snapshots are interchangeable
//! between the two implementations (same hashes, same parameters, same
//! byte layouts; pinned by the cross-language vector tests below).
//!
//! The listener is analytics, not a ledger: [`VocabularyListener::feed`]
//! never fails — a sketch or persistence failure loses one document's
//! counts or keeps the window unsealed, never the document's ingest.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;

use crate::pb::analysis::{
    VocabChannel, VocabChannelSnapshot, VocabChannelStats, VocabHeavyHitter, VocabSnapshot,
};

/// The splitmix64 increment (the golden ratio as a 64-bit odd constant).
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

/// The murmur3 64-bit finalizer: full avalanche in three xorshift-multiply
/// rounds. Ported exactly from the Java reference (wrapping arithmetic, so
/// the hashes — and therefore every sketch byte — match).
fn fmix64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    value ^= value >> 33;
    value = value.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    value ^= value >> 33;
    value
}

/// The base 64-bit hash of a term: FNV-1a over its UTF-8 bytes, avalanched
/// with the murmur3 finalizer.
fn hash_base(term: &str) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in term.as_bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    fmix64(hash)
}

/// The hash function of count-min row `row`: one splitmix64 step over the
/// base hash seeded with a per-row stride.
fn hash_row(base: u64, row: usize) -> u64 {
    fmix64(base.wrapping_add(GOLDEN.wrapping_mul(row as u64 + 1)))
}

/// A count-min sketch: approximate per-term frequencies with a one-sided
/// error (estimates are never below the true count). Depth 5, width 2^17 —
/// five independent hash functions over a 131072-column table of 64-bit
/// counters (5 MiB). Sketches merge cell-wise. Not thread-safe; the owning
/// window state serializes access.
pub struct CountMinSketch {
    depth: usize,
    width: usize,
    table: Vec<u64>,
    total: u64,
}

impl CountMinSketch {
    /// Sketch depth: the number of rows (independent hash functions).
    pub const DEPTH: usize = 5;
    /// Sketch width: columns per row, a power of two for cheap indexing.
    pub const WIDTH: usize = 1 << 17;

    /// A fresh, empty sketch of the standard shape.
    pub fn new() -> Self {
        Self::with_shape(Self::DEPTH, Self::WIDTH)
    }

    /// A sketch of `depth` rows by `width` columns; width must be a
    /// positive power of two.
    pub fn with_shape(depth: usize, width: usize) -> Self {
        assert!(depth > 0 && width > 0 && width.is_power_of_two());
        Self {
            depth,
            width,
            table: vec![0; depth * width],
            total: 0,
        }
    }

    /// Adds `count` occurrences of `term`.
    pub fn add(&mut self, term: &str, count: u64) {
        if count == 0 {
            return;
        }
        let base = hash_base(term);
        for row in 0..self.depth {
            let column = (hash_row(base, row) & (self.width as u64 - 1)) as usize;
            self.table[row * self.width + column] += count;
        }
        self.total += count;
    }

    /// The estimated frequency of `term`: the minimum over the rows.
    /// Never below the true count; within the sketch's error bound above it.
    pub fn estimate(&self, term: &str) -> u64 {
        let base = hash_base(term);
        let mut min = u64::MAX;
        for row in 0..self.depth {
            let column = (hash_row(base, row) & (self.width as u64 - 1)) as usize;
            min = min.min(self.table[row * self.width + column]);
        }
        min
    }

    /// Merges `other` into this sketch, cell-wise. Only meaningful for
    /// sketches of the same shape.
    pub fn merge(&mut self, other: &Self) -> Result<(), String> {
        if other.depth != self.depth || other.width != self.width {
            return Err(format!(
                "sketch shapes differ: {}x{} vs {}x{}",
                other.depth, other.width, self.depth, self.width
            ));
        }
        for (cell, &other_cell) in self.table.iter_mut().zip(&other.table) {
            *cell += other_cell;
        }
        self.total += other.total;
        Ok(())
    }

    /// The total number of occurrences ever added.
    pub fn total_count(&self) -> u64 {
        self.total
    }

    /// The number of rows.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// The number of columns per row.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Serializes the table in row-major order as little-endian unsigned
    /// 64-bit counters — the `cms_table` encoding of
    /// `VocabChannelSnapshot` (`depth * width * 8` bytes).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.table.len() * 8];
        for (i, &value) in self.table.iter().enumerate() {
            out[i * 8..(i + 1) * 8].copy_from_slice(&value.to_le_bytes());
        }
        out
    }

    /// Restores a sketch from [`Self::to_bytes`] output, with the total
    /// occurrence count carried alongside the table.
    pub fn from_bytes(
        depth: usize,
        width: usize,
        bytes: &[u8],
        total: u64,
    ) -> Result<Self, String> {
        let sketch = Self::with_shape(depth, width);
        if bytes.len() != sketch.table.len() * 8 {
            return Err(format!(
                "table byte length {} does not match {}x{}",
                bytes.len(),
                depth,
                width
            ));
        }
        let table = bytes
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("8-byte chunk")))
            .collect();
        Ok(Self {
            depth,
            width,
            table,
            total,
        })
    }
}

impl Default for CountMinSketch {
    fn default() -> Self {
        Self::new()
    }
}

/// A HyperLogLog cardinality sketch over 64-bit hashes. Precision p = 14 →
/// 16384 one-byte registers (16 KiB), standard error ~0.8%. Registers merge
/// by cell-wise maximum, which is what makes the novelty rate computable
/// without ever materializing the vocabulary. Not thread-safe.
#[derive(Clone)]
pub struct HyperLogLog {
    precision: usize,
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// Sketch precision: log2 of the register count.
    pub const PRECISION: usize = 14;

    /// A fresh, empty sketch of the standard precision.
    pub fn new() -> Self {
        Self::with_precision(Self::PRECISION)
    }

    /// A sketch with `2^precision` registers; precision in [4, 16].
    pub fn with_precision(precision: usize) -> Self {
        assert!((4..=16).contains(&precision));
        Self {
            precision,
            registers: vec![0; 1 << precision],
        }
    }

    /// Adds one occurrence of `term`. Repeated adds of the same term are
    /// idempotent up to hash collisions.
    pub fn add(&mut self, term: &str) {
        let hash = hash_base(term);
        // The top p bits choose the register; the rank is the number of
        // leading zeros of the remaining 64 - p bits, plus one.
        let index = (hash >> (64 - self.precision)) as usize;
        let rank = ((hash << self.precision) | (1u64 << (self.precision - 1))).leading_zeros() + 1;
        let register = &mut self.registers[index];
        if rank as u8 > *register {
            *register = rank as u8;
        }
    }

    /// The estimated number of distinct terms added. Small cardinalities
    /// use linear counting; 64-bit hashes make the large-range correction
    /// irrelevant at any corpus scale this engine will see.
    pub fn estimate(&self) -> f64 {
        let mut inverse_sum = 0.0f64;
        let mut zeros = 0u32;
        for &register in &self.registers {
            inverse_sum += 2.0f64.powi(-i32::from(register));
            if register == 0 {
                zeros += 1;
            }
        }
        let register_count = self.registers.len() as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / register_count);
        let raw = alpha * register_count * register_count / inverse_sum;
        if raw <= 2.5 * register_count && zeros > 0 {
            return register_count * (register_count / f64::from(zeros)).ln();
        }
        raw
    }

    /// Merges `other` into this sketch, cell-wise maximum. Only meaningful
    /// for sketches of the same precision.
    pub fn merge(&mut self, other: &Self) -> Result<(), String> {
        if other.precision != self.precision {
            return Err(format!(
                "precisions differ: {} vs {}",
                other.precision, self.precision
            ));
        }
        for (register, &other_register) in self.registers.iter_mut().zip(&other.registers) {
            *register = (*register).max(other_register);
        }
        Ok(())
    }

    /// The precision this sketch was built with.
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// The registers in register order — the `hll_registers` encoding of
    /// `VocabChannelSnapshot` (a copy, one byte per register).
    pub fn to_bytes(&self) -> Vec<u8> {
        self.registers.clone()
    }

    /// Restores a sketch from [`Self::to_bytes`] output.
    pub fn from_bytes(precision: usize, bytes: &[u8]) -> Result<Self, String> {
        let sketch = Self::with_precision(precision);
        if bytes.len() != sketch.registers.len() {
            return Err(format!(
                "register byte length {} does not match precision {}",
                bytes.len(),
                precision
            ));
        }
        Ok(Self {
            precision,
            registers: bytes.to_vec(),
        })
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

/// One heavy hitter: a term and its attributed count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeavyHitter {
    /// The term or token surface form.
    pub term: String,
    /// Number of occurrences the space-saving sketch attributes to it.
    pub count: u64,
}

/// A space-saving top-K sketch: the heaviest terms of the window with
/// near-exact counts. When the table is full, a new term evicts the current
/// minimum and inherits its count plus its own — the classic space-saving
/// rule, which bounds every entry's overestimate by the minimum count in
/// the table. Not thread-safe.
pub struct HeavyHitters {
    capacity: usize,
    counts: HashMap<String, u64>,
}

impl HeavyHitters {
    /// The default list size.
    pub const DEFAULT_CAPACITY: usize = 1024;

    /// A sketch holding at most `capacity` entries (must be positive).
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            counts: HashMap::new(),
        }
    }

    /// Adds `count` occurrences of `term`, evicting the minimum entry when
    /// the table is full.
    pub fn add(&mut self, term: &str, count: u64) {
        assert!(count > 0, "count must be positive");
        if let Some(current) = self.counts.get_mut(term) {
            *current += count;
        } else if self.counts.len() < self.capacity {
            self.counts.insert(term.to_string(), count);
        } else {
            // Space-saving eviction: the newcomer takes the minimum entry's
            // slot and count. A linear scan is fine at K ~ 1024 — it runs
            // only for terms the table has never seen.
            let (min_term, min_count) = self
                .counts
                .iter()
                .min_by_key(|(_, &count)| count)
                .map(|(term, &count)| (term.clone(), count))
                .expect("full table has a minimum");
            self.counts.remove(&min_term);
            self.counts.insert(term.to_string(), min_count + count);
        }
    }

    /// The attributed count of `term`, or 0 when the term is not on the
    /// list. Absence means "not heavy enough to track", not "never seen" —
    /// point queries belong on the count-min sketch.
    pub fn estimate(&self, term: &str) -> u64 {
        self.counts.get(term).copied().unwrap_or(0)
    }

    /// The list, ordered by count descending (ties by term, so snapshots
    /// are deterministic).
    pub fn snapshot(&self) -> Vec<HeavyHitter> {
        let mut entries: Vec<HeavyHitter> = self
            .counts
            .iter()
            .map(|(term, &count)| HeavyHitter {
                term: term.clone(),
                count,
            })
            .collect();
        entries.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.term.cmp(&b.term)));
        entries
    }

    /// The list size K.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Restores a list from a snapshot, replacing any current content.
    pub fn restore(&mut self, entries: Vec<HeavyHitter>) {
        self.counts.clear();
        for entry in entries {
            self.counts.insert(entry.term, entry.count);
        }
    }
}

/// The per-channel state of one vocabulary window: the three sketches plus
/// the document and occurrence counters. Interior mutability is the
/// listener's job (`VocabularyListener` holds the lock).
pub struct WindowState {
    channel: VocabChannel,
    count_min: CountMinSketch,
    hyper_log_log: HyperLogLog,
    heavy_hitters: HeavyHitters,
    documents: u64,
    occurrences: u64,
}

impl WindowState {
    /// A fresh window for `channel` with a `top_k` heavy-hitter list.
    pub fn new(channel: VocabChannel, top_k: usize) -> Self {
        Self {
            channel,
            count_min: CountMinSketch::new(),
            hyper_log_log: HyperLogLog::new(),
            heavy_hitters: HeavyHitters::new(top_k.max(1)),
            documents: 0,
            occurrences: 0,
        }
    }

    /// Adds one term with its frequency from one document.
    pub fn add_term(&mut self, term: &str, frequency: u64) {
        self.count_min.add(term, frequency);
        self.hyper_log_log.add(term);
        self.heavy_hitters.add(term, frequency);
        self.occurrences += frequency;
    }

    /// Counts one more document into the window.
    pub fn add_document(&mut self) {
        self.documents += 1;
    }

    /// The channel this state belongs to.
    pub fn channel(&self) -> VocabChannel {
        self.channel
    }

    /// Documents accumulated in this window.
    pub fn documents(&self) -> u64 {
        self.documents
    }

    /// Total occurrences (with multiplicity) accumulated in this window.
    pub fn occurrences(&self) -> u64 {
        self.occurrences
    }

    /// The estimated distinct-term cardinality of this window.
    pub fn cardinality_estimate(&self) -> f64 {
        self.hyper_log_log.estimate()
    }

    /// The heavy hitters, ordered by count descending.
    pub fn heavy_hitters(&self) -> Vec<HeavyHitter> {
        self.heavy_hitters.snapshot()
    }

    /// The estimated frequency of `term`: its heavy-hitter count when it is
    /// on the list, otherwise the count-min point query. This is the count a
    /// drift comparison uses for every term on the union of the two top-K
    /// lists.
    pub fn estimate(&self, term: &str) -> u64 {
        let heavy = self.heavy_hitters.estimate(term);
        if heavy > 0 {
            heavy
        } else {
            self.count_min.estimate(term)
        }
    }

    /// A detached copy of the cardinality sketch, for union computations.
    pub fn cardinality_sketch(&self) -> HyperLogLog {
        self.hyper_log_log.clone()
    }

    /// Serializes this window's channel state into its persisted form.
    pub fn to_proto(&self) -> VocabChannelSnapshot {
        VocabChannelSnapshot {
            channel: self.channel as i32,
            documents: self.documents,
            term_occurrences: self.occurrences,
            hll_precision: self.hyper_log_log.precision() as u32,
            hll_registers: self.hyper_log_log.to_bytes(),
            cms_depth: self.count_min.depth() as u32,
            cms_width: self.count_min.width() as u32,
            cms_table: self.count_min.to_bytes(),
            heavy_hitters: self
                .heavy_hitters
                .snapshot()
                .into_iter()
                .map(|entry| VocabHeavyHitter {
                    term: entry.term,
                    count: entry.count,
                })
                .collect(),
        }
    }

    /// Restores a window's channel state from its persisted form.
    pub fn from_proto(snapshot: &VocabChannelSnapshot) -> Result<Self, String> {
        let heavy_hitters: Vec<HeavyHitter> = snapshot
            .heavy_hitters
            .iter()
            .map(|h| HeavyHitter {
                term: h.term.clone(),
                count: h.count,
            })
            .collect();
        let mut state = Self::new(
            VocabChannel::try_from(snapshot.channel)
                .map_err(|_| format!("unknown vocab channel {}", snapshot.channel))?,
            snapshot.heavy_hitters.len().max(1),
        );
        state.documents = snapshot.documents;
        state.occurrences = snapshot.term_occurrences;
        state.hyper_log_log =
            HyperLogLog::from_bytes(snapshot.hll_precision as usize, &snapshot.hll_registers)?;
        state.count_min = CountMinSketch::from_bytes(
            snapshot.cms_depth as usize,
            snapshot.cms_width as usize,
            &snapshot.cms_table,
            snapshot.term_occurrences,
        )?;
        state.heavy_hitters.restore(heavy_hitters);
        Ok(state)
    }

    /// Merges `other` (same channel) into this window: cardinality and
    /// count-min merge cell-wise, heavy hitters merge by re-running
    /// space-saving over the combined lists (this is what coordinator-side
    /// aggregation of shard-level snapshots does). Counters sum.
    pub fn merge(&mut self, other: &Self) -> Result<(), String> {
        if other.channel != self.channel {
            return Err(format!(
                "channels differ: {:?} vs {:?}",
                other.channel, self.channel
            ));
        }
        self.hyper_log_log.merge(&other.hyper_log_log)?;
        self.count_min.merge(&other.count_min)?;
        // Re-run space-saving over the combined lists: each list's counts
        // are near-exact for its own heaviest terms, and their per-term
        // sums are the best estimate of the combined distribution's head.
        let capacity = self
            .heavy_hitters
            .capacity()
            .max(other.heavy_hitters.capacity());
        let mut combined: HashMap<String, u64> = HashMap::new();
        for entry in self
            .heavy_hitters
            .snapshot()
            .into_iter()
            .chain(other.heavy_hitters.snapshot())
        {
            *combined.entry(entry.term).or_insert(0) += entry.count;
        }
        let mut entries: Vec<HeavyHitter> = combined
            .into_iter()
            .map(|(term, count)| HeavyHitter { term, count })
            .collect();
        entries.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.term.cmp(&b.term)));
        entries.truncate(capacity);
        self.heavy_hitters = HeavyHitters::new(capacity);
        self.heavy_hitters.restore(entries);
        self.documents += other.documents;
        self.occurrences += other.occurrences;
        Ok(())
    }
}

/// Merges shard-level snapshots into one (coordinator-side aggregation):
/// same-channel states merge per [`WindowState::merge`], timestamps span
/// the earliest start to the latest seal, and the sequence is the maximum
/// seen. Channels present in some snapshots but not others carry through
/// as-is.
pub fn merge_snapshots(snapshots: &[VocabSnapshot]) -> Result<VocabSnapshot, String> {
    let mut by_channel: HashMap<i32, WindowState> = HashMap::new();
    let mut sequence = 0u64;
    let mut started = i64::MAX;
    let mut sealed = i64::MIN;
    for snapshot in snapshots {
        sequence = sequence.max(snapshot.sequence);
        started = started.min(snapshot.started_epoch_millis);
        sealed = sealed.max(snapshot.sealed_epoch_millis);
        for channel_snapshot in &snapshot.channels {
            let state = WindowState::from_proto(channel_snapshot)?;
            by_channel
                .entry(channel_snapshot.channel)
                .and_modify(|merged| {
                    // Same-channel merge cannot fail on shape: snapshots of
                    // one deployment share sketch parameters.
                    if let Err(e) = merged.merge(&state) {
                        eprintln!("vocab: channel merge failed: {e}");
                    }
                })
                .or_insert(state);
        }
    }
    let mut channels: Vec<VocabChannelSnapshot> = by_channel
        .into_values()
        .map(|state| state.to_proto())
        .collect();
    channels.sort_by_key(|c| c.channel);
    Ok(VocabSnapshot {
        sequence,
        started_epoch_millis: started,
        sealed_epoch_millis: sealed,
        channels,
    })
}

/// The drift metrics of one channel comparison.
#[derive(Debug, Clone)]
pub struct DriftMetrics {
    /// Estimated distinct-term count of the older window.
    pub from_cardinality: f64,
    /// Estimated distinct-term count of the newer window.
    pub to_cardinality: f64,
    /// Estimated distinct-term count of the union.
    pub union_cardinality: f64,
    /// `(|union| − |from|) / |to|`, in [0, 1].
    pub novelty_rate: f64,
    /// JS divergence (base 2) over the union of the two heavy-hitter
    /// lists, in [0, 1].
    pub jensen_shannon_divergence: f64,
    /// Whether `embedding_oov_share` is meaningful.
    pub embedding_oov_computed: bool,
    /// Share of the newer window's heavy-hitter token mass absent from
    /// the embedding vocabulary.
    pub embedding_oov_share: f64,
}

/// Computes the drift of `to` (the newer window) relative to `from` (the
/// older window). `embedding_vocabulary` is `Some` only for the TOKENS
/// channel when a model vocabulary was readable.
pub fn compute_drift(
    from: &WindowState,
    to: &WindowState,
    embedding_vocabulary: Option<&HashSet<String>>,
) -> DriftMetrics {
    // Union cardinality by inclusion-exclusion over merged registers: the
    // whole point of HyperLogLog mergeability.
    let mut merged = from.cardinality_sketch();
    // Same-precision merge; a mismatch means the snapshots come from
    // different sketch generations and the union falls back to the sum's
    // upper bound rather than a wrong answer.
    let union_cardinality = match merged.merge(&to.cardinality_sketch()) {
        Ok(()) => merged.estimate(),
        Err(_) => from.cardinality_estimate() + to.cardinality_estimate(),
    };
    let from_cardinality = from.cardinality_estimate();
    let to_cardinality = to.cardinality_estimate();
    let novelty_rate = if to_cardinality > 0.0 {
        ((union_cardinality - from_cardinality) / to_cardinality).max(0.0)
    } else {
        0.0
    };
    let divergence = jensen_shannon(from, to);
    let oov_computed = embedding_vocabulary.is_some() && to.occurrences() > 0;
    let oov_share = match embedding_vocabulary.filter(|_| oov_computed) {
        Some(vocabulary) => oov_share(to, vocabulary),
        None => 0.0,
    };
    DriftMetrics {
        from_cardinality,
        to_cardinality,
        union_cardinality,
        novelty_rate,
        jensen_shannon_divergence: divergence,
        embedding_oov_computed: oov_computed,
        embedding_oov_share: oov_share,
    }
}

/// Jensen-Shannon divergence (base 2, so bounded by 1) between the two term
/// distributions over the union of their heavy-hitter lists. Counts for a
/// term missing from one list come from that window's count-min sketch, so
/// the comparison sees the tail too, at sketch accuracy.
fn jensen_shannon(from: &WindowState, to: &WindowState) -> f64 {
    let mut union: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for entry in from.heavy_hitters().into_iter().chain(to.heavy_hitters()) {
        if seen.insert(entry.term.clone()) {
            union.push(entry.term);
        }
    }
    let mut total_from = 0u64;
    let mut total_to = 0u64;
    let mut counts_from = Vec::with_capacity(union.len());
    let mut counts_to = Vec::with_capacity(union.len());
    for term in &union {
        let cf = from.estimate(term);
        let ct = to.estimate(term);
        total_from += cf;
        total_to += ct;
        counts_from.push(cf);
        counts_to.push(ct);
    }
    if total_from == 0 && total_to == 0 {
        return 0.0;
    }
    if total_from == 0 || total_to == 0 {
        // One side has no mass at all: maximal divergence.
        return 1.0;
    }
    let mut divergence = 0.0f64;
    for j in 0..counts_from.len() {
        let p = counts_from[j] as f64 / total_from as f64;
        let q = counts_to[j] as f64 / total_to as f64;
        let m = (p + q) / 2.0;
        if p > 0.0 {
            divergence += 0.5 * p * (p / m).log2();
        }
        if q > 0.0 {
            divergence += 0.5 * q * (q / m).log2();
        }
    }
    divergence
}

/// The out-of-vocabulary share of the window's token mass, computed over
/// its heavy-hitter list: the tail beyond top-K contributes negligible
/// mass on a realistically skewed corpus, which is the same premise the JS
/// divergence runs on.
fn oov_share(window: &WindowState, vocabulary: &HashSet<String>) -> f64 {
    let mut mass = 0u64;
    let mut oov = 0u64;
    for entry in window.heavy_hitters() {
        mass += entry.count;
        if !vocabulary.contains(&entry.term) {
            oov += entry.count;
        }
    }
    if mass > 0 {
        oov as f64 / mass as f64
    } else {
        0.0
    }
}

/// Loads the surface forms an embedding model covers, read from the model
/// directory rather than from a private API: a `vocab.txt` with one token
/// per line, or a HuggingFace-style `tokenizer.json` whose `model.vocab`
/// maps tokens to ids (BPE-style object) or lists `[token, score]` pairs
/// (Unigram-style array); both yield their tokens.
///
/// Absence is a first-class outcome: no readable vocabulary means the
/// out-of-vocabulary share is not computable, and the drift report says so
/// instead of inventing a number.
pub fn load_embedding_vocabulary(dir: &Path) -> Option<HashSet<String>> {
    let vocab_txt = dir.join("vocab.txt");
    if vocab_txt.is_file() {
        match std::fs::read_to_string(&vocab_txt) {
            Ok(text) => {
                let vocabulary: HashSet<String> = text.lines().map(str::to_string).collect();
                eprintln!(
                    "vocab: loaded embedding vocabulary from {} ({} entries)",
                    vocab_txt.display(),
                    vocabulary.len()
                );
                return Some(vocabulary);
            }
            Err(e) => {
                eprintln!(
                    "vocab: could not read embedding vocabulary from {}: {e}",
                    vocab_txt.display()
                );
                return None;
            }
        }
    }
    let tokenizer_json = dir.join("tokenizer.json");
    if tokenizer_json.is_file() {
        let vocabulary = std::fs::read_to_string(&tokenizer_json)
            .ok()
            .and_then(|text| parse_model_vocab(&text));
        match &vocabulary {
            Some(v) => eprintln!(
                "vocab: loaded embedding vocabulary from {} ({} entries)",
                tokenizer_json.display(),
                v.len()
            ),
            None => eprintln!(
                "vocab: {} carries no readable model.vocab; embedding coverage is not computable",
                tokenizer_json.display()
            ),
        }
        return vocabulary;
    }
    None
}

/// Extracts the tokens of the `"vocab"` member of `"model"` from a
/// tokenizer.json. Two shapes exist in the wild: an object mapping token
/// to id (BPE-style) and an array of `[token, score]` pairs
/// (Unigram-style); both yield their tokens here.
fn parse_model_vocab(text: &str) -> Option<HashSet<String>> {
    let json: serde_json::Value = serde_json::from_str(text).ok()?;
    let vocab = json.get("model")?.get("vocab")?;
    match vocab {
        serde_json::Value::Object(map) => Some(map.keys().cloned().collect()),
        serde_json::Value::Array(pairs) => Some(
            pairs
                .iter()
                .filter_map(|pair| pair.as_array()?.first()?.as_str().map(str::to_string))
                .collect(),
        ),
        _ => None,
    }
}

/// The index entry of one persisted snapshot: everything needed to name it
/// in a drift comparison, without loading its sketches.
#[derive(Debug, Clone)]
pub struct SnapshotMeta {
    /// File name of the snapshot, e.g. "snapshot-3-1780000000000.pb".
    pub name: String,
    /// Monotonic sequence number on this shard's history.
    pub sequence: u64,
    /// When the window that produced the snapshot started, epoch millis.
    pub started_epoch_millis: i64,
    /// When the window was sealed and persisted, epoch millis.
    pub sealed_epoch_millis: i64,
    /// Documents the window accumulated (per channel).
    pub documents: u64,
    /// Size of the snapshot file in bytes.
    pub size_bytes: u64,
}

/// The drift metrics of one channel comparison, tagged with its channel.
#[derive(Debug, Clone)]
pub struct ChannelDrift {
    /// Which channel these metrics describe.
    pub channel: VocabChannel,
    /// The metrics.
    pub metrics: DriftMetrics,
}

/// The reference that names the live window in a drift request.
pub const LIVE: &str = "live";

/// The vocabulary listener: streaming statistics over every term and token
/// the AnalyzeStream ingest path consumes. Cheap to share: behind one
/// `Arc`, all mutating and reading methods take the internal lock, so
/// concurrent sessions never observe a half-applied document.
///
/// Snapshot files are `snapshot-<seq>-<epochMillis>.pb` in the vocab
/// directory — write-then-move, so a crash mid-write never leaves a half
/// snapshot that startup would later fail to index.
pub struct VocabularyListener {
    vocab_dir: PathBuf,
    window_docs: u64,
    top_k: usize,
    inner: Mutex<Inner>,
}

struct Inner {
    terms: WindowState,
    tokens: WindowState,
    window_started_millis: i64,
    next_sequence: u64,
    snapshots: Vec<SnapshotMeta>,
    files_by_sequence: HashMap<u64, PathBuf>,
}

/// Documents per window before automatic rollover.
pub const DEFAULT_WINDOW_DOCS: u64 = 1_000_000;

/// The vocabulary snapshot directory of a shard: `<index path>.vocab/`,
/// mirroring the WAL's `<index path>.wal/` convention.
pub fn vocab_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".vocab");
    PathBuf::from(p)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parses `snapshot-<seq>-<millis>.pb` file names (no regex dependency).
fn parse_snapshot_name(name: &str) -> Option<(u64, i64)> {
    let rest = name.strip_prefix("snapshot-")?.strip_suffix(".pb")?;
    let (seq, millis) = rest.split_once('-')?;
    Some((seq.parse().ok()?, millis.parse().ok()?))
}

/// Reads and decodes one persisted snapshot.
pub fn load_snapshot_file(path: &Path) -> Result<VocabSnapshot, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    VocabSnapshot::decode(bytes.as_slice()).map_err(|e| format!("decode {}: {e}", path.display()))
}

/// Read-only scan of a snapshot directory (the offline-tooling counterpart
/// of the listener's startup scan — no write probe, no live window).
/// Returns `(metadata, path)` pairs ordered by sequence ascending;
/// unreadable snapshots are skipped with a warning.
pub fn scan_snapshot_dir(vocab_dir: &Path) -> io::Result<Vec<(SnapshotMeta, PathBuf)>> {
    let (found, files, _) = scan_snapshots(vocab_dir)?;
    Ok(found
        .into_iter()
        .map(|meta| {
            let path = files
                .get(&meta.sequence)
                .expect("every scanned snapshot has a file")
                .clone();
            (meta, path)
        })
        .collect())
}

/// Restores the per-channel window states of a persisted snapshot.
pub fn states_by_channel(
    snapshot: &VocabSnapshot,
) -> Result<HashMap<VocabChannel, WindowState>, String> {
    let mut states = HashMap::with_capacity(snapshot.channels.len());
    for channel_snapshot in &snapshot.channels {
        let state = WindowState::from_proto(channel_snapshot)?;
        states.insert(state.channel(), state);
    }
    Ok(states)
}

/// Computes the drift of the `to` snapshot relative to the `from`
/// snapshot, per channel — the offline form of
/// [`VocabularyListener::drift`]. `embedding_vocabulary` applies to the
/// TOKENS channel only.
pub fn compute_channel_drift(
    from: &VocabSnapshot,
    to: &VocabSnapshot,
    embedding_vocabulary: Option<&HashSet<String>>,
) -> Result<Vec<ChannelDrift>, String> {
    let from = states_by_channel(from)?;
    let to = states_by_channel(to)?;
    let mut out = Vec::with_capacity(2);
    for channel in [VocabChannel::Terms, VocabChannel::Tokens] {
        let from_state = from
            .get(&channel)
            .ok_or_else(|| format!("from snapshot has no {channel:?} channel"))?;
        let to_state = to
            .get(&channel)
            .ok_or_else(|| format!("to snapshot has no {channel:?} channel"))?;
        let vocabulary = if channel == VocabChannel::Tokens {
            embedding_vocabulary
        } else {
            None
        };
        out.push(ChannelDrift {
            channel,
            metrics: compute_drift(from_state, to_state, vocabulary),
        });
    }
    Ok(out)
}

impl VocabularyListener {
    /// Creates the listener and indexes the prior snapshots in
    /// `vocab_dir` (created when missing, must be writable).
    pub fn create(vocab_dir: &Path, window_docs: u64, top_k: usize) -> io::Result<Self> {
        assert!(window_docs > 0, "window_docs must be positive");
        assert!(top_k > 0, "top_k must be positive");
        std::fs::create_dir_all(vocab_dir)?;
        // A cheap writability probe: the caller degrades to disabled when
        // the directory rejects it.
        let probe = vocab_dir.join(".vocab-write-probe");
        std::fs::write(&probe, b"")?;
        std::fs::remove_file(&probe)?;
        let (snapshots, files_by_sequence, next_sequence) = scan_snapshots(vocab_dir)?;
        if !snapshots.is_empty() {
            eprintln!(
                "vocab: resumed {} prior snapshot(s) from {}",
                snapshots.len(),
                vocab_dir.display()
            );
        }
        Ok(Self {
            vocab_dir: vocab_dir.to_path_buf(),
            window_docs,
            top_k,
            inner: Mutex::new(Inner {
                terms: WindowState::new(VocabChannel::Terms, top_k),
                tokens: WindowState::new(VocabChannel::Tokens, top_k),
                window_started_millis: now_millis(),
                next_sequence,
                snapshots,
                files_by_sequence,
            }),
        })
    }

    /// Feeds one successfully analyzed document: its term-vector entries
    /// into the TERMS channel and its raw token texts into the TOKENS
    /// channel. Never fails — a listener problem must never reach ingest,
    /// so persistence errors are logged and the window carries on.
    pub fn feed<'a>(
        &self,
        terms: impl Iterator<Item = (&'a str, i64)>,
        tokens: impl Iterator<Item = &'a str>,
    ) {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                eprintln!("vocab: listener lock poisoned; this document's counts are lost");
                poisoned.into_inner()
            }
        };
        for (term, frequency) in terms {
            if frequency > 0 {
                guard.terms.add_term(term, frequency as u64);
            }
        }
        for token in tokens {
            guard.tokens.add_term(token, 1);
        }
        guard.terms.add_document();
        guard.tokens.add_document();
        if guard.terms.documents() >= self.window_docs {
            self.rollover_locked(&mut guard, "window size reached");
        }
    }

    /// Seals and persists the live window and starts a fresh one. An empty
    /// window is not persisted.
    pub fn snapshot_now(&self) -> Option<SnapshotMeta> {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.rollover_locked(&mut guard, "explicit snapshot request")
    }

    /// Documents accumulated in the live window.
    pub fn window_documents(&self) -> u64 {
        match self.inner.lock() {
            Ok(guard) => guard.terms.documents(),
            Err(poisoned) => poisoned.into_inner().terms.documents(),
        }
    }

    /// Live-window statistics, one entry per channel.
    pub fn live_stats(&self) -> Vec<VocabChannelStats> {
        let guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        [&guard.terms, &guard.tokens]
            .into_iter()
            .map(|state| VocabChannelStats {
                channel: state.channel() as i32,
                documents: state.documents(),
                term_occurrences: state.occurrences(),
                cardinality_estimate: state.cardinality_estimate(),
                heavy_hitters: state
                    .heavy_hitters()
                    .into_iter()
                    .map(|entry| VocabHeavyHitter {
                        term: entry.term,
                        count: entry.count,
                    })
                    .collect(),
            })
            .collect()
    }

    /// The persisted snapshots, ordered by sequence ascending.
    pub fn snapshots(&self) -> Vec<SnapshotMeta> {
        match self.inner.lock() {
            Ok(guard) => guard.snapshots.clone(),
            Err(poisoned) => poisoned.into_inner().snapshots.clone(),
        }
    }

    /// Seals the live window if it holds documents (graceful-shutdown
    /// counterpart of the Java shutdown hook; Rust has no hook mechanism,
    /// so the node calls this explicitly).
    pub fn persist_on_shutdown(&self) {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.rollover_locked(&mut guard, "process shutdown");
    }

    /// Computes the drift between two windows, per channel. Either
    /// reference may be [`LIVE`] for the current window, a snapshot file
    /// name, or a bare sequence number; a named snapshot's sketches are
    /// loaded from disk for the comparison. `embedding_vocabulary` applies
    /// to the TOKENS channel only (surface forms against the model
    /// vocabulary; TERMS identity is folded/stemmed and not a model input).
    pub fn drift(
        &self,
        from_ref: &str,
        to_ref: &str,
        embedding_vocabulary: Option<&HashSet<String>>,
    ) -> Result<Vec<ChannelDrift>, String> {
        let from = self.resolve(from_ref)?;
        let to = self.resolve(to_ref)?;
        let mut out = Vec::with_capacity(2);
        for channel in [VocabChannel::Terms, VocabChannel::Tokens] {
            let from_state = from
                .get(&channel)
                .ok_or_else(|| format!("from snapshot {from_ref:?} has no {channel:?} channel"))?;
            let to_state = to
                .get(&channel)
                .ok_or_else(|| format!("to snapshot {to_ref:?} has no {channel:?} channel"))?;
            let vocabulary = if channel == VocabChannel::Tokens {
                embedding_vocabulary
            } else {
                None
            };
            out.push(ChannelDrift {
                channel,
                metrics: compute_drift(from_state, to_state, vocabulary),
            });
        }
        Ok(out)
    }

    /// Resolves a drift reference to per-channel window state. The live
    /// window resolves to clones taken under the lock (a drift comparison
    /// must not hold the lock while it computes, nor race a rollover).
    fn resolve(&self, reference: &str) -> Result<HashMap<VocabChannel, WindowState>, String> {
        let reference = reference.trim();
        if reference.is_empty() {
            return Err("snapshot reference must not be empty".to_string());
        }
        if reference.eq_ignore_ascii_case(LIVE) {
            let guard = match self.inner.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            return Ok(HashMap::from([
                (VocabChannel::Terms, clone_state(&guard.terms)),
                (VocabChannel::Tokens, clone_state(&guard.tokens)),
            ]));
        }
        let file = {
            let guard = match self.inner.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            locate_snapshot(&guard, reference)
        };
        let Some(file) = file else {
            return Err(format!("unknown snapshot '{reference}'"));
        };
        let snapshot = load_snapshot_file(&file)?;
        let mut states = HashMap::with_capacity(2);
        for channel_snapshot in &snapshot.channels {
            let state = WindowState::from_proto(channel_snapshot)?;
            states.insert(state.channel(), state);
        }
        Ok(states)
    }

    /// Seals the live window, persists it, and starts fresh. Returns
    /// `None` when the window holds no documents — an empty snapshot is
    /// noise, and every caller (rollover, explicit request, shutdown) is
    /// fine with none.
    fn rollover_locked(&self, guard: &mut Inner, reason: &str) -> Option<SnapshotMeta> {
        let documents = guard.terms.documents();
        if documents == 0 {
            return None;
        }
        let sealed_millis = now_millis();
        let snapshot = VocabSnapshot {
            sequence: guard.next_sequence,
            started_epoch_millis: guard.window_started_millis,
            sealed_epoch_millis: sealed_millis,
            channels: vec![guard.terms.to_proto(), guard.tokens.to_proto()],
        };
        let file_name = format!("snapshot-{}-{sealed_millis}.pb", guard.next_sequence);
        let target = self.vocab_dir.join(&file_name);
        let temp = self.vocab_dir.join(format!("{file_name}.tmp"));
        let persisted = std::fs::write(&temp, snapshot.encode_to_vec())
            .and_then(|()| std::fs::rename(&temp, &target));
        if let Err(e) = persisted {
            eprintln!(
                "vocab: could not persist snapshot {}; the window continues instead of \
                 rolling over: {e}",
                target.display()
            );
            return None;
        }
        let meta = SnapshotMeta {
            size_bytes: std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0),
            name: file_name,
            sequence: snapshot.sequence,
            started_epoch_millis: snapshot.started_epoch_millis,
            sealed_epoch_millis: snapshot.sealed_epoch_millis,
            documents,
        };
        guard.files_by_sequence.insert(guard.next_sequence, target);
        eprintln!(
            "vocab: snapshot {} sealed ({} documents, {})",
            meta.name, documents, reason
        );
        guard.snapshots.push(meta.clone());
        guard.next_sequence += 1;
        guard.terms = WindowState::new(VocabChannel::Terms, self.top_k);
        guard.tokens = WindowState::new(VocabChannel::Tokens, self.top_k);
        guard.window_started_millis = sealed_millis;
        Some(meta)
    }
}

/// A detached copy of a window's state (sketches and counters), so a drift
/// comparison over the live window runs without holding the listener lock.
fn clone_state(state: &WindowState) -> WindowState {
    WindowState::from_proto(&state.to_proto()).expect("own serialization round-trips")
}

/// Finds a snapshot file by file name or bare sequence number.
fn locate_snapshot(guard: &Inner, reference: &str) -> Option<PathBuf> {
    if let Ok(sequence) = reference.parse::<u64>() {
        if let Some(file) = guard.files_by_sequence.get(&sequence) {
            return Some(file.clone());
        }
    }
    guard
        .files_by_sequence
        .values()
        .find(|file| {
            file.file_name()
                .map(|name| name == std::ffi::OsStr::new(reference))
                .unwrap_or(false)
        })
        .cloned()
}

/// Indexes the prior snapshots in the directory: metadata only — the
/// sketches are parsed and immediately dropped; they are loaded again when
/// a drift request names the snapshot. An unreadable snapshot is skipped
/// with a warning: one corrupt file must not disable the listener.
fn scan_snapshots(vocab_dir: &Path) -> io::Result<(Vec<SnapshotMeta>, HashMap<u64, PathBuf>, u64)> {
    let mut found = Vec::new();
    let mut files = HashMap::new();
    for entry in std::fs::read_dir(vocab_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if parse_snapshot_name(&name).is_none() {
            continue;
        }
        match load_snapshot_file(&path) {
            Ok(snapshot) => {
                let documents = snapshot
                    .channels
                    .iter()
                    .map(|c| c.documents)
                    .max()
                    .unwrap_or(0);
                found.push(SnapshotMeta {
                    size_bytes: entry.metadata().map(|m| m.len()).unwrap_or(0),
                    name,
                    sequence: snapshot.sequence,
                    started_epoch_millis: snapshot.started_epoch_millis,
                    sealed_epoch_millis: snapshot.sealed_epoch_millis,
                    documents,
                });
                files.insert(snapshot.sequence, path);
            }
            Err(e) => eprintln!(
                "vocab: skipping unreadable snapshot {}: {e}",
                path.display()
            ),
        }
    }
    found.sort_by_key(|meta| meta.sequence);
    let next_sequence = found
        .iter()
        .map(|meta| meta.sequence)
        .max()
        .map_or(0, |max| max + 1);
    Ok((found, files, next_sequence))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique temp directory per test (the config tests' `target/tmp`
    /// pattern; no tempfile dependency in this crate).
    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "vocab_test_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    /// FNV-1a 64 + murmur3 finalizer over raw bytes — the digest the Java
    /// scratch generator printed over its serialized sketches, so the
    /// cross-language vectors below pin byte-level interchangeability.
    fn byte_digest(bytes: &[u8]) -> u64 {
        let mut hash = FNV_OFFSET;
        for &b in bytes {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        fmix64(hash)
    }

    fn feed_terms(listener: &VocabularyListener, terms: &[(&str, i64)], tokens: &[&str]) {
        listener.feed(terms.iter().copied(), tokens.iter().copied());
    }

    fn channel_drift(drift: &[ChannelDrift], channel: VocabChannel) -> &DriftMetrics {
        &drift.iter().find(|d| d.channel == channel).unwrap().metrics
    }

    // ---- Count-min sketch (mirrors CountMinSketchTest) ----

    /// A Zipf-ish stream: term i occurs ~1/i of the lead term's count.
    fn skewed_stream(
        sketch: &mut CountMinSketch,
        distinct: usize,
        lead_count: u64,
    ) -> Vec<(String, u64)> {
        let mut truth = Vec::new();
        for i in 1..=distinct {
            let term = format!("term-{i}");
            let count = (lead_count / i as u64).max(1);
            truth.push((term.clone(), count));
            for _ in 0..count {
                sketch.add(&term, 1);
            }
        }
        truth
    }

    #[test]
    fn cms_estimates_never_undershoot_and_stay_close_for_heavy_terms() {
        let mut sketch = CountMinSketch::new();
        let truth = skewed_stream(&mut sketch, 2_000, 10_000);

        for (term, count) in &truth {
            assert!(
                sketch.estimate(term) >= *count,
                "estimate of {term} undershoots"
            );
        }
        // Heavy terms sit far above the collision noise floor; their
        // estimates should be within a few percent of the truth.
        for (i, (_, count)) in truth.iter().enumerate().take(20) {
            let ratio = sketch.estimate(&format!("term-{}", i + 1)) as f64 / *count as f64;
            assert!(
                (1.0..=1.05).contains(&ratio),
                "relative error of term-{} out of band: {ratio}",
                i + 1
            );
        }
        // A term that never occurred still gets the one-sided estimate of
        // the noise floor, not a negative number.
        assert_eq!(sketch.estimate("never-seen"), 0);
    }

    #[test]
    fn cms_merge_combines_as_if_one_sketch_saw_everything() {
        let mut left = CountMinSketch::new();
        let mut right = CountMinSketch::new();
        let mut all = CountMinSketch::new();
        for i in 0..500u64 {
            left.add(&format!("term-{i}"), i + 1);
            right.add(&format!("term-{i}"), 2 * (i + 1));
            all.add(&format!("term-{i}"), 3 * (i + 1));
        }
        left.merge(&right).unwrap();

        for i in 0..500 {
            assert_eq!(
                left.estimate(&format!("term-{i}")),
                all.estimate(&format!("term-{i}"))
            );
        }
        assert_eq!(left.total_count(), all.total_count());
    }

    #[test]
    fn cms_snapshot_round_trip_restores_identical_estimates() {
        let mut sketch = CountMinSketch::new();
        let truth = skewed_stream(&mut sketch, 200, 5_000);

        let restored = CountMinSketch::from_bytes(
            sketch.depth(),
            sketch.width(),
            &sketch.to_bytes(),
            sketch.total_count(),
        )
        .unwrap();

        assert_eq!(restored.total_count(), sketch.total_count());
        for (term, _) in &truth {
            assert_eq!(restored.estimate(term), sketch.estimate(term));
        }
    }

    // ---- HyperLogLog (mirrors HyperLogLogTest) ----

    fn hll_filled(prefix: &str, distinct: usize) -> HyperLogLog {
        let mut sketch = HyperLogLog::new();
        for i in 0..distinct {
            sketch.add(&format!("{prefix}{i}"));
        }
        sketch
    }

    #[test]
    fn hll_empty_sketch_estimates_zero() {
        assert_eq!(HyperLogLog::new().estimate(), 0.0);
    }

    #[test]
    fn hll_cardinality_at_10k_distinct() {
        let estimate = hll_filled("term-", 10_000).estimate();
        assert!((0.95..=1.05).contains(&(estimate / 10_000.0)), "{estimate}");
    }

    #[test]
    fn hll_cardinality_at_100k_distinct() {
        let estimate = hll_filled("term-", 100_000).estimate();
        assert!(
            (0.95..=1.05).contains(&(estimate / 100_000.0)),
            "{estimate}"
        );
    }

    #[test]
    fn hll_duplicates_do_not_count() {
        let mut sketch = HyperLogLog::new();
        for i in 0..1_000 {
            sketch.add(&format!("term-{}", i % 100));
        }
        assert!((0.90..=1.10).contains(&(sketch.estimate() / 100.0)));
    }

    #[test]
    fn hll_merge_estimates_the_union() {
        // 60k + 60k distinct terms with a 20k overlap: union is 100k.
        let mut left = hll_filled("term-", 60_000);
        let mut right = HyperLogLog::new();
        for i in 40_000..100_000 {
            right.add(&format!("term-{i}"));
        }

        left.merge(&right).unwrap();

        assert!((0.95..=1.05).contains(&(left.estimate() / 100_000.0)));
    }

    #[test]
    fn hll_snapshot_round_trip_restores_identical_estimate() {
        let sketch = hll_filled("term-", 10_000);
        let restored = HyperLogLog::from_bytes(sketch.precision(), &sketch.to_bytes()).unwrap();
        assert_eq!(restored.estimate(), sketch.estimate());
    }

    // ---- Heavy hitters (mirrors HeavyHittersTest) ----

    #[test]
    fn hh_true_heavy_hitters_survive_the_tail() {
        let mut hitters = HeavyHitters::new(16);
        let heavy: Vec<String> = (0..10).map(|h| format!("heavy-{h}")).collect();
        for round in 0..1_000 {
            for term in &heavy {
                hitters.add(term, 1);
            }
            hitters.add(&format!("tail-{round}"), 1);
        }

        let snapshot = hitters.snapshot();
        assert_eq!(snapshot.len(), 16);
        let counts: HashMap<&str, u64> = snapshot
            .iter()
            .map(|e| (e.term.as_str(), e.count))
            .collect();
        for term in &heavy {
            // Space-saving overestimates by at most the minimum count in
            // the table; at 1000 true occurrences the heavy terms are
            // near-exact.
            let count = *counts.get(term.as_str()).expect("heavy term survives");
            assert!((1_000..=1_050).contains(&count), "{term} at {count}");
        }
        // Ordered by count descending: the heavy block leads.
        for entry in snapshot.iter().take(heavy.len()) {
            assert!(entry.count >= 1_000);
        }
    }

    #[test]
    fn hh_tracked_terms_accumulate_exactly() {
        let mut hitters = HeavyHitters::new(4);
        hitters.add("alpha", 3);
        hitters.add("alpha", 4);
        hitters.add("beta", 2);

        assert_eq!(hitters.estimate("alpha"), 7);
        assert_eq!(hitters.estimate("beta"), 2);
        assert_eq!(hitters.estimate("gamma"), 0);
        let terms: Vec<String> = hitters.snapshot().into_iter().map(|e| e.term).collect();
        assert_eq!(terms, ["alpha", "beta"]);
    }

    #[test]
    fn hh_restore_rebuilds_the_list() {
        let mut hitters = HeavyHitters::new(8);
        hitters.add("alpha", 5);
        let snapshot = hitters.snapshot();

        let mut restored = HeavyHitters::new(8);
        restored.restore(snapshot.clone());

        assert_eq!(restored.estimate("alpha"), 5);
        assert_eq!(restored.snapshot(), snapshot);
    }

    // ---- Window/listener machinery (mirrors VocabularyListenerTest) ----

    #[test]
    fn rollover_persists_a_snapshot_and_starts_fresh() {
        let dir = temp_dir("rollover");
        let listener = VocabularyListener::create(&dir, 3, 16).unwrap();
        for _ in 0..3 {
            feed_terms(
                &listener,
                &[("alpha", 2), ("beta", 1)],
                &["Alpha", "beta", "alpha"],
            );
        }

        // The third document rolls the window over: a snapshot file exists
        // and the live window is empty again.
        let snapshots_on_disk = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.starts_with("snapshot-") && name.ends_with(".pb")
            })
            .count();
        assert_eq!(snapshots_on_disk, 1);
        assert_eq!(listener.window_documents(), 0);
        assert_eq!(listener.snapshots().len(), 1);

        // The next document opens the next window.
        feed_terms(&listener, &[("gamma", 1)], &["gamma"]);
        assert_eq!(listener.window_documents(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn snapshot_round_trip_keeps_identical_estimates() {
        let dir = temp_dir("roundtrip");
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        for _ in 0..10 {
            feed_terms(
                &listener,
                &[("court", 5), ("ruling", 2), ("appeal", 1)],
                &["Court", "court", "ruling"],
            );
        }
        let live_cardinality = listener
            .live_stats()
            .iter()
            .find(|s| s.channel == VocabChannel::Terms as i32)
            .unwrap()
            .cardinality_estimate;

        let meta = listener.snapshot_now().expect("window was fed");

        // Round-trip through the persisted bytes: same cardinality, same
        // heavy hitters, same count-min point estimates.
        let snapshot = load_snapshot_file(&dir.join(&meta.name)).unwrap();
        let channel = snapshot
            .channels
            .iter()
            .find(|c| c.channel == VocabChannel::Terms as i32)
            .unwrap();
        let restored = WindowState::from_proto(channel).unwrap();
        assert_eq!(restored.cardinality_estimate(), live_cardinality);
        assert_eq!(restored.documents(), 10);
        assert_eq!(restored.occurrences(), 80);
        assert_eq!(restored.estimate("court"), 50);
        assert_eq!(restored.estimate("ruling"), 20);
        assert_eq!(restored.estimate("appeal"), 10);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explicit_snapshot_on_an_empty_window_persists_nothing() {
        let dir = temp_dir("empty");
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        assert!(listener.snapshot_now().is_none());
        assert!(listener.snapshots().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn disjoint_windows_have_novelty_one() {
        let dir = temp_dir("disjoint");
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        for _ in 0..5 {
            feed_terms(&listener, &[("alpha", 2), ("beta", 1)], &["alpha", "beta"]);
        }
        let older = listener.snapshot_now().unwrap().name;
        for _ in 0..5 {
            feed_terms(
                &listener,
                &[("gamma", 2), ("delta", 1)],
                &["gamma", "delta"],
            );
        }

        let drift = listener.drift(&older, LIVE, None).unwrap();
        let terms = channel_drift(&drift, VocabChannel::Terms);
        assert!(terms.novelty_rate > 0.9, "{}", terms.novelty_rate);
        // Disjoint vocabularies are also maximally divergent distributions.
        assert!(terms.jensen_shannon_divergence > 0.9);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn identical_distributions_have_no_drift() {
        let dir = temp_dir("identical");
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        for _ in 0..5 {
            feed_terms(&listener, &[("alpha", 2), ("beta", 1)], &["alpha", "beta"]);
        }
        let older = listener.snapshot_now().unwrap().name;
        for _ in 0..5 {
            feed_terms(&listener, &[("alpha", 2), ("beta", 1)], &["alpha", "beta"]);
        }

        let drift = listener.drift(&older, LIVE, None).unwrap();
        let terms = channel_drift(&drift, VocabChannel::Terms);
        assert!(terms.novelty_rate < 0.05, "{}", terms.novelty_rate);
        assert!(terms.jensen_shannon_divergence < 0.01);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn shifted_frequencies_have_positive_divergence_but_no_novelty() {
        let dir = temp_dir("shifted");
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        for _ in 0..5 {
            feed_terms(&listener, &[("alpha", 10), ("beta", 1)], &["alpha", "beta"]);
        }
        let older = listener.snapshot_now().unwrap().name;
        for _ in 0..5 {
            feed_terms(&listener, &[("alpha", 1), ("beta", 10)], &["alpha", "beta"]);
        }

        let drift = listener.drift(&older, LIVE, None).unwrap();
        let terms = channel_drift(&drift, VocabChannel::Terms);
        assert!(terms.novelty_rate < 0.05, "{}", terms.novelty_rate);
        assert!(terms.jensen_shannon_divergence > 0.05);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn embedding_coverage_reads_the_model_vocabulary() {
        let dir = temp_dir("coverage");
        let embeddings = dir.join("embeddings");
        std::fs::create_dir_all(&embeddings).unwrap();
        std::fs::write(embeddings.join("vocab.txt"), "alpha\nbeta\n").unwrap();
        let vocabulary = load_embedding_vocabulary(&embeddings).expect("vocab.txt loads");
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        for _ in 0..5 {
            // Covered: alpha x2, beta x1 per doc. OOV: gamma x1 per doc.
            feed_terms(
                &listener,
                &[("alpha", 2), ("beta", 1), ("gamma", 1)],
                &["alpha", "alpha", "beta", "gamma"],
            );
        }
        let older = listener.snapshot_now().unwrap().name;
        for _ in 0..5 {
            feed_terms(
                &listener,
                &[("alpha", 2), ("beta", 1), ("gamma", 1)],
                &["alpha", "alpha", "beta", "gamma"],
            );
        }

        let drift = listener.drift(&older, LIVE, Some(&vocabulary)).unwrap();
        let tokens = channel_drift(&drift, VocabChannel::Tokens);
        assert!(tokens.embedding_oov_computed);
        // One of four token occurrences per document is OOV.
        assert!((tokens.embedding_oov_share - 0.25).abs() < 0.01);
        // Coverage is a TOKENS property; TERMS never reports it.
        assert!(!channel_drift(&drift, VocabChannel::Terms).embedding_oov_computed);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn tokenizer_json_vocab_is_read_too() {
        let dir = temp_dir("tokenizer");
        let embeddings = dir.join("embeddings");
        std::fs::create_dir_all(&embeddings).unwrap();
        std::fs::write(
            embeddings.join("tokenizer.json"),
            r#"{"model": {"type": "BPE", "vocab": {"alpha": 0, "beä": 1}}}"#,
        )
        .unwrap();
        let vocabulary = load_embedding_vocabulary(&embeddings).expect("tokenizer.json loads");
        assert!(vocabulary.contains("alpha"));
        assert!(vocabulary.contains("beä"));

        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        feed_terms(&listener, &[("alpha", 1)], &["alpha", "gamma"]);

        let drift = listener.drift(LIVE, LIVE, Some(&vocabulary)).unwrap();
        let tokens = channel_drift(&drift, VocabChannel::Tokens);
        assert!(tokens.embedding_oov_computed);
        assert!((tokens.embedding_oov_share - 0.5).abs() < 0.01);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unknown_snapshot_references_are_rejected() {
        let dir = temp_dir("unknown");
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        feed_terms(&listener, &[("alpha", 1)], &["alpha"]);
        let err = listener.drift("snapshot-99-1.pb", LIVE, None).unwrap_err();
        assert!(err.contains("unknown snapshot"), "{err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn restart_resumes_the_snapshot_history() {
        let dir = temp_dir("restart");
        {
            let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
            feed_terms(&listener, &[("alpha", 1)], &["alpha"]);
            listener.snapshot_now();
        }

        let resumed = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        assert_eq!(resumed.snapshots().len(), 1);
        assert_eq!(resumed.snapshots()[0].documents, 1);
        // The next snapshot continues the sequence rather than clobbering.
        feed_terms(&resumed, &[("beta", 1)], &["beta"]);
        assert_eq!(resumed.snapshot_now().unwrap().sequence, 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- Merge across snapshots (coordinator aggregation) ----

    #[test]
    fn window_merge_combines_two_shards_windows() {
        let mut left = WindowState::new(VocabChannel::Terms, 8);
        let mut right = WindowState::new(VocabChannel::Terms, 8);
        for _ in 0..4 {
            left.add_term("alpha", 3);
            left.add_term("beta", 1);
            left.add_document();
        }
        for _ in 0..6 {
            right.add_term("alpha", 1);
            right.add_term("gamma", 2);
            right.add_document();
        }
        let union_cardinality = {
            let mut merged = left.cardinality_sketch();
            merged.merge(&right.cardinality_sketch()).unwrap();
            merged.estimate()
        };

        left.merge(&right).unwrap();

        assert_eq!(left.documents(), 10);
        assert_eq!(left.occurrences(), 4 * 4 + 6 * 3);
        // Exact counts for the heavy terms survive the merge.
        assert_eq!(left.estimate("alpha"), 12 + 6);
        assert_eq!(left.estimate("beta"), 4);
        assert_eq!(left.estimate("gamma"), 12);
        assert_eq!(left.cardinality_estimate(), union_cardinality);
    }

    #[test]
    fn merge_snapshots_aggregates_shard_histories() {
        let dir_a = temp_dir("merge_a");
        let dir_b = temp_dir("merge_b");
        let listener_a = VocabularyListener::create(&dir_a, 1_000, 8).unwrap();
        let listener_b = VocabularyListener::create(&dir_b, 1_000, 8).unwrap();
        for _ in 0..3 {
            feed_terms(
                &listener_a,
                &[("alpha", 2), ("beta", 1)],
                &["alpha", "beta"],
            );
            feed_terms(
                &listener_b,
                &[("alpha", 1), ("gamma", 5)],
                &["alpha", "gamma"],
            );
        }
        let snap_a =
            load_snapshot_file(&dir_a.join(listener_a.snapshot_now().unwrap().name)).unwrap();
        let snap_b =
            load_snapshot_file(&dir_b.join(listener_b.snapshot_now().unwrap().name)).unwrap();

        let merged = merge_snapshots(&[snap_a, snap_b]).unwrap();
        let terms = merged
            .channels
            .iter()
            .find(|c| c.channel == VocabChannel::Terms as i32)
            .unwrap();
        let state = WindowState::from_proto(terms).unwrap();
        assert_eq!(state.documents(), 6);
        // alpha: 3x2 + 3x1 occurrences across the two shards.
        assert_eq!(state.estimate("alpha"), 9);
        assert_eq!(state.estimate("beta"), 3);
        assert_eq!(state.estimate("gamma"), 15);
        assert_eq!(state.occurrences(), 3 * 3 + 3 * 6);
        std::fs::remove_dir_all(&dir_a).ok();
        std::fs::remove_dir_all(&dir_b).ok();
    }

    // ---- Cross-language vectors (generated by the Java reference
    // implementation; see the generator scratch notes in
    // docs/VOCABULARY-INDEX.md). These pin byte-level snapshot
    // interchangeability between the two implementations. ----

    #[test]
    fn cross_language_base_hashes_match_java() {
        assert_eq!(hash_base("the"), 0xcb3f_f435_b889_fb31);
        assert_eq!(hash_base("court"), 0x85ca_717a_7936_b7fc);
        assert_eq!(hash_base("Appeal"), 0x0c5d_e86f_5b4a_1ac6);
        assert_eq!(hash_base("jurisdiction"), 0x081e_e7a8_f1c0_aa28);
        assert_eq!(hash_base("beä"), 0x4997_89b1_495b_f673);
        assert_eq!(hash_base(""), 0xefd0_1f60_ba99_2926);
    }

    #[test]
    fn cross_language_cms_bytes_match_java() {
        let mut sketch = CountMinSketch::new();
        sketch.add("court", 50);
        sketch.add("ruling", 20);
        sketch.add("appeal", 10);

        assert_eq!(sketch.estimate("court"), 50);
        assert_eq!(sketch.estimate("ruling"), 20);
        assert_eq!(sketch.estimate("appeal"), 10);
        assert_eq!(sketch.estimate("never-seen"), 0);
        assert_eq!(sketch.total_count(), 80);
        assert_eq!(byte_digest(&sketch.to_bytes()), 0x1baf_0248_f3e8_fee8);
    }

    #[test]
    fn cross_language_hll_registers_match_java() {
        let mut sketch = HyperLogLog::new();
        for i in 0..1000 {
            sketch.add(&format!("term-{i}"));
        }
        assert!((sketch.estimate() - 995.648975995318).abs() < 1e-9);
        assert_eq!(byte_digest(&sketch.to_bytes()), 0x0f92_de4f_662a_26ba);
    }

    #[test]
    fn cross_language_snapshot_bytes_match_java() {
        let mut state = WindowState::new(VocabChannel::Terms, 4);
        state.add_term("court", 50);
        state.add_term("ruling", 20);
        state.add_term("appeal", 10);
        state.add_term("motion", 5);
        state.add_document();
        state.add_document();
        state.add_document();
        let snapshot = VocabSnapshot {
            sequence: 7,
            started_epoch_millis: 1_700_000_000_000,
            sealed_epoch_millis: 1_700_000_600_000,
            channels: vec![state.to_proto()],
        };
        let bytes = snapshot.encode_to_vec();

        assert_eq!(bytes.len(), 5_259_355);
        assert_eq!(byte_digest(&bytes), 0x040e_602d_29bd_1f28);
        let hitters: Vec<(String, u64)> = state
            .heavy_hitters()
            .into_iter()
            .map(|e| (e.term, e.count))
            .collect();
        assert_eq!(
            hitters,
            [
                ("court".to_string(), 50),
                ("ruling".to_string(), 20),
                ("appeal".to_string(), 10),
                ("motion".to_string(), 5)
            ]
        );
    }
}
