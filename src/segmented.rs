//! The segmented shard (`docs/immutable-segments.md`): sealed segments
//! from a [`SegmentCatalog`] plus one heap tail, read as one shard.
//!
//! Local document ids are one positional space: segment `i` covers
//! `[base_i, base_i + rows_i)` in catalog order, and the tail starts
//! where the last segment ends. Every read routes a document id to its
//! part and offsets the answer back; every aggregate sums the parts.
//! Dictionaries are the one place a union needs its own state: each
//! part's facet, map-key, and map-value ordinals are local to that
//! part's file, so this shard keeps one global dictionary per column
//! (byte-sorted over the sealed parts, with the tail's new values after
//! it) and a remap from each part's ordinals into it. Callers see global
//! ordinals everywhere, so the filter, facet, projection, and highlight
//! machinery works unchanged.
//!
//! Block-max pruning survives sealing: a term's impacts across the
//! sealed parts chain into one [`ImpactCursor`]. A term the tail holds
//! has no impacts until the next flush seals the tail, and the scorer
//! takes its exact heap path for that query — the same answer, slower.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::postings::{
    AnalyzedDoc, Bm25Index, Bm25Store, DocLineage, FileImpacts, ImpactCursor, PostingCallback,
    StoredBinding,
};
use crate::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSummary};

// Heap tails and mapped segments expose the same ordered schema tables.
// Compare them before changing which part supplies a column ordinal.
macro_rules! same_tables {
    ($other:expr, $tail:expr) => {{
        let other = $other;
        let tail = $tail;
        let same_fields = other.field_count() == tail.field_count()
            && (0..tail.field_count()).all(|f| {
                other.field_name(f) == tail.field_name(f)
                    && other.field_has_positions(f) == tail.field_has_positions(f)
                    && other.field_has_sentences(f) == tail.field_has_sentences(f)
            });
        let same_columns = other.facet_count() == tail.facet_count()
            && (0..tail.facet_count()).all(|i| other.facet_name(i) == tail.facet_name(i))
            && other.numeric_count() == tail.numeric_count()
            && (0..tail.numeric_count()).all(|i| other.numeric_name(i) == tail.numeric_name(i))
            && other.integer_count() == tail.integer_count()
            && (0..tail.integer_count()).all(|i| other.integer_name(i) == tail.integer_name(i))
            && other.unsigned_integer_count() == tail.unsigned_integer_count()
            && (0..tail.unsigned_integer_count())
                .all(|i| other.unsigned_integer_name(i) == tail.unsigned_integer_name(i))
            && other.geo_count() == tail.geo_count()
            && (0..tail.geo_count()).all(|i| other.geo_name(i) == tail.geo_name(i))
            && other.map_facet_count() == tail.map_facet_count()
            && (0..tail.map_facet_count())
                .all(|i| other.map_facet_name(i) == tail.map_facet_name(i))
            && other.map_numeric_count() == tail.map_numeric_count()
            && (0..tail.map_numeric_count())
                .all(|i| other.map_numeric_name(i) == tail.map_numeric_name(i));
        same_fields && same_columns
    }};
}

/// One column's global dictionary over the sealed parts and the tail.
#[derive(Debug, Default)]
struct UnionDict {
    /// Values in global ordinal order: the byte-sorted union of the
    /// sealed parts first, then the tail's values that none of them had,
    /// in the order the tail met them.
    values: Vec<String>,
    /// How many leading `values` are the sorted sealed union.
    sorted_len: usize,
    index: HashMap<String, u32>,
    /// Per sealed part: local ordinal → global ordinal.
    remaps: Vec<Vec<u32>>,
    /// The frozen part's local ordinal → global ordinal: the tail remap
    /// as it stood when a seal froze that tail.
    frozen_remap: Vec<u32>,
    /// Tail local ordinal → global ordinal, grown by `sync_tail`.
    tail_remap: Vec<u32>,
}

/// Which part of the shard a local ordinal or document id belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Sealed(usize),
    Frozen,
    Tail,
}

impl UnionDict {
    fn build(parts: &[&[String]]) -> Self {
        let mut values: Vec<String> = parts.iter().flat_map(|d| d.iter().cloned()).collect();
        values.sort_unstable();
        values.dedup();
        let index: HashMap<String, u32> = values
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i as u32))
            .collect();
        let remaps = parts
            .iter()
            .map(|d| d.iter().map(|v| index[v]).collect())
            .collect();
        UnionDict {
            sorted_len: values.len(),
            values,
            index,
            remaps,
            frozen_remap: Vec::new(),
            tail_remap: Vec::new(),
        }
    }

    /// The tail is now the frozen part: its remap moves over whole and a
    /// fresh tail starts with none.
    fn freeze_tail(&mut self) {
        self.frozen_remap = std::mem::take(&mut self.tail_remap);
    }

    /// Extend the tail remap for tail ordinals seen since the last sync.
    fn sync_tail(&mut self, tail_values: &[String]) {
        for value in &tail_values[self.tail_remap.len()..] {
            let ord = match self.index.get(value) {
                Some(&ord) => ord,
                None => {
                    let ord = self.values.len() as u32;
                    self.values.push(value.clone());
                    self.index.insert(value.clone(), ord);
                    ord
                }
            };
            self.tail_remap.push(ord);
        }
    }

    fn sorted(&self) -> bool {
        self.values.len() == self.sorted_len
    }

    fn ord_of(&self, value: &str) -> Option<u32> {
        self.index.get(value).copied()
    }

    fn to_global(&self, source: Source, local: u32) -> Option<u32> {
        let remap = match source {
            Source::Sealed(i) => &self.remaps[i],
            Source::Frozen => &self.frozen_remap,
            Source::Tail => &self.tail_remap,
        };
        remap.get(local as usize).copied()
    }
}

/// Per part: the reverse key maps a routed read needs, cached so a
/// per-document lookup is an index, not a search.
#[derive(Debug, Default)]
struct KeyReverse {
    /// Per part (sealed parts, then the frozen part at `parts`, then the
    /// tail at `parts + 1`): global key ordinal → local key ordinal,
    /// `u32::MAX` when absent.
    per_part: Vec<Vec<u32>>,
}

impl KeyReverse {
    fn build(dict: &UnionDict, frozen_keys: usize, tail_keys: usize) -> Self {
        let n = dict.values.len();
        let reverse_of = |remap: &[u32], keys: usize| {
            let mut reverse = vec![u32::MAX; n];
            for (local, &global) in remap.iter().enumerate().take(keys) {
                reverse[global as usize] = local as u32;
            }
            reverse
        };
        let mut per_part: Vec<Vec<u32>> = dict
            .remaps
            .iter()
            .map(|remap| reverse_of(remap, remap.len()))
            .collect();
        per_part.push(reverse_of(&dict.frozen_remap, frozen_keys));
        per_part.push(reverse_of(&dict.tail_remap, tail_keys));
        KeyReverse { per_part }
    }
}

/// One sealed part's placement in the shard's id space.
#[derive(Debug, Clone, Copy)]
struct Part {
    base: u32,
    rows: u32,
}

/// Where a local document id lives: a sealed part, the frozen part a
/// seal in flight is writing out, or the tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    Sealed { part: usize, local: u32 },
    Frozen { local: u32 },
    Tail { local: u32 },
}

/// A former tail, frozen while a seal writes it out: read-only, shared
/// with the seal through the `Arc`, and served as one more part until
/// the catalog publishes it as a segment.
struct Frozen {
    base: u32,
    rows: u32,
    store: Arc<Bm25Store>,
}

pub struct SegmentedShard {
    catalog: SegmentCatalog,
    set: Arc<OpenedSegmentSet>,
    parts: Vec<Part>,
    frozen: Option<Frozen>,
    tail: Bm25Store,
    tail_base: u32,
    facets: Vec<UnionDict>,
    map_keys: Vec<UnionDict>,
    map_values: Vec<UnionDict>,
    map_key_reverse: Vec<KeyReverse>,
    map_numeric_keys: Vec<UnionDict>,
    map_numeric_key_reverse: Vec<KeyReverse>,
}

impl std::fmt::Debug for SegmentedShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentedShard")
            .field("root", &self.set.root())
            .field("parts", &self.parts)
            .field("frozen_rows", &self.frozen.as_ref().map(|f| f.rows))
            .field("tail_base", &self.tail_base)
            .field("tail_docs", &self.tail.next_doc_id())
            .finish()
    }
}

impl SegmentedShard {
    /// Retain identity metadata for this segment set, frozen seal and tail.
    /// The view does not retain the source blobs or mapped index files.
    pub fn identity_snapshot(&self) -> Result<crate::source_archive::IdentitySnapshot, String> {
        let mut parts = Vec::with_capacity(self.parts.len() + 2);
        for (index, part) in self.parts.iter().enumerate() {
            parts.push((part.base, part.rows, self.reader(index).identity_snapshot()));
        }
        if let Some(frozen) = &self.frozen {
            parts.push((frozen.base, frozen.rows, frozen.store.identity_snapshot()));
        }
        parts.push((
            self.tail_base,
            self.tail.next_doc_id(),
            self.tail.identity_snapshot(),
        ));
        crate::source_archive::IdentitySnapshot::from_parts(parts)
    }

    pub fn document_identity(&self, doc: u32) -> Option<crate::pb::DocumentIdentity> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).document_identity(local),
            Placement::Frozen { local } => self.frozen_store().document_identity(local),
            Placement::Tail { local } => self.tail.document_identity(local),
        }
    }

    pub fn protobuf_source(
        &self,
        doc: u32,
    ) -> std::io::Result<Option<(crate::pb::ProtobufSource, Option<u32>)>> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).protobuf_source(local),
            Placement::Frozen { local } => self.frozen_store().protobuf_source(local),
            Placement::Tail { local } => self.tail.protobuf_source(local),
        }
    }
    /// Open the catalog under `root` with `tail` as the mutable part.
    /// The tail must be an EMPTY store declared with the same field and
    /// column tables every sealed segment has; a mismatch refuses by
    /// name, because a union over different tables has no meaning.
    pub fn open(root: impl Into<PathBuf>, tail: Bm25Store) -> Result<Self, String> {
        Self::open_with(root, tail, crate::segments::VectorLoad::default())
    }

    /// [`Self::open`] with the way sealed vector images are served.
    pub fn open_with(
        root: impl Into<PathBuf>,
        tail: Bm25Store,
        load: crate::segments::VectorLoad,
    ) -> Result<Self, String> {
        let catalog = SegmentCatalog::open_with(root, load)?;
        Self::from_catalog(catalog, tail)
    }

    /// [`Self::open`] over a catalog the caller already holds — the staged
    /// catalog of a compaction shadow (`SegmentCatalog::open_staged`).
    pub fn open_catalog(catalog: SegmentCatalog, tail: Bm25Store) -> Result<Self, String> {
        Self::from_catalog(catalog, tail)
    }

    fn from_catalog(catalog: SegmentCatalog, mut tail: Bm25Store) -> Result<Self, String> {
        if tail.next_doc_id() != 0 {
            return Err("a segmented shard's tail must start empty".to_string());
        }
        let set = catalog.snapshot();
        if let Some(binding) = set.binding() {
            if tail.binding().is_some_and(|held| held != binding) {
                return Err("segment tail and generation mapped bindings disagree".into());
            }
            tail.set_binding(Some(binding.clone()));
        } else if !set.is_empty() && tail.binding().is_some() {
            return Err("a bound tail cannot label a populated unbound segment set".into());
        }
        let mut shard = SegmentedShard {
            catalog,
            set,
            parts: Vec::new(),
            frozen: None,
            tail,
            tail_base: 0,
            facets: Vec::new(),
            map_keys: Vec::new(),
            map_values: Vec::new(),
            map_key_reverse: Vec::new(),
            map_numeric_keys: Vec::new(),
            map_numeric_key_reverse: Vec::new(),
        };
        shard.rebuild()?;
        Ok(shard)
    }

    /// Recompute the parts and dictionaries from the current snapshot.
    fn rebuild(&mut self) -> Result<(), String> {
        let set = Arc::clone(&self.set);
        let mut parts = Vec::with_capacity(set.len());
        let mut next: u64 = 0;
        for i in 0..set.len() {
            let m = set.metadata(i);
            if m.base_label != next {
                return Err(format!(
                    "segment {:?} starts at label {} but the previous part ends at {next}; a \
                     segmented shard needs one contiguous id space",
                    m.segment_id, m.base_label
                ));
            }
            let base = u32::try_from(next).map_err(|_| "segment base exceeds u32".to_string())?;
            let rows = u32::try_from(m.rows).map_err(|_| "segment rows exceed u32".to_string())?;
            self.check_tables(set.bm25(i), &m.segment_id)?;
            parts.push(Part { base, rows });
            next += m.rows;
        }
        self.parts = parts;
        let sealed_end = u32::try_from(next).map_err(|_| "segment rows exceed u32".to_string())?;
        self.tail_base = match self.frozen.as_ref() {
            Some(f) if f.base != sealed_end => {
                return Err(format!(
                    "the frozen part starts at {} but the sealed parts end at {sealed_end}",
                    f.base
                ))
            }
            Some(f) => sealed_end + f.rows,
            None => sealed_end,
        };
        // Global dictionaries per column, over the sealed parts.
        let readers: Vec<&crate::postings::Bm25Reader> =
            (0..set.len()).map(|i| set.bm25(i)).collect();
        self.facets = (0..self.tail.facet_count())
            .map(|fi| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.facet_dictionary(fi))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_keys = (0..self.tail.map_facet_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_facet_keys(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_values = (0..self.tail.map_facet_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_facet_values(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_numeric_keys = (0..self.tail.map_numeric_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_numeric_keys(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        // The frozen part's values enter as the tail's did when it froze,
        // then move to the frozen remap; the real tail syncs after them.
        if let Some(frozen) = self.frozen.as_ref().map(|f| Arc::clone(&f.store)) {
            for (fi, dict) in self.facets.iter_mut().enumerate() {
                dict.sync_tail(frozen.facet_dictionary(fi));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_keys.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_facet_keys(ci));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_values.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_facet_values(ci));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_numeric_keys.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_numeric_keys(ci));
                dict.freeze_tail();
            }
        }
        self.sync_tail();
        Ok(())
    }

    fn rebuild_key_reverse(&mut self) {
        let frozen = self.frozen.as_ref().map(|f| &*f.store);
        self.map_key_reverse = self
            .map_keys
            .iter()
            .enumerate()
            .map(|(ci, dict)| {
                KeyReverse::build(
                    dict,
                    frozen.map_or(0, |f| f.map_facet_keys(ci).len()),
                    self.tail.map_facet_keys(ci).len(),
                )
            })
            .collect();
        self.map_numeric_key_reverse = self
            .map_numeric_keys
            .iter()
            .enumerate()
            .map(|(ci, dict)| {
                KeyReverse::build(
                    dict,
                    frozen.map_or(0, |f| f.map_numeric_keys(ci).len()),
                    self.tail.map_numeric_keys(ci).len(),
                )
            })
            .collect();
    }

    /// Refuse a sealed segment whose tables differ from the tail's.
    fn check_tables(&self, reader: &crate::postings::Bm25Reader, id: &str) -> Result<(), String> {
        if !same_tables!(reader, &self.tail) {
            return Err(format!(
                "segment {id:?} declares a field or column table this shard does not; the \
                 catalog and the node configuration must agree"
            ));
        }
        Ok(())
    }

    /// Fold the tail's new dictionary entries into the global ones. Runs
    /// after every ingest under the shard's write lock.
    pub fn sync_tail(&mut self) {
        for (fi, dict) in self.facets.iter_mut().enumerate() {
            dict.sync_tail(self.tail.facet_dictionary(fi));
        }
        for (ci, dict) in self.map_keys.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_facet_keys(ci));
        }
        for (ci, dict) in self.map_values.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_facet_values(ci));
        }
        for (ci, dict) in self.map_numeric_keys.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_numeric_keys(ci));
        }
        self.rebuild_key_reverse();
    }

    /// Freeze the tail for a seal: the tail becomes a read-only part at
    /// its current rows, `fresh` becomes the tail after it, and the
    /// frozen store is returned shared so the seal can write it with no
    /// lock on the shard. Refuses while a frozen part is still waiting
    /// on its publication.
    /// `rows` is the positional span the segment covers: the tail's
    /// documents, or more when the vector side has rows the document
    /// side does not (a vectors-only shard), never fewer.
    pub fn freeze_tail(&mut self, fresh: Bm25Store, rows: u32) -> Result<Arc<Bm25Store>, String> {
        if self.frozen.is_some() {
            return Err("a seal is already in flight on this shard".to_string());
        }
        if fresh.next_doc_id() != 0 {
            return Err("a fresh tail must start empty".to_string());
        }
        if rows < self.tail.next_doc_id() {
            return Err(format!(
                "a seal of {rows} rows cannot cover the tail's {} documents",
                self.tail.next_doc_id()
            ));
        }
        if !same_tables!(&fresh, &self.tail) {
            return Err("a fresh tail must preserve the shard's field and column tables".into());
        }
        let store = Arc::new(std::mem::replace(&mut self.tail, fresh));
        self.frozen = Some(Frozen {
            base: self.tail_base,
            rows,
            store: Arc::clone(&store),
        });
        self.tail_base += rows;
        for dict in self
            .facets
            .iter_mut()
            .chain(self.map_keys.iter_mut())
            .chain(self.map_values.iter_mut())
            .chain(self.map_numeric_keys.iter_mut())
        {
            dict.freeze_tail();
        }
        self.rebuild_key_reverse();
        Ok(store)
    }

    /// The frozen part, when a seal is in flight: `(base, rows, store)`.
    pub fn frozen(&self) -> Option<(u32, u32, &Arc<Bm25Store>)> {
        self.frozen.as_ref().map(|f| (f.base, f.rows, &f.store))
    }

    fn frozen_store(&self) -> &Bm25Store {
        &self
            .frozen
            .as_ref()
            .expect("a placement named the frozen part")
            .store
    }

    /// The heap parts in row order: the frozen part when one exists,
    /// then the tail, each with its base.
    fn heaps(&self) -> impl Iterator<Item = (u32, &Bm25Store)> {
        self.frozen
            .iter()
            .map(|f| (f.base, &*f.store))
            .chain(std::iter::once((self.tail_base, &self.tail)))
    }

    /// Publish binding metadata without dropping a frozen tail or moving rows.
    pub fn publish_binding(&mut self, binding: &StoredBinding) -> Result<(), String> {
        if self.binding().is_some_and(|held| held != binding) {
            return Err("segment shard is bound to another mapping".into());
        }
        let published = self.catalog.publish_binding(binding)?;
        if published.manifest().segments != self.set.manifest().segments {
            return Err("segment rows changed during binding publication".into());
        }
        self.set = published;
        self.tail.set_binding(Some(binding.clone()));
        Ok(())
    }

    pub fn catalog(&self) -> &SegmentCatalog {
        &self.catalog
    }

    pub fn snapshot(&self) -> &Arc<OpenedSegmentSet> {
        &self.set
    }

    pub fn tail(&self) -> &Bm25Store {
        &self.tail
    }

    /// The tail store for ingest; call [`Self::sync_tail`] afterwards.
    pub fn tail_mut(&mut self) -> &mut Bm25Store {
        &mut self.tail
    }

    /// The local id the tail starts at: the rows sealed so far.
    pub fn tail_base(&self) -> u32 {
        self.tail_base
    }

    pub fn sealed_parts(&self) -> usize {
        self.parts.len()
    }

    /// Where local id `doc` lives.
    pub fn place(&self, doc: u32) -> Placement {
        if doc >= self.tail_base {
            return Placement::Tail {
                local: doc - self.tail_base,
            };
        }
        if let Some(f) = self.frozen.as_ref().filter(|f| doc >= f.base) {
            return Placement::Frozen {
                local: doc - f.base,
            };
        }
        let part = match self.parts.binary_search_by(|p| p.base.cmp(&doc)) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        Placement::Sealed {
            part,
            local: doc - self.parts[part].base,
        }
    }

    fn reader(&self, part: usize) -> &crate::postings::Bm25Reader {
        self.set.bm25(part)
    }

    /// Adopt the catalog snapshot that now contains the frozen part as a
    /// sealed segment. The tail stays as it is: the set's rows must end
    /// where the tail begins, or the shard refuses the snapshot and
    /// keeps serving what it had.
    pub fn republish(&mut self, set: Arc<OpenedSegmentSet>) -> Result<(), String> {
        let expected = self.tail_base;
        let previous = (Arc::clone(&self.set), self.frozen.take());
        self.set = set;
        let outcome = match self.rebuild() {
            Ok(()) if self.tail_base == expected => Ok(()),
            Ok(()) => Err(format!(
                "the published set covers {} rows but the tail starts at {expected}; a \
                 republish must seal exactly the frozen rows",
                self.tail_base
            )),
            Err(error) => Err(error),
        };
        if outcome.is_err() {
            (self.set, self.frozen) = previous;
            self.rebuild()
                .expect("the previous snapshot rebuilt before this republish");
        }
        outcome
    }

    // ---- field table ----

    pub fn field_count(&self) -> usize {
        self.tail.field_count()
    }

    pub fn field_name(&self, f: usize) -> &str {
        self.tail.field_name(f)
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.tail.field_index(name)
    }

    pub fn field_has_positions(&self, f: usize) -> bool {
        self.tail.field_has_positions(f)
    }

    pub fn field_has_sentences(&self, f: usize) -> bool {
        self.tail.field_has_sentences(f)
    }

    /// The analyzer fingerprint of field `f`: the sealed parts' when any
    /// exist (they agree, by the catalog's own check), else the tail's.
    pub fn analysis_fingerprint(&self, f: usize) -> u64 {
        match (self.parts.len(), self.frozen.as_ref()) {
            (0, Some(frozen)) => frozen.store.analysis_fingerprint(f),
            (0, None) => self.tail.analysis_fingerprint(f),
            _ => self.reader(0).analysis_fingerprint(f),
        }
    }

    /// Record field `f`'s analyzer fingerprint on the tail, refusing a
    /// contradiction with the sealed parts or the tail's own record.
    pub fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        let held = match (self.parts.len(), self.frozen.as_ref()) {
            (0, Some(frozen)) => Some(frozen.store.analysis_fingerprint(f)),
            (0, None) => None,
            _ => Some(self.reader(0).analysis_fingerprint(f)),
        };
        if let Some(held) = held {
            if held != 0 && fingerprint != 0 && held != fingerprint {
                return Err(format!(
                    "field {:?} was sealed under analyzer fingerprint {held:#x}; this ingest \
                     analyzes under {fingerprint:#x}",
                    self.tail.field_name(f)
                ));
            }
        }
        self.tail.set_analysis_fingerprint(f, fingerprint)
    }

    pub fn binding(&self) -> Option<&StoredBinding> {
        self.set
            .binding()
            .or_else(|| self.tail.binding())
            .or_else(|| self.frozen.as_ref().and_then(|f| f.store.binding()))
            .or_else(|| {
                (!self.parts.is_empty())
                    .then(|| self.reader(0).binding())
                    .flatten()
            })
    }

    pub fn next_doc_id(&self) -> u32 {
        self.tail_base + self.tail.next_doc_id()
    }

    pub fn doc_count(&self) -> u64 {
        (0..self.parts.len())
            .map(|i| Bm25Index::doc_count(self.reader(i)))
            .sum::<u64>()
            + self.heaps().map(|(_, h)| h.doc_count()).sum::<u64>()
    }

    /// The read surface of field `f` across every part.
    pub fn field(&self, f: usize) -> UnionField<'_> {
        UnionField {
            shard: self,
            fi: f,
            mask: None,
        }
    }

    /// The read surface of field `f` over the sealed parts `mask`
    /// admits (one flag per sealed part, `false` = skipped) plus the
    /// heaps (`docs/segment-pruning.md`). Walks over postings, impacts,
    /// and dictionary scans skip the masked-out parts; reads addressed
    /// by document id answer for every row, so a hit's text, offsets,
    /// and positions resolve whichever part holds it.
    pub fn field_masked(&self, f: usize, mask: Arc<[bool]>) -> UnionField<'_> {
        debug_assert_eq!(mask.len(), self.parts.len());
        UnionField {
            shard: self,
            fi: f,
            mask: Some(mask),
        }
    }

    /// The whole shard scored as its body field over the parts `mask`
    /// admits: the [`Bm25Index`] the scorers take when a filter has
    /// ruled sealed segments out (`docs/segment-pruning.md`).
    pub fn masked(&self, mask: Arc<[bool]>) -> MaskedShard<'_> {
        MaskedShard { shard: self, mask }
    }

    /// `(base, rows)` of every sealed part, in catalog order: the local
    /// document id range each segment covers.
    pub fn sealed_ranges(&self) -> Vec<(u32, u32)> {
        self.parts.iter().map(|p| (p.base, p.rows)).collect()
    }

    /// The summary of every sealed part, in catalog order; `None` for a
    /// segment sealed before summaries were written.
    pub fn part_summaries(&self) -> Vec<Option<&SegmentSummary>> {
        (0..self.parts.len())
            .map(|i| self.set.metadata(i).summary.as_ref())
            .collect()
    }

    /// One flag per sealed part: `true` where NONE of `terms` occurs in
    /// field `f` of that part, so a walk over those terms would touch
    /// no posting there. A dictionary lookup per term per part, no
    /// postings read.
    pub fn parts_lacking_terms(&self, f: usize, terms: &[String]) -> Vec<bool> {
        (0..self.parts.len())
            .map(|i| {
                let view = self.reader(i).field(f);
                terms.iter().all(|term| view.df(term) == 0)
            })
            .collect()
    }

    // ---- facets ----

    pub fn facet_count(&self) -> usize {
        self.tail.facet_count()
    }

    pub fn facet_name(&self, fi: usize) -> &str {
        self.tail.facet_name(fi)
    }

    pub fn facet_index(&self, name: &str) -> Option<usize> {
        self.tail.facet_index(name)
    }

    pub fn facet_value_count(&self, fi: usize) -> usize {
        self.facets[fi].values.len()
    }

    pub fn facet_value(&self, fi: usize, ord: u32) -> &str {
        &self.facets[fi].values[ord as usize]
    }

    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        self.facets[fi].ord_of(value)
    }

    pub fn facet_dictionary(&self, fi: usize) -> &[String] {
        &self.facets[fi].values
    }

    pub fn facet_dictionary_sorted(&self, fi: usize) -> bool {
        self.facets[fi].sorted()
    }

    pub fn facet_ord(&self, fi: usize, doc: u32) -> Option<u32> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self
                .reader(part)
                .facet_ord(fi, local)
                .and_then(|ord| self.facets[fi].to_global(Source::Sealed(part), ord)),
            Placement::Frozen { local } => self
                .frozen_store()
                .facet_ord(fi, local)
                .and_then(|ord| self.facets[fi].to_global(Source::Frozen, ord)),
            Placement::Tail { local } => self
                .tail
                .facet_ord(fi, local)
                .and_then(|ord| self.facets[fi].to_global(Source::Tail, ord)),
        }
    }

    // ---- numerics, integers, geo ----

    pub fn numeric_count(&self) -> usize {
        self.tail.numeric_count()
    }

    pub fn numeric_name(&self, ni: usize) -> &str {
        self.tail.numeric_name(ni)
    }

    pub fn numeric_index(&self, name: &str) -> Option<usize> {
        self.tail.numeric_index(name)
    }

    pub fn numeric_value(&self, ni: usize, doc: u32) -> Option<f64> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).numeric_value(ni, local),
            Placement::Frozen { local } => self.frozen_store().numeric_value(ni, local),
            Placement::Tail { local } => self.tail.numeric_value(ni, local),
        }
    }

    pub fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        let mut acc = (f64::INFINITY, f64::NEG_INFINITY);
        let fold = |acc: &mut (f64, f64), (lo, hi): (f64, f64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).numeric_min_max(ni));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.numeric_min_max(ni));
        }
        acc
    }

    pub fn integer_count(&self) -> usize {
        self.tail.integer_count()
    }

    pub fn integer_name(&self, ii: usize) -> &str {
        self.tail.integer_name(ii)
    }

    pub fn integer_index(&self, name: &str) -> Option<usize> {
        self.tail.integer_index(name)
    }

    pub fn integer_value(&self, ii: usize, doc: u32) -> Option<i64> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).integer_value(ii, local),
            Placement::Frozen { local } => self.frozen_store().integer_value(ii, local),
            Placement::Tail { local } => self.tail.integer_value(ii, local),
        }
    }

    pub fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        let mut acc = (i64::MAX, i64::MIN);
        let fold = |acc: &mut (i64, i64), (lo, hi): (i64, i64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).integer_min_max(ii));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.integer_min_max(ii));
        }
        acc
    }

    pub fn unsigned_integer_count(&self) -> usize {
        self.tail.unsigned_integer_count()
    }

    pub fn unsigned_integer_name(&self, ii: usize) -> &str {
        self.tail.unsigned_integer_name(ii)
    }

    pub fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.tail.unsigned_integer_index(name)
    }

    pub fn unsigned_integer_value(&self, ii: usize, doc: u32) -> Option<u64> {
        match self.place(doc) {
            Placement::Sealed { part, local } => {
                self.reader(part).unsigned_integer_value(ii, local)
            }
            Placement::Frozen { local } => self.frozen_store().unsigned_integer_value(ii, local),
            Placement::Tail { local } => self.tail.unsigned_integer_value(ii, local),
        }
    }

    pub fn unsigned_integer_min_max(&self, ii: usize) -> (u64, u64) {
        let mut acc = (u64::MAX, 0);
        let fold = |acc: &mut (u64, u64), (lo, hi): (u64, u64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).unsigned_integer_min_max(ii));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.unsigned_integer_min_max(ii));
        }
        acc
    }

    pub fn geo_count(&self) -> usize {
        self.tail.geo_count()
    }

    pub fn geo_name(&self, gi: usize) -> &str {
        self.tail.geo_name(gi)
    }

    pub fn geo_index(&self, name: &str) -> Option<usize> {
        self.tail.geo_index(name)
    }

    pub fn geo_value(&self, gi: usize, doc: u32) -> Option<(f64, f64)> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).geo_value(gi, local),
            Placement::Frozen { local } => self.frozen_store().geo_value(gi, local),
            Placement::Tail { local } => self.tail.geo_value(gi, local),
        }
    }

    pub fn geo_bbox(&self, gi: usize) -> (f64, f64, f64, f64) {
        let mut acc = (
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        );
        let fold = |acc: &mut (f64, f64, f64, f64), b: (f64, f64, f64, f64)| {
            if b.0 <= b.1 && b.2 <= b.3 {
                acc.0 = acc.0.min(b.0);
                acc.1 = acc.1.max(b.1);
                acc.2 = acc.2.min(b.2);
                acc.3 = acc.3.max(b.3);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).geo_bbox(gi));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.geo_bbox(gi));
        }
        acc
    }

    // ---- map facets ----

    pub fn map_facet_count(&self) -> usize {
        self.tail.map_facet_count()
    }

    pub fn map_facet_name(&self, ci: usize) -> &str {
        self.tail.map_facet_name(ci)
    }

    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.tail.map_facet_index(name)
    }

    pub fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_keys[ci].ord_of(key)
    }

    pub fn map_facet_keys(&self, ci: usize) -> &[String] {
        &self.map_keys[ci].values
    }

    pub fn map_facet_value_count(&self, ci: usize) -> usize {
        self.map_values[ci].values.len()
    }

    pub fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        &self.map_values[ci].values[ord as usize]
    }

    pub fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        self.map_values[ci].ord_of(value)
    }

    pub fn map_facet_values(&self, ci: usize) -> &[String] {
        &self.map_values[ci].values
    }

    pub fn map_facet_values_sorted(&self, ci: usize) -> bool {
        self.map_values[ci].sorted()
    }

    /// `(source, reverse-table index, local doc)` for a document id.
    fn locate(&self, doc: u32) -> (Source, usize, u32) {
        match self.place(doc) {
            Placement::Sealed { part, local } => (Source::Sealed(part), part, local),
            Placement::Frozen { local } => (Source::Frozen, self.parts.len(), local),
            Placement::Tail { local } => (Source::Tail, self.parts.len() + 1, local),
        }
    }

    pub fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc: u32) -> Option<u32> {
        let (source, reverse_index, local_doc) = self.locate(doc);
        let local_key = *self.map_key_reverse[ci].per_part[reverse_index].get(key_ord as usize)?;
        if local_key == u32::MAX {
            return None;
        }
        let local_value = match source {
            Source::Sealed(part) => self
                .reader(part)
                .map_facet_value_ord(ci, local_key, local_doc)?,
            Source::Frozen => self
                .frozen_store()
                .map_facet_value_ord(ci, local_key, local_doc)?,
            Source::Tail => self.tail.map_facet_value_ord(ci, local_key, local_doc)?,
        };
        self.map_values[ci].to_global(source, local_value)
    }

    // ---- map numerics ----

    pub fn map_numeric_count(&self) -> usize {
        self.tail.map_numeric_count()
    }

    pub fn map_numeric_name(&self, ci: usize) -> &str {
        self.tail.map_numeric_name(ci)
    }

    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.tail.map_numeric_index(name)
    }

    pub fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_numeric_keys[ci].ord_of(key)
    }

    pub fn map_numeric_keys(&self, ci: usize) -> &[String] {
        &self.map_numeric_keys[ci].values
    }

    pub fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        let mut acc = (f64::INFINITY, f64::NEG_INFINITY);
        let fold = |acc: &mut (f64, f64), (lo, hi): (f64, f64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        let reverse = &self.map_numeric_key_reverse[ci].per_part;
        for (i, table) in reverse.iter().enumerate().take(self.parts.len()) {
            let local = table[key_ord as usize];
            if local != u32::MAX {
                fold(&mut acc, self.reader(i).map_numeric_key_min_max(ci, local));
            }
        }
        let heap_tables = [self.parts.len(), self.parts.len() + 1];
        let heap_indexes = if self.frozen.is_some() {
            &heap_tables[..]
        } else {
            &heap_tables[1..]
        };
        for (&index, (_, heap)) in heap_indexes.iter().zip(self.heaps()) {
            let local = reverse[index][key_ord as usize];
            if local != u32::MAX {
                fold(&mut acc, heap.map_numeric_key_min_max(ci, local));
            }
        }
        acc
    }

    pub fn map_numeric_value(&self, ci: usize, key_ord: u32, doc: u32) -> Option<f64> {
        let (source, reverse_index, local_doc) = self.locate(doc);
        let local_key =
            *self.map_numeric_key_reverse[ci].per_part[reverse_index].get(key_ord as usize)?;
        if local_key == u32::MAX {
            return None;
        }
        match source {
            Source::Sealed(part) => self
                .reader(part)
                .map_numeric_value(ci, local_key, local_doc),
            Source::Frozen => self
                .frozen_store()
                .map_numeric_value(ci, local_key, local_doc),
            Source::Tail => self.tail.map_numeric_value(ci, local_key, local_doc),
        }
    }

    // ---- sentences / text ----

    pub fn field_doc_sentences(&self, f: usize, doc: u32) -> Option<Vec<(u32, u32)>> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).field_doc_sentences(f, local),
            Placement::Frozen { local } => self
                .frozen_store()
                .field_doc_sentences(f, local)
                .map(<[(u32, u32)]>::to_vec),
            Placement::Tail { local } => self
                .tail
                .field_doc_sentences(f, local)
                .map(<[(u32, u32)]>::to_vec),
        }
    }

    pub fn text(&self, doc: u32) -> Option<String> {
        match self.place(doc) {
            Placement::Sealed { part, local } => Bm25Index::text(self.reader(part), local),
            Placement::Frozen { local } => self.frozen_store().text(local).map(str::to_string),
            Placement::Tail { local } => self.tail.text(local).map(str::to_string),
        }
    }

    pub fn lineage(&self, doc: u32) -> Option<DocLineage> {
        match self.place(doc) {
            Placement::Sealed { part, local } => Bm25Index::lineage(self.reader(part), local),
            Placement::Frozen { local } => self.frozen_store().lineage(local),
            Placement::Tail { local } => self.tail.lineage(local),
        }
    }

    // ---- ingest ----

    /// Append one analyzed document at the shard-local id `doc`, which
    /// must be at or past the tail's next id.
    pub fn add_document(
        &mut self,
        doc: u32,
        text: String,
        analyzed: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) -> Result<(), String> {
        let local = doc.checked_sub(self.tail_base).ok_or_else(|| {
            format!(
                "document {doc} falls inside the sealed range (tail starts at {})",
                self.tail_base
            )
        })?;
        self.tail
            .add_document_with_lineage(local, text, analyzed, lineage);
        self.sync_tail();
        Ok(())
    }

    /// The directory a new segment is staged from, under the catalog.
    pub fn stage_dir(&self, tag: &str) -> PathBuf {
        self.set
            .root()
            .join(format!(".seal-{tag}-{}", std::process::id()))
    }

    pub fn root(&self) -> &Path {
        self.set.root()
    }
}

/// The read surface of one field across every part of a segmented
/// shard, with document ids in the shard's positional space.
pub struct UnionField<'a> {
    shard: &'a SegmentedShard,
    fi: usize,
    /// Sealed parts to walk (`docs/segment-pruning.md`); `None` walks
    /// them all.
    mask: Option<Arc<[bool]>>,
}

impl<'a> UnionField<'a> {
    fn prefix_iter(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + 'a> {
        use std::{cmp::Reverse, collections::BinaryHeap};
        let mut scans: Vec<_> = self
            .parts()
            .map(|(_, _, v)| v.prefix_iter(prefix))
            .chain(self.heaps().map(|(_, v)| v.prefix_iter(prefix)))
            .collect();
        let mut heap = BinaryHeap::new();
        for (i, scan) in scans.iter_mut().enumerate() {
            if let Some(term) = scan.next() {
                heap.push(Reverse((term, i)));
            }
        }
        Box::new(std::iter::from_fn(move || {
            let Reverse((term, i)) = heap.pop()?;
            if let Some(next) = scans[i].next() {
                heap.push(Reverse((next, i)));
            }
            while heap.peek().is_some_and(|Reverse((next, _))| next == &term) {
                let Reverse((_, i)) = heap.pop().unwrap();
                if let Some(next) = scans[i].next() {
                    heap.push(Reverse((next, i)));
                }
            }
            Some(term)
        }))
    }

    fn admits(&self, part: usize) -> bool {
        self.mask.as_ref().is_none_or(|mask| mask[part])
    }

    fn parts(&self) -> impl Iterator<Item = (u32, u32, crate::postings::FieldView<'a>)> + '_ {
        let shard = self.shard;
        let fi = self.fi;
        shard
            .parts
            .iter()
            .enumerate()
            .filter(move |(i, _)| self.admits(*i))
            .map(move |(i, p)| (p.base, p.rows, shard.reader(i).field(fi)))
    }

    /// The heap parts (frozen, then tail) with their bases.
    fn heaps(&self) -> impl Iterator<Item = (u32, crate::postings::StoreFieldView<'a>)> + '_ {
        let fi = self.fi;
        self.shard
            .heaps()
            .map(move |(base, store)| (base, store.field(fi)))
    }

    fn heap_df(&self, term: &str) -> u32 {
        self.heaps().map(|(_, v)| v.df(term)).sum()
    }

    /// The heap view and local id for a heap placement; `None` for a
    /// sealed one.
    fn heap_of(&self, placement: Placement) -> Option<(crate::postings::StoreFieldView<'a>, u32)> {
        match placement {
            Placement::Sealed { .. } => None,
            Placement::Frozen { local } => Some((self.shard.frozen_store().field(self.fi), local)),
            Placement::Tail { local } => Some((self.shard.tail.field(self.fi), local)),
        }
    }
}

impl Bm25Index for UnionField<'_> {
    fn doc_count(&self) -> u64 {
        self.parts().map(|(_, _, v)| v.doc_count()).sum::<u64>()
            + self.heaps().map(|(_, v)| v.doc_count()).sum::<u64>()
    }
    fn total_doc_length(&self) -> u64 {
        self.parts()
            .map(|(_, _, v)| v.total_doc_length())
            .sum::<u64>()
            + self.heaps().map(|(_, v)| v.total_doc_length()).sum::<u64>()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        let placement = self.shard.place(doc_id);
        match self.heap_of(placement) {
            Some((view, local)) => view.doc_length(local),
            None => {
                let Placement::Sealed { part, local } = placement else {
                    unreachable!("heap_of covers every heap placement")
                };
                self.shard.reader(part).field(self.fi).doc_length(local)
            }
        }
    }
    fn df(&self, term: &str) -> u32 {
        self.parts().map(|(_, _, v)| v.df(term)).sum::<u32>() + self.heap_df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        for (base, _, view) in self.parts() {
            view.for_each_posting(term, &mut |doc, tf, offsets| f(base + doc, tf, offsets));
        }
        for (base, view) in self.heaps() {
            view.for_each_posting(term, &mut |doc, tf, offsets| f(base + doc, tf, offsets));
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        for (base, _, view) in self.parts() {
            view.for_each_doc_tf(term, &mut |doc, tf| f(base + doc, tf));
        }
        for (base, view) in self.heaps() {
            view.for_each_doc_tf(term, &mut |doc, tf| f(base + doc, tf));
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let placement = self.shard.place(doc_id);
        match self.heap_of(placement) {
            Some((view, local)) => view.posting_offsets(term, local),
            None => {
                let Placement::Sealed { part, local } = placement else {
                    unreachable!("heap_of covers every heap placement")
                };
                self.shard
                    .reader(part)
                    .field(self.fi)
                    .posting_offsets(term, local)
            }
        }
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        // A term a heap part (frozen or tail) holds has no impacts until
        // the next seal.
        if self.heap_df(term) > 0 {
            return None;
        }
        let parts: Vec<(u32, u32, FileImpacts<'_>)> = self
            .shard
            .parts
            .iter()
            .enumerate()
            .filter(|(i, _)| self.admits(*i))
            .filter_map(|(i, p)| {
                self.shard
                    .reader(i)
                    .field(self.fi)
                    .file_impacts(term)
                    .map(|c| (p.base, p.rows, c))
            })
            .collect();
        if parts.is_empty() {
            return None;
        }
        Some(ImpactCursor::chain(parts))
    }
    fn has_impacts(&self, term: &str) -> bool {
        if self.heap_df(term) > 0 {
            return false;
        }
        let mut any = false;
        for (_, _, view) in self.parts() {
            if view.df(term) > 0 {
                if !view.has_impacts(term) {
                    return false;
                }
                any = true;
            }
        }
        any
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.shard.text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.shard.lineage(doc_id)
    }
    fn has_positions(&self) -> bool {
        self.shard.field_has_positions(self.fi)
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let placement = self.shard.place(doc_id);
        match self.heap_of(placement) {
            Some((view, local)) => view.posting_positions(term, local),
            None => {
                let Placement::Sealed { part, local } = placement else {
                    unreachable!("heap_of covers every heap placement")
                };
                self.shard
                    .reader(part)
                    .field(self.fi)
                    .posting_positions(term, local)
            }
        }
    }
    fn has_sentences(&self) -> bool {
        self.shard.field_has_sentences(self.fi)
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.shard.field_doc_sentences(self.fi, doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        let mut union: Vec<String> = Vec::new();
        let mut over: Option<usize> = None;
        let mut fold = |result: Result<Vec<String>, usize>| match result {
            Ok(terms) => union.extend(terms),
            Err(count) => over = Some(over.unwrap_or(0).max(count)),
        };
        for (_, _, view) in self.parts() {
            fold(view.expand_prefix(prefix, cap));
        }
        for (_, view) in self.heaps() {
            fold(view.expand_prefix(prefix, cap));
        }
        if let Some(count) = over {
            return Err(count.max(union.len()));
        }
        union.sort_unstable();
        union.dedup();
        if union.len() > cap {
            Err(union.len())
        } else {
            Ok(union)
        }
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        // Sum the posting df of each term across the sealed parts and
        // the heaps: the same term in two parts is one dictionary entry
        // whose df is the sum, exactly what one image of the rows holds.
        let mut union: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
        let mut over: Option<usize> = None;
        let mut fold = |result: Result<Vec<(String, u32)>, usize>| match result {
            Ok(entries) => {
                for (term, df) in entries {
                    *union.entry(term).or_insert(0) += df;
                }
            }
            Err(count) => over = Some(over.unwrap_or(0).max(count)),
        };
        for (_, _, view) in self.parts() {
            fold(view.suggest_prefix(prefix, max_scan));
        }
        for (_, view) in self.heaps() {
            fold(view.suggest_prefix(prefix, max_scan));
        }
        if let Some(count) = over {
            return Err(count.max(union.len()));
        }
        if union.len() > max_scan {
            Err(union.len())
        } else {
            Ok(union.into_iter().collect())
        }
    }
}

/// The shard scores as its body field (field 0), like the other stores.
impl Bm25Index for SegmentedShard {
    fn doc_count(&self) -> u64 {
        self.field(0).doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.field(0).total_doc_length()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field(0).doc_length(doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.field(0).df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        self.field(0).for_each_posting(term, f)
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.field(0).for_each_doc_tf(term, f)
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.field(0).posting_offsets(term, doc_id)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        // The chain borrows the sealed readers, which outlive the view.
        let parts: Vec<(u32, u32, FileImpacts<'_>)> = if self.field(0).heap_df(term) > 0 {
            Vec::new()
        } else {
            self.parts
                .iter()
                .enumerate()
                .filter_map(|(i, p)| {
                    self.reader(i)
                        .field(0)
                        .file_impacts(term)
                        .map(|c| (p.base, p.rows, c))
                })
                .collect()
        };
        if parts.is_empty() {
            None
        } else {
            Some(ImpactCursor::chain(parts))
        }
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.field(0).has_impacts(term)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        SegmentedShard::text(self, doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        SegmentedShard::lineage(self, doc_id)
    }
    fn has_positions(&self) -> bool {
        self.field_has_positions(0)
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        self.field(0).posting_positions(term, doc_id)
    }
    fn has_sentences(&self) -> bool {
        self.field_has_sentences(0)
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.field_doc_sentences(0, doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.field(0).prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.field(0).expand_prefix(prefix, cap)
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.field(0).suggest_prefix(prefix, max_scan)
    }
}

/// A [`SegmentedShard`] scored as its body field over a subset of its
/// sealed parts (`docs/segment-pruning.md`). Every walk (postings,
/// document frequency, impacts, dictionary scans) covers the admitted
/// parts and the heaps; every read by document id covers the whole
/// shard, so a surviving hit resolves its text, offsets, and positions
/// as usual.
pub struct MaskedShard<'a> {
    shard: &'a SegmentedShard,
    mask: Arc<[bool]>,
}

impl MaskedShard<'_> {
    fn body(&self) -> UnionField<'_> {
        self.shard.field_masked(0, Arc::clone(&self.mask))
    }
}

impl Bm25Index for MaskedShard<'_> {
    fn doc_count(&self) -> u64 {
        Bm25Index::doc_count(self.shard)
    }
    fn total_doc_length(&self) -> u64 {
        Bm25Index::total_doc_length(self.shard)
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        Bm25Index::doc_length(self.shard, doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.body().df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        self.body().for_each_posting(term, f)
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.body().for_each_doc_tf(term, f)
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        Bm25Index::posting_offsets(self.shard, term, doc_id)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        // The chain borrows the sealed readers, which outlive the view.
        if self.shard.field(0).heap_df(term) > 0 {
            return None;
        }
        let parts: Vec<(u32, u32, FileImpacts<'_>)> = self
            .shard
            .parts
            .iter()
            .enumerate()
            .filter(|(i, _)| self.mask[*i])
            .filter_map(|(i, p)| {
                self.shard
                    .reader(i)
                    .field(0)
                    .file_impacts(term)
                    .map(|c| (p.base, p.rows, c))
            })
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(ImpactCursor::chain(parts))
        }
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.body().has_impacts(term)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        SegmentedShard::text(self.shard, doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        SegmentedShard::lineage(self.shard, doc_id)
    }
    fn has_positions(&self) -> bool {
        self.shard.field_has_positions(0)
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        Bm25Index::posting_positions(self.shard, term, doc_id)
    }
    fn has_sentences(&self) -> bool {
        self.shard.field_has_sentences(0)
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.shard.field_doc_sentences(0, doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.body().prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.body().expand_prefix(prefix, cap)
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.body().suggest_prefix(prefix, max_scan)
    }
}
