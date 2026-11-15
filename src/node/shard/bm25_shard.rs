//! Storage-shape dispatch and field/column accessors for [`Bm25Shard`].

use crate::node::*;

/// The BM25 half's two storage shapes: the heap builder used during
/// ingest, and the disk-resident mmap reader used after Flush and on
/// startup. Once resident, a shard holds no postings or document texts
/// in heap — only the small per-doc tables.
#[allow(clippy::large_enum_variant)]
pub enum Bm25Shard {
    /// Heap builder (small or append ingests; searchable mid-build).
    Building(Bm25Store),
    /// Disk-spilling bulk builder (fresh persisted shards): bounded heap,
    /// NOT searchable until flushed.
    Spilling(SpillBuilder),
    /// Disk-resident mmap reader over a saved `.bm25` file.
    Resident(Bm25Reader),
    /// Segment catalog plus searchable heap tail (default persisted layout).
    Segmented(SegmentedShard),
}

impl Bm25Shard {
    /// The searchable read surface; `None` while bulk-building (a spill
    /// builder cannot answer term lookups without scanning every run).
    pub(in crate::node) fn as_index(&self) -> Option<&dyn Bm25Index> {
        match self {
            Bm25Shard::Building(s) => Some(s),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(r),
            Bm25Shard::Segmented(g) => Some(g),
        }
    }

    pub(crate) fn next_doc_id(&self) -> u32 {
        match self {
            Bm25Shard::Building(s) => s.next_doc_id(),
            Bm25Shard::Spilling(s) => s.next_doc_id(),
            Bm25Shard::Resident(r) => r.next_doc_id(),
            Bm25Shard::Segmented(g) => g.next_doc_id(),
        }
    }

    pub(in crate::node) fn doc_count(&self) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.doc_count(),
            Bm25Shard::Spilling(s) => s.doc_count(),
            Bm25Shard::Resident(r) => Bm25Index::doc_count(r),
            Bm25Shard::Segmented(g) => Bm25Index::doc_count(g),
        }
    }

    /// Number of fields in the active table.
    pub(crate) fn field_count(&self) -> usize {
        match self {
            Bm25Shard::Building(s) => s.field_count(),
            Bm25Shard::Spilling(s) => s.field_count(),
            Bm25Shard::Resident(r) => r.field_count(),
            Bm25Shard::Segmented(g) => g.field_count(),
        }
    }

    /// The name of field `f` in the active table.
    pub(crate) fn field_name(&self, f: usize) -> &str {
        match self {
            Bm25Shard::Building(s) => s.field_name(f),
            Bm25Shard::Spilling(s) => s.field_name(f),
            Bm25Shard::Resident(r) => r.field_name(f),
            Bm25Shard::Segmented(g) => g.field_name(f),
        }
    }

    /// The mapped-plan binding persisted with this shard, if any.
    pub(crate) fn binding(&self) -> Option<&crate::postings::StoredBinding> {
        match self {
            Bm25Shard::Building(s) => s.binding(),
            Bm25Shard::Spilling(s) => s.binding(),
            Bm25Shard::Resident(r) => r.binding(),
            Bm25Shard::Segmented(g) => g.binding(),
        }
    }

    /// The derived-column declaration the store was written under.
    pub(crate) fn derived(&self) -> Option<&crate::postings::StoredDerived> {
        match self {
            Bm25Shard::Building(s) => s.derived(),
            Bm25Shard::Spilling(s) => s.derived(),
            Bm25Shard::Resident(r) => r.derived(),
            Bm25Shard::Segmented(g) => g.derived(),
        }
    }

    pub fn protobuf_source(
        &self,
        doc: u32,
    ) -> std::io::Result<Option<(crate::pb::ProtobufSource, Option<u32>)>> {
        match self {
            Bm25Shard::Building(s) => s.protobuf_source(doc),
            Bm25Shard::Spilling(s) => s.protobuf_source(doc),
            Bm25Shard::Resident(s) => s.protobuf_source(doc),
            Bm25Shard::Segmented(s) => s.protobuf_source(doc),
        }
    }

    /// Capture bindings from the state being scored, before releasing its lock.
    pub fn identity_snapshot(&self) -> Result<crate::source_archive::IdentitySnapshot, String> {
        match self {
            Bm25Shard::Building(s) => Ok(s.identity_snapshot()),
            Bm25Shard::Spilling(s) => Ok(s.identity_snapshot()),
            Bm25Shard::Resident(s) => Ok(s.identity_snapshot()),
            Bm25Shard::Segmented(s) => s.identity_snapshot(),
        }
    }

    pub fn document_identity(&self, doc: u32) -> Option<crate::pb::DocumentIdentity> {
        match self {
            Bm25Shard::Building(s) => s.document_identity(doc),
            Bm25Shard::Spilling(s) => s.document_identity(doc),
            Bm25Shard::Resident(s) => s.document_identity(doc),
            Bm25Shard::Segmented(s) => s.document_identity(doc),
        }
    }

    /// Field `f`'s analyzer fingerprint in the active table (`0` = unknown,
    /// which never enforces).
    pub(crate) fn analysis_fingerprint(&self, f: usize) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.analysis_fingerprint(f),
            Bm25Shard::Spilling(s) => s.analysis_fingerprint(f),
            Bm25Shard::Resident(r) => r.analysis_fingerprint(f),
            Bm25Shard::Segmented(g) => g.analysis_fingerprint(f),
        }
    }

    /// Whether field `f` stores token positions in active storage.
    pub(in crate::node) fn field_has_positions(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_positions(f),
            Bm25Shard::Spilling(s) => s.field_has_positions(f),
            Bm25Shard::Resident(r) => r.field_has_positions(f),
            Bm25Shard::Segmented(g) => g.field_has_positions(f),
        }
    }

    /// Whether field `f` stores sentence spans in active storage.
    pub(in crate::node) fn field_has_sentences(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_sentences(f),
            Bm25Shard::Spilling(s) => s.field_has_sentences(f),
            Bm25Shard::Resident(r) => r.field_has_sentences(f),
            Bm25Shard::Segmented(g) => g.field_has_sentences(f),
        }
    }

    /// Record field `f`'s analyzer fingerprint, refusing a contradiction.
    /// A disk-resident shard is not being written to, so it has nothing
    /// to record.
    pub(in crate::node) fn set_analysis_fingerprint(
        &mut self,
        f: usize,
        fingerprint: u64,
    ) -> Result<(), String> {
        match self {
            Bm25Shard::Building(s) => s.set_analysis_fingerprint(f, fingerprint),
            Bm25Shard::Spilling(s) => s.set_analysis_fingerprint(f, fingerprint),
            Bm25Shard::Resident(_) => Ok(()),
            Bm25Shard::Segmented(g) => g.set_analysis_fingerprint(f, fingerprint),
        }
    }

    /// The table index of the field named `name`, if present. `None`
    /// while bulk-building (no searchable surface to resolve against).
    pub(in crate::node) fn field_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.field_index(name),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => r.field_index(name),
            Bm25Shard::Segmented(g) => g.field_index(name),
        }
    }

    /// The facet-table index of the facet field named `name`, if the
    /// active table has it.
    pub(crate) fn facet_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.facet_index(name),
            Bm25Shard::Spilling(s) => s.facet_index(name),
            Bm25Shard::Resident(r) => r.facet_index(name),
            Bm25Shard::Segmented(g) => g.facet_index(name),
        }
    }

    /// Number of distinct values facet field `fi` holds. Counting only
    /// runs against searchable shapes, so the Spilling arm is
    /// unreachable (a spilling shard refused the query already).
    pub(in crate::node) fn facet_value_count(&self, fi: usize) -> usize {
        match self {
            Bm25Shard::Building(s) => s.facet_value_count(fi),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_value_count(fi),
            Bm25Shard::Segmented(g) => g.facet_value_count(fi),
        }
    }

    /// The value of facet field `fi` at ordinal `ord`.
    pub(in crate::node) fn facet_value(&self, fi: usize, ord: u32) -> &str {
        match self {
            Bm25Shard::Building(s) => s.facet_value(fi, ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_value(fi, ord),
            Bm25Shard::Segmented(g) => g.facet_value(fi, ord),
        }
    }

    /// The ordinal of `doc_id`'s value for facet field `fi`.
    pub(in crate::node) fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.facet_ord(fi, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_ord(fi, doc_id),
            Bm25Shard::Segmented(g) => g.facet_ord(fi, doc_id),
        }
    }

    /// Ordinal of `value` in facet field `fi`'s dictionary, or `None` if absent.
    pub(in crate::node) fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.facet_value_ord_of(fi, value),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_value_ord_of(fi, value),
            Bm25Shard::Segmented(g) => g.facet_value_ord_of(fi, value),
        }
    }

    /// The numeric-table index of the numeric field named `name`, if
    /// the active table has it.
    pub(crate) fn numeric_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.numeric_index(name),
            Bm25Shard::Spilling(s) => s.numeric_index(name),
            Bm25Shard::Resident(r) => r.numeric_index(name),
            Bm25Shard::Segmented(g) => g.numeric_index(name),
        }
    }

    /// (min, max) of numeric field `ni` over present values. Scoring
    /// only runs against searchable shapes, so Spilling is unreachable.
    pub(in crate::node) fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        match self {
            Bm25Shard::Building(s) => s.numeric_min_max(ni),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.numeric_min_max(ni),
            Bm25Shard::Segmented(g) => g.numeric_min_max(ni),
        }
    }

    /// `doc_id`'s value for numeric field `ni`.
    pub(in crate::node) fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        match self {
            Bm25Shard::Building(s) => s.numeric_value(ni, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.numeric_value(ni, doc_id),
            Bm25Shard::Segmented(g) => g.numeric_value(ni, doc_id),
        }
    }

    /// The integer-table index of the i64 field named `name`, if the
    /// active table has it.
    pub(crate) fn integer_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.integer_index(name),
            Bm25Shard::Spilling(s) => s.integer_index(name),
            Bm25Shard::Resident(r) => r.integer_index(name),
            Bm25Shard::Segmented(g) => g.integer_index(name),
        }
    }

    pub(crate) fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.unsigned_integer_index(name),
            Bm25Shard::Spilling(s) => s.unsigned_integer_index(name),
            Bm25Shard::Resident(r) => r.unsigned_integer_index(name),
            Bm25Shard::Segmented(g) => g.unsigned_integer_index(name),
        }
    }

    /// (min, max) of integer field `ii` over present values; the empty
    /// range (i64::MAX, i64::MIN) when the column holds none. Scoring
    /// only runs against searchable shapes, so Spilling is unreachable.
    pub(in crate::node) fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        match self {
            Bm25Shard::Building(s) => s.integer_min_max(ii),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.integer_min_max(ii),
            Bm25Shard::Segmented(g) => g.integer_min_max(ii),
        }
    }

    /// Exact unsigned extrema; the inverted range (u64::MAX, 0) means empty.
    pub(in crate::node) fn unsigned_integer_min_max(&self, ui: usize) -> (u64, u64) {
        match self {
            Bm25Shard::Building(s) => s.unsigned_integer_min_max(ui),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.unsigned_integer_min_max(ui),
            Bm25Shard::Segmented(g) => g.unsigned_integer_min_max(ui),
        }
    }

    /// `doc_id`'s value for integer field `ii`.
    pub(in crate::node) fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        match self {
            Bm25Shard::Building(s) => s.integer_value(ii, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.integer_value(ii, doc_id),
            Bm25Shard::Segmented(g) => g.integer_value(ii, doc_id),
        }
    }

    pub(in crate::node) fn unsigned_integer_value(&self, ii: usize, doc_id: u32) -> Option<u64> {
        match self {
            Bm25Shard::Building(s) => s.unsigned_integer_value(ii, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.unsigned_integer_value(ii, doc_id),
            Bm25Shard::Segmented(g) => g.unsigned_integer_value(ii, doc_id),
        }
    }

    /// The geo-table index of the geo field named `name`, if the
    /// active table has it.
    pub(in crate::node) fn geo_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.geo_index(name),
            Bm25Shard::Spilling(s) => s.geo_index(name),
            Bm25Shard::Resident(r) => r.geo_index(name),
            Bm25Shard::Segmented(g) => g.geo_index(name),
        }
    }

    /// `doc_id`'s (lat, lon) for geo field `gi`. Scoring only runs
    /// against searchable shapes, so Spilling is unreachable.
    pub(in crate::node) fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        match self {
            Bm25Shard::Building(s) => s.geo_value(gi, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.geo_value(gi, doc_id),
            Bm25Shard::Segmented(g) => g.geo_value(gi, doc_id),
        }
    }

    /// The index of the map-facet column named `name`.
    pub(in crate::node) fn map_facet_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_index(name),
            Bm25Shard::Spilling(s) => s.map_facet_index(name),
            Bm25Shard::Resident(r) => r.map_facet_index(name),
            Bm25Shard::Segmented(g) => g.map_facet_index(name),
        }
    }

    /// The key ordinal of `key` in map-facet column `ci`.
    pub(in crate::node) fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_key_ord(ci, key),
            Bm25Shard::Segmented(g) => g.map_facet_key_ord(ci, key),
        }
    }

    /// Number of distinct values map-facet column `ci` holds.
    pub(in crate::node) fn map_facet_value_count(&self, ci: usize) -> usize {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_count(ci),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_count(ci),
            Bm25Shard::Segmented(g) => g.map_facet_value_count(ci),
        }
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    pub(in crate::node) fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value(ci, ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value(ci, ord),
            Bm25Shard::Segmented(g) => g.map_facet_value(ci, ord),
        }
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in
    /// map-facet column `ci`.
    pub(in crate::node) fn map_facet_value_ord(
        &self,
        ci: usize,
        key_ord: u32,
        doc_id: u32,
    ) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_ord(ci, key_ord, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_ord(ci, key_ord, doc_id),
            Bm25Shard::Segmented(g) => g.map_facet_value_ord(ci, key_ord, doc_id),
        }
    }

    /// The ordinal of `value` in map-facet column `ci`'s value
    /// dictionary, `None` when this shard never ingested it.
    pub(in crate::node) fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_ord_of(ci, value),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_ord_of(ci, value),
            Bm25Shard::Segmented(g) => g.map_facet_value_ord_of(ci, value),
        }
    }

    /// The index of the map-numeric column named `name`.
    pub(in crate::node) fn map_numeric_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_index(name),
            Bm25Shard::Spilling(s) => s.map_numeric_index(name),
            Bm25Shard::Resident(r) => r.map_numeric_index(name),
            Bm25Shard::Segmented(g) => g.map_numeric_index(name),
        }
    }

    pub(in crate::node) fn map_integer_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_integer_index(name),
            Bm25Shard::Spilling(s) => s.map_integer_index(name),
            Bm25Shard::Resident(r) => r.map_integer_index(name),
            Bm25Shard::Segmented(g) => g.map_integer_index(name),
        }
    }

    pub(in crate::node) fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_unsigned_integer_index(name),
            Bm25Shard::Spilling(s) => s.map_unsigned_integer_index(name),
            Bm25Shard::Resident(r) => r.map_unsigned_integer_index(name),
            Bm25Shard::Segmented(g) => g.map_unsigned_integer_index(name),
        }
    }

    pub(in crate::node) fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_integer_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(s) => s.map_integer_key_ord(ci, key),
            Bm25Shard::Segmented(s) => s.map_integer_key_ord(ci, key),
        }
    }
    pub(in crate::node) fn map_integer_value(
        &self,
        ci: usize,
        key_ord: u32,
        row: u32,
    ) -> Option<i64> {
        match self {
            Bm25Shard::Building(s) => s.map_integer_value(ci, key_ord, row),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(s) => s.map_integer_value(ci, key_ord, row),
            Bm25Shard::Segmented(s) => s.map_integer_value(ci, key_ord, row),
        }
    }
    pub(in crate::node) fn map_unsigned_integer_key_ord(
        &self,
        ci: usize,
        key: &str,
    ) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_unsigned_integer_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(s) => s.map_unsigned_integer_key_ord(ci, key),
            Bm25Shard::Segmented(s) => s.map_unsigned_integer_key_ord(ci, key),
        }
    }
    pub(in crate::node) fn map_unsigned_integer_value(
        &self,
        ci: usize,
        key_ord: u32,
        row: u32,
    ) -> Option<u64> {
        match self {
            Bm25Shard::Building(s) => s.map_unsigned_integer_value(ci, key_ord, row),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(s) => s.map_unsigned_integer_value(ci, key_ord, row),
            Bm25Shard::Segmented(s) => s.map_unsigned_integer_value(ci, key_ord, row),
        }
    }
    pub(in crate::node) fn map_integer_key_min_max(
        &self,
        ci: usize,
        key_ord: u32,
    ) -> Option<(i64, i64)> {
        match self {
            Bm25Shard::Building(s) => s.map_integer_key_min_max(ci, key_ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(s) => s.map_integer_key_min_max(ci, key_ord),
            Bm25Shard::Segmented(s) => s.map_integer_key_min_max(ci, key_ord),
        }
    }
    pub(in crate::node) fn map_unsigned_integer_key_min_max(
        &self,
        ci: usize,
        key_ord: u32,
    ) -> Option<(u64, u64)> {
        match self {
            Bm25Shard::Building(s) => s.map_unsigned_integer_key_min_max(ci, key_ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(s) => s.map_unsigned_integer_key_min_max(ci, key_ord),
            Bm25Shard::Segmented(s) => s.map_unsigned_integer_key_min_max(ci, key_ord),
        }
    }
    /// The key ordinal of `key` in map-numeric column `ci`.
    pub(in crate::node) fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_key_ord(ci, key),
            Bm25Shard::Segmented(g) => g.map_numeric_key_ord(ci, key),
        }
    }

    /// (min, max) of map-numeric column `ci` under `key_ord`.
    pub(in crate::node) fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_key_min_max(ci, key_ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_key_min_max(ci, key_ord),
            Bm25Shard::Segmented(g) => g.map_numeric_key_min_max(ci, key_ord),
        }
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`.
    pub(in crate::node) fn map_numeric_value(
        &self,
        ci: usize,
        key_ord: u32,
        doc_id: u32,
    ) -> Option<f64> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_value(ci, key_ord, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_value(ci, key_ord, doc_id),
            Bm25Shard::Segmented(g) => g.map_numeric_value(ci, key_ord, doc_id),
        }
    }

    /// Field `f` as its own searchable [`Bm25Index`]; `None` while
    /// bulk-building, exactly like [`Self::as_index`].
    pub(in crate::node) fn field_view(&self, f: usize) -> Option<Box<dyn Bm25Index + '_>> {
        match self {
            Bm25Shard::Building(s) => Some(Box::new(s.field(f))),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(Box::new(r.field(f))),
            Bm25Shard::Segmented(g) => Some(Box::new(g.field(f))),
        }
    }

    /// [`Self::field_view`] restricted to sealed parts admitted by `mask` on a
    /// segmented shard (falling back to [`Self::field_view`] on other layouts).
    pub(in crate::node) fn field_view_masked(
        &self,
        f: usize,
        mask: &Arc<[bool]>,
    ) -> Option<Box<dyn Bm25Index + '_>> {
        match self {
            Bm25Shard::Segmented(g) => Some(Box::new(g.field_masked(f, Arc::clone(mask)))),
            _ => self.field_view(f),
        }
    }

    /// Resolve parsed score stages against this shard's numeric tables and
    /// min/max bounds (`None` columns evaluate as absent/identity).
    pub(in crate::node) fn resolve_chain(
        &self,
        specs: &[ScoreStageSpec],
    ) -> crate::scorefn::ScoreChain {
        use crate::scorefn::ColumnRef;
        crate::scorefn::ScoreChain {
            stages: specs
                .iter()
                .map(|spec| {
                    let ScoreStageSpec {
                        op,
                        column,
                        key,
                        typed_map,
                    } = spec;
                    // A geo stage reads a geo-point column and nothing
                    // else: its op carries the origin, and its bound is
                    // identity, so there is no min/max to lift.
                    if matches!(op, crate::scorefn::StageOp::MultGeoDecay { .. }) {
                        return crate::scorefn::Stage {
                            op: *op,
                            column: self.geo_index(column).map(ColumnRef::Geo),
                            min_max: (f64::NAN, f64::NAN),
                        };
                    }
                    let (column, min_max) = if key.is_none() {
                        // Column names are unique across numeric families.
                        if let Some(ni) = self.numeric_index(column) {
                            (Some(ColumnRef::Numeric(ni)), Some(self.numeric_min_max(ni)))
                        } else if let Some(ii) = self.integer_index(column) {
                            (
                                Some(ColumnRef::Integer(ii)),
                                Some(int_min_max_as_f64(self.integer_min_max(ii))),
                            )
                        } else {
                            let ui = self.unsigned_integer_index(column);
                            (
                                ui.map(ColumnRef::UnsignedInteger),
                                ui.map(|ui| uint_min_max_as_f64(self.unsigned_integer_min_max(ui))),
                            )
                        }
                    } else if *typed_map && self.map_integer_index(column).is_some() {
                        let ci = self.map_integer_index(column).expect("checked signed map");
                        let key_ord = self.map_integer_key_ord(ci, key.as_deref().unwrap());
                        (
                            key_ord.map(|key_ord| ColumnRef::MapIntegerKey {
                                column: ci,
                                key_ord,
                            }),
                            key_ord
                                .and_then(|k| self.map_integer_key_min_max(ci, k))
                                .map(int_min_max_as_f64),
                        )
                    } else if *typed_map && self.map_unsigned_integer_index(column).is_some() {
                        let ci = self
                            .map_unsigned_integer_index(column)
                            .expect("checked unsigned map");
                        let key_ord =
                            self.map_unsigned_integer_key_ord(ci, key.as_deref().unwrap());
                        (
                            key_ord.map(|key_ord| ColumnRef::MapUnsignedIntegerKey {
                                column: ci,
                                key_ord,
                            }),
                            key_ord
                                .and_then(|k| self.map_unsigned_integer_key_min_max(ci, k))
                                .map(uint_min_max_as_f64),
                        )
                    } else {
                        // Map stage: both the column and the key must
                        // resolve; bounds lift from the KEY's min/max.
                        let key = key.as_deref().expect("map selector is present");
                        let hit = self
                            .map_numeric_index(column)
                            .and_then(|ci| self.map_numeric_key_ord(ci, key).map(|k| (ci, k)));
                        (
                            hit.map(|(ci, key_ord)| ColumnRef::MapKey {
                                column: ci,
                                key_ord,
                            }),
                            hit.map(|(ci, key_ord)| self.map_numeric_key_min_max(ci, key_ord)),
                        )
                    };
                    crate::scorefn::Stage {
                        op: *op,
                        column,
                        min_max: min_max.unwrap_or((f64::NAN, f64::NAN)),
                    }
                })
                .collect(),
        }
    }

    /// Open a saved `.bm25` file (`v6` through `v8`) as a disk-resident mmap reader.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        Bm25Reader::open(path).map(Bm25Shard::Resident)
    }
}
