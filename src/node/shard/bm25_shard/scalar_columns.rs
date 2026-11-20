//! Facet, numeric, integer and geo column accessors of [`Bm25Shard`].

use super::*;

impl Bm25Shard {
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
}
