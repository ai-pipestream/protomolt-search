//! Keyed map column accessors of [`Bm25Shard`].

use super::*;

impl Bm25Shard {
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
}
