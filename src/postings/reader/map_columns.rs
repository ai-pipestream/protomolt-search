//! Map-column accessors on [`Bm25Reader`].

use super::Bm25Reader;
use crate::postings::columns::dictionary_lookup;

impl Bm25Reader {
    /// A document's pair range [start, end) in a map column, from the
    /// offsets section's prefix sums.
    fn map_pair_range(&self, offsets_off: u64, doc_id: u32) -> Option<(usize, usize)> {
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let at = |i: usize| {
            u32::from_le_bytes(
                self.map[offsets_off as usize + 4 * i..offsets_off as usize + 4 * i + 4]
                    .try_into()
                    .expect("4 bytes"),
            ) as usize
        };
        Some((at(slot), at(slot + 1)))
    }

    /// The index of the map-facet column named `name`.
    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.map_facets.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-facet column `ci`.
    pub fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        dictionary_lookup(
            &self.map_facets[ci].keys,
            self.map_facets[ci].keys_sorted,
            key,
        )
    }

    /// The keys of map-facet column `ci`, in ordinal order.
    pub fn map_facet_keys(&self, ci: usize) -> &[String] {
        &self.map_facets[ci].keys
    }

    /// The keys of map-numeric column `ci`, in ordinal order.
    pub fn map_numeric_keys(&self, ci: usize) -> &[String] {
        &self.map_numerics[ci].keys
    }

    /// Number of map-facet columns.
    pub fn map_facet_count(&self) -> usize {
        self.map_facets.len()
    }

    /// The name of map-facet column `ci`.
    pub fn map_facet_name(&self, ci: usize) -> &str {
        &self.map_facets[ci].name
    }

    /// Number of exact i64 map columns.
    pub fn map_integer_count(&self) -> usize {
        self.map_integers.len()
    }
    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_integer_name(&self, ci: usize) -> &str {
        &self.map_integers[ci].name
    }
    /// Look up an exact i64 map column by name.
    pub fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.map_integers.iter().position(|c| c.name == name)
    }
    /// Map keys in this reader's ordinal order.
    pub fn map_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_integers[ci].reader.keys
    }
    /// Find a key, including the empty string.
    pub fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_integers[ci].reader.key_ord(key)
    }
    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(i64, i64)> {
        self.map_integers[ci].reader.bounds(key)
    }
    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<i64> {
        let column = &self.map_integers[ci];
        column
            .reader
            .value(&self.map[column.off..column.off + column.len], key, row)
    }

    /// Number of exact u64 map columns.
    pub fn map_unsigned_integer_count(&self) -> usize {
        self.map_unsigned_integers.len()
    }
    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_unsigned_integer_name(&self, ci: usize) -> &str {
        &self.map_unsigned_integers[ci].name
    }
    /// Look up an exact u64 map column by name.
    pub fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.map_unsigned_integers
            .iter()
            .position(|c| c.name == name)
    }
    /// Map keys in this reader's ordinal order.
    pub fn map_unsigned_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_unsigned_integers[ci].reader.keys
    }
    /// Find a key, including the empty string.
    pub fn map_unsigned_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_unsigned_integers[ci].reader.key_ord(key)
    }
    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_unsigned_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(u64, u64)> {
        self.map_unsigned_integers[ci].reader.bounds(key)
    }
    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_unsigned_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<u64> {
        let column = &self.map_unsigned_integers[ci];
        column
            .reader
            .value(&self.map[column.off..column.off + column.len], key, row)
    }

    /// Number of map-numeric columns.
    pub fn map_numeric_count(&self) -> usize {
        self.map_numerics.len()
    }

    /// The name of map-numeric column `ci`.
    pub fn map_numeric_name(&self, ci: usize) -> &str {
        &self.map_numerics[ci].name
    }

    /// Number of distinct values map-facet column `ci` holds.
    pub fn map_facet_value_count(&self, ci: usize) -> usize {
        self.map_facets[ci].values.len()
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    pub fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        &self.map_facets[ci].values[ord as usize]
    }

    /// The ordinal of `value` in map-facet column `ci`'s value
    /// dictionary, `None` when never ingested — the linear scan
    /// [`Self::facet_value_ord_of`] explains.
    pub fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        dictionary_lookup(
            &self.map_facets[ci].values,
            self.map_facets[ci].values_sorted,
            value,
        )
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in
    /// map-facet column `ci`, `None` when absent: two offset reads and
    /// a binary search of the document's (strictly key-ordered) pairs.
    pub fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        let c = &self.map_facets[ci];
        let (start, end) = self.map_pair_range(c.offsets_off, doc_id)?;
        let pair = |i: usize| {
            let off = c.pairs_off as usize + 8 * i;
            (
                u32::from_le_bytes(self.map[off..off + 4].try_into().expect("4 bytes")),
                u32::from_le_bytes(self.map[off + 4..off + 8].try_into().expect("4 bytes")),
            )
        };
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (k, v) = pair(mid);
            match k.cmp(&key_ord) {
                std::cmp::Ordering::Equal => return Some(v),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-numeric column `ci`.
    pub fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_numerics[ci]
            .keys
            .iter()
            .position(|k| k == key)
            .map(|p| p as u32)
    }

    /// (min, max) of map-numeric column `ci` under `key_ord`, from the
    /// key dictionary's write-time metadata (validated at open).
    pub fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        self.map_numerics[ci].key_min_max[key_ord as usize]
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`,
    /// `None` when absent.
    pub fn map_numeric_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        let c = &self.map_numerics[ci];
        let (start, end) = self.map_pair_range(c.offsets_off, doc_id)?;
        let pair = |i: usize| {
            let off = c.pairs_off as usize + 12 * i;
            (
                u32::from_le_bytes(self.map[off..off + 4].try_into().expect("4 bytes")),
                f64::from_bits(u64::from_le_bytes(
                    self.map[off + 4..off + 12].try_into().expect("8 bytes"),
                )),
            )
        };
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (k, v) = pair(mid);
            match k.cmp(&key_ord) {
                std::cmp::Ordering::Equal => return Some(v),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }
}
