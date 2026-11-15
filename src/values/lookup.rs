//! The column lookup surface resolution reads.

/// Column lookup surface a shard provides to resolution — name to
/// table index per family, plus map key dictionaries. Implemented by
/// the node's shard wrapper over both the heap store and the mmap
/// reader.
pub trait ColumnLookup {
    /// f64 column index for `name`.
    fn numeric_index(&self, name: &str) -> Option<usize>;
    /// i64 column index for `name`.
    fn integer_index(&self, name: &str) -> Option<usize>;
    /// u64 column index for `name`.
    fn unsigned_integer_index(&self, name: &str) -> Option<usize>;
    /// Facet column index for `name`.
    fn facet_index(&self, name: &str) -> Option<usize>;
    /// Map-numeric column index for `name`.
    fn map_numeric_index(&self, name: &str) -> Option<usize>;
    /// Key ordinal in map-numeric column `ci`'s key dictionary.
    fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32>;
    /// Signed map column and key lookup.
    fn map_integer_index(&self, name: &str) -> Option<usize>;
    fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32>;
    /// Unsigned map column and key lookup.
    fn map_unsigned_integer_index(&self, name: &str) -> Option<usize>;
    fn map_unsigned_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32>;
    /// Map-facet column index for `name`.
    fn map_facet_index(&self, name: &str) -> Option<usize>;
    /// Key ordinal in map-facet column `ci`'s key dictionary.
    fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32>;
    /// Value ordinal of `value` in facet column `ci`'s dictionary.
    fn facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32>;
    /// Value ordinal of `value` in map-facet column `ci`'s value
    /// dictionary.
    fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32>;
    /// Whether this lookup types an INGEST evaluation (a document's own
    /// values, or a declaration checked against the tables before any
    /// document arrives). The ingest-only vocabulary — `hash.fnv64()`
    /// and `stable_key()` — resolves here and
    /// is refused by name at query time, where the derived column that
    /// stores the value is the read.
    fn at_ingest(&self) -> bool {
        false
    }
}

/// Declared numeric families used to type-check materialization before a
/// document arrives. Missing values do not erase a declared column's type.
pub struct NumericTypes<'a> {
    /// f64 column names.
    pub numerics: &'a [String],
    /// i64 column names.
    pub integers: &'a [String],
    /// u64 column names.
    pub unsigned_integers: &'a [String],
    /// Map-numeric column names.
    pub map_numerics: &'a [String],
    /// Exact signed and unsigned map column names.
    pub map_integers: &'a [String],
    pub map_unsigned_integers: &'a [String],
    /// Facet column names: inputs of `hash.fnv64()` and of string
    /// comparisons in a derived column.
    pub facets: &'a [String],
}

impl ColumnLookup for NumericTypes<'_> {
    fn at_ingest(&self) -> bool {
        true
    }
    fn numeric_index(&self, name: &str) -> Option<usize> {
        self.numerics.iter().position(|n| n == name)
    }
    fn integer_index(&self, name: &str) -> Option<usize> {
        self.integers.iter().position(|n| n == name)
    }
    fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.unsigned_integers.iter().position(|n| n == name)
    }
    fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics.iter().position(|n| n == name)
    }
    fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.map_integers.iter().position(|n| n == name)
    }
    fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.map_unsigned_integers.iter().position(|n| n == name)
    }
    fn map_integer_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        Some(0)
    }
    fn map_unsigned_integer_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        Some(0)
    }
    // Keys can be absent per document; the declared family still fixes their type.
    fn map_numeric_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        Some(0)
    }
    fn facet_index(&self, name: &str) -> Option<usize> {
        self.facets.iter().position(|n| n == name)
    }
    fn map_facet_index(&self, _: &str) -> Option<usize> {
        None
    }
    fn map_facet_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
    fn facet_value_ord_of(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
    fn map_facet_value_ord_of(&self, _: usize, _: &str) -> Option<u32> {
        None
    }
}
