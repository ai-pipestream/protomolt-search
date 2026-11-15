//! The value environment one document provides at ingest.

use std::collections::HashMap;

use super::lookup::ColumnLookup;

/// The value environment one AddDocumentsRequest provides: its own
/// numeric families by name (timestamps as epoch-micro integers), its
/// facet strings, and its stable routing key. A string is never a
/// RESULT — materialization computes numbers — but a facet feeds
/// `hash.fnv64()` and `==`/`!=` comparisons.
#[derive(Debug, Default)]
pub struct IngestEnv {
    /// f64 values by column name.
    pub numerics: HashMap<String, f64>,
    /// i64 values by column name.
    pub integers: HashMap<String, i64>,
    /// u64 values by column name.
    pub unsigned_integers: HashMap<String, u64>,
    /// Map-numeric values by (column, key).
    pub map_numerics: HashMap<(String, String), f64>,
    /// Exact signed and unsigned map inputs.
    pub map_integers: HashMap<(String, String), i64>,
    pub map_unsigned_integers: HashMap<(String, String), u64>,
    /// Facet strings by column name.
    pub facets: HashMap<String, String>,
    /// The document's opaque stable routing key, when it has one.
    pub stable_key: Option<Vec<u8>>,
}

// Resolution-only lookup: the returned indexes identify types, not row storage.
// eval_ingest_inner reads the original maps by name after this type check.
impl ColumnLookup for IngestEnv {
    fn numeric_index(&self, name: &str) -> Option<usize> {
        self.numerics.contains_key(name).then_some(0)
    }
    fn integer_index(&self, name: &str) -> Option<usize> {
        self.integers.contains_key(name).then_some(0)
    }
    fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.unsigned_integers.contains_key(name).then_some(0)
    }
    fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics
            .keys()
            .any(|(n, _)| n == name)
            .then_some(0)
    }
    fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.map_integers
            .keys()
            .any(|(n, _)| n == name)
            .then_some(0)
    }
    fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.map_unsigned_integers
            .keys()
            .any(|(n, _)| n == name)
            .then_some(0)
    }
    fn map_integer_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        Some(0)
    }
    fn map_unsigned_integer_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        Some(0)
    }
    fn map_numeric_key_ord(&self, _: usize, _: &str) -> Option<u32> {
        Some(0)
    }
    fn facet_index(&self, name: &str) -> Option<usize> {
        self.facets.contains_key(name).then_some(0)
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
    fn at_ingest(&self) -> bool {
        true
    }
}

/// One evaluated ingest value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IngestVal {
    /// Lands in the i64 family.
    Int(i64),
    /// CEL uint, preserved without conversion.
    Uint(u64),
    /// Lands in the f64 family.
    Double(f64),
    /// A comparison or logic result. Never stored: the kind check
    /// refuses it naming the ternary as the fix.
    Bool(bool),
}
