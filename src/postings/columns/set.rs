//! Unified per-document column table (`ColumnSet`) shared by `Bm25Store` and `SpillBuilder`.

use std::io;

use super::map_stores::{MapFacetStore, MapNumericStore};
use super::metadata::{binding_entry_size, derived_entry_size, StoredBinding, StoredDerived};
use super::stores::{
    dictionary_sorted, validate_column_names, FacetStore, GeoStore, IntStore, NumericStore,
    UintStore,
};

/// Typed per-document column family stored in a v7/v8 `.bm25` shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnKind {
    /// Dictionary-encoded opaque string column (`COLUMN_KIND_FACET`).
    Facet,
    /// Finite `f64` scalar column (`COLUMN_KIND_F64`).
    Numeric,
    /// Signed `i64` scalar column with explicit presence (`COLUMN_KIND_I64_PRESENT`).
    Integer,
    /// Unsigned `u64` scalar column with explicit presence (`COLUMN_KIND_U64`).
    UnsignedInteger,
    /// `(lat, lon)` WGS-84 point column (`COLUMN_KIND_GEO`).
    Geo,
    /// Dictionary-encoded `map<string, string>` column (`COLUMN_KIND_MAP_FACET`).
    MapFacet,
    /// Finite `map<string, f64>` column (`COLUMN_KIND_MAP_F64`).
    MapNumeric,
    /// Exact `map<string, i64>` column (`COLUMN_KIND_MAP_I64`).
    MapInteger,
    /// Exact `map<string, u64>` column (`COLUMN_KIND_MAP_U64`).
    MapUnsignedInteger,
}

impl ColumnKind {
    /// Human-readable plural label for builder assertion diagnostics.
    pub(in crate::postings) const fn plural_label(self) -> &'static str {
        match self {
            Self::Facet => "facet fields",
            Self::Numeric => "numeric fields",
            Self::Integer | Self::UnsignedInteger => "integer fields",
            Self::Geo => "geo fields",
            Self::MapFacet | Self::MapNumeric | Self::MapInteger | Self::MapUnsignedInteger => {
                "map columns"
            }
        }
    }
}

/// Validate a text-field table: at least one field, non-empty unique names fitting `u16`.
pub(in crate::postings) fn validate_text_field_names(names: &[&str], owner: &str) {
    assert!(!names.is_empty(), "a {owner} needs at least one field");
    for (i, name) in names.iter().enumerate() {
        assert!(!name.is_empty(), "field {i} has an empty name");
        assert!(
            u16::try_from(name.len()).is_ok(),
            "field name exceeds u16 length"
        );
        assert!(!names[..i].contains(name), "duplicate field name {name:?}");
    }
}

/// Record a field's analyzer fingerprint, refusing contradictions once set.
pub(in crate::postings) fn merge_analysis_fingerprint(
    held: &mut u64,
    field_name: &str,
    fingerprint: u64,
) -> Result<(), String> {
    if fingerprint == 0 {
        return Ok(());
    }
    match *held {
        0 => {
            *held = fingerprint;
            Ok(())
        }
        existing if existing == fingerprint => Ok(()),
        existing => Err(format!(
            "field {field_name:?} was built with analyzer fingerprint {existing:#x} but this \
             document carries {fingerprint:#x}; one column holds one term identity"
        )),
    }
}

/// All nine per-document typed column tables plus shard-level binding and
/// derived-column metadata, shared identically by `Bm25Store` and `SpillBuilder`.
#[derive(Debug, Default)]
pub(in crate::postings) struct ColumnSet {
    pub(in crate::postings) facets: Vec<FacetStore>,
    pub(in crate::postings) numerics: Vec<NumericStore>,
    pub(in crate::postings) map_facets: Vec<MapFacetStore>,
    pub(in crate::postings) map_numerics: Vec<MapNumericStore>,
    pub(in crate::postings) map_integers: Vec<crate::integer_map::Store<i64>>,
    pub(in crate::postings) map_unsigned_integers: Vec<crate::integer_map::Store<u64>>,
    pub(in crate::postings) integers: Vec<IntStore>,
    pub(in crate::postings) unsigned_integers: Vec<UintStore>,
    pub(in crate::postings) geos: Vec<GeoStore>,
    pub(in crate::postings) binding: Option<StoredBinding>,
    pub(in crate::postings) derived: Option<StoredDerived>,
}

impl ColumnSet {
    /// Whether no typed columns or inline metadata records are declared.
    pub(in crate::postings) fn is_empty(&self) -> bool {
        self.facets.is_empty()
            && self.numerics.is_empty()
            && self.map_facets.is_empty()
            && self.map_numerics.is_empty()
            && self.map_integers.is_empty()
            && self.map_unsigned_integers.is_empty()
            && self.integers.is_empty()
            && self.unsigned_integers.is_empty()
            && self.geos.is_empty()
            && self.binding.is_none()
            && self.derived.is_none()
    }

    /// Number of column-table entries contributed by `ColumnSet` itself
    /// (excluding field-attached entries and protobuf sources).
    pub(in crate::postings) fn table_entry_count(&self) -> usize {
        self.facets.len()
            + self.numerics.len()
            + self.map_facets.len()
            + self.map_numerics.len()
            + self.map_integers.len()
            + self.map_unsigned_integers.len()
            + self.integers.len()
            + self.unsigned_integers.len()
            + self.geos.len()
            + usize::from(self.binding.is_some())
            + usize::from(self.derived.is_some())
    }

    /// Encoded byte size of all header column-table entries owned by `ColumnSet`
    /// (excluding the leading `u32` entry count, field entries, and sources).
    pub(in crate::postings) fn table_entries_size(&self) -> u64 {
        self.facets
            .iter()
            .map(|f| 2 + f.name.len() as u64 + 1 + 4 + 8 + 8)
            .sum::<u64>()
            + self
                .numerics
                .iter()
                .map(|n| 2 + n.name.len() as u64 + 1 + 8 + 8 + 8)
                .sum::<u64>()
            + self
                .map_facets
                .iter()
                .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 4 + 8 * 4)
                .sum::<u64>()
            + self
                .map_numerics
                .iter()
                .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 8 * 3)
                .sum::<u64>()
            + self
                .map_integers
                .iter()
                .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                .sum::<u64>()
            + self
                .map_unsigned_integers
                .iter()
                .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                .sum::<u64>()
            + self
                .integers
                .iter()
                .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                .sum::<u64>()
            + self
                .unsigned_integers
                .iter()
                .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                .sum::<u64>()
            + self
                .geos
                .iter()
                .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 5)
                .sum::<u64>()
            + binding_entry_size(self.binding.as_ref())
            + derived_entry_size(self.derived.as_ref())
    }

    /// Declare the column table for `kind`, validating non-empty unique `u16`-bounded names.
    pub(in crate::postings) fn configure(&mut self, kind: ColumnKind, names: &[&str]) {
        validate_column_names(names);
        match kind {
            ColumnKind::Facet => {
                self.facets = names.iter().map(|n| FacetStore::new(n)).collect();
            }
            ColumnKind::Numeric => {
                self.numerics = names.iter().map(|n| NumericStore::new(n)).collect();
            }
            ColumnKind::Integer => {
                self.integers = names.iter().map(|n| IntStore::new(n)).collect();
            }
            ColumnKind::UnsignedInteger => {
                self.unsigned_integers = names.iter().map(|n| UintStore::new(n)).collect();
            }
            ColumnKind::Geo => {
                self.geos = names.iter().map(|n| GeoStore::new(n)).collect();
            }
            ColumnKind::MapFacet => {
                self.map_facets = names.iter().map(|n| MapFacetStore::new(n)).collect();
            }
            ColumnKind::MapNumeric => {
                self.map_numerics = names.iter().map(|n| MapNumericStore::new(n)).collect();
            }
            ColumnKind::MapInteger => {
                self.map_integers = names
                    .iter()
                    .map(|n| crate::integer_map::Store::new(n))
                    .collect();
            }
            ColumnKind::MapUnsignedInteger => {
                self.map_unsigned_integers = names
                    .iter()
                    .map(|n| crate::integer_map::Store::new(n))
                    .collect();
            }
        }
    }

    /// Number of columns declared for `kind`.
    pub(in crate::postings) fn count(&self, kind: ColumnKind) -> usize {
        match kind {
            ColumnKind::Facet => self.facets.len(),
            ColumnKind::Numeric => self.numerics.len(),
            ColumnKind::Integer => self.integers.len(),
            ColumnKind::UnsignedInteger => self.unsigned_integers.len(),
            ColumnKind::Geo => self.geos.len(),
            ColumnKind::MapFacet => self.map_facets.len(),
            ColumnKind::MapNumeric => self.map_numerics.len(),
            ColumnKind::MapInteger => self.map_integers.len(),
            ColumnKind::MapUnsignedInteger => self.map_unsigned_integers.len(),
        }
    }

    /// Name of column `index` within `kind`. Panics when out of range.
    pub(in crate::postings) fn name(&self, kind: ColumnKind, index: usize) -> &str {
        match kind {
            ColumnKind::Facet => &self.facets[index].name,
            ColumnKind::Numeric => &self.numerics[index].name,
            ColumnKind::Integer => &self.integers[index].name,
            ColumnKind::UnsignedInteger => &self.unsigned_integers[index].name,
            ColumnKind::Geo => &self.geos[index].name,
            ColumnKind::MapFacet => &self.map_facets[index].name,
            ColumnKind::MapNumeric => &self.map_numerics[index].name,
            ColumnKind::MapInteger => &self.map_integers[index].name,
            ColumnKind::MapUnsignedInteger => &self.map_unsigned_integers[index].name,
        }
    }

    /// Index of column `name` within `kind`, if declared.
    pub(in crate::postings) fn index(&self, kind: ColumnKind, name: &str) -> Option<usize> {
        match kind {
            ColumnKind::Facet => self.facets.iter().position(|c| c.name == name),
            ColumnKind::Numeric => self.numerics.iter().position(|c| c.name == name),
            ColumnKind::Integer => self.integers.iter().position(|c| c.name == name),
            ColumnKind::UnsignedInteger => {
                self.unsigned_integers.iter().position(|c| c.name == name)
            }
            ColumnKind::Geo => self.geos.iter().position(|c| c.name == name),
            ColumnKind::MapFacet => self.map_facets.iter().position(|c| c.name == name),
            ColumnKind::MapNumeric => self.map_numerics.iter().position(|c| c.name == name),
            ColumnKind::MapInteger => self.map_integers.iter().position(|c| c.name == name),
            ColumnKind::MapUnsignedInteger => self
                .map_unsigned_integers
                .iter()
                .position(|c| c.name == name),
        }
    }

    pub(in crate::postings) fn set_facet(&mut self, fi: usize, doc_id: u32, value: &str) {
        self.facets[fi].set(doc_id, value);
    }

    pub(in crate::postings) fn set_numeric(&mut self, ni: usize, doc_id: u32, value: f64) {
        self.numerics[ni].set(doc_id, value);
    }

    pub(in crate::postings) fn set_integer(&mut self, ii: usize, doc_id: u32, value: i64) {
        self.integers[ii].set(doc_id, value);
    }

    pub(in crate::postings) fn set_unsigned_integer(&mut self, ii: usize, doc_id: u32, value: u64) {
        self.unsigned_integers[ii].set(doc_id, value);
    }

    pub(in crate::postings) fn set_geo(&mut self, gi: usize, doc_id: u32, lat: f64, lon: f64) {
        self.geos[gi].set(doc_id, lat, lon);
    }

    pub(in crate::postings) fn set_map_facet(
        &mut self,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: &str,
    ) {
        self.map_facets[ci].set(doc_id, key, value);
    }

    pub(in crate::postings) fn set_map_numeric(
        &mut self,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: f64,
    ) {
        self.map_numerics[ci].set(doc_id, key, value);
    }

    pub(in crate::postings) fn set_map_integer(
        &mut self,
        n_slots: usize,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: i64,
    ) -> io::Result<()> {
        set_integer_map_entry(&mut self.map_integers, n_slots, ci, doc_id, key, value)
    }

    pub(in crate::postings) fn set_map_unsigned_integer(
        &mut self,
        n_slots: usize,
        ci: usize,
        doc_id: u32,
        key: &str,
        value: u64,
    ) -> io::Result<()> {
        set_integer_map_entry(
            &mut self.map_unsigned_integers,
            n_slots,
            ci,
            doc_id,
            key,
            value,
        )
    }

    pub(in crate::postings) fn facet_dictionary_sorted(&self, fi: usize) -> bool {
        dictionary_sorted(&self.facets[fi].dict)
    }

    pub(in crate::postings) fn map_facet_values_sorted(&self, ci: usize) -> bool {
        dictionary_sorted(&self.map_facets[ci].values)
    }
}

fn set_integer_map_entry<T: crate::integer_map::Integer>(
    columns: &mut [crate::integer_map::Store<T>],
    n_slots: usize,
    ci: usize,
    doc_id: u32,
    key: &str,
    value: T,
) -> io::Result<()> {
    if doc_id as usize >= n_slots {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "integer map document slot does not exist",
        ));
    }
    columns
        .get_mut(ci)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "integer map column does not exist",
            )
        })?
        .set(doc_id, key, value)
}
