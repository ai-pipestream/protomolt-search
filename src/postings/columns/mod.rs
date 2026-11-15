//! Typed heap column stores, `ColumnSet`, metadata records, and mmapped column slices.

mod map_stores;
mod metadata;
mod set;
mod slices;
mod stores;

pub use self::metadata::{StoredBinding, StoredDerived};
pub use self::set::ColumnKind;

pub(in crate::postings) use self::map_stores::{
    integer_map_sections, write_integer_map_entries, write_map_facet_column,
    write_map_numeric_column, MapFacetStore, MapNumericStore,
};
pub(in crate::postings) use self::metadata::{
    decode_binding_entry, decode_derived_entry, write_binding_entry, write_derived_entry,
};
pub(in crate::postings) use self::set::{
    merge_analysis_fingerprint, validate_text_field_names, ColumnSet,
};
pub(in crate::postings) use self::slices::{
    FacetSlice, GeoSlice, IntegerMapSlice, IntegerSlice, MapFacetSlice, MapNumericSlice,
    NumericSlice, UnsignedSlice,
};
pub(in crate::postings) use self::stores::{
    dictionary_lookup, dictionary_sorted, integer_present, write_facet_column, FacetStore,
    GeoStore, IntStore, NumericStore, UintStore,
};

/// The ordinal marking "this document has no value for this facet
/// field" in a facet column, in heap and on disk alike.
pub const FACET_ABSENT: u32 = u32::MAX;

/// Legacy kind-4 absence marker. New integer columns use a presence bitmap;
/// this constant is only used when decoding the old on-disk representation.
pub const INTEGER_ABSENT: i64 = i64::MIN;

/// The pair marking "this document has no point" in a geo-point column.
/// BOTH halves are `NaN`: a geo column has a `NaN`
/// to spend on each axis, and spending both keeps the sentinel
/// unambiguous. A pair with exactly one `NaN` is neither a point nor an
/// absence — the reader refuses it as corruption rather than guessing
/// which half to believe.
pub const GEO_ABSENT: (f64, f64) = (f64::NAN, f64::NAN);
