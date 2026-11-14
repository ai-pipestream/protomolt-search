//! Memory-mapped column slice descriptors held by `Bm25Reader`.

#[cfg(doc)]
use super::stores::IntStore;

/// Per-facet read state of one open v7 file: the decoded value
/// dictionary (small — one entry per distinct value) plus the map
/// offset of the fixed-stride ords section, which stays on disk.
pub(in crate::postings) struct FacetSlice {
    pub(in crate::postings) name: String,
    /// Values in ordinal order, decoded eagerly at open.
    pub(in crate::postings) dict: Vec<String>,
    /// Whether `dict` is in strict byte order, checked at open: true
    /// for every file this writer produces, and what makes an ordinal
    /// range an exact string range (`docs/prefix-terms.md`). A file
    /// written with first-seen ordinals refuses string ordering by name.
    pub(in crate::postings) sorted: bool,
    /// Absolute offset of the ords section (`n_slots x u32`).
    pub(in crate::postings) ords_off: u64,
}

/// Per-numeric-column read state of one open v7 file: the min/max
/// bound metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
pub(in crate::postings) struct NumericSlice {
    pub(in crate::postings) name: String,
    /// Min over present values (`NaN` when the column holds none).
    pub(in crate::postings) min: f64,
    /// Max over present values (`NaN` when the column holds none).
    pub(in crate::postings) max: f64,
    /// Absolute offset of the vals section (`n_slots x f64` bits).
    pub(in crate::postings) vals_off: u64,
}

/// Per-map-facet-column read state (`docs/map-columns.md`): both
/// dictionaries decoded eagerly (one entry per distinct key/value);
/// offsets and pairs sections stay in the map.
pub(in crate::postings) struct MapFacetSlice {
    pub(in crate::postings) name: String,
    /// Keys in ordinal order.
    pub(in crate::postings) keys: Vec<String>,
    /// Values in ordinal order.
    pub(in crate::postings) values: Vec<String>,
    /// Whether `keys` / `values` are in strict byte order (see
    /// [`FacetSlice::sorted`]).
    pub(in crate::postings) keys_sorted: bool,
    pub(in crate::postings) values_sorted: bool,
    /// Absolute offset of the offsets section (`(n_slots + 1) x u32`).
    pub(in crate::postings) offsets_off: u64,
    /// Absolute offset of the pairs section (8 B `(key_ord, value_ord)`
    /// entries, strictly key-ordered per document).
    pub(in crate::postings) pairs_off: u64,
}

/// Per-integer-map-column read state (`COLUMN_KIND_MAP_I64` / `COLUMN_KIND_MAP_U64`).
pub(in crate::postings) struct IntegerMapSlice<T: crate::integer_map::Integer> {
    pub(in crate::postings) name: String,
    pub(in crate::postings) off: usize,
    pub(in crate::postings) len: usize,
    pub(in crate::postings) reader: crate::integer_map::Reader<T>,
}

/// Per-map-numeric-column read state: the key dictionary with its
/// per-key min/max bound metadata; offsets and pairs stay in the map.
pub(in crate::postings) struct MapNumericSlice {
    pub(in crate::postings) name: String,
    /// Keys in ordinal order.
    pub(in crate::postings) keys: Vec<String>,
    /// Per-key `(min, max)` over present values, parallel to `keys`.
    pub(in crate::postings) key_min_max: Vec<(f64, f64)>,
    /// Absolute offset of the offsets section (`(n_slots + 1) x u32`).
    pub(in crate::postings) offsets_off: u64,
    /// Absolute offset of the pairs section (12 B `(key_ord, f64 bits)`
    /// entries, strictly key-ordered per document).
    pub(in crate::postings) pairs_off: u64,
}

/// Per-integer-column read state of one open v7 file: the min/max
/// bound metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
pub(in crate::postings) struct IntegerSlice {
    pub(in crate::postings) name: String,
    /// Min over present values (`i64::MAX` when the column holds none;
    /// see [`IntStore::min_max`] for why the empty range is inverted).
    pub(in crate::postings) min: i64,
    /// Max over present values (`i64::MIN` when the column holds none).
    pub(in crate::postings) max: i64,
    /// Absolute offset of the vals section (`n_slots x i64`).
    pub(in crate::postings) vals_off: u64,
    /// `None` decodes the legacy kind-4 sentinel.
    pub(in crate::postings) presence_off: Option<u64>,
}

pub(in crate::postings) struct UnsignedSlice {
    pub(in crate::postings) name: String,
    pub(in crate::postings) min: u64,
    pub(in crate::postings) max: u64,
    pub(in crate::postings) vals_off: u64,
}

/// Per-geo-column read state of one open v7 file: the bounding-box
/// metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
pub(in crate::postings) struct GeoSlice {
    pub(in crate::postings) name: String,
    /// The column's bounding box over present points, all four `NaN` when
    /// there are none (the kind-1 empty convention). Validated against a
    /// full scan at open.
    pub(in crate::postings) bbox: (f64, f64, f64, f64),
    /// Absolute offset of the vals section (`n_slots x (f64, f64)`).
    pub(in crate::postings) vals_off: u64,
}
