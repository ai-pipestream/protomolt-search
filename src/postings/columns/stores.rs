//! Scalar heap column stores (`FacetStore`, `NumericStore`, `IntegerStore`, `GeoStore`) and dictionary helpers.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use super::super::format::{write_u16, write_u32, write_u64};
use super::FACET_ABSENT;

/// One dictionary-encoded facet column: the value dictionary in
/// ordinal (first-seen) order, and a per-slot ordinal table parallel
/// to the shared slot space. Facet values are opaque strings — never
/// analyzed — counted exactly as ingested.
#[derive(Debug)]
pub(in crate::postings) struct FacetStore {
    /// Facet field name from the schema.
    pub(in crate::postings) name: String,
    /// Values in ordinal order.
    pub(in crate::postings) dict: Vec<String>,
    /// value → ordinal (the dictionary's inverse, heap only).
    pub(in crate::postings) index: HashMap<String, u32>,
    /// Per-slot ordinal ([`FACET_ABSENT`] = no value). May be shorter
    /// than the slot count; missing trailing slots read as absent.
    pub(in crate::postings) ords: Vec<u32>,
}

impl FacetStore {
    pub(in crate::postings) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            dict: Vec::new(),
            index: HashMap::new(),
            ords: Vec::new(),
        }
    }

    pub(in crate::postings) fn ord(&self, slot: usize) -> Option<u32> {
        match self.ords.get(slot).copied() {
            None | Some(FACET_ABSENT) => None,
            some => some,
        }
    }

    /// Record `doc_id`'s value, interning it in the dictionary. One
    /// value per (document, facet field); a second write for the same
    /// slot panics (callers validate duplicates into a refusal before
    /// applying).
    pub(in crate::postings) fn set(&mut self, doc_id: u32, value: &str) {
        let slot = doc_id as usize;
        let ord = match self.index.get(value) {
            Some(&ord) => ord,
            None => {
                let ord = u32::try_from(self.dict.len()).expect("facet dictionary exceeds u32");
                assert!(
                    ord != FACET_ABSENT,
                    "facet dictionary exhausted u32 ordinals"
                );
                self.dict.push(value.to_string());
                self.index.insert(value.to_string(), ord);
                ord
            }
        };
        if self.ords.len() <= slot {
            self.ords.resize(slot + 1, FACET_ABSENT);
        }
        assert!(
            self.ords[slot] == FACET_ABSENT,
            "doc {doc_id} already has a value for facet field {:?}",
            self.name
        );
        self.ords[slot] = ord;
    }
}

/// Validate a column name list: non-empty unique names that fit `u16` lengths.
pub(in crate::postings) fn validate_column_names(names: &[&str]) {
    for (i, name) in names.iter().enumerate() {
        assert!(!name.is_empty(), "facet field {i} has an empty name");
        assert!(
            u16::try_from(name.len()).is_ok(),
            "facet field name exceeds u16 length"
        );
        assert!(
            !names[..i].contains(name),
            "duplicate facet field name {name:?}"
        );
    }
}

/// One `f64` numeric column: per-slot values
/// parallel to the shared slot space, `NaN` = the document has no value.
/// min/max over present values are computed at write time into the v7
/// column table (the bound metadata score-function chains lift with).
#[derive(Debug)]
pub(in crate::postings) struct NumericStore {
    /// Numeric field name from the schema.
    pub(in crate::postings) name: String,
    /// Per-slot value (`NaN` = no value). May be shorter than the slot
    /// count; missing trailing slots read as absent.
    pub(in crate::postings) vals: Vec<f64>,
}

impl NumericStore {
    pub(in crate::postings) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vals: Vec::new(),
        }
    }

    pub(in crate::postings) fn value(&self, slot: usize) -> Option<f64> {
        match self.vals.get(slot).copied() {
            None => None,
            Some(v) if v.is_nan() => None,
            some => some,
        }
    }

    /// Record `doc_id`'s value. Finite values only (`NaN` is the absence
    /// sentinel, infinities break the bound algebra — callers validate
    /// into a refusal); one value per (document, field), a second write
    /// panics.
    pub(in crate::postings) fn set(&mut self, doc_id: u32, value: f64) {
        assert!(
            value.is_finite(),
            "numeric field {:?}: non-finite value for doc {doc_id}",
            self.name
        );
        let slot = doc_id as usize;
        if self.vals.len() <= slot {
            self.vals.resize(slot + 1, f64::NAN);
        }
        assert!(
            self.vals[slot].is_nan(),
            "doc {doc_id} already has a value for numeric field {:?}",
            self.name
        );
        self.vals[slot] = value;
    }

    /// `(min, max)` over present values; `(NaN, NaN)` when none are.
    pub(in crate::postings) fn min_max(&self) -> (f64, f64) {
        let mut min = f64::NAN;
        let mut max = f64::NAN;
        for &v in &self.vals {
            if v.is_nan() {
                continue;
            }
            if min.is_nan() || v < min {
                min = v;
            }
            if max.is_nan() || v > max {
                max = v;
            }
        }
        (min, max)
    }
}

pub(in crate::postings) trait ColumnInteger: Copy + Ord + Default {
    const MIN: Self;
    const MAX: Self;
    fn bits(self) -> u64;
}

impl ColumnInteger for i64 {
    const MIN: Self = i64::MIN;
    const MAX: Self = i64::MAX;
    fn bits(self) -> u64 {
        self as u64
    }
}

impl ColumnInteger for u64 {
    const MIN: Self = u64::MIN;
    const MAX: Self = u64::MAX;
    fn bits(self) -> u64 {
        self
    }
}

pub(in crate::postings) type IntStore = IntegerStore<i64>;
pub(in crate::postings) type UintStore = IntegerStore<u64>;

/// One full-domain integer column. Presence never consumes a numeric value.
#[derive(Debug)]
pub(in crate::postings) struct IntegerStore<T> {
    pub(in crate::postings) name: String,
    pub(in crate::postings) vals: Vec<T>,
    pub(in crate::postings) present: Vec<u8>,
}

pub(in crate::postings) fn integer_present(bitmap: &[u8], slot: usize) -> bool {
    bitmap
        .get(slot / 8)
        .is_some_and(|byte| byte & (1 << (slot % 8)) != 0)
}

impl<T: ColumnInteger> IntegerStore<T> {
    pub(in crate::postings) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vals: Vec::new(),
            present: Vec::new(),
        }
    }

    pub(in crate::postings) fn value(&self, slot: usize) -> Option<T> {
        integer_present(&self.present, slot).then(|| self.vals[slot])
    }

    /// One value per document and field; a second write is a caller error.
    pub(in crate::postings) fn set(&mut self, doc_id: u32, value: T) {
        let slot = doc_id as usize;
        assert!(
            !integer_present(&self.present, slot),
            "doc {doc_id} already has a value for integer field {:?}",
            self.name
        );
        if self.vals.len() <= slot {
            self.vals.resize(slot + 1, T::default());
            self.present.resize((slot + 1).div_ceil(8), 0);
        }
        self.vals[slot] = value;
        self.present[slot / 8] |= 1 << (slot % 8);
    }

    /// The inverted range `(MAX, MIN)` means no values; a real `MIN` is present.
    pub(in crate::postings) fn min_max(&self) -> (T, T) {
        self.vals
            .iter()
            .enumerate()
            .filter(|(slot, _)| integer_present(&self.present, *slot))
            .fold((T::MAX, T::MIN), |(min, max), (_, &v)| {
                (min.min(v), max.max(v))
            })
    }

    pub(in crate::postings) fn write(&self, w: &mut impl Write, n_slots: usize) -> io::Result<()> {
        for slot in 0..n_slots {
            write_u64(w, self.value(slot).unwrap_or_default().bits())?;
        }
        let bytes = n_slots.div_ceil(8);
        let held = self.present.len().min(bytes);
        w.write_all(&self.present[..held])?;
        io::copy(&mut io::repeat(0).take((bytes - held) as u64), w)?;
        Ok(())
    }
}

/// One geo-point column: per-slot `(lat, lon)`
/// pairs parallel to the shared slot space, BOTH `NaN` = the document has
/// no point. The column's bounding box is computed at write time into
/// the v7 column table, on the pattern kind 1 set: metadata the reader
/// re-derives and compares, never trusts.
///
/// One pair per slot at a fixed 16 B stride rather than two `f64`
/// columns: a point is one value, and splitting it would let a lat
/// survive a lost lon. The absence sentinel is the pair `(NaN, NaN)` for
/// the same reason — a half-`NaN` pair is not a sparser point, it is a
/// corrupt one, and the reader refuses it.
#[derive(Debug)]
pub(in crate::postings) struct GeoStore {
    /// Geo field name from the schema.
    pub(in crate::postings) name: String,
    /// Per-slot `(lat, lon)` (`(NaN, NaN)` = no value). May be shorter than
    /// the slot count; missing trailing slots read as absent.
    pub(in crate::postings) vals: Vec<(f64, f64)>,
}

impl GeoStore {
    pub(in crate::postings) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vals: Vec::new(),
        }
    }

    pub(in crate::postings) fn value(&self, slot: usize) -> Option<(f64, f64)> {
        match self.vals.get(slot).copied() {
            None => None,
            Some((lat, lon)) if lat.is_nan() && lon.is_nan() => None,
            some => some,
        }
    }

    /// Record `doc_id`'s point. Coordinates must be finite and on the
    /// globe (callers validate these into refusals first); one point per
    /// (document, field), a second write panics.
    pub(in crate::postings) fn set(&mut self, doc_id: u32, lat: f64, lon: f64) {
        assert!(
            lat.is_finite() && (-90.0..=90.0).contains(&lat),
            "geo field {:?}: latitude {lat} is not a finite degree in [-90, 90] (doc {doc_id})",
            self.name
        );
        assert!(
            lon.is_finite() && (-180.0..=180.0).contains(&lon),
            "geo field {:?}: longitude {lon} is not a finite degree in [-180, 180] (doc {doc_id})",
            self.name
        );
        let slot = doc_id as usize;
        if self.vals.len() <= slot {
            self.vals.resize(slot + 1, (f64::NAN, f64::NAN));
        }
        assert!(
            self.vals[slot].0.is_nan() && self.vals[slot].1.is_nan(),
            "doc {doc_id} already has a point for geo field {:?}",
            self.name
        );
        self.vals[slot] = (lat, lon);
    }

    /// The column's bounding box `(min_lat, max_lat, min_lon, max_lon)`
    /// over present points; all four `NaN` when none are, the same empty
    /// convention kind 1 uses.
    pub(in crate::postings) fn bbox(&self) -> (f64, f64, f64, f64) {
        let (mut min_lat, mut max_lat) = (f64::NAN, f64::NAN);
        let (mut min_lon, mut max_lon) = (f64::NAN, f64::NAN);
        for &(lat, lon) in &self.vals {
            if lat.is_nan() && lon.is_nan() {
                continue;
            }
            if min_lat.is_nan() || lat < min_lat {
                min_lat = lat;
            }
            if max_lat.is_nan() || lat > max_lat {
                max_lat = lat;
            }
            if min_lon.is_nan() || lon < min_lon {
                min_lon = lon;
            }
            if max_lon.is_nan() || lon > max_lon {
                max_lon = lon;
            }
        }
        (min_lat, max_lat, min_lon, max_lon)
    }
}

/// A dictionary's byte order: `(order, remap)` where `order[new] = old`
/// and `remap[old] = new`. Every dictionary is written in this order at
/// flush: a reader can then resolve a string
/// range or prefix to ONE ordinal range, while the heap store keeps its
/// first-seen ordinals and remaps at write time. Byte order is the
/// order stock CEL compares strings in, so the two never disagree.
fn sorted_dictionary(dict: &[String]) -> (Vec<u32>, Vec<u32>) {
    let mut order: Vec<u32> = (0..dict.len() as u32).collect();
    order.sort_by(|&a, &b| dict[a as usize].as_bytes().cmp(dict[b as usize].as_bytes()));
    let mut remap = vec![0u32; dict.len()];
    for (new, &old) in order.iter().enumerate() {
        remap[old as usize] = new as u32;
    }
    (order, remap)
}

/// One value's ordinal in a dictionary: a binary search when the
/// dictionary is in byte order, a linear scan otherwise.
pub(in crate::postings) fn dictionary_lookup(
    dict: &[String],
    sorted: bool,
    value: &str,
) -> Option<u32> {
    if sorted {
        dict.binary_search_by(|v| v.as_bytes().cmp(value.as_bytes()))
            .ok()
            .map(|p| p as u32)
    } else {
        dict.iter().position(|v| v == value).map(|p| p as u32)
    }
}

/// [`sorted_dictionary`], or the identity order (first-seen ordinals) for
/// the oracle writer the tests use to prove first-seen files still open
/// and refuse string ordering by name.
pub(super) fn dictionary_order(dict: &[String], byte_order: bool) -> (Vec<u32>, Vec<u32>) {
    if byte_order {
        sorted_dictionary(dict)
    } else {
        let identity: Vec<u32> = (0..dict.len() as u32).collect();
        (identity.clone(), identity)
    }
}

/// Whether a dictionary is in strict byte order — what the reader checks
/// at open to know an ordinal range is meaningful, and what the heap
/// store answers about its first-seen order.
pub(in crate::postings) fn dictionary_sorted(dict: &[String]) -> bool {
    dict.windows(2)
        .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
}

/// Write a facet column's sections: the dictionary in byte order, then
/// the per-slot ordinals remapped to it.
pub(in crate::postings) fn write_facet_column<W: Write>(
    w: &mut W,
    facet: &FacetStore,
    n_slots: usize,
    byte_order: bool,
) -> io::Result<()> {
    let (order, remap) = dictionary_order(&facet.dict, byte_order);
    for &old in &order {
        let value = &facet.dict[old as usize];
        write_u16(w, value.len() as u16)?;
        w.write_all(value.as_bytes())?;
    }
    for slot in 0..n_slots {
        let ord = facet.ords.get(slot).copied().unwrap_or(FACET_ABSENT);
        write_u32(
            w,
            if ord == FACET_ABSENT {
                FACET_ABSENT
            } else {
                remap[ord as usize]
            },
        )?;
    }
    Ok(())
}
