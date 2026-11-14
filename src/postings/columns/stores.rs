//! Typed heap column stores, dictionary encoders, and section write helpers.

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

/// One `f64` numeric column (`docs/score-functions.md`): per-slot values
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

/// One geo-point column (`docs/geo-columns.md`): per-slot `(lat, lon)`
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

/// One `map<string, string>` column (`docs/map-columns.md`): interned
/// key and value dictionaries plus per-slot `(key_ord, value_ord)` pair
/// lists, kept sorted by key ordinal within each document. At most one
/// value per `(document, key)` — map semantics.
#[derive(Debug)]
pub(in crate::postings) struct MapFacetStore {
    /// Map column name from the schema.
    pub(in crate::postings) name: String,
    /// Keys in ordinal (first-seen) order.
    pub(in crate::postings) keys: Vec<String>,
    /// key → ordinal (heap only).
    pub(in crate::postings) key_index: HashMap<String, u32>,
    /// Values in ordinal (first-seen) order, one dictionary for the
    /// whole column (values are shared across keys).
    pub(in crate::postings) values: Vec<String>,
    /// value → ordinal (heap only).
    pub(in crate::postings) value_index: HashMap<String, u32>,
    /// Per-slot pair lists, sorted by key ordinal. May be shorter than
    /// the slot count; missing trailing slots read as empty.
    pub(in crate::postings) pairs: Vec<Vec<(u32, u32)>>,
}

impl MapFacetStore {
    pub(in crate::postings) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            keys: Vec::new(),
            key_index: HashMap::new(),
            values: Vec::new(),
            value_index: HashMap::new(),
            pairs: Vec::new(),
        }
    }

    /// The value ordinal of `doc`'s entry under `key_ord`, `None` when
    /// the document has no entry for the key.
    pub(in crate::postings) fn value_ord(&self, slot: usize, key_ord: u32) -> Option<u32> {
        let list = self.pairs.get(slot)?;
        list.binary_search_by_key(&key_ord, |&(k, _)| k)
            .ok()
            .map(|i| list[i].1)
    }

    /// Record `doc_id`'s entry: intern key and value, insert sorted by
    /// key ordinal. A second entry under the same `(document, key)`
    /// panics (callers validate duplicates into a refusal).
    pub(in crate::postings) fn set(&mut self, doc_id: u32, key: &str, value: &str) {
        let key_ord = intern(&mut self.keys, &mut self.key_index, key, &self.name);
        let value_ord = intern(&mut self.values, &mut self.value_index, value, &self.name);
        let slot = doc_id as usize;
        if self.pairs.len() <= slot {
            self.pairs.resize_with(slot + 1, Vec::new);
        }
        let list = &mut self.pairs[slot];
        match list.binary_search_by_key(&key_ord, |&(k, _)| k) {
            Ok(_) => panic!(
                "doc {doc_id} already has an entry under key {key:?} in map column {:?}",
                self.name
            ),
            Err(pos) => list.insert(pos, (key_ord, value_ord)),
        }
    }
}

/// One `map<string, f64>` column: interned key dictionary plus per-slot
/// `(key_ord, value)` pair lists, sorted by key ordinal. Per-key min/max
/// are computed at write time into the column table — the bound
/// metadata map-keyed score stages lift with.
#[derive(Debug)]
pub(in crate::postings) struct MapNumericStore {
    /// Map column name from the schema.
    pub(in crate::postings) name: String,
    /// Keys in ordinal (first-seen) order.
    pub(in crate::postings) keys: Vec<String>,
    /// key → ordinal (heap only).
    pub(in crate::postings) key_index: HashMap<String, u32>,
    /// Per-slot pair lists, sorted by key ordinal; values are finite
    /// (callers validate).
    pub(in crate::postings) pairs: Vec<Vec<(u32, f64)>>,
}

impl MapNumericStore {
    pub(in crate::postings) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            keys: Vec::new(),
            key_index: HashMap::new(),
            pairs: Vec::new(),
        }
    }

    /// `doc`'s value under `key_ord`, `None` when absent.
    pub(in crate::postings) fn value(&self, slot: usize, key_ord: u32) -> Option<f64> {
        let list = self.pairs.get(slot)?;
        list.binary_search_by_key(&key_ord, |&(k, _)| k)
            .ok()
            .map(|i| list[i].1)
    }

    /// Record `doc_id`'s entry; finite values only, one per
    /// `(document, key)` — same contract shape as [`MapFacetStore::set`].
    pub(in crate::postings) fn set(&mut self, doc_id: u32, key: &str, value: f64) {
        assert!(
            value.is_finite(),
            "map column {:?}: non-finite value under key {key:?} for doc {doc_id}",
            self.name
        );
        let key_ord = intern(&mut self.keys, &mut self.key_index, key, &self.name);
        let slot = doc_id as usize;
        if self.pairs.len() <= slot {
            self.pairs.resize_with(slot + 1, Vec::new);
        }
        let list = &mut self.pairs[slot];
        match list.binary_search_by_key(&key_ord, |&(k, _)| k) {
            Ok(_) => panic!(
                "doc {doc_id} already has an entry under key {key:?} in map column {:?}",
                self.name
            ),
            Err(pos) => list.insert(pos, (key_ord, value)),
        }
    }

    /// `(min, max)` per key ordinal over present values; `(NaN, NaN)` for
    /// keys that ended up with no entries (cannot happen through `set`,
    /// but the encoding tolerates it).
    pub(in crate::postings) fn key_min_max(&self) -> Vec<(f64, f64)> {
        let mut mm = vec![(f64::NAN, f64::NAN); self.keys.len()];
        for list in &self.pairs {
            for &(k, v) in list {
                let (min, max) = &mut mm[k as usize];
                if min.is_nan() || v < *min {
                    *min = v;
                }
                if max.is_nan() || v > *max {
                    *max = v;
                }
            }
        }
        mm
    }
}

/// Write a map column's offsets section (`(n_slots + 1) x u32` prefix
/// sums of per-doc pair counts) followed by its pairs section, one
/// `write_pair` per entry in slot order.
fn write_map_offsets_and_pairs<W: Write, T>(
    w: &mut W,
    n_slots: usize,
    pairs: &[Vec<T>],
    mut write_pair: impl FnMut(&mut W, &T) -> io::Result<()>,
) -> io::Result<()> {
    let mut running = 0u64;
    write_u32(w, 0)?;
    for slot in 0..n_slots {
        running += pairs.get(slot).map_or(0, |l| l.len()) as u64;
        write_u32(
            w,
            u32::try_from(running).expect("map column exceeds u32 total pairs"),
        )?;
    }
    for list in pairs.iter().take(n_slots) {
        for pair in list {
            write_pair(w, pair)?;
        }
    }
    Ok(())
}

/// A dictionary's byte order: `(order, remap)` where `order[new] = old`
/// and `remap[old] = new`. Every dictionary is written in this order at
/// flush (`docs/prefix-terms.md`): a reader can then resolve a string
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

/// [`sorted_dictionary`], or the identity order (first-seen ordinals, the
/// layout every file before 2026-09-02 has) for the legacy oracle
/// writer the tests use to prove old files still open and refuse string
/// ordering by name.
fn dictionary_order(dict: &[String], byte_order: bool) -> (Vec<u32>, Vec<u32>) {
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

/// Write a map-facet column's sections with both dictionaries in byte
/// order and every pair list remapped and re-sorted by the new key
/// ordinal (the reader binary-searches pairs by key ordinal).
pub(in crate::postings) fn write_map_facet_column<W: Write>(
    w: &mut W,
    c: &MapFacetStore,
    n_slots: usize,
    byte_order: bool,
) -> io::Result<()> {
    let (key_order, key_remap) = dictionary_order(&c.keys, byte_order);
    let (value_order, value_remap) = dictionary_order(&c.values, byte_order);
    for &old in &key_order {
        let key = &c.keys[old as usize];
        write_u16(w, key.len() as u16)?;
        w.write_all(key.as_bytes())?;
    }
    for &old in &value_order {
        let value = &c.values[old as usize];
        write_u16(w, value.len() as u16)?;
        w.write_all(value.as_bytes())?;
    }
    let pairs: Vec<Vec<(u32, u32)>> = c
        .pairs
        .iter()
        .map(|list| {
            let mut remapped: Vec<(u32, u32)> = list
                .iter()
                .map(|&(k, v)| (key_remap[k as usize], value_remap[v as usize]))
                .collect();
            remapped.sort_unstable();
            remapped
        })
        .collect();
    write_map_offsets_and_pairs(w, n_slots, &pairs, |w, &(k, v)| {
        write_u32(w, k)?;
        write_u32(w, v)
    })
}

/// Write a map-numeric column's sections with the key dictionary in
/// byte order (each entry carrying ITS key's min/max) and every pair
/// list remapped and re-sorted.
pub(in crate::postings) fn write_map_numeric_column<W: Write>(
    w: &mut W,
    c: &MapNumericStore,
    n_slots: usize,
    byte_order: bool,
) -> io::Result<()> {
    let (key_order, key_remap) = dictionary_order(&c.keys, byte_order);
    let mm = c.key_min_max();
    for &old in &key_order {
        let key = &c.keys[old as usize];
        let (min, max) = mm[old as usize];
        write_u16(w, key.len() as u16)?;
        w.write_all(key.as_bytes())?;
        write_u64(w, min.to_bits())?;
        write_u64(w, max.to_bits())?;
    }
    let pairs: Vec<Vec<(u32, f64)>> = c
        .pairs
        .iter()
        .map(|list| {
            let mut remapped: Vec<(u32, f64)> = list
                .iter()
                .map(|&(k, v)| (key_remap[k as usize], v))
                .collect();
            remapped.sort_by_key(|&(k, _)| k);
            remapped
        })
        .collect();
    write_map_offsets_and_pairs(w, n_slots, &pairs, |w, &(k, v)| {
        write_u32(w, k)?;
        write_u64(w, v.to_bits())
    })
}

pub(in crate::postings) fn integer_map_sections<T: crate::integer_map::Integer>(
    columns: &[crate::integer_map::Store<T>],
    rows: u32,
    cursor: &mut u64,
) -> io::Result<Vec<(u64, u64)>> {
    columns
        .iter()
        .map(|column| {
            let len = column.encoded_len(rows)?;
            let section = (*cursor, len);
            *cursor = cursor.checked_add(len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "integer map section overflow")
            })?;
            Ok(section)
        })
        .collect()
}

pub(in crate::postings) fn write_integer_map_entries<T: crate::integer_map::Integer>(
    w: &mut impl Write,
    columns: &[crate::integer_map::Store<T>],
    sections: &[(u64, u64)],
    kind: u8,
) -> io::Result<()> {
    for (column, &(off, len)) in columns.iter().zip(sections) {
        write_u16(w, column.name.len() as u16)?;
        w.write_all(column.name.as_bytes())?;
        w.write_all(&[kind])?;
        write_u64(w, off)?;
        write_u64(w, len)?;
    }
    Ok(())
}

/// Intern `value` into a dictionary, returning its ordinal.
fn intern(dict: &mut Vec<String>, index: &mut HashMap<String, u32>, value: &str, col: &str) -> u32 {
    match index.get(value) {
        Some(&ord) => ord,
        None => {
            let ord = u32::try_from(dict.len())
                .unwrap_or_else(|_| panic!("dictionary of column {col:?} exceeds u32"));
            assert!(
                ord != FACET_ABSENT,
                "dictionary of column {col:?} exhausted u32 ordinals"
            );
            dict.push(value.to_string());
            index.insert(value.to_string(), ord);
            ord
        }
    }
}
