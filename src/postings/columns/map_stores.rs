//! Map column heap stores (`MapFacetStore`, `MapNumericStore`) and map section serialization.

use std::collections::HashMap;
use std::io::{self, Write};

use super::super::format::{write_u16, write_u32, write_u64};
use super::stores::dictionary_order;
use super::FACET_ABSENT;

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
