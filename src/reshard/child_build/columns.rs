//! A document's column values written into the child's BM25 store.

use std::collections::{HashMap, HashSet};

use crate::pb::AddDocumentsRequest;
use crate::postings::SpillBuilder;

use super::ColumnTables;

/// One column family's name-to-slot index, built once per child.
struct Slots {
    label: &'static str,
    index: HashMap<String, usize>,
}

impl Slots {
    fn new(label: &'static str, table: &[String]) -> Result<Self, String> {
        let mut index = HashMap::with_capacity(table.len());
        for (slot, name) in table.iter().enumerate() {
            if index.insert(name.clone(), slot).is_some() {
                return Err(format!("duplicate {label} column {name:?} in child table"));
            }
        }
        Ok(Self { label, index })
    }

    fn get(&self, local: u32, name: &str) -> Result<usize, String> {
        self.index.get(name).copied().ok_or_else(|| {
            format!(
                "record at child slot {local}: {} column {name:?} is absent from the replay table",
                self.label
            )
        })
    }
}

/// Pre-indexed slot lookup for all nine column families of one child.
pub(super) struct ColumnSlots {
    facets: Slots,
    numerics: Slots,
    map_facets: Slots,
    map_numerics: Slots,
    map_integers: Slots,
    map_unsigned_integers: Slots,
    integers: Slots,
    unsigned_integers: Slots,
    geo: Slots,
}

impl ColumnSlots {
    pub(super) fn new(tables: &ColumnTables) -> Result<Self, String> {
        Ok(Self {
            facets: Slots::new("facet", &tables.facets)?,
            numerics: Slots::new("numeric", &tables.numerics)?,
            map_facets: Slots::new("map-facet", &tables.map_facets)?,
            map_numerics: Slots::new("map-numeric", &tables.map_numerics)?,
            map_integers: Slots::new("integer map", &tables.map_integers)?,
            map_unsigned_integers: Slots::new(
                "unsigned integer map",
                &tables.map_unsigned_integers,
            )?,
            integers: Slots::new("integer", &tables.integers)?,
            unsigned_integers: Slots::new("unsigned", &tables.unsigned_integers)?,
            geo: Slots::new("geo", &tables.geo)?,
        })
    }
}

/// Refuses a second value for one scalar slot, or one map slot and key.
struct Seen<'a> {
    local: u32,
    label: &'static str,
    keys: HashSet<(usize, &'a str)>,
}

impl<'a> Seen<'a> {
    fn new(local: u32, label: &'static str) -> Self {
        Self {
            local,
            label,
            keys: HashSet::new(),
        }
    }

    fn insert(&mut self, slot: usize, key: &'a str) -> Result<(), String> {
        if self.keys.insert((slot, key)) {
            return Ok(());
        }
        Err(format!(
            "record at child slot {}: {} column slot {slot} key {key:?} repeats",
            self.local, self.label
        ))
    }
}

/// Write every column value the document carries at `local`.
pub(super) fn set_columns(
    builder: &mut SpillBuilder,
    slots: &ColumnSlots,
    local: u32,
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    set_keyword_columns(builder, slots, local, doc)?;
    set_map_integer_columns(builder, slots, local, doc)?;
    set_integer_columns(builder, slots, local, doc)?;
    let mut seen = Seen::new(local, "geo");
    for e in &doc.geo_points {
        let gi = slots.geo.get(local, &e.field)?;
        seen.insert(gi, "")?;
        builder.set_geo(gi, local, e.lat, e.lon);
    }
    Ok(())
}

fn set_keyword_columns(
    builder: &mut SpillBuilder,
    slots: &ColumnSlots,
    local: u32,
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    let mut seen = Seen::new(local, "facet");
    for fv in &doc.facets {
        let fi = slots.facets.get(local, &fv.field)?;
        seen.insert(fi, "")?;
        builder.set_facet(fi, local, &fv.value);
    }
    let mut seen = Seen::new(local, "numeric");
    for nv in &doc.numerics {
        let ni = slots.numerics.get(local, &nv.field)?;
        seen.insert(ni, "")?;
        builder.set_numeric(ni, local, nv.value);
    }
    let mut seen = Seen::new(local, "map-facet");
    for e in &doc.map_facets {
        let ci = slots.map_facets.get(local, &e.field)?;
        seen.insert(ci, &e.key)?;
        builder.set_map_facet(ci, local, &e.key, &e.value);
    }
    let mut seen = Seen::new(local, "map-numeric");
    for e in &doc.map_numerics {
        let ci = slots.map_numerics.get(local, &e.field)?;
        seen.insert(ci, &e.key)?;
        builder.set_map_numeric(ci, local, &e.key, e.value);
    }
    Ok(())
}

fn set_map_integer_columns(
    builder: &mut SpillBuilder,
    slots: &ColumnSlots,
    local: u32,
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    for e in &doc.map_integers {
        let ci = slots.map_integers.get(local, &e.field)?;
        builder
            .set_map_integer(ci, local, &e.key, e.value)
            .map_err(|e| format!("integer map replay: {e}"))?;
    }
    for e in &doc.map_unsigned_integers {
        let ci = slots.map_unsigned_integers.get(local, &e.field)?;
        builder
            .set_map_unsigned_integer(ci, local, &e.key, e.value)
            .map_err(|e| format!("integer map replay: {e}"))?;
    }
    Ok(())
}

/// Integers, unsigned integers, and timestamps. Timestamps re-run the
/// node's conversion from the instant the WAL kept: the child gets the
/// same epoch micros the parent stored, not a copy of a copy. Integers and
/// timestamps share one slot table, so one value per slot across both.
fn set_integer_columns(
    builder: &mut SpillBuilder,
    slots: &ColumnSlots,
    local: u32,
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    let mut seen = Seen::new(local, "integer");
    for e in &doc.integers {
        let ii = slots.integers.get(local, &e.field)?;
        seen.insert(ii, "")?;
        builder.set_integer(ii, local, e.value);
    }
    let mut unsigned_seen = Seen::new(local, "unsigned");
    for value in &doc.unsigned_integers {
        let column = slots.unsigned_integers.get(local, &value.field)?;
        unsigned_seen.insert(column, "")?;
        builder.set_unsigned_integer(column, local, value.value);
    }
    for e in &doc.timestamps {
        let ii = slots.integers.get(local, &e.field)?;
        seen.insert(ii, "")?;
        let ts = e.value.as_ref().ok_or_else(|| {
            format!(
                "record at child slot {local}: timestamp field {:?} carries no instant",
                e.field
            )
        })?;
        let micros = crate::node::timestamp_to_epoch_micros(&e.field, ts)
            .map_err(|e| format!("record at child slot {local}: {}", e.message()))?;
        builder.set_integer(ii, local, micros);
    }
    Ok(())
}
