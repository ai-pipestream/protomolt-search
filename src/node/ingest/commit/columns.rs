//! Typed column and map-entry validation for single-document commits.

use std::collections::HashSet;

use crate::node::*;

/// Resolved column indices and validated values for a single document row.
pub(super) struct ValidatedColumns<'a> {
    pub(super) facets: Vec<(usize, String)>,
    pub(super) numerics: Vec<(usize, f64)>,
    pub(super) map_facets: Vec<(usize, &'a str, &'a str)>,
    pub(super) map_numerics: Vec<(usize, &'a str, f64)>,
    pub(super) map_integers: Vec<(usize, &'a str, i64)>,
    pub(super) map_unsigned_integers: Vec<(usize, &'a str, u64)>,
    pub(super) integers: Vec<(usize, i64)>,
    pub(super) unsigned_integers: Vec<(usize, u64)>,
    pub(super) geo_points: Vec<(usize, f64, f64)>,
}

struct MapColumnSpec<'a> {
    table_label: &'static str,
    cli_flag: &'static str,
    lookup: &'a dyn Fn(&str) -> Option<usize>,
}

impl<'a> ValidatedColumns<'a> {
    /// Validate every scalar, map, timestamp, and geo column against `shard`
    /// before mutating in-memory stores or appending to the WAL.
    pub(super) fn validate(
        shard: &Bm25Shard,
        doc: &'a AddDocumentsRequest,
    ) -> Result<Self, Status> {
        let facets = validate_facets(shard, doc)?;
        let numerics = validate_numerics(shard, doc)?;
        let map_facets = validate_map_entries(
            &doc.map_facets,
            MapColumnSpec {
                table_label: "map-facet",
                cli_flag: "--map-facet-fields",
                lookup: &|name| shard.map_facet_index(name),
            },
            |e| (e.field.as_str(), e.key.as_str()),
            |e| Ok(e.value.as_str()),
        )?;
        let map_numerics = validate_map_entries(
            &doc.map_numerics,
            MapColumnSpec {
                table_label: "map-numeric",
                cli_flag: "--map-numeric-fields",
                lookup: &|name| shard.map_numeric_index(name),
            },
            |e| (e.field.as_str(), e.key.as_str()),
            |e| {
                if !e.value.is_finite() {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} has a non-finite value; omit absent entries",
                        e.field, e.key
                    )));
                }
                Ok(e.value)
            },
        )?;
        let map_integers = validate_map_entries(
            &doc.map_integers,
            MapColumnSpec {
                table_label: "map-integer",
                cli_flag: "--map-integer-fields",
                lookup: &|name| shard.map_integer_index(name),
            },
            |e| (e.field.as_str(), e.key.as_str()),
            |e| Ok(e.value),
        )?;
        let map_unsigned_integers = validate_map_entries(
            &doc.map_unsigned_integers,
            MapColumnSpec {
                table_label: "map-unsigned-integer",
                cli_flag: "--map-unsigned-integer-fields",
                lookup: &|name| shard.map_unsigned_integer_index(name),
            },
            |e| (e.field.as_str(), e.key.as_str()),
            |e| Ok(e.value),
        )?;
        let integers = validate_integers_and_timestamps(shard, doc)?;
        let unsigned_integers = validate_unsigned_integers(shard, doc)?;
        let geo_points = validate_geo_points(shard, doc)?;
        Ok(Self {
            facets,
            numerics,
            map_facets,
            map_numerics,
            map_integers,
            map_unsigned_integers,
            integers,
            unsigned_integers,
            geo_points,
        })
    }
}

fn validate_map_entries<'a, E, V>(
    entries: &'a [E],
    spec: MapColumnSpec<'_>,
    field_key: impl Fn(&'a E) -> (&'a str, &'a str),
    extract_val: impl Fn(&'a E) -> Result<V, Status>,
) -> Result<Vec<(usize, &'a str, V)>, Status> {
    let mut seen = HashSet::with_capacity(entries.len());
    let mut slots = Vec::with_capacity(entries.len());
    for entry in entries {
        let (field, key) = field_key(entry);
        if !seen.insert((field, key)) {
            return Err(Status::invalid_argument(format!(
                "map column {field:?} key {key:?} repeats in one document (a map holds one \
                 value per key)"
            )));
        }
        let Some(ci) = (spec.lookup)(field) else {
            return Err(Status::invalid_argument(format!(
                "unknown map column {field:?}; this shard's {} table ({}) does not have it",
                spec.table_label, spec.cli_flag
            )));
        };
        slots.push((ci, key, extract_val(entry)?));
    }
    Ok(slots)
}

fn validate_facets(
    shard: &Bm25Shard,
    doc: &AddDocumentsRequest,
) -> Result<Vec<(usize, String)>, Status> {
    let mut seen = HashSet::with_capacity(doc.facets.len());
    let mut slots = Vec::with_capacity(doc.facets.len());
    for fv in &doc.facets {
        if !seen.insert(fv.field.as_str()) {
            return Err(Status::invalid_argument(format!(
                "facet field {:?} repeats in one document",
                fv.field
            )));
        }
        let Some(fi) = shard.facet_index(&fv.field) else {
            return Err(Status::invalid_argument(format!(
                "unknown facet field {:?}; this shard's facet table \
                 (--facet-fields) does not have it",
                fv.field
            )));
        };
        slots.push((fi, fv.value.clone()));
    }
    Ok(slots)
}

fn validate_numerics(
    shard: &Bm25Shard,
    doc: &AddDocumentsRequest,
) -> Result<Vec<(usize, f64)>, Status> {
    let mut seen = HashSet::with_capacity(doc.numerics.len());
    let mut slots = Vec::with_capacity(doc.numerics.len());
    for nv in &doc.numerics {
        if !seen.insert(nv.field.as_str()) {
            return Err(Status::invalid_argument(format!(
                "numeric field {:?} repeats in one document",
                nv.field
            )));
        }
        let Some(ni) = shard.numeric_index(&nv.field) else {
            return Err(Status::invalid_argument(format!(
                "unknown numeric field {:?}; this shard's numeric table \
                 (--numeric-fields) does not have it",
                nv.field
            )));
        };
        if !nv.value.is_finite() {
            return Err(Status::invalid_argument(format!(
                "numeric field {:?} has a non-finite value; omit absent values instead",
                nv.field
            )));
        }
        slots.push((ni, nv.value));
    }
    Ok(slots)
}

fn validate_integers_and_timestamps(
    shard: &Bm25Shard,
    doc: &AddDocumentsRequest,
) -> Result<Vec<(usize, i64)>, Status> {
    let total = doc.integers.len() + doc.timestamps.len();
    let mut seen = HashSet::with_capacity(total);
    let mut slots = Vec::with_capacity(total);
    let resolve = |field: &str, seen: &HashSet<&str>| -> Result<usize, Status> {
        if seen.contains(field) {
            return Err(Status::invalid_argument(format!(
                "integer field {field:?} repeats in one document (integers and \
                 timestamps name the same columns)"
            )));
        }
        shard.integer_index(field).ok_or_else(|| {
            Status::invalid_argument(format!(
                "unknown integer field {field:?}; this shard's integer table \
                 (--integer-fields) does not have it"
            ))
        })
    };
    for iv in &doc.integers {
        let ii = resolve(&iv.field, &seen)?;
        seen.insert(&iv.field);
        slots.push((ii, iv.value));
    }
    for tv in &doc.timestamps {
        let ii = resolve(&tv.field, &seen)?;
        seen.insert(&tv.field);
        let Some(ts) = tv.value.as_ref() else {
            return Err(Status::invalid_argument(format!(
                "timestamp field {:?} carries no instant; omit the entry instead",
                tv.field
            )));
        };
        slots.push((ii, timestamp_to_epoch_micros(&tv.field, ts)?));
    }
    Ok(slots)
}

fn validate_unsigned_integers(
    shard: &Bm25Shard,
    doc: &AddDocumentsRequest,
) -> Result<Vec<(usize, u64)>, Status> {
    let mut seen = HashSet::with_capacity(doc.unsigned_integers.len());
    let mut slots = Vec::with_capacity(doc.unsigned_integers.len());
    for value in &doc.unsigned_integers {
        if !seen.insert(value.field.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unsigned integer field {:?} repeats in one document",
                value.field
            )));
        }
        let column = shard.unsigned_integer_index(&value.field).ok_or_else(|| {
            Status::invalid_argument(format!(
                "unknown unsigned integer field {:?}; declare it in --unsigned-integer-fields",
                value.field
            ))
        })?;
        slots.push((column, value.value));
    }
    Ok(slots)
}

fn validate_geo_points(
    shard: &Bm25Shard,
    doc: &AddDocumentsRequest,
) -> Result<Vec<(usize, f64, f64)>, Status> {
    let mut seen = HashSet::with_capacity(doc.geo_points.len());
    let mut slots = Vec::with_capacity(doc.geo_points.len());
    for gp in &doc.geo_points {
        if !seen.insert(gp.field.as_str()) {
            return Err(Status::invalid_argument(format!(
                "geo field {:?} repeats in one document (a document holds one point per column)",
                gp.field
            )));
        }
        let gi = shard.geo_index(&gp.field).ok_or_else(|| {
            Status::invalid_argument(format!(
                "unknown geo field {:?}; this shard's geo table (--geo-fields) does not have it",
                gp.field
            ))
        })?;
        validate_lat_lon(&format!("geo field {:?}", gp.field), gp.lat, gp.lon)?;
        slots.push((gi, gp.lat, gp.lon));
    }
    Ok(slots)
}
