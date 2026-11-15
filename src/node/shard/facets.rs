//! Facet counting, range-facet bucketing, and geographic filter validation.

use crate::node::*;

impl Bm25Shard {
    /// Count plain, map-keyed, and range facets plus column stats and distinct
    /// values over one shared match-set bitmap (every document matching at least
    /// one queried term and passing `filter`, independent of `k` and `min_score`).
    /// Unknown columns return `known: false` so the coordinator refuses only
    /// columns unknown across all shards.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    pub(in crate::node) fn count_facets(
        &self,
        views: &[(&dyn Bm25Index, &[String])],
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        stats_fields: &[String],
        cardinality_fields: &[String],
        filter: crate::bm25::FilterCtx,
    ) -> Result<
        (
            Vec<crate::pb::FacetFieldCounts>,
            Vec<crate::pb::RangeFacetCounts>,
            Vec<crate::pb::ColumnStats>,
            Vec<crate::pb::FacetDistinct>,
        ),
        Status,
    > {
        let n_slots = self.next_doc_id() as usize;
        let mut bits = vec![0u64; n_slots.div_ceil(64)];
        for &(view, terms) in views {
            for term in terms {
                view.for_each_doc_tf(term, &mut |doc_id, _tf| {
                    bits[doc_id as usize / 64] |= 1u64 << (doc_id % 64);
                });
            }
        }
        // One filter evaluation per matched document, exactly like the
        // ordinal resolution below — never per posting.
        if let Some((doc_filter, cols)) = filter {
            for (wi, bits_word) in bits.iter_mut().enumerate() {
                let mut w = *bits_word;
                while w != 0 {
                    let doc = (wi * 64) as u32 + w.trailing_zeros();
                    if !doc_filter.passes(doc, cols) {
                        *bits_word &= !(1u64 << (doc % 64));
                    }
                    w &= w - 1;
                }
            }
        }
        // Count matched documents per ordinal slot; map facets binary-search
        // each document's sorted `(key_ord, val_ord)` pairs.
        let count_by = |resolve: &dyn Fn(u32) -> Option<u32>, n_values: usize| -> Vec<u64> {
            let mut counts = vec![0u64; n_values];
            for (wi, &word) in bits.iter().enumerate() {
                let mut w = word;
                while w != 0 {
                    let doc = (wi * 64) as u32 + w.trailing_zeros();
                    if let Some(ord) = resolve(doc) {
                        counts[ord as usize] += 1;
                    }
                    w &= w - 1;
                }
            }
            counts
        };
        // Dictionary (ordinal) order — deterministic per shard; the
        // coordinator sorts the merged counts.
        let to_wire = |counts: Vec<u64>, value_of: &dyn Fn(u32) -> String| {
            counts
                .iter()
                .enumerate()
                .filter(|&(_, &c)| c > 0)
                .map(|(ord, &c)| crate::pb::FacetCount {
                    value: value_of(ord as u32),
                    count: c,
                })
                .collect()
        };
        let mut out: Vec<crate::pb::FacetFieldCounts> = facet_fields
            .iter()
            .map(|name| {
                let Some(fi) = self.facet_index(name) else {
                    return crate::pb::FacetFieldCounts {
                        field: name.clone(),
                        known: false,
                        counts: Vec::new(),
                        key: String::new(),
                        map_key: None,
                    };
                };
                let counts = count_by(&|doc| self.facet_ord(fi, doc), self.facet_value_count(fi));
                crate::pb::FacetFieldCounts {
                    field: name.clone(),
                    known: true,
                    counts: to_wire(counts, &|ord| self.facet_value(fi, ord).to_string()),
                    key: String::new(),
                    map_key: None,
                }
            })
            .collect();
        for req in map_facet_fields {
            // Known = the column exists AND its key dictionary has the
            // key: a key no shard ever ingested would otherwise make a
            // typo'd drill-down read as zero results everywhere.
            let resolved = self
                .map_facet_index(&req.column)
                .and_then(|ci| self.map_facet_key_ord(ci, &req.key).map(|k| (ci, k)));
            let Some((ci, key_ord)) = resolved else {
                out.push(crate::pb::FacetFieldCounts {
                    field: req.column.clone(),
                    known: false,
                    counts: Vec::new(),
                    key: req.key.clone(),
                    map_key: Some(req.key.clone()),
                });
                continue;
            };
            let counts = count_by(
                &|doc| self.map_facet_value_ord(ci, key_ord, doc),
                self.map_facet_value_count(ci),
            );
            out.push(crate::pb::FacetFieldCounts {
                field: req.column.clone(),
                known: true,
                counts: to_wire(counts, &|ord| self.map_facet_value(ci, ord).to_string()),
                key: req.key.clone(),
                map_key: Some(req.key.clone()),
            });
        }
        // Range facets: one value read per matched document, then a
        // binary search of the (validated) edge list. Buckets are
        // half-open [edges[i], edges[i+1]), so a value sitting exactly
        // on an interior edge lands in the upper bucket; a value below
        // the first edge or at/above the last is counted in NO bucket.
        // There are deliberately no implicit tail buckets — the caller
        // asked for these intervals and gets these intervals.
        let ranges = range_facet_fields
            .iter()
            .map(|req| {
                let intervals = crate::rangefacet::Intervals::new(req)
                    .expect("range facets validated before counting");
                let source =
                    self.resolve_range_column(&req.column, req.map_key(), req.typed_map.is_some());
                let mut counts = vec![0u64; intervals.len()];
                if let Some(source) = source {
                    for (wi, &word) in bits.iter().enumerate() {
                        let mut w = word;
                        while w != 0 {
                            let doc = (wi * 64) as u32 + w.trailing_zeros();
                            if let Some(bucket) = self
                                .range_value(source, doc)
                                .and_then(|value| intervals.bucket(value))
                            {
                                counts[bucket] += 1;
                            }
                            w &= w - 1;
                        }
                    }
                }
                intervals.response(counts, source.is_some())
            })
            .collect();
        // Column stats: one value read per matched document per field,
        // over the same bitmap. Absence contributes nothing — `count`
        // is the number of documents that HELD a value, which is what
        // makes mean = sum / count honest.
        let stats = stats_fields
            .iter()
            .map(|name| {
                use crate::pb::ScalarValueType as Type;
                let source = self.resolve_range_column(name, None, false);
                let ty = match source {
                    Some(RangeSource::Numeric(_)) => Type::Number,
                    Some(RangeSource::Integer(_)) => Type::Integer,
                    Some(RangeSource::Unsigned(_)) => Type::UnsignedInteger,
                    _ => Type::Unspecified,
                };
                let mut out = crate::column_stats::Collector::new(name, ty);
                if let Some(source) = source {
                    for (wi, &word) in bits.iter().enumerate() {
                        let mut w = word;
                        while w != 0 {
                            let doc = (wi * 64) as u32 + w.trailing_zeros();
                            if let Some(value) = self.range_value(source, doc) {
                                out.observe(value)?;
                            }
                            w &= w - 1;
                        }
                    }
                }
                out.finish()
            })
            .collect::<Result<Vec<_>, Status>>()?;
        // Distinct facet values in the match set: an ordinal bitset
        // over this shard's dictionary, then the VALUES — ordinals are
        // shard-local, so strings are the only union-able currency the
        // coordinator can merge.
        let distinct = cardinality_fields
            .iter()
            .map(|name| {
                let Some(fi) = self.facet_index(name) else {
                    return crate::pb::FacetDistinct {
                        field: name.clone(),
                        known: false,
                        values: Vec::new(),
                    };
                };
                let n_values = self.facet_value_count(fi);
                let mut present = vec![0u64; n_values.div_ceil(64)];
                for (wi, &word) in bits.iter().enumerate() {
                    let mut w = word;
                    while w != 0 {
                        let doc = (wi * 64) as u32 + w.trailing_zeros();
                        if let Some(ord) = self.facet_ord(fi, doc) {
                            present[ord as usize / 64] |= 1u64 << (ord % 64);
                        }
                        w &= w - 1;
                    }
                }
                let values = (0..n_values as u32)
                    .filter(|&ord| present[ord as usize / 64] >> (ord % 64) & 1 == 1)
                    .map(|ord| self.facet_value(fi, ord).to_string())
                    .collect();
                crate::pb::FacetDistinct {
                    field: name.clone(),
                    known: true,
                    values,
                }
            })
            .collect();
        Ok((out, ranges, stats, distinct))
    }

    /// Resolve a range facet's column against THIS shard's tables:
    /// with no key the f64, i64 and u64 tables (one name space
    /// across kinds, so at most one answers), with a key the
    /// map-numeric column and its key ordinal; typed map requests also resolve
    /// i64/u64 map tables. `None` = this shard
    /// cannot resolve it, which answers `known: false`.
    pub(in crate::node) fn resolve_range_column(
        &self,
        column: &str,
        key: Option<&str>,
        typed_map: bool,
    ) -> Option<RangeSource> {
        if key.is_none() {
            if let Some(ni) = self.numeric_index(column) {
                return Some(RangeSource::Numeric(ni));
            }
            return self
                .integer_index(column)
                .map(RangeSource::Integer)
                .or_else(|| {
                    self.unsigned_integer_index(column)
                        .map(RangeSource::Unsigned)
                });
        }
        let key = key?;
        if typed_map {
            if let Some(ci) = self.map_integer_index(column) {
                return self
                    .map_integer_key_ord(ci, key)
                    .map(|key_ord| RangeSource::MapInteger {
                        column: ci,
                        key_ord,
                    });
            }
            if let Some(ci) = self.map_unsigned_integer_index(column) {
                return self.map_unsigned_integer_key_ord(ci, key).map(|key_ord| {
                    RangeSource::MapUnsigned {
                        column: ci,
                        key_ord,
                    }
                });
            }
        }
        let ci = self.map_numeric_index(column)?;
        let key_ord = self.map_numeric_key_ord(ci, key)?;
        Some(RangeSource::MapKey {
            column: ci,
            key_ord,
        })
    }

    /// Keep each value in its numeric domain for exact interval comparison.
    pub(in crate::node) fn range_value(
        &self,
        source: RangeSource,
        doc_id: u32,
    ) -> Option<crate::filter::NumBound> {
        use crate::filter::NumBound;
        match source {
            RangeSource::Numeric(ni) => self.numeric_value(ni, doc_id).map(NumBound::F),
            RangeSource::Integer(ii) => self.integer_value(ii, doc_id).map(NumBound::I),
            RangeSource::Unsigned(ui) => self.unsigned_integer_value(ui, doc_id).map(NumBound::U),
            RangeSource::MapKey { column, key_ord } => self
                .map_numeric_value(column, key_ord, doc_id)
                .map(NumBound::F),
            RangeSource::MapInteger { column, key_ord } => self
                .map_integer_value(column, key_ord, doc_id)
                .map(NumBound::I),
            RangeSource::MapUnsigned { column, key_ord } => self
                .map_unsigned_integer_value(column, key_ord, doc_id)
                .map(NumBound::U),
        }
    }
}

/// A range facet's resolved column on THIS shard. The numeric shapes a
/// bucketable value can live in; see [`Bm25Shard::resolve_range_column`].
#[derive(Debug, Clone, Copy)]
pub(in crate::node) enum RangeSource {
    /// Index into the shard's f64 table.
    Numeric(usize),
    /// Index into the shard's i64 table.
    Integer(usize),
    /// Index into the shard's u64 table.
    Unsigned(usize),
    /// A map-numeric column and a key ordinal in ITS key dictionary.
    MapKey {
        /// Index into the shard's map-numeric table.
        column: usize,
        /// Key ordinal within that column's key dictionary.
        key_ord: u32,
    },
    /// Exact signed map entry and its dictionary-local key.
    MapInteger { column: usize, key_ord: u32 },
    /// Exact unsigned map entry and its dictionary-local key.
    MapUnsigned { column: usize, key_ord: u32 },
}

/// Validate range-facet edge lists: each field requires at least two finite
/// edges in strictly ascending order.
pub(crate) fn validate_range_facet_fields(
    fields: &[crate::pb::RangeFacetField],
) -> Result<(), Status> {
    for req in fields {
        crate::rangefacet::Intervals::new(req)?;
    }
    Ok(())
}

/// Refuse a (lat, lon) pair that is not a finite degree pair on the
/// globe, naming `what`. Shared by ingest, geo filters, and geo score
/// stages: a coordinate that cannot exist is a producer bug wherever it
/// arrives, and the engine says so rather than clamping it onto the
/// nearest pole.
pub(in crate::node) fn validate_lat_lon(what: &str, lat: f64, lon: f64) -> Result<(), Status> {
    if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
        return Err(Status::invalid_argument(format!(
            "{what}: latitude {lat} is not a finite degree in [-90, 90]"
        )));
    }
    if !lon.is_finite() || !(-180.0..=180.0).contains(&lon) {
        return Err(Status::invalid_argument(format!(
            "{what}: longitude {lon} is not a finite degree in [-180, 180]"
        )));
    }
    Ok(())
}

/// Validate geo filters locally and resolve each into a [`crate::geo::GeoRegion`],
/// rejecting empty column names, unset regions, non-finite/out-of-range degrees,
/// inverted latitude or antimeridian-crossing longitude bounds, and non-positive
/// or unspecified-metric radii.
pub(crate) fn validate_geo_filters(
    filters: &[crate::pb::GeoFilter],
) -> Result<Vec<crate::geo::GeoRegion>, Status> {
    filters.iter().map(validate_geo_filter).collect()
}

/// Validate a single [`crate::pb::GeoFilter`] (shared by top-level `geo_filters`
/// and compiled filter-tree geo leaves).
pub(crate) fn validate_geo_filter(
    f: &crate::pb::GeoFilter,
) -> Result<crate::geo::GeoRegion, Status> {
    if f.column.is_empty() {
        return Err(Status::invalid_argument(
            "geo filter: a filter names the geo column it reads",
        ));
    }
    let named = format!("geo filter {:?}", f.column);
    match f.region.as_ref() {
        Some(crate::pb::geo_filter::Region::Bbox(b)) => {
            validate_lat_lon(&format!("{named} bbox min"), b.min_lat, b.min_lon)?;
            validate_lat_lon(&format!("{named} bbox max"), b.max_lat, b.max_lon)?;
            if b.min_lat > b.max_lat {
                return Err(Status::invalid_argument(format!(
                    "{named}: min_lat {} is above max_lat {}",
                    b.min_lat, b.max_lat
                )));
            }
            if b.min_lon > b.max_lon {
                return Err(Status::invalid_argument(format!(
                    "{named}: min_lon {} is east of max_lon {}, which would describe \
                             an antimeridian-crossing box. This increment REFUSES wraparound \
                             boxes rather than guessing: send two boxes, one each side of \
                             180 degrees, and union the results yourself.",
                    b.min_lon, b.max_lon
                )));
            }
            Ok(crate::geo::GeoRegion::Bbox {
                min_lat: b.min_lat,
                max_lat: b.max_lat,
                min_lon: b.min_lon,
                max_lon: b.max_lon,
            })
        }
        Some(crate::pb::geo_filter::Region::Radius(r)) => {
            validate_lat_lon(&format!("{named} radius origin"), r.lat, r.lon)?;
            if !r.meters.is_finite() || r.meters <= 0.0 {
                return Err(Status::invalid_argument(format!(
                    "{named}: radius meters {} must be finite and above zero",
                    r.meters
                )));
            }
            let metric = match crate::pb::GeoMetric::try_from(r.metric) {
                Ok(crate::pb::GeoMetric::Haversine) => crate::geo::GeoMetric::Haversine,
                Ok(crate::pb::GeoMetric::Manhattan) => crate::geo::GeoMetric::Manhattan,
                Ok(crate::pb::GeoMetric::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "{named}: unknown geo metric {}",
                        r.metric
                    )));
                }
            };
            Ok(crate::geo::GeoRegion::Radius {
                lat: r.lat,
                lon: r.lon,
                meters: r.meters,
                metric,
            })
        }
        None => Err(Status::invalid_argument(format!(
            "{named}: no region set; a filter must name a bbox or a radius"
        ))),
    }
}

/// Convert a `google.protobuf.Timestamp` to epoch microseconds for `i64`
/// storage, flooring sub-microsecond `nanos` toward negative infinity and
/// rejecting out-of-range seconds or invalid `nanos`.
pub(crate) fn timestamp_to_epoch_micros(
    field: &str,
    ts: &prost_types::Timestamp,
) -> Result<i64, Status> {
    crate::protobuf::validate_timestamp(field, ts)?;
    let micros = ts
        .seconds
        .checked_mul(1_000_000)
        .and_then(|s| s.checked_add(i64::from(ts.nanos) / 1_000))
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "timestamp field {field:?}: {} seconds does not fit i64 epoch micros",
                ts.seconds
            ))
        })?;
    Ok(micros)
}

/// Every requested range facet answered unknown with no buckets — what
/// a shard with no lexical half (and therefore no columns) reports.
pub(in crate::node) fn unknown_range_counts(
    fields: &[crate::pb::RangeFacetField],
) -> Vec<crate::pb::RangeFacetCounts> {
    fields
        .iter()
        .map(|req| {
            let intervals = crate::rangefacet::Intervals::new(req)
                .expect("range facets validated before unknown response");
            intervals.response(vec![0; intervals.len()], false)
        })
        .collect()
}
