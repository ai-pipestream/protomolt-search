//! Request validation for range facets, geo filters and timestamps.

use super::*;

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
