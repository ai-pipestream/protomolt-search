//! Geo-point primitives: distances, regions, and resolved filters
//! (`docs/geo-columns.md`).
//!
//! The engine owns geo PRIMITIVES — a coordinate is a column value, a
//! region is a predicate over it, a distance is a monotone score
//! transform. It does not own road networks: travel time and energy are
//! an enrichment sidecar's job, and their outputs arrive back here as
//! ordinary map-numeric columns keyed by anchor
//! (`docs/plans/routing-enrichment.md`).
//!
//! Two rules hold everywhere in this module and are pinned in tests:
//!
//! - **Edges belong to the region.** A point exactly on a bbox edge is
//!   inside; a distance exactly equal to a radius is inside. Half-open
//!   is the right rule for BUCKETS, which must partition without
//!   double-counting (`docs/range-facets.md`); a filter partitions
//!   nothing, so the surprising rule would be the exclusive one.
//! - **Absence fails every filter.** A document with no point is not
//!   inside any region. That is exact, not degradation.

/// Mean Earth radius in meters (WGS84 R1 = (2a + b) / 3). Pinned here
/// so every haversine in the engine, on every node, computes the same
/// bits: distributed results are only bitwise equal to the monolith's
/// if the constant is a constant and not a per-call-site opinion.
pub const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Meters per degree of latitude on the pinned sphere: one degree of
/// arc, `R * pi / 180`. Exact for a sphere, which is the model this
/// module commits to (see [`EARTH_RADIUS_M`]).
pub const M_PER_DEG_LAT: f64 = EARTH_RADIUS_M * std::f64::consts::PI / 180.0;

/// Meters per degree of longitude AT THE EQUATOR. A parallel at
/// latitude `phi` is shorter by `cos(phi)`, which is why the Manhattan
/// distance below scales by the ORIGIN'S cosine rather than pretending
/// the factor is constant.
pub const M_PER_DEG_LON: f64 = M_PER_DEG_LAT;

/// Great-circle distance in meters between two (lat, lon) degree pairs
/// on the sphere of radius [`EARTH_RADIUS_M`].
///
/// The `asin` form (rather than `atan2`) with the argument clamped at 1
/// is the numerically well-behaved one for small separations, which is
/// the case that matters here: two courthouses in the same city differ
/// in the last few bits of `a`, and an unclamped `sqrt(a)` can round
/// just past 1 for antipodal inputs and hand back NaN.
pub fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().min(1.0).asin()
}

/// Local Manhattan distance in meters: meters along the meridian plus
/// meters along the parallel AT THE ORIGIN'S latitude.
///
/// `|dlat| * M_PER_DEG_LAT + |dlon| * M_PER_DEG_LON * cos(origin_lat)`,
/// pinned exactly so. This is a CITY-SCALE approximation and is
/// documented as one: it uses a single cosine (the origin's) instead of
/// integrating along the path, and it does NOT wrap around the
/// antimeridian — a longitude difference of 359 degrees measures as 359
/// degrees, not 1. Both are fine for "how far across town" and wrong
/// for "how far across the Pacific"; the caller picks the metric, and
/// haversine is the one that is right everywhere.
pub fn manhattan_meters(origin_lat: f64, origin_lon: f64, lat: f64, lon: f64) -> f64 {
    (lat - origin_lat).abs() * M_PER_DEG_LAT
        + (lon - origin_lon).abs() * M_PER_DEG_LON * origin_lat.to_radians().cos()
}

/// Which distance function a radius filter or a decay stage measures
/// with. Wire enum `GeoMetric`, resolved once at request parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoMetric {
    /// [`haversine_meters`].
    Haversine,
    /// [`manhattan_meters`].
    Manhattan,
}

impl GeoMetric {
    /// Distance in meters from `(origin_lat, origin_lon)` to
    /// `(lat, lon)` under this metric. The origin is the first pair
    /// because Manhattan is not symmetric in it (the cosine is the
    /// origin's).
    pub fn meters(self, origin_lat: f64, origin_lon: f64, lat: f64, lon: f64) -> f64 {
        match self {
            GeoMetric::Haversine => haversine_meters(origin_lat, origin_lon, lat, lon),
            GeoMetric::Manhattan => manhattan_meters(origin_lat, origin_lon, lat, lon),
        }
    }
}

/// The region a filtered document's point must lie in. Validated at
/// request parse (finite, in range, `min <= max` on both bbox axes,
/// `meters > 0`), so nothing here re-checks.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoRegion {
    /// An axis-aligned box, all four edges INCLUSIVE.
    Bbox {
        /// Southern edge.
        min_lat: f64,
        /// Northern edge.
        max_lat: f64,
        /// Western edge. Never above `max_lon`: an antimeridian-crossing
        /// box is refused at parse rather than reinterpreted.
        min_lon: f64,
        /// Eastern edge.
        max_lon: f64,
    },
    /// A disc, its boundary INCLUSIVE (`distance <= meters`).
    Radius {
        /// Origin latitude in degrees.
        lat: f64,
        /// Origin longitude in degrees.
        lon: f64,
        /// Radius in meters (> 0, finite).
        meters: f64,
        /// The distance the radius measures.
        metric: GeoMetric,
    },
}

impl GeoRegion {
    /// Whether `(lat, lon)` lies in this region. Edges are inside.
    pub fn contains(&self, lat: f64, lon: f64) -> bool {
        match *self {
            GeoRegion::Bbox {
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            } => lat >= min_lat && lat <= max_lat && lon >= min_lon && lon <= max_lon,
            GeoRegion::Radius {
                lat: olat,
                lon: olon,
                meters,
                metric,
            } => metric.meters(olat, olon, lat, lon) <= meters,
        }
    }
}

/// One geo filter resolved against THIS shard's geo table.
#[derive(Debug, Clone, Copy)]
pub struct GeoFilter {
    /// Index into the shard's geo table; `None` when the shard lacks
    /// the column, in which case EVERY document fails the filter — its
    /// documents genuinely hold no location, so "not inside" is the
    /// exact answer, not a degraded one. (The coordinator refuses a
    /// column NO shard knows: the typo rule.)
    pub column: Option<usize>,
    /// The region.
    pub region: GeoRegion,
}

/// A request's geo filters, resolved for one shard. ALL must pass — AND
/// semantics, as the wire documents — and an empty set passes
/// everything, so a filterless query takes a path bit-identical to the
/// unfiltered scorers.
#[derive(Debug, Clone, Default)]
pub struct GeoFilters {
    /// Filters in request-list order. Order is irrelevant to the result
    /// (conjunction is commutative and the predicates are pure) but is
    /// kept anyway so a failure is attributable to a request position.
    pub filters: Vec<GeoFilter>,
}

impl GeoFilters {
    /// Whether `doc_id` survives every filter. A document with no value
    /// in a filtered column fails: no location is inside no region.
    pub fn passes(&self, doc_id: u32, columns: &dyn crate::scorefn::NumericRead) -> bool {
        self.filters.iter().all(|f| match f.column {
            None => false,
            Some(gi) => match columns.geo_value(gi, doc_id) {
                None => false,
                Some((lat, lon)) => f.region.contains(lat, lon),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pinned constants and both metrics against hand-computed
    /// values, plus the boundary rule both regions promise.
    #[test]
    fn distances_and_regions_match_hand_computed_values() {
        // One degree of latitude anywhere is one degree of arc.
        let d = haversine_meters(0.0, 0.0, 1.0, 0.0);
        assert!(
            (d - M_PER_DEG_LAT).abs() < 1e-6,
            "one degree of latitude is R*pi/180: {d} vs {M_PER_DEG_LAT}"
        );
        // A degree of longitude at 60N is half a degree at the equator
        // (cos 60 = 1/2), to the precision the spherical model has.
        let equator = haversine_meters(0.0, 0.0, 0.0, 1.0);
        let at_60 = haversine_meters(60.0, 0.0, 60.0, 1.0);
        assert!((at_60 / equator - 0.5).abs() < 1e-4, "{at_60} vs {equator}");
        // Identical points are exactly zero, not a rounding crumb.
        assert_eq!(haversine_meters(38.9, -77.0, 38.9, -77.0), 0.0);
        assert_eq!(manhattan_meters(38.9, -77.0, 38.9, -77.0), 0.0);
        // Antipodal: half the circumference, and no NaN from a sqrt
        // rounding past 1.
        let anti = haversine_meters(0.0, 0.0, 0.0, 180.0);
        assert!(
            (anti - EARTH_RADIUS_M * std::f64::consts::PI).abs() < 1e-3,
            "{anti}"
        );
        // Manhattan is the pinned sum, exactly.
        let m = manhattan_meters(45.0, 10.0, 46.0, 11.0);
        assert_eq!(
            m.to_bits(),
            (M_PER_DEG_LAT + M_PER_DEG_LON * 45.0f64.to_radians().cos()).to_bits()
        );

        let bbox = GeoRegion::Bbox {
            min_lat: 10.0,
            max_lat: 20.0,
            min_lon: -5.0,
            max_lon: 5.0,
        };
        for (lat, lon) in [(10.0, -5.0), (20.0, 5.0), (10.0, 5.0), (20.0, -5.0)] {
            assert!(bbox.contains(lat, lon), "corner ({lat}, {lon}) is inside");
        }
        assert!(!bbox.contains(9.999_999, 0.0));
        assert!(!bbox.contains(15.0, 5.000_001));

        // distance == meters is inside; one ULP past it is not.
        let edge = haversine_meters(0.0, 0.0, 0.0, 1.0);
        let disc = GeoRegion::Radius {
            lat: 0.0,
            lon: 0.0,
            meters: edge,
            metric: GeoMetric::Haversine,
        };
        assert!(disc.contains(0.0, 1.0), "distance exactly == meters is in");
        let tighter = GeoRegion::Radius {
            lat: 0.0,
            lon: 0.0,
            meters: edge.next_down(),
            metric: GeoMetric::Haversine,
        };
        assert!(!tighter.contains(0.0, 1.0));
    }
}
