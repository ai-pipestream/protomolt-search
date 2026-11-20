//! String range and prefix predicates over facet dictionaries.

use super::*;
use crate::filter::ResolvedLeaf;

/// The inclusive-or-exclusive byte bounds and prefix of one string predicate.
struct StringBounds<'a> {
    min: Option<(&'a str, bool)>,
    max: Option<(&'a str, bool)>,
    prefix: Option<&'a str>,
}

impl StringBounds<'_> {
    fn admits(&self, value: &str) -> bool {
        let v = value.as_bytes();
        let above = self.min.is_none_or(|(b, exclusive)| {
            if exclusive {
                v > b.as_bytes()
            } else {
                v >= b.as_bytes()
            }
        });
        let below = self.max.is_none_or(|(b, exclusive)| {
            if exclusive {
                v < b.as_bytes()
            } else {
                v <= b.as_bytes()
            }
        });
        above && below && self.prefix.is_none_or(|p| v.starts_with(p.as_bytes()))
    }

    /// The contiguous ordinal range of a byte-sorted dictionary.
    fn ord_range(&self, dict: &[String]) -> (u32, u32) {
        let (min, max, prefix) = (self.min, self.max, self.prefix);
        let lo = dict.partition_point(|v| {
            let v = v.as_bytes();
            let under_min = min.is_some_and(|(b, exclusive)| {
                if exclusive {
                    v <= b.as_bytes()
                } else {
                    v < b.as_bytes()
                }
            });
            under_min || prefix.is_some_and(|p| v < p.as_bytes())
        });
        let hi = dict.partition_point(|v| {
            let v = v.as_bytes();
            let in_prefix = prefix.is_none_or(|p| v < p.as_bytes() || v.starts_with(p.as_bytes()));
            let under_max = max.is_none_or(|(b, exclusive)| {
                if exclusive {
                    v < b.as_bytes()
                } else {
                    v <= b.as_bytes()
                }
            });
            in_prefix && under_max
        });
        (lo as u32, hi.max(lo) as u32)
    }

    /// The admitted ordinals of a dictionary in first-seen order.
    fn admitted_ords(&self, dict: &[String]) -> Vec<u32> {
        dict.iter()
            .enumerate()
            .filter(|(_, v)| self.admits(v))
            .map(|(ord, _)| ord as u32)
            .collect()
    }
}

fn unordered(what: String) -> Status {
    Status::failed_precondition(format!(
        "{what} was written with a first-seen (unordered) dictionary; string ordering \
         and prefixes need a dictionary in byte order, which this version writes at \
         flush — rebuild or reshard the generation"
    ))
}

impl Bm25Shard {
    /// Resolve a string range or prefix over a facet or map-facet column to a
    /// contiguous ordinal range when byte-sorted (refusing unordered resident
    /// files) or to an explicit ordinal set on unsorted heap builders.
    pub(in crate::node) fn resolve_string_predicate(
        &self,
        column: &str,
        key: Option<&str>,
        min: Option<(&str, bool)>,
        max: Option<(&str, bool)>,
        prefix: Option<&str>,
    ) -> Result<ResolvedLeaf, Status> {
        let bounds = StringBounds { min, max, prefix };
        match key {
            None => self.resolve_facet_strings(column, &bounds),
            Some(key) => self.resolve_map_facet_strings(column, key, &bounds),
        }
    }

    fn resolve_facet_strings(
        &self,
        column: &str,
        bounds: &StringBounds<'_>,
    ) -> Result<ResolvedLeaf, Status> {
        let Some(fi) = self.facet_index(column) else {
            return Ok(ResolvedLeaf::FacetOrdRange {
                column: None,
                lo: 0,
                hi: 0,
            });
        };
        let (dict, sorted) = match self {
            Bm25Shard::Building(s) => (s.facet_dictionary(fi), s.facet_dictionary_sorted(fi)),
            Bm25Shard::Segmented(g) => (g.facet_dictionary(fi), g.facet_dictionary_sorted(fi)),
            Bm25Shard::Resident(r) => {
                if !r.facet_dictionary_sorted(fi) {
                    return Err(unordered(format!("facet column {column:?}")));
                }
                (r.facet_dictionary(fi), true)
            }
            Bm25Shard::Spilling(_) => {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                ))
            }
        };
        Ok(if sorted {
            let (lo, hi) = bounds.ord_range(dict);
            ResolvedLeaf::FacetOrdRange {
                column: Some(fi),
                lo,
                hi,
            }
        } else {
            ResolvedLeaf::Facet {
                column: Some(fi),
                ords: bounds.admitted_ords(dict),
            }
        })
    }

    fn resolve_map_facet_strings(
        &self,
        column: &str,
        key: &str,
        bounds: &StringBounds<'_>,
    ) -> Result<ResolvedLeaf, Status> {
        let Some((ci, key_ord)) = self
            .map_facet_index(column)
            .and_then(|ci| self.map_facet_key_ord(ci, key).map(|k| (ci, k)))
        else {
            return Ok(ResolvedLeaf::MapFacetOrdRange {
                target: None,
                lo: 0,
                hi: 0,
            });
        };
        let (dict, sorted) = match self {
            Bm25Shard::Building(s) => (s.map_facet_values(ci), s.map_facet_values_sorted(ci)),
            Bm25Shard::Segmented(g) => (g.map_facet_values(ci), g.map_facet_values_sorted(ci)),
            Bm25Shard::Resident(r) => {
                if !r.map_facet_values_sorted(ci) {
                    return Err(unordered(format!("map-facet column {column:?} (values)")));
                }
                (r.map_facet_values(ci), true)
            }
            Bm25Shard::Spilling(_) => {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                ))
            }
        };
        Ok(if sorted {
            let (lo, hi) = bounds.ord_range(dict);
            ResolvedLeaf::MapFacetOrdRange {
                target: Some((ci, key_ord)),
                lo,
                hi,
            }
        } else {
            ResolvedLeaf::MapFacet {
                target: Some((ci, key_ord)),
                ords: bounds.admitted_ords(dict),
            }
        })
    }
}
