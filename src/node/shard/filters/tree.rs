//! Filter tree resolution against a shard's column tables.

use super::*;
use crate::filter::{MapKeyRef, ResolvedFilter, ResolvedLeaf};
use crate::pb::filter_expr::Expr;

fn sorted_ords(mut ords: Vec<u32>) -> Vec<u32> {
    ords.sort_unstable();
    ords.dedup();
    ords
}

impl Bm25Shard {
    /// Resolve a validated filter tree against this shard's column tables and
    /// dictionaries; columns or map keys absent on this shard resolve to empty/absent.
    pub(in crate::node) fn resolve_filter(
        &self,
        expr: &crate::pb::FilterExpr,
    ) -> Result<ResolvedFilter, Status> {
        Ok(
            match expr.expr.as_ref().expect("filter validated before resolve") {
                Expr::And(list) => ResolvedFilter::And(
                    list.exprs
                        .iter()
                        .map(|e| self.resolve_filter(e))
                        .collect::<Result<_, _>>()?,
                ),
                Expr::Or(list) => ResolvedFilter::Or(
                    list.exprs
                        .iter()
                        .map(|e| self.resolve_filter(e))
                        .collect::<Result<_, _>>()?,
                ),
                Expr::Not(child) => ResolvedFilter::Not(Box::new(self.resolve_filter(child)?)),
                Expr::StringRange(p) => ResolvedFilter::Leaf(self.resolve_string_predicate(
                    &p.column,
                    p.map_key.as_deref(),
                    p.min.as_ref().map(|b| (b.value.as_str(), b.exclusive)),
                    p.max.as_ref().map(|b| (b.value.as_str(), b.exclusive)),
                    None,
                )?),
                Expr::StringPrefix(p) => ResolvedFilter::Leaf(self.resolve_string_predicate(
                    &p.column,
                    p.map_key.as_deref(),
                    None,
                    None,
                    Some(p.prefix.as_str()),
                )?),
                Expr::Facet(p) => ResolvedFilter::Leaf(self.resolve_facet_leaf(p)),
                Expr::Number(p) => ResolvedFilter::Leaf(self.resolve_number_leaf(p)),
                Expr::MapFacet(p) => ResolvedFilter::Leaf(self.resolve_map_facet_leaf(p)),
                Expr::MapNumber(p) => ResolvedFilter::Leaf(self.resolve_map_number_leaf(p)),
                Expr::MapHasKey(p) => {
                    ResolvedFilter::Leaf(ResolvedLeaf::MapHasKey(self.map_key_ref(p)))
                }
                Expr::Has(p) => ResolvedFilter::Leaf(ResolvedLeaf::Has {
                    facet: self.facet_index(&p.column),
                    numeric: self.numeric_index(&p.column),
                    integer: self.integer_index(&p.column),
                    unsigned_integer: self.unsigned_integer_index(&p.column),
                    geo: self.geo_index(&p.column),
                }),
                Expr::Geo(g) => ResolvedFilter::Leaf(ResolvedLeaf::Geo {
                    column: self.geo_index(&g.column),
                    region: validate_geo_filter(g).expect("filter validated before resolve"),
                }),
            },
        )
    }

    fn resolve_facet_leaf(&self, p: &crate::pb::FacetPredicate) -> ResolvedLeaf {
        let column = self.facet_index(&p.column);
        let ords = match column {
            None => Vec::new(),
            Some(fi) => sorted_ords(
                p.values
                    .iter()
                    .filter_map(|v| self.facet_value_ord_of(fi, v))
                    .collect(),
            ),
        };
        ResolvedLeaf::Facet { column, ords }
    }

    fn resolve_number_leaf(&self, p: &crate::pb::NumberPredicate) -> ResolvedLeaf {
        let lo = p.min.as_ref().and_then(crate::filter::edge_of);
        let hi = p.max.as_ref().and_then(crate::filter::edge_of);
        // i64 first, then f64 — the RangeFacetField resolution
        // order, so the two number surfaces can never disagree
        // about which table a name means.
        if let Some(ii) = self.integer_index(&p.column) {
            let (lo, hi) = crate::filter::int_range(&lo, &hi);
            ResolvedLeaf::IntRange { column: ii, lo, hi }
        } else if let Some(ii) = self.unsigned_integer_index(&p.column) {
            let (lo, hi) = crate::filter::uint_range(&lo, &hi);
            ResolvedLeaf::UintRange { column: ii, lo, hi }
        } else if let Some(ni) = self.numeric_index(&p.column) {
            ResolvedLeaf::F64Range { column: ni, lo, hi }
        } else {
            ResolvedLeaf::NumberUnknown
        }
    }

    fn resolve_map_facet_leaf(&self, p: &crate::pb::MapFacetPredicate) -> ResolvedLeaf {
        let target = self
            .map_facet_index(&p.column)
            .and_then(|ci| self.map_facet_key_ord(ci, &p.key).map(|k| (ci, k)));
        let ords = match target {
            None => Vec::new(),
            Some((ci, _)) => sorted_ords(
                p.values
                    .iter()
                    .filter_map(|v| self.map_facet_value_ord_of(ci, v))
                    .collect(),
            ),
        };
        ResolvedLeaf::MapFacet { target, ords }
    }

    fn resolve_map_number_leaf(&self, p: &crate::pb::MapNumberPredicate) -> ResolvedLeaf {
        let lo = p.min.as_ref().and_then(crate::filter::edge_of);
        let hi = p.max.as_ref().and_then(crate::filter::edge_of);
        if let Some(ci) = self.map_integer_index(&p.column) {
            let target = self.map_integer_key_ord(ci, &p.key).map(|key| (ci, key));
            let (lo, hi) = crate::filter::int_range(&lo, &hi);
            ResolvedLeaf::MapIntRange { target, lo, hi }
        } else if let Some(ci) = self.map_unsigned_integer_index(&p.column) {
            let target = self
                .map_unsigned_integer_key_ord(ci, &p.key)
                .map(|key| (ci, key));
            let (lo, hi) = crate::filter::uint_range(&lo, &hi);
            ResolvedLeaf::MapUintRange { target, lo, hi }
        } else {
            let target = self
                .map_numeric_index(&p.column)
                .and_then(|ci| self.map_numeric_key_ord(ci, &p.key).map(|key| (ci, key)));
            ResolvedLeaf::MapNumber { target, lo, hi }
        }
    }

    /// Map-facet, map-numeric, map-integer, then map-unsigned: the one
    /// order, shared with `filter_columns_known`.
    fn map_key_ref(&self, p: &crate::pb::MapKeyPredicate) -> MapKeyRef {
        if let Some(ci) = self.map_facet_index(&p.column) {
            MapKeyRef::Facet {
                column: ci,
                key_ord: self.map_facet_key_ord(ci, &p.key),
            }
        } else if let Some(ci) = self.map_numeric_index(&p.column) {
            MapKeyRef::Numeric {
                column: ci,
                key_ord: self.map_numeric_key_ord(ci, &p.key),
            }
        } else if let Some(ci) = self.map_integer_index(&p.column) {
            MapKeyRef::Integer {
                column: ci,
                key_ord: self.map_integer_key_ord(ci, &p.key),
            }
        } else if let Some(ci) = self.map_unsigned_integer_index(&p.column) {
            MapKeyRef::UnsignedInteger {
                column: ci,
                key_ord: self.map_unsigned_integer_key_ord(ci, &p.key),
            }
        } else {
            MapKeyRef::Unknown
        }
    }
}
