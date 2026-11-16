//! One document's request values as the filter dialect reads them, and
//! the three-valued evaluation of a filter over them.

use std::cmp::Ordering;

use crate::filter::{edge_of, int_range, Edge, Tri};
use crate::pb;

/// One document's request values, indexed the way the filter dialect
/// reads a stored row: facets, integers (timestamps among them, as
/// epoch microseconds), numerics, map entries, and geo points, each by
/// column name.
pub struct DocColumns<'a> {
    doc: &'a pb::AddDocumentsRequest,
    timestamps: Vec<(&'a str, i64)>,
}

impl<'a> DocColumns<'a> {
    pub fn of(doc: &'a pb::AddDocumentsRequest) -> Result<Self, String> {
        let mut timestamps = Vec::with_capacity(doc.timestamps.len());
        for tv in &doc.timestamps {
            let Some(ts) = tv.value.as_ref() else {
                return Err(format!("timestamp field {:?} carries no instant", tv.field));
            };
            let micros = crate::node::timestamp_to_epoch_micros(&tv.field, ts)
                .map_err(|status| status.message().to_string())?;
            timestamps.push((tv.field.as_str(), micros));
        }
        Ok(DocColumns { doc, timestamps })
    }

    fn facet(&self, column: &str) -> Option<&str> {
        self.doc
            .facets
            .iter()
            .find(|f| f.field == column)
            .map(|f| f.value.as_str())
    }

    fn integer(&self, column: &str) -> Option<i64> {
        self.doc
            .integers
            .iter()
            .find(|v| v.field == column)
            .map(|v| v.value)
            .or_else(|| {
                self.timestamps
                    .iter()
                    .find(|(name, _)| *name == column)
                    .map(|(_, micros)| *micros)
            })
    }

    fn numeric(&self, column: &str) -> Option<f64> {
        self.doc
            .numerics
            .iter()
            .find(|v| v.field == column)
            .map(|v| v.value)
    }

    fn unsigned_integer(&self, column: &str) -> Option<u64> {
        self.doc
            .unsigned_integers
            .iter()
            .find(|v| v.field == column)
            .map(|v| v.value)
    }

    fn map_facet(&self, column: &str, key: &str) -> Option<&str> {
        self.doc
            .map_facets
            .iter()
            .find(|e| e.field == column && e.key == key)
            .map(|e| e.value.as_str())
    }

    fn map_numeric(&self, column: &str, key: &str) -> Option<f64> {
        self.doc
            .map_numerics
            .iter()
            .find(|e| e.field == column && e.key == key)
            .map(|e| e.value)
    }

    fn map_number(&self, column: &str, key: &str) -> Option<NumberValue> {
        self.doc
            .map_integers
            .iter()
            .find(|e| e.field == column && e.key == key)
            .map(|e| NumberValue::Int(e.value))
            .or_else(|| {
                self.doc
                    .map_unsigned_integers
                    .iter()
                    .find(|e| e.field == column && e.key == key)
                    .map(|e| NumberValue::Uint(e.value))
            })
            .or_else(|| self.map_numeric(column, key).map(NumberValue::Float))
    }

    fn geo(&self, column: &str) -> Option<(f64, f64)> {
        self.doc
            .geo_points
            .iter()
            .find(|g| g.field == column)
            .map(|g| (g.lat, g.lon))
    }

    fn has(&self, column: &str) -> bool {
        self.facet(column).is_some()
            || self.integer(column).is_some()
            || self.unsigned_integer(column).is_some()
            || self.numeric(column).is_some()
            || self.geo(column).is_some()
    }
}

fn in_number_range(value: NumberValue, min: Option<Edge>, max: Option<Edge>) -> bool {
    match value {
        NumberValue::Int(v) => {
            let (lo, hi) = int_range(&min, &max);
            v >= lo && v <= hi
        }
        NumberValue::Uint(v) => {
            let (lo, hi) = crate::filter::uint_range(&min, &max);
            v >= lo && v <= hi
        }
        NumberValue::Float(v) => {
            min.as_ref().is_none_or(|e| e.admits_from_below(v))
                && max.as_ref().is_none_or(|e| e.admits_from_above(v))
        }
    }
}

#[derive(Clone, Copy)]
enum NumberValue {
    Int(i64),
    Uint(u64),
    Float(f64),
}

pub(super) fn string_in_range(
    value: &str,
    min: Option<&pb::StringBound>,
    max: Option<&pb::StringBound>,
) -> bool {
    let above = min.is_none_or(|b| match value.as_bytes().cmp(b.value.as_bytes()) {
        Ordering::Greater => true,
        Ordering::Equal => !b.exclusive,
        Ordering::Less => false,
    });
    let below = max.is_none_or(|b| match value.as_bytes().cmp(b.value.as_bytes()) {
        Ordering::Less => true,
        Ordering::Equal => !b.exclusive,
        Ordering::Greater => false,
    });
    above && below
}

/// A validated filter tree evaluated over one document's request
/// values with the shard's three-valued rules:
/// comparisons are UNKNOWN on absence, the presence tests are total,
/// `!` keeps UNKNOWN, and a document is in the set only on TRUE.
pub fn eval_document(expr: &pb::FilterExpr, doc: &DocColumns<'_>) -> Tri {
    use pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) => list
            .exprs
            .iter()
            .fold(Tri::True, |acc, child| acc.and(eval_document(child, doc))),
        Some(Expr::Or(list)) => list
            .exprs
            .iter()
            .fold(Tri::False, |acc, child| acc.or(eval_document(child, doc))),
        Some(Expr::Not(child)) => !eval_document(child, doc),
        Some(Expr::Facet(p)) => match doc.facet(&p.column) {
            None => Tri::Unknown,
            Some(value) => Tri::from(p.values.iter().any(|v| v == value)),
        },

        Some(Expr::Number(p)) => number_tri(p, doc),

        Some(Expr::MapFacet(p)) => match doc.map_facet(&p.column, &p.key) {
            None => Tri::Unknown,
            Some(value) => Tri::from(p.values.iter().any(|v| v == value)),
        },
        Some(Expr::MapNumber(p)) => match doc.map_number(&p.column, &p.key) {
            None => Tri::Unknown,
            Some(v) => Tri::from(in_number_range(
                v,
                p.min.as_ref().and_then(edge_of),
                p.max.as_ref().and_then(edge_of),
            )),
        },
        Some(Expr::MapHasKey(p)) => Tri::from(
            doc.map_facet(&p.column, &p.key).is_some()
                || doc.map_number(&p.column, &p.key).is_some(),
        ),
        Some(Expr::Has(p)) => Tri::from(doc.has(&p.column)),
        Some(Expr::Geo(g)) => match doc.geo(&g.column) {
            None => Tri::Unknown,
            Some((lat, lon)) => match crate::node::validate_geo_filter(g) {
                Ok(region) => Tri::from(region.contains(lat, lon)),
                Err(_) => Tri::Unknown,
            },
        },

        Some(Expr::StringRange(p)) => match string_operand(doc, &p.column, p.map_key.as_deref()) {
            None => Tri::Unknown,
            Some(v) => Tri::from(string_in_range(v, p.min.as_ref(), p.max.as_ref())),
        },
        Some(Expr::StringPrefix(p)) => match string_operand(doc, &p.column, p.map_key.as_deref()) {
            None => Tri::Unknown,
            Some(v) => Tri::from(v.as_bytes().starts_with(p.prefix.as_bytes())),
        },

        None => Tri::Unknown,
    }
}

/// A number predicate over whichever numeric column kind holds the value.
fn number_tri(p: &pb::NumberPredicate, doc: &DocColumns<'_>) -> Tri {
    let min = p.min.as_ref().and_then(edge_of);
    let max = p.max.as_ref().and_then(edge_of);
    if let Some(v) = doc.integer(&p.column) {
        Tri::from(in_number_range(NumberValue::Int(v), min, max))
    } else if let Some(v) = doc.unsigned_integer(&p.column) {
        Tri::from(in_number_range(NumberValue::Uint(v), min, max))
    } else if let Some(v) = doc.numeric(&p.column) {
        Tri::from(in_number_range(NumberValue::Float(v), min, max))
    } else {
        Tri::Unknown
    }
}

/// The string a range or prefix predicate reads: the facet, unless the
/// predicate is the map variant or names a key.
fn string_operand<'d>(doc: &'d DocColumns<'_>, column: &str, key: Option<&str>) -> Option<&'d str> {
    match key {
        None => doc.facet(column),
        Some(key) => doc.map_facet(column, key),
    }
}
