//! Membership and the function vocabulary of the predicate compiler.

use super::CelError;

use super::ast::{Ast, RelOp};
use super::predicate::{compile_number_relation, int_literal, side_of, wrap, Side};
use super::refuse;
use crate::pb;

/// `in` has two compiled shapes: `"key" in map_column` (key presence)
/// and `column in [v, ...]` (membership).
pub(super) fn compile_in(lhs: Side, rhs: Side) -> Result<pb::FilterExpr, CelError> {
    match (lhs, rhs) {
        (Side::Str(key), Side::Column(column)) => Ok(wrap(pb::filter_expr::Expr::MapHasKey(
            pb::MapKeyPredicate { column, key },
        ))),
        (Side::Column(_) | Side::MapAccess(..), Side::Column(name)) => Err(refuse(format!(
            "`in {name}` needs a string literal key on the left (`\"k\" in {name}`) or \
             a list on the right"
        ))),

        (col @ (Side::Column(_) | Side::MapAccess(..)), Side::List(items)) => {
            compile_membership(col, items)
        }

        (Side::Str(_), Side::List(_)) | (_, Side::List(_)) => Err(refuse(
            "`in` needs a column on the left of a list (`column in [..]`)",
        )),
        _ => Err(refuse(
            "`in` compiles as `column in [values]` or `\"key\" in map_column`",
        )),
    }
}

/// `column in [v, ...]`: a facet value set for strings, a disjunction of
/// point ranges for numbers.
fn compile_membership(col: Side, items: Vec<Ast>) -> Result<pb::FilterExpr, CelError> {
    if items.is_empty() {
        return Err(refuse(
            "membership in an empty list is a predicate nobody meant to send",
        ));
    }
    let mut strings = Vec::new();
    let mut numbers = Vec::new();
    for item in &items {
        match side_of(item)? {
            Side::Str(v) => strings.push(v),
            Side::Int(v) => numbers.push(pb::filter_bound::Value::Int(v)),
            Side::Uint(v) => numbers.push(pb::filter_bound::Value::Uint(v)),
            Side::Float(v) if v.is_finite() => numbers.push(pb::filter_bound::Value::Num(v)),
            Side::Float(v) => {
                return Err(refuse(format!(
                    "float literal {v} is not finite; NaN and infinity bound \
                     nothing a column can hold"
                )));
            }
            _ => {
                return Err(refuse(
                    "a membership list holds literals only (strings or numbers)",
                ));
            }
        }
    }
    match (strings.is_empty(), numbers.is_empty()) {
        (false, true) => Ok(match &col {
            Side::Column(name) => wrap(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
                column: name.clone(),
                values: strings,
            })),
            Side::MapAccess(name, key) => {
                wrap(pb::filter_expr::Expr::MapFacet(pb::MapFacetPredicate {
                    column: name.clone(),
                    key: key.clone(),
                    values: strings,
                }))
            }
            _ => unreachable!("matched above"),
        }),
        (true, false) => {
            // Numeric membership is a disjunction of point
            // ranges: exact per element, and the leaf cap
            // bounds the fan-out.
            let eqs = numbers
                .into_iter()
                .map(|v| compile_number_relation(&col, RelOp::Eq, v))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(if eqs.len() == 1 {
                eqs.into_iter().next().expect("len checked")
            } else {
                wrap(pb::filter_expr::Expr::Or(pb::FilterList { exprs: eqs }))
            })
        }
        (false, false) => Err(refuse(
            "a membership list mixes strings and numbers; one column holds one \
             kind",
        )),
        (true, true) => unreachable!("non-empty list produced no literals"),
    }
}

/// The compiled function vocabulary, everything else refused by name
/// with the reason.
pub(super) fn compile_call(name: &str, args: &[Ast]) -> Result<pb::FilterExpr, CelError> {
    match name {
        "has" => {
            if args.len() != 1 {
                return Err(refuse("has() takes exactly one column"));
            }
            match &args[0] {
                Ast::Ident(column) => Ok(wrap(pb::filter_expr::Expr::Has(pb::HasPredicate {
                    column: column.clone(),
                }))),
                Ast::MapAccess(col, key) => Err(refuse(format!(
                    "has() on a map entry; key presence is '{key}' in {col}"
                ))),
                _ => Err(refuse("has() takes a column name")),
            }
        }
        "timestamp" => Err(refuse(
            "timestamp() is a value, not a predicate; compare a column against it \
             (`decided >= timestamp(\"2015-01-01T00:00:00Z\")`)",
        )),
        "within_bbox" => {
            let (column, rest) = geo_args(name, args, 4)?;
            Ok(wrap(pb::filter_expr::Expr::Geo(pb::GeoFilter {
                column,
                region: Some(pb::geo_filter::Region::Bbox(pb::GeoBbox {
                    min_lat: rest[0],
                    max_lat: rest[1],
                    min_lon: rest[2],
                    max_lon: rest[3],
                })),
            })))
        }

        "within_radius" | "within_radius_manhattan" => compile_radius(name, args),

        "matches" => Err(refuse(
            "matches() (regex) is not supported: patterns do not compile to dictionary \
             predicates, and a regex engine is a dependency this engine deliberately \
             does not link",
        )),

        "startsWith" => compile_starts_with(args),

        "endsWith" | "contains" => Err(refuse(format!(
            "{name}() does not compile: a byte-sorted dictionary resolves prefixes \
             (startsWith) and ranges, not suffixes or substrings"
        ))),
        "all" | "exists" | "exists_one" | "filter" | "map" => Err(refuse(format!(
            "the {name}() macro (a comprehension) does not compile to an index predicate"
        ))),
        "size" => Err(refuse("size() does not compile to an index predicate")),
        "int" | "uint" | "double" | "string" | "bool" | "bytes" | "dyn" | "type" => {
            Err(refuse(format!(
                "the {name}() conversion does not compile; compare the column in its \
                 own type (the literal's type picks the table)"
            )))
        }
        "duration" => Err(refuse(
            "duration() does not compile; timestamps are compared directly \
             (`decided >= timestamp(...)`)",
        )),
        other => Err(refuse(format!(
            "function {other}() is not in the compiled vocabulary: comparisons, `in`, \
             has(), timestamp(), within_bbox(), within_radius(), \
             within_radius_manhattan()"
        ))),
    }
}

fn compile_radius(name: &str, args: &[Ast]) -> Result<pb::FilterExpr, CelError> {
    let (column, rest) = geo_args(name, args, 3)?;
    let metric = if name == "within_radius" {
        pb::GeoMetric::Haversine
    } else {
        pb::GeoMetric::Manhattan
    };
    Ok(wrap(pb::filter_expr::Expr::Geo(pb::GeoFilter {
        column,
        region: Some(pb::geo_filter::Region::Radius(pb::GeoRadius {
            lat: rest[0],
            lon: rest[1],
            meters: rest[2],
            metric: metric.into(),
        })),
    })))
}

/// Method-call syntax puts the receiver first: `court.startsWith("ca")` is
/// `Call("startsWith", [court, "ca"])`. A prefix is a dictionary range on a
/// byte-sorted dictionary.
fn compile_starts_with(args: &[Ast]) -> Result<pb::FilterExpr, CelError> {
    if args.len() != 2 {
        return Err(refuse(
            "startsWith() takes a column receiver and one string",
        ));
    }
    let (column, map_key) = match side_of(&args[0])? {
        Side::Column(name) => (name, None),
        Side::MapAccess(name, key) => (name, Some(key)),
        _ => {
            return Err(refuse(
                "startsWith() reads a facet column or a map entry; the receiver \
                 must be a column",
            ))
        }
    };
    let prefix = match &args[1] {
        Ast::Str(v) => v.clone(),
        _ => return Err(refuse("startsWith() takes a string literal prefix")),
    };
    if prefix.is_empty() {
        return Err(refuse(
            "startsWith(\"\") matches every value; presence is has(column)",
        ));
    }
    Ok(wrap(pb::filter_expr::Expr::StringPrefix(
        pb::StringPrefixPredicate {
            column,
            map_key,
            prefix,
        },
    )))
}

/// Geo function arguments: a column identifier plus `n` numeric
/// literals.
fn geo_args(name: &str, args: &[Ast], n: usize) -> Result<(String, Vec<f64>), CelError> {
    if args.len() != n + 1 {
        return Err(refuse(format!(
            "{name}() takes a geo column and {n} numbers, got {} arguments",
            args.len()
        )));
    }
    let column = match &args[0] {
        Ast::Ident(c) => c.clone(),
        _ => {
            return Err(refuse(format!(
                "{name}(): the first argument is the geo column name"
            )));
        }
    };
    let mut rest = Vec::with_capacity(n);
    for a in &args[1..] {
        rest.push(match a {
            Ast::Int(v) => int_literal(*v)? as f64,
            Ast::Float(v) => *v,
            _ => {
                return Err(refuse(format!(
                    "{name}(): coordinates and distances are numeric literals"
                )));
            }
        });
    }
    Ok((column, rest))
}
