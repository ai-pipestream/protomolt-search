//! Leaf enumeration for the coordinator's typo refusal.

use super::lookup::ColumnLookup;
use crate::pb;

/// One column-read leaf of a value expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueLeaf {
    /// A plain column read.
    Column(String),
    /// A string, f64, i64 or u64 map entry read.
    Map {
        /// Map column name.
        column: String,
        /// Entry key.
        key: String,
    },
}

impl ValueLeaf {
    /// Human-readable form for refusal messages.
    pub fn describe(&self) -> String {
        match self {
            ValueLeaf::Column(name) => name.clone(),
            ValueLeaf::Map { column, key } => format!("{column}[{key:?}]"),
        }
    }
}

/// Collect the column-read leaves of one expression, depth-first, the
/// order `projection_leaves_known` flags follow.
pub fn column_leaves(expr: &pb::ValueExpr, out: &mut Vec<ValueLeaf>) {
    use pb::value_expr::Expr as V;
    match expr.expr.as_ref() {
        Some(V::Column(name)) => out.push(ValueLeaf::Column(name.clone())),
        Some(V::Map(read)) => out.push(ValueLeaf::Map {
            column: read.column.clone(),
            key: read.key.clone(),
        }),
        Some(V::Arith(arith)) => {
            if let Some(left) = arith.left.as_ref() {
                column_leaves(left, out);
            }
            if let Some(right) = arith.right.as_ref() {
                column_leaves(right, out);
            }
        }
        Some(V::ToDouble(inner)) | Some(V::Negate(inner)) | Some(V::Not(inner)) => {
            column_leaves(inner, out)
        }
        Some(V::Compare(cmp)) => {
            if let Some(left) = cmp.left.as_ref() {
                column_leaves(left, out);
            }
            if let Some(right) = cmp.right.as_ref() {
                column_leaves(right, out);
            }
        }
        Some(V::Logic(logic)) => {
            for child in &logic.children {
                column_leaves(child, out);
            }
        }
        Some(V::Func(func)) => {
            for a in &func.args {
                column_leaves(a, out);
            }
        }
        Some(V::Ternary(t)) => {
            for part in [
                t.cond.as_ref(),
                t.then_value.as_ref(),
                t.else_value.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                column_leaves(part, out);
            }
        }
        Some(V::UintLiteral(_))
        | Some(V::IntLiteral(_))
        | Some(V::FloatLiteral(_))
        | Some(V::BoolLiteral(_))
        | Some(V::StringLiteral(_))
        | Some(V::StableKeyHash(_))
        | None => {}
    }
}

/// Whether the expression reads the document's stable routing key
/// (`hash.fnv64(stable_key())`), the one input that is not a column.
pub fn reads_stable_key(expr: &pb::ValueExpr) -> bool {
    use pb::value_expr::Expr as V;
    match expr.expr.as_ref() {
        Some(V::StableKeyHash(_)) => true,
        Some(V::Arith(arith)) => {
            arith.left.as_deref().is_some_and(reads_stable_key)
                || arith.right.as_deref().is_some_and(reads_stable_key)
        }
        Some(V::ToDouble(inner)) | Some(V::Negate(inner)) | Some(V::Not(inner)) => {
            reads_stable_key(inner)
        }
        Some(V::Compare(cmp)) => {
            cmp.left.as_deref().is_some_and(reads_stable_key)
                || cmp.right.as_deref().is_some_and(reads_stable_key)
        }
        Some(V::Logic(logic)) => logic.children.iter().any(reads_stable_key),
        Some(V::Func(func)) => func.args.iter().any(reads_stable_key),
        Some(V::Ternary(t)) => [
            t.cond.as_deref(),
            t.then_value.as_deref(),
            t.else_value.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(reads_stable_key),
        Some(V::Column(_))
        | Some(V::Map(_))
        | Some(V::UintLiteral(_))
        | Some(V::IntLiteral(_))
        | Some(V::FloatLiteral(_))
        | Some(V::BoolLiteral(_))
        | Some(V::StringLiteral(_))
        | None => false,
    }
}

/// Whether THIS shard resolves one leaf: the column in some family,
/// and for a map read the key in the column's key dictionary — the
/// same rule filter leaves use.
pub fn leaf_known(leaf: &ValueLeaf, cols: &dyn ColumnLookup) -> bool {
    match leaf {
        ValueLeaf::Column(name) => {
            cols.numeric_index(name).is_some()
                || cols.integer_index(name).is_some()
                || cols.unsigned_integer_index(name).is_some()
                || cols.facet_index(name).is_some()
        }
        ValueLeaf::Map { column, key } => {
            let num = cols
                .map_numeric_index(column)
                .and_then(|ci| cols.map_numeric_key_ord(ci, key))
                .is_some();
            let facet = cols
                .map_facet_index(column)
                .and_then(|ci| cols.map_facet_key_ord(ci, key))
                .is_some();
            num || facet
                || cols
                    .map_integer_index(column)
                    .and_then(|ci| cols.map_integer_key_ord(ci, key))
                    .is_some()
                || cols
                    .map_unsigned_integer_index(column)
                    .and_then(|ci| cols.map_unsigned_integer_key_ord(ci, key))
                    .is_some()
        }
    }
}
