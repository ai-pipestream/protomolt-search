//! Compiled filter predicates: validation of
//! the wire [`crate::pb::FilterExpr`] tree, the per-shard resolved
//! form, and its three-valued evaluation.
//!
//! The division of labor:
//! CEL is the SURFACE syntax (`src/cel.rs` compiles it at the
//! coordinator), this module is the predicate ENGINE, and nothing here
//! ever sees CEL text. The coordinator compiles once; every shard then
//! resolves column names and string values against its OWN dictionaries
//! — ordinals are shard-local and never travel — and evaluates the
//! resolved tree per candidate document, immediately before the floor
//! test and heap insertion (`crate::bm25::FilterCtx`).
//!
//! Three rules hold everywhere in this module and are pinned in tests:
//!
//! - **Absence is UNKNOWN, and only TRUE survives.** A comparison on a
//!   document that lacks the value is [`Tri::Unknown`]; and/or/not are
//!   Kleene; a document whose whole tree is not [`Tri::True`] is
//!   filtered out. This is the SQL rule, chosen over stock CEL's
//!   missing-field ERROR (which would fail the whole scan on the first
//!   absent value) and over proto-default zeros (which would make an
//!   unset string equal `""` — a silent lie). The presence tests —
//!   `has(col)`, `"k" in m` — are TOTAL, true or false, never unknown:
//!   they are the one thing absence can pass, and the escape hatch a
//!   query uses to see it.
//! - **Numbers compare exactly, across domains.** An i64 or u64 bound
//!   against an f64 column is compared as the integer it says
//!   ([`cmp_f64_i64`], [`cmp_f64_u64`]), never rounded through f64; an f64
//!   bound against an integer column is normalized to exact bounds with
//!   the exclusivity folded in. No comparison in this module is
//!   performed in a domain that could reorder it.
//! - **A filter only REMOVES documents.** Every block-max bound stays
//!   a valid upper bound over the survivors, so the pruning stack
//!   needs no new math; what changes is only WHERE the test sits (the
//!   heap gate), which `crate::bm25` owns.

#![deny(clippy::result_large_err)]

use std::cmp::Ordering;
use std::ops::Not;

/// Deepest tree the wire accepts. Validation, resolution, and eval all
/// recurse; the cap keeps them honest about stack depth and sits well
/// under prost's own decode recursion limit, so a tree that decodes is
/// a tree the engine can walk.
pub const MAX_DEPTH: usize = 32;

/// Most leaves the wire accepts. The per-leaf `filter_columns_known`
/// handshake is positional, and a filter is a predicate, not a
/// program; past a few hundred leaves the request is neither.
pub const MAX_LEAVES: usize = 256;

/// Kleene three-valued logic over `False < Unknown < True`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tri {
    /// The document fails the predicate.
    False,
    /// The document lacks the value the predicate reads: neither pass
    /// nor fail. At the top level this filters the document out — only
    /// [`Tri::True`] survives — but under `!` it stays Unknown rather
    /// than flipping, which is what keeps `!(court == "x")` from
    /// silently admitting every court-less document.
    Unknown,
    /// The document passes the predicate.
    True,
}

impl From<bool> for Tri {
    fn from(b: bool) -> Self {
        if b {
            Tri::True
        } else {
            Tri::False
        }
    }
}

impl Tri {
    /// Kleene conjunction: the minimum under `False < Unknown < True`.
    pub fn and(self, other: Tri) -> Tri {
        self.min(other)
    }

    /// Kleene disjunction: the maximum.
    pub fn or(self, other: Tri) -> Tri {
        self.max(other)
    }
}

impl Not for Tri {
    type Output = Tri;

    /// Kleene negation: True and False swap, Unknown stays.
    fn not(self) -> Tri {
        match self {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        }
    }
}

/// One bound value of a numeric range, kept in the domain the request
/// wrote it in — conversion happens per comparison, exactly, never by
/// rounding the bound into the column's domain.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumBound {
    /// An integer bound.
    I(i64),
    /// A full-domain unsigned integer bound.
    U(u64),
    /// A float bound (finite; validation refuses the rest). `-0.0` is
    /// normalized to `+0.0` at resolve so [`f64::total_cmp`] agrees
    /// with IEEE equality on the one pair where they differ.
    F(f64),
}

/// A resolved range edge: the bound plus whether the bound itself is
/// outside the range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Edge {
    /// The bound value.
    pub value: NumBound,
    /// `>` / `<` rather than `>=` / `<=`.
    pub exclusive: bool,
}

/// Exact ordering of an f64 column value against an i64 bound. The f64
/// is never converted to i64 nor the i64 to f64 — above 2^53 either
/// conversion can round across the bound — so the comparison happens
/// piecewise: infinities first, then the 2^63 range edges (2^63 is
/// exactly representable), then integer part with the fraction as the
/// tiebreak.
pub fn cmp_f64_i64(x: f64, n: i64) -> Ordering {
    debug_assert!(!x.is_nan(), "stored column values are never NaN");
    if x.is_infinite() {
        return if x > 0.0 {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    // 2^63 as f64, exact. Every i64 is below it; -2^63 is i64::MIN.
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    if x >= TWO_63 {
        return Ordering::Greater;
    }
    if x < -TWO_63 {
        return Ordering::Less;
    }
    // |x| <= 2^63 with x != 2^63, so floor(x) is in [-2^63, 2^63) and
    // the cast is exact.
    let fx = x.floor();
    let xi = fx as i64;
    match xi.cmp(&n) {
        // Equal integer parts: a positive fraction pushes x above n.
        Ordering::Equal => {
            if x > fx {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        o => o,
    }
}

/// Exact ordering of a non-NaN double against an unsigned integer.
/// The 2^64 boundary is exact; within it the integer part fits in u64.
pub fn cmp_f64_u64(x: f64, n: u64) -> Ordering {
    debug_assert!(!x.is_nan(), "stored column values are never NaN");
    if x < 0.0 {
        return Ordering::Less;
    }
    if x >= 18_446_744_073_709_551_616.0 {
        return Ordering::Greater;
    }
    let floor = x.floor();
    match (floor as u64).cmp(&n) {
        Ordering::Equal if x > floor => Ordering::Greater,
        order => order,
    }
}

impl Edge {
    /// Whether column value `v` (non-NaN) sits at or above this edge
    /// as a LOWER bound.
    pub(crate) fn admits_from_below(&self, v: f64) -> bool {
        let v = if v == 0.0 { 0.0 } else { v };
        let ord = match self.value {
            NumBound::F(b) => v.total_cmp(&b),
            NumBound::I(n) => cmp_f64_i64(v, n),
            NumBound::U(n) => cmp_f64_u64(v, n),
        };
        if self.exclusive {
            ord == Ordering::Greater
        } else {
            ord != Ordering::Less
        }
    }

    /// Whether column value `v` (non-NaN) sits at or below this edge
    /// as an UPPER bound.
    pub(crate) fn admits_from_above(&self, v: f64) -> bool {
        let v = if v == 0.0 { 0.0 } else { v };
        let ord = match self.value {
            NumBound::F(b) => v.total_cmp(&b),
            NumBound::I(n) => cmp_f64_i64(v, n),
            NumBound::U(n) => cmp_f64_u64(v, n),
        };
        if self.exclusive {
            ord == Ordering::Less
        } else {
            ord != Ordering::Greater
        }
    }
}

/// The smallest i64 an f64 LOWER bound admits, `None` when it admits
/// no i64 at all (the empty range). Integer arithmetic in i128 — for a
/// huge float, `floor()+1.0` cannot even represent the +1 — with the
/// saturating float-to-int cast covering the far tails.
fn int_lower(b: &Edge) -> Option<i64> {
    let lo: i128 = match b.value {
        NumBound::I(n) => i128::from(n) + i128::from(b.exclusive),
        NumBound::U(n) => i128::from(n) + i128::from(b.exclusive),
        NumBound::F(x) => {
            if b.exclusive {
                (x.floor() as i128).saturating_add(1)
            } else {
                x.ceil() as i128
            }
        }
    };
    if lo > i128::from(i64::MAX) {
        None
    } else {
        Some(lo.max(i128::from(i64::MIN)) as i64)
    }
}

/// The largest i64 an f64 UPPER bound admits, `None` when it admits
/// none.
fn int_upper(b: &Edge) -> Option<i64> {
    let hi: i128 = match b.value {
        NumBound::I(n) => i128::from(n) - i128::from(b.exclusive),
        NumBound::U(n) => i128::from(n) - i128::from(b.exclusive),
        NumBound::F(x) => {
            if b.exclusive {
                (x.ceil() as i128).saturating_sub(1)
            } else {
                x.floor() as i128
            }
        }
    };
    if hi < i128::from(i64::MIN) {
        None
    } else {
        Some(hi.min(i128::from(i64::MAX)) as i64)
    }
}

/// Normalize a resolved range onto an i64 column: inclusive `[lo, hi]`
/// with the exclusivity and any float bounds folded into exact integer
/// edges. `lo > hi` is the honest encoding of an empty range.
pub fn int_range(min: &Option<Edge>, max: &Option<Edge>) -> (i64, i64) {
    let lo = match min {
        None => Some(i64::MIN),
        Some(b) => int_lower(b),
    };
    let hi = match max {
        None => Some(i64::MAX),
        Some(b) => int_upper(b),
    };
    match (lo, hi) {
        (Some(lo), Some(hi)) => (lo, hi),
        // A bound past the end of i64 admits nothing.
        _ => (1, 0),
    }
}

/// Normalize signed, unsigned or finite double bounds onto inclusive u64
/// edges. The inverted range (1, 0) is empty; absence is evaluated separately.
pub fn uint_range(min: &Option<Edge>, max: &Option<Edge>) -> (u64, u64) {
    const TWO_64: f64 = 18_446_744_073_709_551_616.0;
    let lower = |b: &Edge| -> Option<u64> {
        let value = match b.value {
            NumBound::I(n) => i128::from(n) + i128::from(b.exclusive),
            NumBound::U(n) => i128::from(n) + i128::from(b.exclusive),
            NumBound::F(x) => {
                if x < 0.0 {
                    return Some(0);
                }
                if x >= TWO_64 {
                    return None;
                }
                if b.exclusive {
                    (x.floor() as i128) + 1
                } else {
                    x.ceil() as i128
                }
            }
        };
        u64::try_from(value.max(0)).ok()
    };
    let upper = |b: &Edge| -> Option<u64> {
        let value = match b.value {
            NumBound::I(n) => i128::from(n) - i128::from(b.exclusive),
            NumBound::U(n) => i128::from(n) - i128::from(b.exclusive),
            NumBound::F(x) => {
                if x < 0.0 {
                    return None;
                }
                if x >= TWO_64 {
                    return Some(u64::MAX);
                }
                if b.exclusive {
                    (x.ceil() as i128) - 1
                } else {
                    x.floor() as i128
                }
            }
        };
        u64::try_from(value.min(i128::from(u64::MAX))).ok()
    };
    match (
        min.as_ref().map_or(Some(0), lower),
        max.as_ref().map_or(Some(u64::MAX), upper),
    ) {
        (Some(lo), Some(hi)) => (lo, hi),
        _ => (1, 0),
    }
}

mod resolved;
#[cfg(test)]
mod tests;
mod validate;
mod walk;

pub use self::resolved::{DocFilter, MapKeyRef, PhraseGate, ResolvedFilter, ResolvedLeaf};
pub use self::validate::validate_filter;

crate::domain_error::domain_error! {
    /// Why a wire filter tree was refused, converted to transport status
    /// only at the RPC edge.
    #[derive(Debug, Clone)]
    pub enum FilterError {
        InvalidArgument => invalid_argument,
    }
    foreign: Geo
}
pub use self::walk::{edge_of, leaf_count, walk_leaves, LeafRef};
