//! Compiled filter predicates (`docs/cel-filters.md`): validation of
//! the wire [`crate::pb::FilterExpr`] tree, the per-shard resolved
//! form, and its three-valued evaluation.
//!
//! The division of labor is the one `docs/score-functions.md` pinned:
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
//! - **Numbers compare exactly, across domains.** An i64 bound against
//!   an f64 column is compared as the integer it says
//!   ([`cmp_f64_i64`]), never rounded through f64; an f64 bound
//!   against an i64 column is normalized to exact integer bounds with
//!   the exclusivity folded in. No comparison in this module is
//!   performed in a domain that could reorder it.
//! - **A filter only REMOVES documents.** Every block-max bound stays
//!   a valid upper bound over the survivors, so the pruning stack
//!   needs no new math; what changes is only WHERE the test sits (the
//!   heap gate), which `crate::bm25` owns.

use std::cmp::Ordering;
use std::ops::Not;

use tonic::Status;

use crate::scorefn::NumericRead;

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

impl Edge {
    /// Whether column value `v` (non-NaN) sits at or above this edge
    /// as a LOWER bound.
    pub(crate) fn admits_from_below(&self, v: f64) -> bool {
        let v = if v == 0.0 { 0.0 } else { v };
        let ord = match self.value {
            NumBound::F(b) => v.total_cmp(&b),
            NumBound::I(n) => cmp_f64_i64(v, n),
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
        NumBound::F(x) => {
            if b.exclusive {
                (x.floor() as i128) + 1
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
        NumBound::F(x) => {
            if b.exclusive {
                (x.ceil() as i128) - 1
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

/// A map-key presence target, resolved per shard: which map family the
/// column name landed in (map-facet first, then map-numeric), and the
/// key's ordinal there — `None` when this shard never ingested the
/// key, which makes the (total) test false for every document.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MapKeyRef {
    /// The name is a map-facet column here.
    Facet {
        /// Index into the shard's map-facet table.
        column: usize,
        /// The key's ordinal in that column's key dictionary.
        key_ord: Option<u32>,
    },
    /// The name is a map-numeric column here.
    Numeric {
        /// Index into the shard's map-numeric table.
        column: usize,
        /// The key's ordinal in that column's key dictionary.
        key_ord: Option<u32>,
    },
    /// This shard has no map column of the name: every document here
    /// genuinely lacks the key, so the test is false, not unknown.
    Unknown,
}

/// One leaf of a resolved filter tree: indices into THIS shard's
/// tables, string values already down-converted to ordinals. A
/// `column: None` means the shard lacks the column — every document
/// evaluates as the absent case, which is exact (the coordinator
/// refuses a leaf NO shard resolves).
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedLeaf {
    /// Facet membership: the document's ordinal must be one of `ords`
    /// (sorted). Values the shard's dictionary lacks resolved to
    /// nothing: they can match none of its documents.
    Facet {
        /// Index into the shard's facet table.
        column: Option<usize>,
        /// Accepted value ordinals, sorted ascending.
        ords: Vec<u32>,
    },
    /// Range over an i64 column, bounds pre-normalized to inclusive
    /// i64 by [`int_range`]; `lo > hi` is the empty range.
    IntRange {
        /// Index into the shard's integer table.
        column: usize,
        /// Inclusive lower edge.
        lo: i64,
        /// Inclusive upper edge.
        hi: i64,
    },
    /// Range over an f64 column, edges compared exactly in whichever
    /// domain each bound was written.
    F64Range {
        /// Index into the shard's numeric table.
        column: usize,
        /// Lower edge, `None` = unbounded below.
        lo: Option<Edge>,
        /// Upper edge, `None` = unbounded above.
        hi: Option<Edge>,
    },
    /// A number predicate whose column this shard has in NEITHER
    /// numeric family: every document is the absent (unknown) case.
    NumberUnknown,
    /// Map-facet membership under one key.
    MapFacet {
        /// (column, key ordinal), `None` when this shard lacks the
        /// column or never ingested the key.
        target: Option<(usize, u32)>,
        /// Accepted value ordinals, sorted ascending.
        ords: Vec<u32>,
    },
    /// Range under one key of a map-numeric column.
    MapNumber {
        /// (column, key ordinal), `None` as in [`ResolvedLeaf::MapFacet`].
        target: Option<(usize, u32)>,
        /// Lower edge.
        lo: Option<Edge>,
        /// Upper edge.
        hi: Option<Edge>,
    },
    /// Key presence in a map column (total).
    MapHasKey(MapKeyRef),
    /// Value presence under a name in any scalar family (total).
    Has {
        /// The name's index in the facet table, if any.
        facet: Option<usize>,
        /// ... in the f64 table.
        numeric: Option<usize>,
        /// ... in the i64 table.
        integer: Option<usize>,
        /// ... in the geo table.
        geo: Option<usize>,
    },
    /// Geo containment, the existing filter family as a leaf.
    Geo {
        /// Index into the shard's geo table.
        column: Option<usize>,
        /// The region (validated at request parse).
        region: crate::geo::GeoRegion,
    },
    /// A string range or prefix over a facet column resolved to the
    /// ordinal range `[lo, hi)` of a byte-sorted dictionary
    /// (`docs/prefix-terms.md`); `lo >= hi` is the empty range.
    FacetOrdRange {
        /// Index into the shard's facet table.
        column: Option<usize>,
        /// Inclusive lower ordinal.
        lo: u32,
        /// Exclusive upper ordinal.
        hi: u32,
    },
    /// The same over one key of a map-facet column, on its value
    /// dictionary.
    MapFacetOrdRange {
        /// (column, key ordinal), `None` as in [`ResolvedLeaf::MapFacet`].
        target: Option<(usize, u32)>,
        /// Inclusive lower ordinal.
        lo: u32,
        /// Exclusive upper ordinal.
        hi: u32,
    },
}

impl ResolvedLeaf {
    /// This leaf's verdict on `doc_id`. Comparisons answer
    /// [`Tri::Unknown`] on absence; the presence tests are total.
    fn eval(&self, doc_id: u32, cols: &dyn NumericRead) -> Tri {
        match self {
            ResolvedLeaf::Facet { column, ords } => match column {
                None => Tri::Unknown,
                Some(fi) => match cols.facet_ord(*fi, doc_id) {
                    None => Tri::Unknown,
                    Some(ord) => Tri::from(ords.binary_search(&ord).is_ok()),
                },
            },
            ResolvedLeaf::IntRange { column, lo, hi } => match cols.int_value(*column, doc_id) {
                None => Tri::Unknown,
                Some(v) => Tri::from(v >= *lo && v <= *hi),
            },
            ResolvedLeaf::F64Range { column, lo, hi } => match cols.value(*column, doc_id) {
                None => Tri::Unknown,
                Some(v) => Tri::from(
                    lo.as_ref().is_none_or(|e| e.admits_from_below(v))
                        && hi.as_ref().is_none_or(|e| e.admits_from_above(v)),
                ),
            },
            ResolvedLeaf::NumberUnknown => Tri::Unknown,
            ResolvedLeaf::MapFacet { target, ords } => match target {
                None => Tri::Unknown,
                Some((ci, key_ord)) => match cols.map_facet_value_ord(*ci, *key_ord, doc_id) {
                    None => Tri::Unknown,
                    Some(ord) => Tri::from(ords.binary_search(&ord).is_ok()),
                },
            },
            ResolvedLeaf::MapNumber { target, lo, hi } => match target {
                None => Tri::Unknown,
                Some((ci, key_ord)) => match cols.map_value(*ci, *key_ord, doc_id) {
                    None => Tri::Unknown,
                    Some(v) => Tri::from(
                        lo.as_ref().is_none_or(|e| e.admits_from_below(v))
                            && hi.as_ref().is_none_or(|e| e.admits_from_above(v)),
                    ),
                },
            },
            ResolvedLeaf::MapHasKey(target) => match target {
                MapKeyRef::Facet {
                    column,
                    key_ord: Some(k),
                } => Tri::from(cols.map_facet_value_ord(*column, *k, doc_id).is_some()),
                MapKeyRef::Numeric {
                    column,
                    key_ord: Some(k),
                } => Tri::from(cols.map_value(*column, *k, doc_id).is_some()),
                // The key (or the whole column) was never ingested
                // here: no document has it. False, and total — "k" in
                // an empty map is false in CEL, not an error.
                _ => Tri::False,
            },
            ResolvedLeaf::Has {
                facet,
                numeric,
                integer,
                geo,
            } => Tri::from(
                facet.is_some_and(|fi| cols.facet_ord(fi, doc_id).is_some())
                    || numeric.is_some_and(|ni| cols.value(ni, doc_id).is_some())
                    || integer.is_some_and(|ii| cols.int_value(ii, doc_id).is_some())
                    || geo.is_some_and(|gi| cols.geo_value(gi, doc_id).is_some()),
            ),
            ResolvedLeaf::Geo { column, region } => match column {
                None => Tri::Unknown,
                Some(gi) => match cols.geo_value(*gi, doc_id) {
                    None => Tri::Unknown,
                    Some((lat, lon)) => Tri::from(region.contains(lat, lon)),
                },
            },
            ResolvedLeaf::FacetOrdRange { column, lo, hi } => match column {
                None => Tri::Unknown,
                Some(fi) => match cols.facet_ord(*fi, doc_id) {
                    None => Tri::Unknown,
                    Some(ord) => Tri::from(*lo <= ord && ord < *hi),
                },
            },
            ResolvedLeaf::MapFacetOrdRange { target, lo, hi } => match target {
                None => Tri::Unknown,
                Some((ci, key_ord)) => match cols.map_facet_value_ord(*ci, *key_ord, doc_id) {
                    None => Tri::Unknown,
                    Some(ord) => Tri::from(*lo <= ord && ord < *hi),
                },
            },
        }
    }
}

/// A resolved filter tree: [`crate::pb::FilterExpr`] with this shard's
/// indices and ordinals in place of names and strings.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedFilter {
    /// Kleene AND over the children.
    And(Vec<ResolvedFilter>),
    /// Kleene OR over the children.
    Or(Vec<ResolvedFilter>),
    /// Kleene NOT.
    Not(Box<ResolvedFilter>),
    /// A leaf predicate.
    Leaf(ResolvedLeaf),
}

impl ResolvedFilter {
    /// The tree's verdict on `doc_id`, with short-circuiting that is
    /// invisible in the result (Kleene AND/OR are commutative and
    /// associative; only False/True can end a walk early).
    pub fn eval(&self, doc_id: u32, cols: &dyn NumericRead) -> Tri {
        match self {
            ResolvedFilter::And(children) => {
                let mut acc = Tri::True;
                for c in children {
                    match c.eval(doc_id, cols) {
                        Tri::False => return Tri::False,
                        v => acc = acc.and(v),
                    }
                }
                acc
            }
            ResolvedFilter::Or(children) => {
                let mut acc = Tri::False;
                for c in children {
                    match c.eval(doc_id, cols) {
                        Tri::True => return Tri::True,
                        v => acc = acc.or(v),
                    }
                }
                acc
            }
            ResolvedFilter::Not(child) => !child.eval(doc_id, cols),
            ResolvedFilter::Leaf(leaf) => leaf.eval(doc_id, cols),
        }
    }
}

/// Everything a request filters by, resolved for one shard: the
/// standalone geo filters and the compiled tree, ANDed. This is what
/// [`crate::bm25::FilterCtx`] carries to the heap gate; an empty
/// DocFilter is never built (the ctx is `None` instead), so a
/// filterless query takes a path bit-identical to the unfiltered
/// scorers.
#[derive(Debug, Clone, Default)]
pub struct DocFilter<'a> {
    /// Generation tombstones. A set bit is rejected before any column work.
    pub deleted: Option<std::sync::Arc<Vec<u64>>>,
    /// The `geo_filters` field, resolved (`docs/geo-columns.md`).
    pub geo: crate::geo::GeoFilters,
    /// The `filter` tree, resolved; `None` when the request sent none.
    pub pred: Option<ResolvedFilter>,
    /// Ordered-window phrase constraints (`docs/phrase-proximity.md`),
    /// one per constrained field, all of which must hold. Evaluated
    /// last: the cheaper predicates above reject most documents before
    /// a positions read is paid. Empty when the request carried none.
    pub phrase: Vec<PhraseGate<'a>>,
}

/// One field's phrase constraint, resolved for one shard: the field's
/// index view (which must carry positions — the node refused the leg
/// otherwise), the leg's terms, and the query's term sequence with its
/// slop. A document passes when [`crate::proximity::phrase_matches`]
/// finds the window; the gate only ever REMOVES documents, so every
/// block-max bound over the survivors stays a bound.
#[derive(Clone)]
pub struct PhraseGate<'a> {
    pub index: &'a dyn crate::postings::Bm25Index,
    pub terms: &'a [String],
    pub sequence: Vec<usize>,
    pub slop: u32,
}

impl std::fmt::Debug for PhraseGate<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhraseGate")
            .field("terms", &self.terms)
            .field("sequence", &self.sequence)
            .field("slop", &self.slop)
            .finish_non_exhaustive()
    }
}

impl DocFilter<'_> {
    /// Whether every filter family passes. The tree passes only on
    /// [`Tri::True`] — Unknown is a document the filter cannot vouch
    /// for, and admitting it would make absence a way to sneak past a
    /// predicate. Phrase gates are checked last, after the column
    /// predicates have had their chance to reject without a positions
    /// read.
    pub fn passes(&self, doc_id: u32, cols: &dyn NumericRead) -> bool {
        !self.deleted.as_ref().is_some_and(|words| {
            words
                .get(doc_id as usize / 64)
                .is_some_and(|word| word & (1u64 << (doc_id % 64)) != 0)
        }) && self.geo.passes(doc_id, cols)
            && self
                .pred
                .as_ref()
                .is_none_or(|p| p.eval(doc_id, cols) == Tri::True)
            && self.phrase.iter().all(|gate| {
                crate::proximity::phrase_matches(
                    gate.index,
                    gate.terms,
                    &gate.sequence,
                    gate.slop,
                    doc_id,
                )
            })
    }
}

/// One leaf of the WIRE tree, borrowed. [`walk_leaves`] hands these
/// out in depth-first order — the ONE definition of the order the
/// positional `filter_columns_known` handshake uses, shared by the
/// node (answering) and the coordinator (refusing), so the two sides
/// cannot disagree about which flag is whose.
#[derive(Debug, Clone, Copy)]
pub enum LeafRef<'a> {
    /// A facet predicate.
    Facet(&'a crate::pb::FacetPredicate),
    /// A number predicate.
    Number(&'a crate::pb::NumberPredicate),
    /// A map-facet predicate.
    MapFacet(&'a crate::pb::MapFacetPredicate),
    /// A map-number predicate.
    MapNumber(&'a crate::pb::MapNumberPredicate),
    /// A map-key presence test.
    MapHasKey(&'a crate::pb::MapKeyPredicate),
    /// A column presence test.
    Has(&'a crate::pb::HasPredicate),
    /// A geo leaf.
    Geo(&'a crate::pb::GeoFilter),
    /// A string range on a facet column or map-facet value.
    StringRange(&'a crate::pb::StringRangePredicate),
    /// A string prefix on a facet column or map-facet value.
    StringPrefix(&'a crate::pb::StringPrefixPredicate),
}

impl LeafRef<'_> {
    /// `kind column[key]` for refusal messages: which table the caller
    /// should be checking for the spelling.
    pub fn describe(&self) -> String {
        match self {
            LeafRef::Facet(p) => format!("facet column {:?}", p.column),
            LeafRef::Number(p) => format!("numeric column {:?}", p.column),
            LeafRef::MapFacet(p) => format!("map-facet column {:?} key {:?}", p.column, p.key),
            LeafRef::MapNumber(p) => {
                format!("map-numeric column {:?} key {:?}", p.column, p.key)
            }
            LeafRef::MapHasKey(p) => format!("map column {:?}", p.column),
            LeafRef::Has(p) => format!("column {:?}", p.column),
            LeafRef::Geo(g) => format!("geo column {:?}", g.column),
            LeafRef::StringRange(p) => describe_string_target(&p.column, &p.key),
            LeafRef::StringPrefix(p) => describe_string_target(&p.column, &p.key),
        }
    }
}

fn describe_string_target(column: &str, key: &str) -> String {
    if key.is_empty() {
        format!("facet column {column:?}")
    } else {
        format!("map-facet column {column:?} key {key:?}")
    }
}

/// Visit every leaf of `expr` in depth-first, field-order sequence
/// (and/or children left to right, then a not's child). Assumes a
/// validated tree; an unset oneof contributes nothing.
pub fn walk_leaves<'a>(expr: &'a crate::pb::FilterExpr, visit: &mut dyn FnMut(LeafRef<'a>)) {
    use crate::pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) | Some(Expr::Or(list)) => {
            for child in &list.exprs {
                walk_leaves(child, visit);
            }
        }
        Some(Expr::Not(child)) => walk_leaves(child, visit),
        Some(Expr::Facet(p)) => visit(LeafRef::Facet(p)),
        Some(Expr::Number(p)) => visit(LeafRef::Number(p)),
        Some(Expr::MapFacet(p)) => visit(LeafRef::MapFacet(p)),
        Some(Expr::MapNumber(p)) => visit(LeafRef::MapNumber(p)),
        Some(Expr::MapHasKey(p)) => visit(LeafRef::MapHasKey(p)),
        Some(Expr::Has(p)) => visit(LeafRef::Has(p)),
        Some(Expr::Geo(g)) => visit(LeafRef::Geo(g)),
        Some(Expr::StringRange(p)) => visit(LeafRef::StringRange(p)),
        Some(Expr::StringPrefix(p)) => visit(LeafRef::StringPrefix(p)),
        None => {}
    }
}

/// Number of leaves a validated tree holds — the length both sides
/// expect of `filter_columns_known`.
pub fn leaf_count(expr: &crate::pb::FilterExpr) -> usize {
    let mut n = 0;
    walk_leaves(expr, &mut |_| n += 1);
    n
}

/// Resolve one wire bound into an [`Edge`], normalizing `-0.0` so
/// [`f64::total_cmp`] cannot disagree with IEEE equality. Assumes a
/// validated tree (the oneof is set and floats are finite).
pub fn edge_of(b: &crate::pb::FilterBound) -> Option<Edge> {
    use crate::pb::filter_bound::Value;
    let value = match b.value.as_ref()? {
        Value::Int(n) => NumBound::I(*n),
        Value::Num(x) => NumBound::F(if *x == 0.0 { 0.0 } else { *x }),
    };
    Some(Edge {
        value,
        exclusive: b.exclusive,
    })
}

/// Validate a wire filter tree: shape (every oneof set, connectives
/// non-empty, depth and leaf caps) and every leaf's own contract, each
/// refusal by name. Runs on the coordinator BEFORE fan-out — a
/// malformed filter refuses even when the query has no match set — and
/// again on every node, which trusts no caller.
pub fn validate_filter(expr: &crate::pb::FilterExpr) -> Result<(), Status> {
    let mut leaves = 0usize;
    validate_node(expr, 1, &mut leaves)?;
    if leaves > MAX_LEAVES {
        return Err(Status::invalid_argument(format!(
            "filter has {leaves} leaves; the limit is {MAX_LEAVES} — a filter is a \
             predicate, not a program"
        )));
    }
    Ok(())
}

fn validate_node(
    expr: &crate::pb::FilterExpr,
    depth: usize,
    leaves: &mut usize,
) -> Result<(), Status> {
    use crate::pb::filter_expr::Expr;
    if depth > MAX_DEPTH {
        return Err(Status::invalid_argument(format!(
            "filter tree is deeper than {MAX_DEPTH} levels"
        )));
    }
    match &expr.expr {
        None => Err(Status::invalid_argument(
            "filter: a node with no expression set; an empty filter is the field left \
             absent, never an empty node",
        )),
        Some(Expr::And(list)) | Some(Expr::Or(list)) => {
            if list.exprs.is_empty() {
                return Err(Status::invalid_argument(
                    "filter: and/or with no children; a connective with nothing to \
                     connect is refused rather than resolved to a vacuous truth value",
                ));
            }
            for child in &list.exprs {
                validate_node(child, depth + 1, leaves)?;
            }
            Ok(())
        }
        Some(Expr::Not(child)) => validate_node(child, depth + 1, leaves),
        Some(Expr::Facet(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter facet predicate: a predicate names the column it reads",
                ));
            }
            if p.values.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "filter facet predicate on {:?}: at least one value; membership in \
                     the empty set is a predicate nobody meant to send",
                    p.column
                )));
            }
            Ok(())
        }
        Some(Expr::Number(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter number predicate: a predicate names the column it reads",
                ));
            }
            validate_bound(&p.column, p.min.as_ref())?;
            validate_bound(&p.column, p.max.as_ref())?;
            if p.min.is_none() && p.max.is_none() {
                return Err(Status::invalid_argument(format!(
                    "filter number predicate on {:?}: at least one bound; bare presence \
                     is has({})",
                    p.column, p.column
                )));
            }
            Ok(())
        }
        Some(Expr::MapFacet(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter map-facet predicate: a predicate names the column it reads",
                ));
            }
            if p.values.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "filter map-facet predicate on {:?}[{:?}]: at least one value",
                    p.column, p.key
                )));
            }
            Ok(())
        }
        Some(Expr::MapNumber(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter map-number predicate: a predicate names the column it reads",
                ));
            }
            validate_bound(&p.column, p.min.as_ref())?;
            validate_bound(&p.column, p.max.as_ref())?;
            if p.min.is_none() && p.max.is_none() {
                return Err(Status::invalid_argument(format!(
                    "filter map-number predicate on {:?}[{:?}]: at least one bound; key \
                     presence is `{:?} in {}`",
                    p.column, p.key, p.key, p.column
                )));
            }
            Ok(())
        }
        Some(Expr::MapHasKey(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter map-key predicate: a predicate names the column it reads",
                ));
            }
            Ok(())
        }
        Some(Expr::Has(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter has predicate: a predicate names the column it reads",
                ));
            }
            Ok(())
        }
        Some(Expr::Geo(g)) => {
            *leaves += 1;
            crate::node::validate_geo_filter(g).map(|_| ())
        }
        Some(Expr::StringRange(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter string-range predicate: a predicate names the column it reads",
                ));
            }
            if p.min.is_none() && p.max.is_none() {
                return Err(Status::invalid_argument(format!(
                    "filter string-range predicate on {:?}: at least one bound; bare \
                     presence is has({})",
                    p.column, p.column
                )));
            }
            Ok(())
        }
        Some(Expr::StringPrefix(p)) => {
            *leaves += 1;
            if p.column.is_empty() {
                return Err(Status::invalid_argument(
                    "filter string-prefix predicate: a predicate names the column it reads",
                ));
            }
            if p.prefix.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "filter string-prefix predicate on {:?}: an empty prefix matches every \
                     value, which is has({})",
                    p.column, p.column
                )));
            }
            Ok(())
        }
    }
}

/// One bound's own contract: the oneof is set and a float bound is
/// finite. (An integer bound has nothing to check — every i64 is a
/// legal bound.)
fn validate_bound(column: &str, b: Option<&crate::pb::FilterBound>) -> Result<(), Status> {
    use crate::pb::filter_bound::Value;
    match b {
        None => Ok(()),
        Some(bound) => match bound.value.as_ref() {
            None => Err(Status::invalid_argument(format!(
                "filter bound on {column:?}: no value set; a bound says the number it \
                 bounds by"
            ))),
            Some(Value::Num(x)) if !x.is_finite() => Err(Status::invalid_argument(format!(
                "filter bound on {column:?}: {x} is not finite; NaN and infinity bound \
                 nothing a column can hold"
            ))),
            Some(_) => Ok(()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb;

    /// Kleene tables, pinned value by value: SQL semantics, not stock
    /// CEL's error propagation and not two-valued shortcuts.
    #[test]
    fn kleene_tables_are_sql() {
        use Tri::*;
        let all = [True, False, Unknown];
        for a in all {
            assert_eq!(a.and(True), a);
            assert_eq!(a.and(False), False);
            assert_eq!(a.or(False), a);
            assert_eq!(a.or(True), True);
            assert_eq!(a.and(a), a);
            assert_eq!(a.or(a), a);
            for b in all {
                assert_eq!(a.and(b), b.and(a), "and commutes");
                assert_eq!(a.or(b), b.or(a), "or commutes");
                // De Morgan holds in Kleene logic.
                assert_eq!(!a.and(b), (!a).or(!b));
            }
        }
        assert_eq!(Unknown.and(Unknown), Unknown);
        assert_eq!(Unknown.or(Unknown), Unknown);
        assert_eq!(!Unknown, Unknown, "negation cannot launder absence");
        assert_eq!(!True, False);
        assert_eq!(!False, True);
    }

    /// The exact f64-vs-i64 comparison at the values where a rounded
    /// comparison would lie: above 2^53, at the 2^63 edges, and on
    /// fractions.
    #[test]
    fn cross_domain_comparison_is_exact() {
        use Ordering::*;
        assert_eq!(cmp_f64_i64(5.5, 5), Greater);
        assert_eq!(cmp_f64_i64(5.5, 6), Less);
        assert_eq!(cmp_f64_i64(5.0, 5), Equal);
        assert_eq!(cmp_f64_i64(-5.5, -5), Less);
        assert_eq!(cmp_f64_i64(-5.5, -6), Greater);
        assert_eq!(cmp_f64_i64(-0.0, 0), Equal);
        // 2^53 + 1 is not representable: the f64 next to it is 2^53.
        // A comparison through `as f64` would call them equal.
        let two53 = 9_007_199_254_740_992i64; // 2^53
        assert_eq!(cmp_f64_i64(two53 as f64, two53 + 1), Less);
        assert_eq!(cmp_f64_i64(two53 as f64, two53), Equal);
        // 2^63 is above every i64; i64::MAX as f64 ROUNDS to 2^63,
        // which is exactly why the comparison must not go through it.
        assert_eq!(cmp_f64_i64(9_223_372_036_854_775_808.0, i64::MAX), Greater);
        assert_eq!(cmp_f64_i64(-9_223_372_036_854_775_808.0, i64::MIN), Equal);
        assert_eq!(cmp_f64_i64(f64::INFINITY, i64::MAX), Greater);
        assert_eq!(cmp_f64_i64(f64::NEG_INFINITY, i64::MIN), Less);
        // The value just below 2^63 (exact f64) vs i64::MAX.
        let below = 9_223_372_036_854_774_784.0f64; // 2^63 - 1024
        assert_eq!(cmp_f64_i64(below, i64::MAX), Less);
    }

    /// Float bounds onto i64 columns become exact integer edges; the
    /// +1 of an exclusive bound happens in integer math where a float
    /// +1 would round away.
    #[test]
    fn int_range_normalization_is_exact() {
        let edge = |value, exclusive| Some(Edge { value, exclusive });
        // year > 1989.5 -> lo 1990; year >= 1990.0 -> lo 1990.
        assert_eq!(int_range(&edge(NumBound::F(1989.5), true), &None).0, 1990);
        assert_eq!(int_range(&edge(NumBound::F(1990.0), false), &None).0, 1990);
        // year > 1990.0 -> lo 1991; year < 1990.0 -> hi 1989.
        assert_eq!(int_range(&edge(NumBound::F(1990.0), true), &None).0, 1991);
        assert_eq!(int_range(&None, &edge(NumBound::F(1990.0), true)).1, 1989);
        // Exclusive integer bounds at the ends: > i64::MAX admits
        // nothing; >= stays inclusive.
        assert_eq!(int_range(&edge(NumBound::I(i64::MAX), true), &None), (1, 0));
        assert_eq!(
            int_range(&edge(NumBound::I(i64::MAX), false), &None),
            (i64::MAX, i64::MAX)
        );
        assert_eq!(int_range(&None, &edge(NumBound::I(i64::MIN), true)), (1, 0));
        // A float bound at 2^62: floor()+1.0 would round back to 2^62
        // (the ULP there is 1024); the i128 arithmetic must not.
        let x = 4_611_686_018_427_387_904.0f64; // 2^62 exactly
        assert_eq!(
            int_range(&edge(NumBound::F(x), true), &None).0,
            4_611_686_018_427_387_905i64
        );
        // Bounds far outside i64 clamp to the full range (below) or
        // empty (above).
        assert_eq!(
            int_range(&edge(NumBound::F(-1e300), false), &None).0,
            i64::MIN
        );
        assert_eq!(int_range(&edge(NumBound::F(1e300), false), &None), (1, 0));
        assert_eq!(
            int_range(&None, &edge(NumBound::F(1e300), false)).1,
            i64::MAX
        );
    }

    /// A read surface with one column of each kind, two documents:
    /// doc 0 holds every value, doc 1 holds none.
    struct TwoDocs;
    impl NumericRead for TwoDocs {
        fn value(&self, _ni: usize, doc_id: u32) -> Option<f64> {
            (doc_id == 0).then_some(2.5)
        }
        fn map_value(&self, _ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
            (doc_id == 0 && key_ord == 7).then_some(9.0)
        }
        fn int_value(&self, _ii: usize, doc_id: u32) -> Option<i64> {
            (doc_id == 0).then_some(1990)
        }
        fn geo_value(&self, _gi: usize, doc_id: u32) -> Option<(f64, f64)> {
            (doc_id == 0).then_some((38.0, -77.0))
        }
        fn facet_ord(&self, _fi: usize, doc_id: u32) -> Option<u32> {
            (doc_id == 0).then_some(3)
        }
        fn map_facet_value_ord(&self, _ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
            (doc_id == 0 && key_ord == 4).then_some(11)
        }
    }

    /// Absence semantics, leaf by leaf: comparisons answer Unknown on
    /// the value-less document, presence tests answer False — and only
    /// True survives [`DocFilter::passes`].
    #[test]
    fn absence_is_unknown_and_presence_is_total() {
        let cols = TwoDocs;
        let facet = ResolvedLeaf::Facet {
            column: Some(0),
            ords: vec![3],
        };
        assert_eq!(facet.eval(0, &cols), Tri::True);
        assert_eq!(facet.eval(1, &cols), Tri::Unknown);
        let missing_value = ResolvedLeaf::Facet {
            column: Some(0),
            ords: vec![9],
        };
        assert_eq!(
            missing_value.eval(0, &cols),
            Tri::False,
            "held a DIFFERENT value"
        );
        assert_eq!(missing_value.eval(1, &cols), Tri::Unknown);
        let unknown_col = ResolvedLeaf::Facet {
            column: None,
            ords: vec![],
        };
        assert_eq!(unknown_col.eval(0, &cols), Tri::Unknown);

        let int = ResolvedLeaf::IntRange {
            column: 0,
            lo: 1990,
            hi: i64::MAX,
        };
        assert_eq!(int.eval(0, &cols), Tri::True);
        assert_eq!(int.eval(1, &cols), Tri::Unknown);
        let empty = ResolvedLeaf::IntRange {
            column: 0,
            lo: 1,
            hi: 0,
        };
        assert_eq!(
            empty.eval(0, &cols),
            Tri::False,
            "empty range fails the present"
        );
        assert_eq!(
            empty.eval(1, &cols),
            Tri::Unknown,
            "and stays unknown for the absent"
        );

        let f64r = ResolvedLeaf::F64Range {
            column: 0,
            lo: Some(Edge {
                value: NumBound::I(2),
                exclusive: false,
            }),
            hi: Some(Edge {
                value: NumBound::F(2.5),
                exclusive: false,
            }),
        };
        assert_eq!(f64r.eval(0, &cols), Tri::True, "2.5 in [2, 2.5]");
        assert_eq!(f64r.eval(1, &cols), Tri::Unknown);
        let f64x = ResolvedLeaf::F64Range {
            column: 0,
            lo: None,
            hi: Some(Edge {
                value: NumBound::F(2.5),
                exclusive: true,
            }),
        };
        assert_eq!(f64x.eval(0, &cols), Tri::False, "2.5 < 2.5 is false");

        let map_facet = ResolvedLeaf::MapFacet {
            target: Some((0, 4)),
            ords: vec![11],
        };
        assert_eq!(map_facet.eval(0, &cols), Tri::True);
        assert_eq!(map_facet.eval(1, &cols), Tri::Unknown);

        let map_num = ResolvedLeaf::MapNumber {
            target: Some((0, 7)),
            lo: Some(Edge {
                value: NumBound::I(9),
                exclusive: false,
            }),
            hi: None,
        };
        assert_eq!(map_num.eval(0, &cols), Tri::True);
        assert_eq!(map_num.eval(1, &cols), Tri::Unknown);

        // Presence tests: total, absence is False, never Unknown.
        let has_key = ResolvedLeaf::MapHasKey(MapKeyRef::Facet {
            column: 0,
            key_ord: Some(4),
        });
        assert_eq!(has_key.eval(0, &cols), Tri::True);
        assert_eq!(has_key.eval(1, &cols), Tri::False);
        let has_key_never = ResolvedLeaf::MapHasKey(MapKeyRef::Unknown);
        assert_eq!(has_key_never.eval(0, &cols), Tri::False);
        let has = ResolvedLeaf::Has {
            facet: Some(0),
            numeric: None,
            integer: None,
            geo: None,
        };
        assert_eq!(has.eval(0, &cols), Tri::True);
        assert_eq!(has.eval(1, &cols), Tri::False);

        let geo = ResolvedLeaf::Geo {
            column: Some(0),
            region: crate::geo::GeoRegion::Bbox {
                min_lat: 30.0,
                max_lat: 40.0,
                min_lon: -80.0,
                max_lon: -70.0,
            },
        };
        assert_eq!(geo.eval(0, &cols), Tri::True);
        assert_eq!(geo.eval(1, &cols), Tri::Unknown);

        // NOT cannot launder absence into a match, and has() is the
        // escape hatch that can see it.
        let not_facet = ResolvedFilter::Not(Box::new(ResolvedFilter::Leaf(facet)));
        assert_eq!(not_facet.eval(1, &cols), Tri::Unknown);
        let not_has = ResolvedFilter::Not(Box::new(ResolvedFilter::Leaf(has)));
        assert_eq!(not_has.eval(1, &cols), Tri::True);

        // Only True passes the gate.
        let gate = |pred: ResolvedFilter, doc: u32| {
            DocFilter {
                deleted: None,
                geo: crate::geo::GeoFilters::default(),
                pred: Some(pred),
                phrase: Vec::new(),
            }
            .passes(doc, &cols)
        };
        assert!(gate(not_has.clone(), 1));
        assert!(!gate(not_facet.clone(), 1), "Unknown is filtered out");
        // Kleene rescue: Unknown OR True is True.
        let rescued = ResolvedFilter::Or(vec![not_facet, not_has]);
        assert!(gate(rescued, 1));
    }

    fn leaf(column: &str) -> pb::FilterExpr {
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
                column: column.into(),
                values: vec!["v".into()],
            })),
        }
    }

    /// Shape validation: refusals for the empty node, empty
    /// connectives, bound-less numbers, valueless facets, non-finite
    /// bounds, and the depth and leaf caps.
    #[test]
    fn validation_refuses_by_name() {
        let refused = |e: &pb::FilterExpr, needle: &str| {
            let err = validate_filter(e).expect_err(needle);
            assert!(
                err.message().contains(needle),
                "{needle:?} not in {:?}",
                err.message()
            );
        };
        refused(&pb::FilterExpr { expr: None }, "no expression set");
        refused(
            &pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::And(pb::FilterList { exprs: vec![] })),
            },
            "no children",
        );
        refused(
            &pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
                    column: "c".into(),
                    values: vec![],
                })),
            },
            "at least one value",
        );
        refused(
            &pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                    column: "n".into(),
                    min: None,
                    max: None,
                })),
            },
            "at least one bound",
        );
        refused(
            &pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                    column: "n".into(),
                    min: Some(pb::FilterBound {
                        value: Some(pb::filter_bound::Value::Num(f64::NAN)),
                        exclusive: false,
                    }),
                    max: None,
                })),
            },
            "not finite",
        );
        refused(
            &pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                    column: "n".into(),
                    min: Some(pb::FilterBound {
                        value: None,
                        exclusive: false,
                    }),
                    max: None,
                })),
            },
            "no value set",
        );
        refused(
            &pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Has(pb::HasPredicate {
                    column: String::new(),
                })),
            },
            "names the column",
        );

        // Depth: a chain of NOTs one past the cap.
        let mut deep = leaf("c");
        for _ in 0..MAX_DEPTH {
            deep = pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Not(Box::new(deep))),
            };
        }
        refused(&deep, "deeper than");

        // Leaves: one past the cap under a single AND.
        let wide = pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::And(pb::FilterList {
                exprs: (0..=MAX_LEAVES).map(|_| leaf("c")).collect(),
            })),
        };
        refused(&wide, "leaves");

        // And the legal versions of each cap pass.
        let mut ok = leaf("c");
        for _ in 0..MAX_DEPTH - 1 {
            ok = pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Not(Box::new(ok))),
            };
        }
        validate_filter(&ok).expect("depth at the cap is legal");
        let ok_wide = pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::And(pb::FilterList {
                exprs: (0..MAX_LEAVES).map(|_| leaf("c")).collect(),
            })),
        };
        validate_filter(&ok_wide).expect("leaf count at the cap is legal");
    }

    /// The positional contract: leaves surface in depth-first,
    /// left-to-right order, the order both sides of the
    /// `filter_columns_known` handshake derive independently.
    #[test]
    fn walk_order_is_depth_first() {
        let tree = pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::And(pb::FilterList {
                exprs: vec![
                    leaf("a"),
                    pb::FilterExpr {
                        expr: Some(pb::filter_expr::Expr::Or(pb::FilterList {
                            exprs: vec![
                                leaf("b"),
                                pb::FilterExpr {
                                    expr: Some(pb::filter_expr::Expr::Not(Box::new(leaf("c")))),
                                },
                            ],
                        })),
                    },
                    leaf("d"),
                ],
            })),
        };
        let mut seen = Vec::new();
        walk_leaves(&tree, &mut |l| {
            if let LeafRef::Facet(p) = l {
                seen.push(p.column.clone());
            }
        });
        assert_eq!(seen, ["a", "b", "c", "d"]);
        assert_eq!(leaf_count(&tree), 4);
    }
}
