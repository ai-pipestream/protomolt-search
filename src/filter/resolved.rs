//! The per-shard resolved filter: leaves over this shard's tables and
//! ordinals, the Kleene tree over them, and the document gate.

use super::{Edge, Tri};
use crate::scorefn::NumericRead;

/// A map-key presence target, resolved per shard: which map family the
/// column name landed in (map-facet first, then map-numeric), and the
/// key's ordinal there — `None` when this shard never ingested the
/// key, which makes the (total) test false for every document.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MapKeyRef {
    /// Key presence in an exact signed map column.
    Integer { column: usize, key_ord: Option<u32> },
    /// Key presence in an exact unsigned map column.
    UnsignedInteger { column: usize, key_ord: Option<u32> },
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
    /// i64 by [`int_range`](super::int_range); `lo > hi` is the empty range.
    IntRange {
        /// Index into the shard's integer table.
        column: usize,
        /// Inclusive lower edge.
        lo: i64,
        /// Inclusive upper edge.
        hi: i64,
    },
    /// Range over a u64 column, normalized by [`uint_range`](super::uint_range).
    UintRange {
        /// Index into the shard's unsigned integer table.
        column: usize,
        /// Inclusive lower edge.
        lo: u64,
        /// Inclusive upper edge.
        hi: u64,
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
    /// A number predicate whose column this shard has in no
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
    /// Exact signed map range, normalized to inclusive integer bounds.
    MapIntRange {
        target: Option<(usize, u32)>,
        lo: i64,
        hi: i64,
    },
    /// Exact unsigned map range, normalized to inclusive integer bounds.
    MapUintRange {
        target: Option<(usize, u32)>,
        lo: u64,
        hi: u64,
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
        /// ... in the u64 table.
        unsigned_integer: Option<usize>,
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
    /// ordinal range `[lo, hi)` of a byte-sorted dictionary; `lo >= hi` is the empty range.
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
    pub(crate) fn eval(&self, doc_id: u32, cols: &dyn NumericRead) -> Tri {
        use ResolvedLeaf as L;
        match self {
            L::Facet { column, ords } => {
                tri_of(column.and_then(|fi| cols.facet_ord(fi, doc_id)), |ord| {
                    ords.binary_search(&ord).is_ok()
                })
            }
            L::IntRange { column, lo, hi } => {
                tri_of(cols.int_value(*column, doc_id), |v| v >= *lo && v <= *hi)
            }
            L::UintRange { column, lo, hi } => {
                tri_of(cols.uint_value(*column, doc_id), |v| v >= *lo && v <= *hi)
            }
            L::F64Range { column, lo, hi } => {
                tri_of(cols.value(*column, doc_id), |v| within(v, lo, hi))
            }
            L::NumberUnknown => Tri::Unknown,
            L::MapFacet { target, ords } => tri_of(
                target.and_then(|(ci, key)| cols.map_facet_value_ord(ci, key, doc_id)),
                |ord| ords.binary_search(&ord).is_ok(),
            ),
            L::MapNumber { target, lo, hi } => tri_of(
                target.and_then(|(ci, key)| cols.map_value(ci, key, doc_id)),
                |v| within(v, lo, hi),
            ),
            L::MapIntRange { target, lo, hi } => tri_of(
                target.and_then(|(ci, key)| cols.map_int_value(ci, key, doc_id)),
                |v| v >= *lo && v <= *hi,
            ),
            L::MapUintRange { target, lo, hi } => tri_of(
                target.and_then(|(ci, key)| cols.map_uint_value(ci, key, doc_id)),
                |v| v >= *lo && v <= *hi,
            ),
            L::MapHasKey(target) => target.eval(doc_id, cols),
            L::Has {
                facet,
                numeric,
                integer,
                unsigned_integer,
                geo,
            } => Tri::from(
                facet.is_some_and(|fi| cols.facet_ord(fi, doc_id).is_some())
                    || numeric.is_some_and(|ni| cols.value(ni, doc_id).is_some())
                    || integer.is_some_and(|ii| cols.int_value(ii, doc_id).is_some())
                    || unsigned_integer.is_some_and(|ii| cols.uint_value(ii, doc_id).is_some())
                    || geo.is_some_and(|gi| cols.geo_value(gi, doc_id).is_some()),
            ),
            L::Geo { column, region } => tri_of(
                column.and_then(|gi| cols.geo_value(gi, doc_id)),
                |(lat, lon)| region.contains(lat, lon),
            ),
            L::FacetOrdRange { column, lo, hi } => {
                tri_of(column.and_then(|fi| cols.facet_ord(fi, doc_id)), |ord| {
                    *lo <= ord && ord < *hi
                })
            }
            L::MapFacetOrdRange { target, lo, hi } => tri_of(
                target.and_then(|(ci, key)| cols.map_facet_value_ord(ci, key, doc_id)),
                |ord| *lo <= ord && ord < *hi,
            ),
        }
    }
}

impl MapKeyRef {
    /// Whether `doc_id` holds the key: total, never unknown.
    fn eval(&self, doc_id: u32, cols: &dyn NumericRead) -> Tri {
        let held = match *self {
            MapKeyRef::Integer {
                column,
                key_ord: Some(key),
            } => cols.map_int_value(column, key, doc_id).is_some(),
            MapKeyRef::UnsignedInteger {
                column,
                key_ord: Some(key),
            } => cols.map_uint_value(column, key, doc_id).is_some(),
            MapKeyRef::Facet {
                column,
                key_ord: Some(key),
            } => cols.map_facet_value_ord(column, key, doc_id).is_some(),
            MapKeyRef::Numeric {
                column,
                key_ord: Some(key),
            } => cols.map_value(column, key, doc_id).is_some(),
            // The key (or the whole column) was never ingested here: no
            // document has it. False, and total — "k" in an empty map is
            // false in CEL, not an error.
            _ => false,
        };
        Tri::from(held)
    }
}

/// A comparison's verdict: unknown on an absent value, else the test.
fn tri_of<T>(value: Option<T>, test: impl FnOnce(T) -> bool) -> Tri {
    value.map_or(Tri::Unknown, |v| Tri::from(test(v)))
}

/// Whether `v` lies within the optional edges.
fn within(v: f64, lo: &Option<Edge>, hi: &Option<Edge>) -> bool {
    lo.as_ref().is_none_or(|e| e.admits_from_below(v))
        && hi.as_ref().is_none_or(|e| e.admits_from_above(v))
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
    /// The `geo_filters` field, resolved.
    pub geo: crate::geo::GeoFilters,
    /// The `filter` tree, resolved; `None` when the request sent none.
    pub pred: Option<ResolvedFilter>,
    /// Ordered-window phrase constraints,
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
    pub alternatives: Vec<Vec<usize>>,
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
                std::iter::once(&gate.sequence)
                    .chain(&gate.alternatives)
                    .any(|sequence| {
                        crate::proximity::phrase_matches(
                            gate.index, gate.terms, sequence, gate.slop, doc_id,
                        )
                    })
            })
    }
}
