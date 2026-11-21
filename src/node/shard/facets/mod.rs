//! Facet counting, range-facet bucketing, and geographic filter validation.

use crate::node::*;

mod validate;

pub(crate) use validate::{
    timestamp_to_epoch_micros, validate_geo_filter, validate_geo_filters,
    validate_range_facet_fields,
};
pub(in crate::node) use validate::{unknown_range_counts, validate_lat_lon};

/// What one facet pass counts over the match set of its queried views.
pub(in crate::node) struct FacetCountRequest<'a> {
    /// Each searched field's read surface with the terms queried on it.
    pub(in crate::node) views: &'a [(&'a dyn Bm25Index, &'a [String])],
    pub(in crate::node) facet_fields: &'a [String],
    pub(in crate::node) map_facet_fields: &'a [crate::pb::MapFacetField],
    pub(in crate::node) range_facet_fields: &'a [crate::pb::RangeFacetField],
    pub(in crate::node) stats_fields: &'a [String],
    pub(in crate::node) cardinality_fields: &'a [String],
    pub(in crate::node) filter: crate::bm25::FilterCtx<'a>,
}

/// Plain and map-keyed facet counts, range facets, column stats and distinct
/// values, each parallel to its request list.
pub(in crate::node) type FacetPassCounts = (
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
    Vec<crate::pb::ColumnStats>,
    Vec<crate::pb::FacetDistinct>,
);

/// The match set of one facet pass: one bit per shard slot.
struct MatchSet(Vec<u64>);

impl MatchSet {
    /// The matched documents, ascending.
    fn docs(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().enumerate().flat_map(|(wi, &word)| {
            let mut w = word;
            std::iter::from_fn(move || {
                (w != 0).then(|| {
                    let doc = (wi * 64) as u32 + w.trailing_zeros();
                    w &= w - 1;
                    doc
                })
            })
        })
    }

    /// Count matched documents per ordinal slot; map facets binary-search
    /// each document's sorted `(key_ord, val_ord)` pairs.
    fn count_by(&self, resolve: &dyn Fn(u32) -> Option<u32>, n_values: usize) -> Vec<u64> {
        let mut counts = vec![0u64; n_values];
        for doc in self.docs() {
            if let Some(ord) = resolve(doc) {
                counts[ord as usize] += 1;
            }
        }
        counts
    }
}

/// Dictionary (ordinal) order — deterministic per shard; the coordinator
/// sorts the merged counts.
fn to_wire(counts: Vec<u64>, value_of: &dyn Fn(u32) -> String) -> Vec<crate::pb::FacetCount> {
    counts
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(ord, &c)| crate::pb::FacetCount {
            value: value_of(ord as u32),
            count: c,
        })
        .collect()
}

impl Bm25Shard {
    /// Count plain, map-keyed, and range facets plus column stats and distinct
    /// values over one shared match-set bitmap (every document matching at least
    /// one queried term and passing `filter`, independent of `k` and `min_score`).
    /// Unknown columns return `known: false` so the coordinator refuses only
    /// columns unknown across all shards.
    pub(in crate::node) fn count_facets(
        &self,
        req: FacetCountRequest<'_>,
    ) -> Result<FacetPassCounts, Status> {
        let set = self.match_set(req.views, req.filter);
        let mut out: Vec<crate::pb::FacetFieldCounts> = req
            .facet_fields
            .iter()
            .map(|name| self.plain_facet_counts(name, &set))
            .collect();
        for field in req.map_facet_fields {
            out.push(self.map_facet_counts(field, &set));
        }
        let ranges = req
            .range_facet_fields
            .iter()
            .map(|field| self.range_facet_counts(field, &set))
            .collect();
        let stats = req
            .stats_fields
            .iter()
            .map(|name| self.column_stats(name, &set))
            .collect::<Result<Vec<_>, Status>>()?;
        let distinct = req
            .cardinality_fields
            .iter()
            .map(|name| self.facet_distinct(name, &set))
            .collect();
        Ok((out, ranges, stats, distinct))
    }

    fn match_set(
        &self,
        views: &[(&dyn Bm25Index, &[String])],
        filter: crate::bm25::FilterCtx,
    ) -> MatchSet {
        let n_slots = self.next_doc_id() as usize;
        let mut bits = vec![0u64; n_slots.div_ceil(64)];
        for &(view, terms) in views {
            for term in terms {
                view.for_each_doc_tf(term, &mut |doc_id, _tf| {
                    bits[doc_id as usize / 64] |= 1u64 << (doc_id % 64);
                });
            }
        }
        // One filter evaluation per matched document, exactly like the
        // ordinal resolution of each count — never per posting.
        if let Some((doc_filter, cols)) = filter {
            for (wi, bits_word) in bits.iter_mut().enumerate() {
                let mut w = *bits_word;
                while w != 0 {
                    let doc = (wi * 64) as u32 + w.trailing_zeros();
                    if !doc_filter.passes(doc, cols) {
                        *bits_word &= !(1u64 << (doc % 64));
                    }
                    w &= w - 1;
                }
            }
        }
        MatchSet(bits)
    }

    fn plain_facet_counts(&self, name: &str, set: &MatchSet) -> crate::pb::FacetFieldCounts {
        let Some(fi) = self.facet_index(name) else {
            return crate::pb::FacetFieldCounts {
                field: name.to_owned(),
                known: false,
                counts: Vec::new(),
                map_key: None,
            };
        };
        let counts = set.count_by(&|doc| self.facet_ord(fi, doc), self.facet_value_count(fi));
        crate::pb::FacetFieldCounts {
            field: name.to_owned(),
            known: true,
            counts: to_wire(counts, &|ord| self.facet_value(fi, ord).to_string()),
            map_key: None,
        }
    }

    fn map_facet_counts(
        &self,
        req: &crate::pb::MapFacetField,
        set: &MatchSet,
    ) -> crate::pb::FacetFieldCounts {
        // Known = the column exists AND its key dictionary has the
        // key: a key no shard ever ingested would otherwise make a
        // typo'd drill-down read as zero results everywhere.
        let resolved = self
            .map_facet_index(&req.column)
            .and_then(|ci| self.map_facet_key_ord(ci, &req.key).map(|k| (ci, k)));
        let Some((ci, key_ord)) = resolved else {
            return crate::pb::FacetFieldCounts {
                field: req.column.clone(),
                known: false,
                counts: Vec::new(),
                map_key: Some(req.key.clone()),
            };
        };
        let counts = set.count_by(
            &|doc| self.map_facet_value_ord(ci, key_ord, doc),
            self.map_facet_value_count(ci),
        );
        crate::pb::FacetFieldCounts {
            field: req.column.clone(),
            known: true,
            counts: to_wire(counts, &|ord| self.map_facet_value(ci, ord).to_string()),
            map_key: Some(req.key.clone()),
        }
    }

    /// Range facets: one value read per matched document, then a
    /// binary search of the (validated) edge list. Buckets are
    /// half-open `[edges[i], edges[i+1])`, so a value sitting exactly
    /// on an interior edge lands in the upper bucket; a value below
    /// the first edge or at/above the last is counted in NO bucket.
    /// There are deliberately no implicit tail buckets — the caller
    /// asked for these intervals and gets these intervals.
    fn range_facet_counts(
        &self,
        req: &crate::pb::RangeFacetField,
        set: &MatchSet,
    ) -> crate::pb::RangeFacetCounts {
        let intervals =
            crate::rangefacet::Intervals::new(req).expect("range facets validated before counting");
        let source = self.resolve_range_column(&req.column, req.map_key.as_deref());
        let mut counts = vec![0u64; intervals.len()];
        if let Some(source) = source {
            for doc in set.docs() {
                if let Some(bucket) = self
                    .range_value(source, doc)
                    .and_then(|value| intervals.bucket(value))
                {
                    counts[bucket] += 1;
                }
            }
        }
        intervals.response(counts, source.is_some())
    }

    /// Column stats: one value read per matched document per field,
    /// over the same bitmap. Absence contributes nothing — `count`
    /// is the number of documents that HELD a value, which is what
    /// makes mean = sum / count honest.
    fn column_stats(&self, name: &str, set: &MatchSet) -> Result<crate::pb::ColumnStats, Status> {
        use crate::pb::ScalarValueType as Type;
        let source = self.resolve_range_column(name, None);
        let ty = match source {
            Some(RangeSource::Numeric(_)) => Type::Number,
            Some(RangeSource::Integer(_)) => Type::Integer,
            Some(RangeSource::Unsigned(_)) => Type::UnsignedInteger,
            _ => Type::Unspecified,
        };
        let mut out = crate::column_stats::Collector::new(name, ty);
        if let Some(source) = source {
            for doc in set.docs() {
                if let Some(value) = self.range_value(source, doc) {
                    out.observe(value)?;
                }
            }
        }
        out.finish()
    }

    /// Distinct facet values in the match set: an ordinal bitset
    /// over this shard's dictionary, then the VALUES — ordinals are
    /// shard-local, so strings are the only union-able currency the
    /// coordinator can merge.
    fn facet_distinct(&self, name: &str, set: &MatchSet) -> crate::pb::FacetDistinct {
        let Some(fi) = self.facet_index(name) else {
            return crate::pb::FacetDistinct {
                field: name.to_owned(),
                known: false,
                values: Vec::new(),
            };
        };
        let n_values = self.facet_value_count(fi);
        let mut present = vec![0u64; n_values.div_ceil(64)];
        for doc in set.docs() {
            if let Some(ord) = self.facet_ord(fi, doc) {
                present[ord as usize / 64] |= 1u64 << (ord % 64);
            }
        }
        let values = (0..n_values as u32)
            .filter(|&ord| present[ord as usize / 64] >> (ord % 64) & 1 == 1)
            .map(|ord| self.facet_value(fi, ord).to_string())
            .collect();
        crate::pb::FacetDistinct {
            field: name.to_owned(),
            known: true,
            values,
        }
    }

    /// Resolve a range facet's column against THIS shard's tables:
    /// with no key the f64, i64 and u64 tables (one name space
    /// across kinds, so at most one answers), with a key the f64, i64
    /// or u64 map column and its key ordinal. `None` = this shard
    /// cannot resolve it, which answers `known: false`.
    pub(in crate::node) fn resolve_range_column(
        &self,
        column: &str,
        key: Option<&str>,
    ) -> Option<RangeSource> {
        if key.is_none() {
            if let Some(ni) = self.numeric_index(column) {
                return Some(RangeSource::Numeric(ni));
            }
            return self
                .integer_index(column)
                .map(RangeSource::Integer)
                .or_else(|| {
                    self.unsigned_integer_index(column)
                        .map(RangeSource::Unsigned)
                });
        }
        let key = key?;
        if let Some(ci) = self.map_integer_index(column) {
            return self
                .map_integer_key_ord(ci, key)
                .map(|key_ord| RangeSource::MapInteger {
                    column: ci,
                    key_ord,
                });
        }
        if let Some(ci) = self.map_unsigned_integer_index(column) {
            return self.map_unsigned_integer_key_ord(ci, key).map(|key_ord| {
                RangeSource::MapUnsigned {
                    column: ci,
                    key_ord,
                }
            });
        }
        let ci = self.map_numeric_index(column)?;
        let key_ord = self.map_numeric_key_ord(ci, key)?;
        Some(RangeSource::MapKey {
            column: ci,
            key_ord,
        })
    }

    /// Keep each value in its numeric domain for exact interval comparison.
    pub(in crate::node) fn range_value(
        &self,
        source: RangeSource,
        doc_id: u32,
    ) -> Option<crate::filter::NumBound> {
        use crate::filter::NumBound;
        match source {
            RangeSource::Numeric(ni) => self.numeric_value(ni, doc_id).map(NumBound::F),
            RangeSource::Integer(ii) => self.integer_value(ii, doc_id).map(NumBound::I),
            RangeSource::Unsigned(ui) => self.unsigned_integer_value(ui, doc_id).map(NumBound::U),
            RangeSource::MapKey { column, key_ord } => self
                .map_numeric_value(column, key_ord, doc_id)
                .map(NumBound::F),
            RangeSource::MapInteger { column, key_ord } => self
                .map_integer_value(column, key_ord, doc_id)
                .map(NumBound::I),
            RangeSource::MapUnsigned { column, key_ord } => self
                .map_unsigned_integer_value(column, key_ord, doc_id)
                .map(NumBound::U),
        }
    }
}

/// A range facet's resolved column on THIS shard. The numeric shapes a
/// bucketable value can live in; see [`Bm25Shard::resolve_range_column`].
#[derive(Debug, Clone, Copy)]
pub(in crate::node) enum RangeSource {
    /// Index into the shard's f64 table.
    Numeric(usize),
    /// Index into the shard's i64 table.
    Integer(usize),
    /// Index into the shard's u64 table.
    Unsigned(usize),
    /// A map-numeric column and a key ordinal in ITS key dictionary.
    MapKey {
        /// Index into the shard's map-numeric table.
        column: usize,
        /// Key ordinal within that column's key dictionary.
        key_ord: u32,
    },
    /// Exact signed map entry and its dictionary-local key.
    MapInteger { column: usize, key_ord: u32 },
    /// Exact unsigned map entry and its dictionary-local key.
    MapUnsigned { column: usize, key_ord: u32 },
}
