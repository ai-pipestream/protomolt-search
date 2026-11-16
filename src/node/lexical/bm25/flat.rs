//! Flat single-field query execution: filters, phrases, and facets.

use super::{flat_index, FlatRun, FlatView};
use crate::node::*;
use crate::scorefn::NumericRead;

impl FlatRun<'_> {
    pub(super) fn execute(
        self,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        let guard = read_shard(&self.node.state);
        guard.check_stats_epoch(
            self.req.expected_stats_epoch,
            &self.req.expected_stats_incarnation,
        )?;
        guard.check_query_analysis("body", self.req.analysis_fingerprint)?;
        let filter = self.filter(&guard)?;
        let store = guard.bm25.as_ref();
        let prune = store.map_or_else(SegmentPrune::default, |store| {
            prune_segments(
                store,
                filter.as_ref().and_then(|f| f.pred.as_ref()),
                self.node.knobs.segment_pruning(),
            )
        });
        let masked = match (store, prune.mask.as_ref()) {
            (Some(Bm25Shard::Segmented(segmented)), Some(mask)) => {
                Some(segmented.masked(Arc::clone(mask)))
            }
            _ => None,
        };
        let masked = masked.as_ref().map(|index| index as &dyn Bm25Index);
        let mut response = self.facets(store, masked, filter.as_ref())?;
        self.known_columns(store, &mut response)?;
        let projections = self.projections(store, &mut response)?;
        response.hits = match store {
            Some(store) if self.req.k > 0 => self.hits(
                FlatView {
                    store,
                    index: flat_index(store, masked),
                    filter: filter.as_ref(),
                    projections: &projections,
                },
                live,
                candidate,
            )?,
            _ => Vec::new(),
        };
        response.kth_best = if response.hits.len() == self.req.k as usize {
            response
                .hits
                .last()
                .map(|hit| bm25::floor_seed(hit.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        response.segments_total = prune.stats.segments_total;
        response.segments_skipped = prune.stats.segments_skipped;
        Ok(response)
    }

    fn filter<'b>(
        &'b self,
        guard: &'b ShardState,
    ) -> Result<Option<crate::filter::DocFilter<'b>>, Status> {
        let req = self.req;
        let Some(store) = guard.bm25.as_ref().filter(|_| {
            !req.geo_filters.is_empty()
                || req.filter.is_some()
                || req.phrase.is_some()
                || guard.live_docs.has_deletes()
        }) else {
            return Ok(None);
        };
        let phrase = self.phrases(store)?;
        Ok(Some(crate::filter::DocFilter {
            deleted: guard.live_docs.words(),
            geo: store.resolve_geo_filters(&req.geo_filters, &self.regions),
            pred: req
                .filter
                .as_ref()
                .map(|filter| store.resolve_filter(filter))
                .transpose()?,
            phrase,
        }))
    }

    fn phrases<'b>(
        &'b self,
        store: &'b Bm25Shard,
    ) -> Result<Vec<crate::filter::PhraseGate<'b>>, Status> {
        let Some(constraint) = self.req.phrase.as_ref() else {
            return Ok(Vec::new());
        };
        let index = flat_index(store, None);
        if !index.has_positions() {
            return Err(Status::failed_precondition(
                "field \"body\" has no token positions on this shard; a phrase or \
                 slop query needs --position-fields=body and a rebuilt generation",
            ));
        }
        crate::proximity::validate_leg(constraint, self.req.terms.len())
            .map_err(Status::invalid_argument)?;
        Ok(vec![crate::filter::PhraseGate {
            index,
            terms: &self.req.terms,
            sequence: constraint.sequence.iter().map(|&i| i as usize).collect(),
            alternatives: constraint
                .alternatives
                .iter()
                .map(|path| path.terms.iter().map(|&i| i as usize).collect())
                .collect(),
            slop: constraint.slop,
        }])
    }

    fn facets(
        &self,
        store: Option<&Bm25Shard>,
        masked: Option<&dyn Bm25Index>,
        filter: Option<&crate::filter::DocFilter<'_>>,
    ) -> Result<Bm25QueryResponse, Status> {
        let req = self.req;
        let Some(store) = store.filter(|_| {
            !req.facet_fields.is_empty()
                || !req.map_facet_fields.is_empty()
                || !req.range_facet_fields.is_empty()
                || !req.stats_fields.is_empty()
                || !req.cardinality_fields.is_empty()
        }) else {
            return Ok(self.empty_facets());
        };
        let columns = ShardNumericRead(store);
        let (facets, range_facets, stats, distinct) =
            store.count_facets(crate::node::shard::FacetCountRequest {
                views: &[(flat_index(store, masked), &req.terms)],
                facet_fields: &req.facet_fields,
                map_facet_fields: &req.map_facet_fields,
                range_facet_fields: &req.range_facet_fields,
                stats_fields: &req.stats_fields,
                cardinality_fields: &req.cardinality_fields,
                filter: filter.map(|filter| (filter, &columns as &dyn NumericRead)),
            })?;
        Ok(Bm25QueryResponse {
            facets,
            range_facets,
            stats,
            distinct,
            ..Default::default()
        })
    }

    fn empty_facets(&self) -> Bm25QueryResponse {
        let req = self.req;
        Bm25QueryResponse {
            facets: req
                .facet_fields
                .iter()
                .map(|name| (name.clone(), None))
                .chain(
                    req.map_facet_fields
                        .iter()
                        .map(|map| (map.column.clone(), Some(map.key.clone()))),
                )
                .map(|(field, key)| crate::pb::FacetFieldCounts {
                    field,
                    map_key: key,
                    ..Default::default()
                })
                .collect(),
            range_facets: unknown_range_counts(&req.range_facet_fields),
            stats: req
                .stats_fields
                .iter()
                .map(|field| crate::pb::ColumnStats {
                    field: field.clone(),
                    ..Default::default()
                })
                .collect(),
            distinct: req
                .cardinality_fields
                .iter()
                .map(|field| crate::pb::FacetDistinct {
                    field: field.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}
