//! Flat query hits: known columns, projections, scoring, rendering.

use super::{explain_stages, min_score_floor, FlatRun, FlatView, BULK_BUILD};
use crate::node::*;
use crate::scorefn::NumericRead;

impl FlatRun<'_> {
    pub(super) fn known_columns(
        &self,
        store: Option<&Bm25Shard>,
        response: &mut Bm25QueryResponse,
    ) -> Result<(), Status> {
        response.stage_columns_known = match store {
            Some(Bm25Shard::Spilling(_)) if !self.stages.is_empty() => {
                return Err(Status::failed_precondition(BULK_BUILD));
            }
            Some(store) => store
                .resolve_chain(&self.stages)
                .stages
                .iter()
                .map(|stage| stage.column.is_some())
                .collect(),
            None => vec![false; self.stages.len()],
        };
        response.geo_columns_known = store.map_or_else(
            || vec![false; self.req.geo_filters.len()],
            |store| store.geo_columns_known(&self.req.geo_filters),
        );
        response.filter_columns_known = match (store, self.req.filter.as_ref()) {
            (Some(store), Some(filter)) => store.filter_columns_known(filter),
            (None, Some(filter)) => vec![false; crate::filter::leaf_count(filter)],
            (_, None) => Vec::new(),
        };
        let mut leaves = Vec::new();
        for projection in &self.req.projections {
            if let Some(expression) = projection.expr.as_ref() {
                crate::values::column_leaves(expression, &mut leaves);
            }
        }
        response.projection_leaves_known = store.map_or_else(
            || vec![false; leaves.len()],
            |store| {
                leaves
                    .iter()
                    .map(|leaf| crate::values::leaf_known(leaf, store))
                    .collect()
            },
        );
        Ok(())
    }

    pub(super) fn projections(
        &self,
        store: Option<&Bm25Shard>,
        response: &mut Bm25QueryResponse,
    ) -> Result<Vec<crate::values::ResolvedValue>, Status> {
        let empty = crate::values::NumericTypes {
            numerics: &[],
            integers: &[],
            unsigned_integers: &[],
            map_numerics: &[],
            map_integers: &[],
            map_unsigned_integers: &[],
            facets: &[],
        };
        let columns: &dyn crate::values::ColumnLookup =
            store.map_or(&empty as &dyn crate::values::ColumnLookup, |store| store);
        let (projections, types) = self
            .req
            .projections
            .iter()
            .map(|projection| {
                let expression = projection.expr.as_ref().ok_or_else(|| {
                    Status::invalid_argument("projection: empty compiled expression")
                })?;
                crate::values::resolve(expression, columns)
                    .map(|(value, ty)| (value, crate::pb::ScalarValueType::from(ty) as i32))
            })
            .collect::<Result<Vec<_>, Status>>()?
            .into_iter()
            .unzip();
        response.projection_types = types;
        Ok(projections)
    }

    pub(super) fn hits(
        &self,
        view: FlatView<'_>,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let req = self.req;
        let chain = view.store.resolve_chain(&self.stages);
        let columns = ShardNumericRead(view.store);
        let filter = view.filter.map(|f| (f, &columns as &dyn NumericRead));
        let request = bm25::TopK::new(req.k as usize)
            .floor(min_score_floor(req.min_score))
            .chain((!self.stages.is_empty()).then_some((&chain, &columns)))
            .filter(filter)
            .live(live)
            .candidate(candidate)
            .block_max(self.node.config.block_max);
        let docs = bm25::top_k(view.index, &req.terms, &self.stats, self.params, request);
        FlatHits {
            req,
            slot_offset: self.node.config.slot_offset,
            store: view.store,
            chain,
            highlight: highlight_plan(view.store, req.highlight.as_ref())?,
            projections: view.projections,
            query: [bm25::FieldQuery {
                index: view.index,
                terms: &req.terms,
                stats: self.stats.clone(),
                params: self.params,
                weight: 1.0,
            }],
        }
        .collect(docs)
    }
}

struct FlatHits<'a> {
    req: &'a Bm25QueryRequest,
    slot_offset: u64,
    store: &'a Bm25Shard,
    chain: crate::scorefn::ScoreChain,
    highlight: Option<crate::highlight::Plan>,
    projections: &'a [crate::values::ResolvedValue],
    query: [bm25::FieldQuery<'a>; 1],
}

impl FlatHits<'_> {
    fn collect(self, docs: Vec<bm25::ScoredDoc>) -> Result<Vec<Bm25Hit>, Status> {
        let presence = self.req.explain.then(|| {
            let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
            bm25::breakdown(&self.query, &ids)
        });
        docs.into_iter()
            .enumerate()
            .map(|(index, doc)| {
                self.hit(
                    doc,
                    presence.as_ref().map(|presence| presence[index].as_slice()),
                )
            })
            .collect()
    }

    fn hit(
        &self,
        doc: bm25::ScoredDoc,
        presence: Option<&[bm25::TermPresence]>,
    ) -> Result<Bm25Hit, Status> {
        let body = self.store.field_name(0);
        let columns = ShardNumericRead(self.store);
        let explain = presence.map(|presence| {
            let mut explain = explain_terms(&self.query, &[body], presence, None);
            explain_stages(
                &mut explain,
                &self.chain,
                &self.req.score_stages,
                doc.doc_id,
                &columns,
            );
            explain
        });
        let snippets = match self.highlight.as_ref() {
            Some(plan) => {
                let occurrences: Vec<_> = doc
                    .term_offsets
                    .iter()
                    .flat_map(|(term, offsets)| offsets.iter().map(move |&span| (*term, span)))
                    .collect();
                cut_snippets(plan, self.query[0].index, body, doc.doc_id, &occurrences)?
            }
            None => Vec::new(),
        };
        Ok(Bm25Hit {
            identity: self.store.document_identity(doc.doc_id),
            explain,
            snippets,
            projected: self
                .projections
                .iter()
                .map(|value| {
                    projected_value(crate::values::eval(value, doc.doc_id, &columns), self.store)
                })
                .collect(),
            doc_id: self.slot_offset + u64::from(doc.doc_id),
            score: doc.score as f32,
            terms: doc
                .term_offsets
                .into_iter()
                .map(|(term, offsets)| TermOccurrences {
                    term: self.req.terms[term].clone(),
                    field: String::new(),
                    offsets: offsets
                        .into_iter()
                        .map(|(start, end)| OffsetSpan { start, end })
                        .collect(),
                })
                .collect(),
        })
    }
}
