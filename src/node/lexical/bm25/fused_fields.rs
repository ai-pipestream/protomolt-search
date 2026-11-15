//! Per-leg field views, masks, phrase gates, and hit rendering for fused queries.

use super::fused::fused_has_facets;
use crate::node::*;

pub(super) struct FusedFields<'a> {
    req: &'a Bm25QueryRequest,
    views: Vec<Box<dyn Bm25Index + 'a>>,
    legs: Vec<usize>,
}

pub(super) type FusedPhrase = (usize, Vec<f64>);

impl<'a> FusedFields<'a> {
    pub(super) fn new(req: &'a Bm25QueryRequest, store: &'a Bm25Shard) -> Result<Self, Status> {
        if store.as_index().is_none() {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            ));
        }
        let mut fields = Self {
            req,
            views: Vec::new(),
            legs: Vec::new(),
        };
        for (li, leg) in req.fields.iter().enumerate() {
            if let Some(fi) = store.field_index(&leg.field) {
                fields
                    .views
                    .push(store.field_view(fi).expect("searchable, checked above"));
                fields.legs.push(li);
            }
        }
        Ok(fields)
    }

    pub(super) fn apply_mask(&mut self, store: &'a Bm25Shard, mask: Option<&Arc<[bool]>>) {
        if let Some(mask) = mask {
            self.views = self
                .legs
                .iter()
                .map(|&li| {
                    let fi = store
                        .field_index(&self.req.fields[li].field)
                        .expect("the field resolved above");
                    store
                        .field_view_masked(fi, mask)
                        .expect("searchable, checked above")
                })
                .collect();
        }
    }

    pub(super) fn phrase_gates(&self) -> Result<Vec<crate::filter::PhraseGate<'_>>, Status> {
        let mut gates = Vec::new();
        for (li, leg) in self.req.fields.iter().enumerate() {
            let Some(phrase) = leg.phrase.as_ref() else {
                continue;
            };
            let Some(vi) = self.legs.iter().position(|&candidate| candidate == li) else {
                return Err(Status::failed_precondition(format!(
                    "phrase field {:?} is absent from this shard; a phrase cannot be \
                     served on part of the fleet",
                    leg.field
                )));
            };
            if !self.views[vi].has_positions() {
                return Err(Status::failed_precondition(format!(
                    "field {:?} has no token positions on this shard; a phrase or slop \
                     query needs --position-fields={} and a rebuilt generation",
                    leg.field, leg.field
                )));
            }
            crate::proximity::validate_leg(phrase, leg.terms.len())
                .map_err(Status::invalid_argument)?;
            gates.push(crate::filter::PhraseGate {
                index: self.views[vi].as_ref(),
                terms: &leg.terms,
                sequence: phrase.sequence.iter().map(|&i| i as usize).collect(),
                alternatives: phrase
                    .alternatives
                    .iter()
                    .map(|p| p.terms.iter().map(|&i| i as usize).collect())
                    .collect(),
                slop: phrase.slop,
            });
        }
        Ok(gates)
    }

    pub(super) fn count_facets(
        &self,
        store: &Bm25Shard,
        filter: bm25::FilterCtx,
        response: &mut Bm25QueryResponse,
    ) -> Result<(), Status> {
        if fused_has_facets(self.req) {
            let pairs: Vec<(&dyn Bm25Index, &[String])> = self
                .views
                .iter()
                .zip(&self.legs)
                .map(|(view, &li)| (view.as_ref(), self.req.fields[li].terms.as_slice()))
                .collect();
            (response.facets, response.range_facets, _, _) = store.count_facets(
                &pairs,
                &self.req.facet_fields,
                &self.req.map_facet_fields,
                &self.req.range_facet_fields,
                &[],
                &[],
                filter,
            )?;
        }
        Ok(())
    }

    pub(super) fn queries(&self) -> Result<Vec<bm25::FieldQuery<'_>>, Status> {
        self.views
            .iter()
            .zip(&self.legs)
            .map(|(view, &li)| {
                let leg = &self.req.fields[li];
                Ok(bm25::FieldQuery {
                    index: view.as_ref(),
                    terms: &leg.terms,
                    stats: bm25::CorpusStats {
                        doc_count: self.req.global_doc_count,
                        total_doc_length: leg.global_total_doc_length,
                        dfs: leg.global_doc_frequencies.clone(),
                    },
                    params: params_from(leg.k1, leg.b)?,
                    weight: if leg.weight == 0.0 {
                        1.0
                    } else {
                        f64::from(leg.weight)
                    },
                })
            })
            .collect()
    }

    pub(super) fn phrase_view(
        &self,
        phrase: Option<(usize, &[f32])>,
    ) -> Result<Option<FusedPhrase>, Status> {
        let Some((leg, weights)) = phrase else {
            return Ok(None);
        };
        let view = self
            .legs
            .iter()
            .position(|&candidate| candidate == leg)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "phrase field {:?} is absent from this shard; rebuild the complete generation",
                    self.req.fields[leg].field
                ))
            })?;
        let weights = weights.iter().copied().map(f64::from).collect();
        Ok(Some((view, weights)))
    }

    pub(super) fn render_hits(
        &self,
        store: &Bm25Shard,
        queries: &[bm25::FieldQuery],
        docs: Vec<bm25::FusedDoc>,
        slot_offset: u64,
        phrase: Option<&FusedPhrase>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let highlight = highlight_plan(store, self.req.highlight.as_ref())?;
        let body = store.field_name(0);
        let text_index = store.as_index().ok_or_else(|| {
            Status::failed_precondition("bm25 bulk build in progress; Flush first")
        })?;
        let presence = self.req.explain.then(|| {
            let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
            bm25::breakdown(queries, &ids)
        });
        let leg_names: Vec<&str> = self
            .legs
            .iter()
            .map(|&li| self.req.fields[li].field.as_str())
            .collect();
        docs.into_iter()
            .enumerate()
            .map(|(hit_index, doc)| {
                let explain = presence.as_ref().map(|presence| {
                    explain_terms(
                        queries,
                        &leg_names,
                        &presence[hit_index],
                        phrase.map(|(view, weights)| (*view, weights.as_slice())),
                    )
                });
                let snippets = match highlight.as_ref() {
                    Some(plan) => cut_snippets(
                        plan,
                        text_index,
                        body,
                        doc.doc_id,
                        &self.body_occurrences(&doc, body),
                    )?,
                    None => Vec::new(),
                };
                Ok(Bm25Hit {
                    identity: store.document_identity(doc.doc_id),
                    explain,
                    snippets,
                    projected: Vec::new(),
                    doc_id: slot_offset + u64::from(doc.doc_id),
                    score: doc.score as f32,
                    terms: doc
                        .term_offsets
                        .into_iter()
                        .map(|(fi, ti, offsets)| {
                            let leg = &self.req.fields[self.legs[fi]];
                            TermOccurrences {
                                term: leg.terms[ti].clone(),
                                field: leg.field.clone(),
                                offsets: offsets
                                    .into_iter()
                                    .map(|(start, end)| OffsetSpan { start, end })
                                    .collect(),
                            }
                        })
                        .collect(),
                })
            })
            .collect()
    }

    pub(super) fn body_occurrences(
        &self,
        doc: &bm25::FusedDoc,
        body: &str,
    ) -> Vec<(usize, (u32, u32))> {
        doc.term_offsets
            .iter()
            .filter(|(fi, _, _)| self.req.fields[self.legs[*fi]].field == body)
            .flat_map(|(fi, ti, offsets)| {
                let key = (self.legs[*fi] << 20) | *ti;
                offsets.iter().map(move |&span| (key, span))
            })
            .collect()
    }
}
