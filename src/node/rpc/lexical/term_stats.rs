//! Live term and field statistics for coordinator-side global BM25.

use crate::node::*;

/// The answer for a field this shard does not hold: zeros, and `known: false`.
fn unknown_field_stats(field: &crate::pb::FieldTerms) -> crate::pb::FieldStats {
    crate::pb::FieldStats {
        sentences: false,
        total_doc_length: 0,
        doc_frequencies: vec![0; field.terms.len()],
        known: false,
        positions: false,
    }
}

/// `(doc_count, total_doc_length, doc_frequencies, field_stats)` over the
/// shard's live, visible documents.
fn store_term_stats(
    req: &TermStatsRequest,
    guard: &ShardState,
    store: &Bm25Shard,
    visibility: Option<&crate::boolean_bits::Bits>,
) -> Result<(u64, u64, Vec<u32>, Vec<crate::pb::FieldStats>), Status> {
    let index = store.as_index();
    // Per-field shares: a shard without a named field
    // answers zeros — that IS its share of the globals.
    let field_stats = req
        .fields
        .iter()
        .map(|ft| match store.field_index(&ft.field) {
            Some(fi) => {
                let view = store.field_view(fi);
                let (total_doc_length, doc_frequencies) = live_field_stats(
                    view.as_ref(),
                    &ft.terms,
                    &guard.live_docs,
                    store.next_doc_id(),
                    visibility,
                );
                crate::pb::FieldStats {
                    sentences: store.field_has_sentences(fi),
                    total_doc_length,
                    doc_frequencies,
                    known: true,
                    positions: store.field_has_positions(fi),
                }
            }
            None => unknown_field_stats(ft),
        })
        .collect();
    let (total_doc_length, doc_frequencies) = live_field_stats(
        index,
        &req.terms,
        &guard.live_docs,
        store.next_doc_id(),
        visibility,
    );
    Ok((
        live_document_count(store, &guard.live_docs, visibility),
        total_doc_length,
        doc_frequencies,
        field_stats,
    ))
}

impl NodeServiceImpl {
    fn live_term_stats(
        &self,
        req: &TermStatsRequest,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::visibility::validate_stats_request(req)?;
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let visibility_filter = req
            .visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref());
        let guard = read_shard(&self.state);
        if req.version_only {
            let (_, visibility_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &[], visibility_filter);
            return Ok(Response::new(TermStatsResponse {
                version_only: true,
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known,
                ..Default::default()
            }));
        }
        let (_, visibility_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], visibility_filter);
        let visibility = if visibility_filter.is_some() {
            let slots = guard
                .bm25
                .as_ref()
                .map_or(0, |store| store.next_doc_id() as usize);
            let (_, allow, _) = resolve_shard_filters(
                guard.bm25.as_ref(),
                None,
                slots,
                ShardFilterInputs {
                    geo: &[],
                    regions: &[],
                    tree: visibility_filter,
                },
                self.knobs.segment_pruning(),
                None,
            )?;
            allow
        } else {
            None
        };
        let (doc_count, total_doc_length, doc_frequencies, field_stats) = match guard.bm25.as_ref()
        {
            Some(store) => store_term_stats(req, &guard, store, visibility.as_ref())?,
            None => (
                0,
                0,
                req.terms.iter().map(|_| 0).collect(),
                // No postings at all: this shard knows no field, which is
                // a different statement from "the field does not exist".
                req.fields.iter().map(unknown_field_stats).collect(),
            ),
        };
        Ok(Response::new(TermStatsResponse {
            version_only: false,
            doc_count,
            total_doc_length,
            doc_frequencies,
            field_stats,
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
        }))
    }

    pub(in crate::node::rpc) async fn handle_term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::metrics::timed(Route::TermStats, request, |request| async move {
            self.live_term_stats(&request.into_inner())
        })
        .await
    }
}
