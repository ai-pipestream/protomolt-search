//! The configured Stage-2 reranker over a route's Stage-1 shortlist.

use std::collections::HashMap;

use tonic::Status;

use super::run::Base;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::{QueryHit, QuerySignal, RerankCandidatePayload};
use crate::turbo_bridge::rerank::{RerankCandidate, RerankSpec, RerankedHit};

/// What a route's shards already told the adapter about its hits: the
/// candidate payloads carried inline, and the shard that owns each doc.
#[derive(Default)]
pub(super) struct RerankSources {
    inline: HashMap<u64, RerankCandidatePayload>,
    owners: HashMap<u64, usize>,
}

impl RerankSources {
    /// Keep the first inline payload seen for a doc.
    pub(super) fn note_candidate(&mut self, doc_id: u64, payload: Option<&RerankCandidatePayload>) {
        if let Some(payload) = payload {
            self.inline.entry(doc_id).or_insert_with(|| payload.clone());
        }
    }

    /// Keep the first owning shard seen for a doc.
    pub(super) fn note_owner(&mut self, doc_id: u64, shard: usize) {
        self.owners.entry(doc_id).or_insert(shard);
    }
}

/// Run the coordinator's reranker, when one is configured, over the
/// pool's shortlist. The query text is the spec's override, else
/// `fallback_text`. Returns whether a reranker is configured, which is
/// what the `executed` echo reports.
pub(super) async fn turbo_rerank(
    base: &Base<'_>,
    fallback_text: &str,
    sources: &RerankSources,
    hits: &mut Vec<QueryHit>,
) -> Result<bool, Status> {
    let Some(spec) = base.coordinator.rerank_spec() else {
        return Ok(false);
    };
    let query_text = spec.query_text_override.as_deref().unwrap_or(fallback_text);
    apply_turbo_rerank(base, spec, query_text, sources, hits).await?;
    Ok(true)
}

/// Rerank and highlight the Stage-1 shortlist using inline candidate payloads when carried by shards.
async fn apply_turbo_rerank(
    base: &Base<'_>,
    spec: &RerankSpec,
    query_text: &str,
    sources: &RerankSources,
    hits: &mut Vec<QueryHit>,
) -> Result<(), Status> {
    if hits.is_empty() || spec.shortlist_size == 0 {
        return Ok(());
    }
    crate::turbo_bridge::remaining_deadline_duration(base.finish_deadline_us)?;
    let limit = hits.len().min(spec.shortlist_size);
    let shortlist_pairs: Vec<(u64, f64)> = hits[..limit]
        .iter()
        .map(|h| (h.doc_id, f64::from(h.score)))
        .collect();
    let fallback = fetch_missing(base, sources, &hits[..limit], &shortlist_pairs).await?;
    let candidates = shortlist_candidates(&shortlist_pairs, &sources.inline, &fallback);
    let reranked = crate::turbo_bridge::rerank::rerank_and_highlight_shortlist(
        spec,
        query_text,
        &candidates,
        base.finish_deadline_us,
    )
    .await?;
    reorder(hits, limit, reranked);
    Ok(())
}

/// Fetch the shortlist candidates no shard carried inline, routed to
/// the shard that owns each: the one the route reported, else the one
/// the hit's document key routes to.
async fn fetch_missing(
    base: &Base<'_>,
    sources: &RerankSources,
    shortlist: &[QueryHit],
    shortlist_pairs: &[(u64, f64)],
) -> Result<HashMap<u64, RerankCandidate>, Status> {
    let missing_pairs: Vec<(u64, f64)> = shortlist_pairs
        .iter()
        .copied()
        .filter(|(doc_id, _)| !sources.inline.contains_key(doc_id))
        .collect();
    if missing_pairs.is_empty() {
        return Ok(HashMap::new());
    }
    let resolved_owners = resolve_owners(base.coordinator, sources, shortlist);
    Ok(base
        .coordinator
        .fetch_rerank_candidates_routed(&missing_pairs, &resolved_owners, base.finish_deadline_us)
        .await?
        .into_iter()
        .map(|c| (c.doc_id, c))
        .collect())
}

fn resolve_owners(
    coordinator: &CoordinatorServiceImpl,
    sources: &RerankSources,
    shortlist: &[QueryHit],
) -> HashMap<u64, usize> {
    let mut resolved_owners = sources.owners.clone();
    for hit in shortlist {
        if sources.inline.contains_key(&hit.doc_id) || resolved_owners.contains_key(&hit.doc_id) {
            continue;
        }
        if let Some(identity) = hit.identity.as_ref() {
            if !identity.document_key.is_empty() {
                if let Ok((_, shard)) = coordinator.route_stable_key(&identity.document_key) {
                    resolved_owners.insert(hit.doc_id, shard);
                }
            }
        }
    }
    resolved_owners
}

/// One reranker input per shortlist hit, in shortlist order: the inline
/// payload, else the fetched candidate, else an empty text.
fn shortlist_candidates(
    shortlist_pairs: &[(u64, f64)],
    inline_candidates: &HashMap<u64, RerankCandidatePayload>,
    fallback: &HashMap<u64, RerankCandidate>,
) -> Vec<RerankCandidate> {
    let mut candidates = Vec::with_capacity(shortlist_pairs.len());
    for &(doc_id, prior_score) in shortlist_pairs {
        if let Some(inline) = inline_candidates.get(&doc_id) {
            candidates.push(RerankCandidate {
                doc_id,
                prior_score,
                text: inline.text.clone(),
                sentence_spans: inline
                    .sentence_spans
                    .iter()
                    .map(|s| (s.start, s.end))
                    .collect(),
                lineage_span_start: inline.lineage.as_ref().map_or(0, |l| l.span_start),
            });
        } else if let Some(fetched) = fallback.get(&doc_id) {
            candidates.push(fetched.clone());
        } else {
            candidates.push(RerankCandidate {
                doc_id,
                prior_score,
                text: String::new(),
                sentence_spans: Vec::new(),
                lineage_span_start: 0,
            });
        }
    }
    candidates
}

/// Put the reranked shortlist first, in the reranker's order with its
/// score, snippets and signal; then any shortlist hit it did not
/// return; then the hits past the shortlist, untouched.
fn reorder(hits: &mut Vec<QueryHit>, limit: usize, reranked: Vec<RerankedHit>) {
    let mut reordered = Vec::with_capacity(hits.len());
    let mut old_shortlist: Vec<Option<QueryHit>> = hits.drain(..limit).map(Some).collect();
    for item in reranked {
        if let Some(mut hit) = old_shortlist
            .get_mut(item.input_index)
            .and_then(Option::take)
        {
            hit.score = item.calibrated_probability as f32;
            hit.snippets = item.snippets;
            hit.signals.push(QuerySignal {
                id: "turbo_rerank".to_string(),
                score: hit.score,
            });
            reordered.push(hit);
        }
    }
    for leftover in old_shortlist.into_iter().flatten() {
        reordered.push(leftover);
    }
    reordered.append(hits);
    *hits = reordered;
}
