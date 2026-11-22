//! The configured Stage-2 reranker over a route's Stage-1 shortlist.

use std::collections::{HashMap, HashSet};

use tonic::Status;

use super::run::Base;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::{QueryHit, QuerySignal, RerankCandidatePayload};
use crate::turbo_bridge::rerank::{RerankCandidate, RerankSpec, RerankedHit};

/// Inline candidate payloads and shard ownership for the Stage-1 hits
/// that fall within the configured reranker's shortlist window.
///
/// When no reranker is configured (or the shortlist is empty), `shortlist`
/// is `None` and every `note_*` call is a no-op that neither allocates
/// map storage nor clones [`RerankCandidatePayload`] values.
#[derive(Default)]
pub(super) struct RerankSources {
    shortlist: Option<HashSet<u64>>,
    inline: HashMap<u64, RerankCandidatePayload>,
    owners: HashMap<u64, usize>,
}

impl RerankSources {
    /// Disabled collector for routes without inline shard metadata or
    /// requests where no reranker is configured.
    pub(super) fn disabled() -> Self {
        Self::default()
    }

    /// Pre-sized collector scoped to the winning `spec.shortlist_size`
    /// prefix of `hits`, or disabled when `spec` is `None` or empty.
    pub(super) fn for_shortlist(spec: Option<&RerankSpec>, hits: &[QueryHit]) -> Self {
        let limit = spec.map_or(0, |s| hits.len().min(s.shortlist_size));
        if limit == 0 {
            return Self::disabled();
        }
        let mut shortlist = HashSet::with_capacity(limit);
        for hit in &hits[..limit] {
            shortlist.insert(hit.doc_id);
        }
        let cap = shortlist.len();
        Self {
            shortlist: Some(shortlist),
            inline: HashMap::with_capacity(cap),
            owners: HashMap::with_capacity(cap),
        }
    }

    /// Whether this collector accepts inline metadata for any document.
    pub(super) fn is_enabled(&self) -> bool {
        self.shortlist.is_some()
    }

    /// Whether every shortlisted document already has an inline payload.
    pub(super) fn has_all_inline(&self) -> bool {
        self.shortlist
            .as_ref()
            .is_some_and(|ids| self.inline.len() >= ids.len())
    }

    fn accepts(&self, doc_id: u64) -> bool {
        self.shortlist
            .as_ref()
            .is_some_and(|ids| ids.contains(&doc_id))
    }

    /// Keep the first non-empty inline payload seen for a shortlisted doc.
    pub(super) fn note_candidate(&mut self, doc_id: u64, payload: Option<&RerankCandidatePayload>) {
        if !self.accepts(doc_id) {
            return;
        }
        if let Some(payload) = payload.filter(|p| !p.text.is_empty()) {
            self.inline.entry(doc_id).or_insert_with(|| payload.clone());
        }
    }

    /// Keep the first owning shard seen for a shortlisted doc.
    pub(super) fn note_owner(&mut self, doc_id: u64, shard: usize) {
        if !self.accepts(doc_id) {
            return;
        }
        self.owners.entry(doc_id).or_insert(shard);
    }
}

/// Run the coordinator's reranker, when one is configured and `query_text`
/// is non-empty, over the pool's shortlist. Returns whether Stage-2 reranking
/// executed, which is what the `executed` echo reports.
pub(super) async fn turbo_rerank(
    base: &Base<'_>,
    query_text: &str,
    sources: &RerankSources,
    hits: &mut Vec<QueryHit>,
) -> Result<bool, Status> {
    let Some(spec) = base.coordinator.rerank_spec() else {
        return Ok(false);
    };
    let query_text = query_text.trim();
    if query_text.is_empty() {
        return Ok(false);
    }
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
    let candidates = shortlist_candidates(&shortlist_pairs, &sources.inline, &fallback)?;
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
    let fetched = base
        .coordinator
        .fetch_rerank_candidates_routed(&missing_pairs, &resolved_owners, base.finish_deadline_us)
        .await?;
    let mut by_doc = HashMap::with_capacity(fetched.len());
    for candidate in fetched {
        by_doc.insert(candidate.doc_id, candidate);
    }
    Ok(by_doc)
}

fn resolve_owners(
    coordinator: &CoordinatorServiceImpl,
    sources: &RerankSources,
    shortlist: &[QueryHit],
) -> HashMap<u64, usize> {
    let mut resolved_owners = HashMap::with_capacity(shortlist.len());
    resolved_owners.extend(sources.owners.iter().map(|(&doc, &shard)| (doc, shard)));
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
/// payload, else the fetched candidate; refuses when any shortlisted hit
/// lacks stored document text.
fn shortlist_candidates(
    shortlist_pairs: &[(u64, f64)],
    inline_candidates: &HashMap<u64, RerankCandidatePayload>,
    fallback: &HashMap<u64, RerankCandidate>,
) -> Result<Vec<RerankCandidate>, Status> {
    let mut candidates = Vec::with_capacity(shortlist_pairs.len());
    for &(doc_id, prior_score) in shortlist_pairs {
        if let Some(inline) = inline_candidates
            .get(&doc_id)
            .filter(|p| !p.text.is_empty())
        {
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
        } else if let Some(fetched) = fallback.get(&doc_id).filter(|c| !c.text.is_empty()) {
            candidates.push(fetched.clone());
        } else {
            return Err(Status::failed_precondition(format!(
                "rerank candidate {doc_id} has no stored document text"
            )));
        }
    }
    Ok(candidates)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turbo_bridge::rerank::RerankBackend;

    fn sample_hits(count: u64) -> Vec<QueryHit> {
        (0..count)
            .map(|doc_id| QueryHit {
                doc_id,
                score: (count - doc_id) as f32,
                ..Default::default()
            })
            .collect()
    }

    fn payload(doc_id: u64) -> RerankCandidatePayload {
        RerankCandidatePayload {
            text: format!("passage-{doc_id}"),
            ..Default::default()
        }
    }

    #[test]
    fn disabled_sources_skip_map_allocation_and_payload_clones() {
        let hits = sample_hits(64);
        let mut sources = RerankSources::for_shortlist(None, &hits);
        assert!(!sources.is_enabled());
        assert_eq!(sources.inline.capacity(), 0);
        assert_eq!(sources.owners.capacity(), 0);
        for doc_id in 0..64 {
            let p = payload(doc_id);
            sources.note_owner(doc_id, 1);
            sources.note_candidate(doc_id, Some(&p));
        }
        assert!(sources.inline.is_empty());
        assert!(sources.owners.is_empty());
        assert_eq!(sources.inline.capacity(), 0);
        assert_eq!(sources.owners.capacity(), 0);
    }

    #[test]
    fn shortlist_sources_retain_only_shortlist_ids_with_presized_maps() {
        let mut spec = RerankSpec::new(RerankBackend::DeterministicCalibrated {
            platt_slope: 1.5,
            platt_intercept: 0.0,
        });
        spec.shortlist_size = 4;
        let hits = sample_hits(32);
        let mut sources = RerankSources::for_shortlist(Some(&spec), &hits);
        assert!(sources.is_enabled());
        assert!(sources.inline.capacity() >= 4);
        assert!(sources.owners.capacity() >= 4);
        let inline_cap = sources.inline.capacity();
        let owners_cap = sources.owners.capacity();
        for doc_id in 0..32 {
            let p = payload(doc_id);
            sources.note_owner(doc_id, (doc_id % 3) as usize);
            sources.note_candidate(doc_id, Some(&p));
        }
        assert_eq!(sources.inline.len(), 4);
        assert_eq!(sources.owners.len(), 4);
        assert_eq!(sources.inline.capacity(), inline_cap);
        assert_eq!(sources.owners.capacity(), owners_cap);
        assert!(sources.has_all_inline());
        for doc_id in 0..4 {
            assert_eq!(sources.inline[&doc_id].text, format!("passage-{doc_id}"));
        }
        for doc_id in 4..32 {
            assert!(!sources.inline.contains_key(&doc_id));
            assert!(!sources.owners.contains_key(&doc_id));
        }
    }
}
