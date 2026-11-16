//! In-process `libturbo` rerank execution.

use std::sync::{Arc, OnceLock};

use tonic::Status;

use super::snippet::{build_wire_snippet, sort_reranked_hits};
use super::{RerankCandidate, RerankSpec, RerankedHit};

pub(super) async fn rerank(
    spec: &RerankSpec,
    query_text: &str,
    shortlist: &[RerankCandidate],
    engine: &Arc<turbo_client::LocalRerankEngine>,
) -> Result<Vec<RerankedHit>, Status> {
    run_blocking(Arc::clone(blocking_limit()), || {
        let engine = Arc::clone(engine);
        let query = query_text.to_owned();
        let spec = spec.clone();
        let candidates: Arc<[RerankCandidate]> = Arc::from(shortlist);
        move || rerank_candidates(&spec, &query, &candidates, &engine)
    })
    .await?
}

fn blocking_limit() -> &'static Arc<tokio::sync::Semaphore> {
    static LIMIT: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    LIMIT.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(2);
        Arc::new(tokio::sync::Semaphore::new(workers))
    })
}

async fn run_blocking<T: Send + 'static, F: FnOnce() -> T + Send + 'static>(
    limit: Arc<tokio::sync::Semaphore>,
    prepare: impl FnOnce() -> F,
) -> Result<T, Status> {
    let permit = limit
        .acquire_owned()
        .await
        .map_err(|_| Status::unavailable("local rerank executor closed"))?;
    let job = prepare();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        job()
    })
    .await
    .map_err(|e| Status::internal(format!("local rerank join error: {e}")))
}

fn rerank_candidates(
    spec: &RerankSpec,
    query: &str,
    candidates: &[RerankCandidate],
    engine: &turbo_client::LocalRerankEngine,
) -> Result<Vec<RerankedHit>, Status> {
    let pairs: Vec<(&str, &[_])> = candidates
        .iter()
        .map(|c| (c.text.as_str(), &[][..]))
        .collect();
    let hits = engine
        .rerank_and_highlight(
            query,
            &pairs,
            turbo_client::LocalRerankOptions::default(),
            spec.max_snippets_per_candidate,
            &spec.language,
            None,
        )
        .map_err(|e| Status::internal(format!("local rerank failed: {e}")))?;

    let mut out = Vec::with_capacity(hits.len());
    for (rank, h) in hits.into_iter().enumerate() {
        let cand = candidates.get(h.candidate_index).ok_or_else(|| {
            Status::internal("local rerank returned out-of-range candidate index")
        })?;
        let mut snippets = Vec::new();
        if rank < spec.highlight_top_k {
            for s in h.snippets {
                let (u8_start, u8_end) = s.spans.utf8;
                if let Some(snip) = build_wire_snippet(
                    &spec.field,
                    &cand.text,
                    &cand.sentence_spans,
                    u8_start,
                    u8_end,
                    if spec.compose_parent_lineage_offsets {
                        cand.lineage_span_start
                    } else {
                        0
                    },
                ) {
                    snippets.push(snip);
                }
            }
        }
        out.push(RerankedHit {
            input_index: h.candidate_index,
            doc_id: cand.doc_id,
            raw_logit: h.score.raw_logit,
            calibrated_probability: h.score.calibrated_probability.clamp(0.0, 1.0),
            snippets,
        });
    }
    sort_reranked_hits(&mut out);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_waiter_keeps_permit_until_blocking_work_finishes() {
        let limit = Arc::new(tokio::sync::Semaphore::new(1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let caller_limit = Arc::clone(&limit);
        let caller = tokio::spawn(async move {
            run_blocking(caller_limit, || {
                move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }
            })
            .await
        });
        started_rx.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(limit.available_permits(), 0);
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            run_blocking(Arc::clone(&limit), || -> fn() {
                panic!("must not clone a queued shortlist")
            })
        )
        .await
        .is_err());
        release_tx.send(()).unwrap();
        let _permit = tokio::time::timeout(std::time::Duration::from_secs(2), limit.acquire())
            .await
            .unwrap()
            .unwrap();
    }
}
