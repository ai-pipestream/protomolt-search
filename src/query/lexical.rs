//! The single lexical leaf, in relevance order: the `Bm25Search` route.

use tonic::{Request, Status};

use super::pool::{answer_pool, Answer};
use super::rerank::{turbo_rerank, RerankSources};
use super::response::{leaf_signal, matched, ms, scored_hit};
use super::run::Run;
use crate::pb::search_service_server::SearchService;
use crate::pb::{Bm25SearchRequest, Bm25SearchResponse, LexicalQuery, QueryHit, QueryResponse};

pub(super) async fn execute_lexical(
    mut run: Run<'_>,
    id: &str,
    query: &LexicalQuery,
) -> Result<QueryResponse, Status> {
    let base = run.base;
    let t_sel = std::time::Instant::now();
    let request = bm25_request(&mut run, query);
    let response = base
        .coordinator
        .bm25_search(crate::metrics::nested(Request::new(request)))
        .await?
        .into_inner();
    if let Some(p) = run.prof.as_mut() {
        p.selection_ms = ms(t_sel);
        p.segments_total = response.segments_total;
        p.segments_skipped = response.segments_skipped;
    }
    let mut synonym_expansions = response.synonym_expansions.clone();
    if let Some(bound) = &run.one_pass {
        synonym_expansions.extend(bound.synonym_expansions.iter().cloned());
    }
    let mut hits = lexical_hits(&run, id, &response)?;
    let reranked = turbo_rerank(&base, &query.text, &RerankSources::default(), &mut hits).await?;
    let route = if reranked {
        "bm25_search:turbo_rerank"
    } else {
        "bm25_search"
    };
    let request_id = base.req.request_id.clone();
    let mut answer = match answer_pool(&mut run, hits, route, request_id).await? {
        Answer::Deepened(response) => return Ok(response),
        Answer::Paged(response) => response,
    };
    answer.field_details_redacted = response.field_details_redacted;
    answer.synonym_expansions = synonym_expansions;
    Ok(answer)
}

/// The route request. The lexical route carries projections and
/// highlighting natively, so both ride along.
fn bm25_request(run: &mut Run<'_>, query: &LexicalQuery) -> Bm25SearchRequest {
    let req = run.base.req;
    Bm25SearchRequest {
        text: run.effective_text(query),
        k: run.depths.fetch_k,
        analysis: query.analysis.clone(),
        score_stages: query.score_stages.clone(),
        phrase: query.phrase,
        prefixes: query.prefixes.clone(),
        geo_filters: run.base.plan.geo_filters.clone(),
        filter: std::mem::take(&mut run.filter),
        projections: req.projections.clone(),
        highlight: req.highlight.clone(),
        synonyms: query.synonyms.clone(),
        synonyms_off: query.synonyms_off,
        explain: req.explain,
        pre_analyzed_terms: run.residual_terms(query),
        ..Default::default()
    }
}

fn lexical_hits(
    run: &Run<'_>,
    id: &str,
    response: &Bm25SearchResponse,
) -> Result<Vec<QueryHit>, Status> {
    let matched = matched(vec![id.to_string()], &run.base.plan.filter_ids);
    let mut hits: Vec<QueryHit> = response
        .hits
        .iter()
        .map(|h| QueryHit {
            identity: h.identity.clone(),
            snippets: h.snippets.clone(),
            projected: h.projected.clone(),
            ..scored_hit(h.doc_id, h.score, leaf_signal(id, h.score), matched.clone())
        })
        .collect();
    if run.base.req.explain {
        for (hit, source) in hits.iter_mut().zip(&response.hits) {
            hit.explain = Some(crate::explain::lexical(
                id,
                source,
                &response.prefix_expansions,
                &response.synonym_expansions,
            )?);
        }
    }
    Ok(hits)
}
