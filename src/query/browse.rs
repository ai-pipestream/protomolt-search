//! The unscored routes: a filter-only browse in id (or column) order,
//! and a sorted lexical leaf walking its exact term membership in
//! column order.

use tonic::Status;

use super::cursor::Cursor;
use super::refuse;
use super::response::{done, fill_projected, finish_prof, matched, ms, scored_hit};
use super::run::Run;
use crate::coordinator::{BrowseAfter, BrowseRows, RequestFilters};
use crate::pb::{LexicalQuery, QueryHit, QueryResponse};

/// What one browse walks and how it reports it. A sorted lexical leaf
/// names its analyzed terms, the fingerprint of the analysis that
/// produced them, and its request id; a filter-only browse has no
/// terms and no id. `executed` is the route echo.
struct BrowseWalk<'a> {
    terms: Vec<String>,
    analysis_fingerprint: u64,
    id: Option<&'a str>,
    executed: &'a str,
}

/// A filter-only browse. A pool aggregation folds over the filter's
/// exact match set, which the aggregation fan-out evaluates on the
/// shards directly; `matched` is that set's size, whatever page was
/// asked for.
pub(super) async fn execute_filter_browse(mut run: Run<'_>) -> Result<QueryResponse, Status> {
    let walk = BrowseWalk {
        terms: Vec::new(),
        analysis_fingerprint: 0,
        id: None,
        executed: "browse",
    };
    let mut response = execute_browse(&mut run, walk).await?;
    if let Some(compiled) = &run.pool_aggregate {
        let filters = RequestFilters::compile(&run.base.plan.geo_filters, &run.filter)?;
        response.aggregate = Some(
            run.base
                .coordinator
                .fanout_aggregate(&filters, compiled, None)
                .await?,
        );
    }
    Ok(response)
}

/// A sorted lexical leaf: its exact term membership, walked in column
/// order (no relevance shape rides along, by validation). Analysis
/// happens once here; the shards read the same postings the bitmap
/// route would.
pub(super) async fn execute_sorted_lexical(
    mut run: Run<'_>,
    id: &str,
    query: &LexicalQuery,
) -> Result<QueryResponse, Status> {
    let base = run.base;
    if run.pool_aggregate.is_some() {
        return Err(refuse(
            "aggregate over a sorted lexical leaf is not served: the leaf's term \
             membership is walked page by page and never held as a set; sort by \
             relevance (a pool) or name the membership as a filter (a browse)",
        ));
    }
    let pre_terms = run.residual_terms(query);
    let terms = if !pre_terms.is_empty() {
        pre_terms
    } else {
        base.coordinator
            .analyze_terms(&query.text, query.analysis.as_ref())
            .await?
    };
    if terms.is_empty() {
        let mut response = done(
            base.req.request_id.clone(),
            Vec::new(),
            "browse_shard:lexical",
            String::new(),
            finish_prof(run.prof.take(), base.t_total),
        );
        response.executed.push_str("(no terms)");
        return Ok(response);
    }
    let walk = BrowseWalk {
        terms,
        analysis_fingerprint: crate::analyzer::analysis_fingerprint(query.analysis.as_ref()),
        id: Some(id),
        executed: "browse_shard:lexical",
    };
    execute_browse(&mut run, walk).await
}

async fn execute_browse(run: &mut Run<'_>, walk: BrowseWalk<'_>) -> Result<QueryResponse, Status> {
    let base = run.base;
    let (req, plan) = (base.req, base.plan);
    if plan.geo_filters.is_empty() && run.filter.is_empty() && walk.terms.is_empty() {
        return Err(refuse(
            "an empty browse (no filter at all) would page the whole corpus in id \
                     order; name at least one filter",
        ));
    }
    let compiled = RequestFilters::compile(&plan.geo_filters, &run.filter)?;
    let sort: Vec<crate::pb::BrowseSort> = req
        .sort
        .iter()
        .map(|s| crate::pb::BrowseSort {
            map: s.map.clone(),
            column: s.column.clone(),
            descending: s.descending,
        })
        .collect();
    let cursor = run.cursor.as_ref();
    let after = browse_after(cursor, sort.len())?;
    let base_rank = cursor.map_or(0, |c| c.rank);
    let t_sel = std::time::Instant::now();
    let rows = base
        .coordinator
        .fanout_browse(
            req.k,
            after,
            &sort,
            &walk.terms,
            walk.analysis_fingerprint,
            &compiled,
        )
        .await?;
    if let Some(p) = run.prof.as_mut() {
        p.selection_ms = ms(t_sel);
        p.segments_total = rows.prune.segments_total;
        p.segments_skipped = rows.prune.segments_skipped;
    }
    let search_ids: Vec<String> = walk.id.map(str::to_string).into_iter().collect();
    let mut hits = browse_hits(&rows, base_rank, &matched(search_ids, &plan.filter_ids));
    let next = next_cursor(&rows, &hits, req.k);
    fill_projected(base.coordinator, &run.projections, &mut hits, &mut run.prof).await?;
    Ok(done(
        req.request_id.clone(),
        hits,
        walk.executed,
        next,
        finish_prof(run.prof.take(), base.t_total),
    ))
}

/// The boundary a cursor resumes the browse after. A cursor resumes the
/// query that minted it: a sorted token carries the key boundary, a
/// plain one only the id, and mixing the two shapes is a different
/// query.
fn browse_after(
    cursor: Option<&Cursor>,
    sort_columns: usize,
) -> Result<Option<BrowseAfter>, Status> {
    let Some(c) = cursor else {
        return Ok(None);
    };
    match (sort_columns == 0, &c.keys) {
        (false, Some(keys)) if keys.len() == sort_columns => Ok(Some(BrowseAfter {
            id: c.doc_id,
            keys: keys.clone(),
        })),
        (true, None) => Ok(Some(BrowseAfter {
            id: c.doc_id,
            keys: Vec::new(),
        })),
        _ => Err(refuse(
            "this cursor came from a browse with a different sort; the \
                                 rest of the request must repeat the query that minted it",
        )),
    }
}

/// The page's hits. No relevance score exists on this route; the id
/// (or column) order IS the order, and rank counts on across pages.
fn browse_hits(rows: &BrowseRows, base_rank: u32, matched: &[String]) -> Vec<QueryHit> {
    rows.ids
        .iter()
        .enumerate()
        .map(|(i, &doc_id)| QueryHit {
            rank: base_rank + (i + 1) as u32,
            sort_key: rows
                .values
                .get(i)
                .and_then(|v| v.first())
                .map_or(0.0, crate::sortkeys::Value::as_f64),
            sort_values: rows
                .values
                .get(i)
                .map(|v| v.iter().map(crate::sortkeys::Value::to_pb).collect())
                .unwrap_or_default(),
            ..scored_hit(doc_id, 0.0, Vec::new(), matched.to_vec())
        })
        .collect()
}

/// The cursor after a full page: the last hit's rank and id, with its
/// sort keys when a column order was applied.
fn next_cursor(rows: &BrowseRows, hits: &[QueryHit], k: u32) -> String {
    if k == 0 || hits.len() != k as usize {
        return String::new();
    }
    hits.last()
        .map(|h| {
            Cursor {
                rank: h.rank,
                score_bits: 0,
                keys: rows
                    .sorted
                    .then(|| rows.keys.last().expect("full page").clone()),
                doc_id: h.doc_id,
            }
            .encode()
        })
        .unwrap_or_default()
}
