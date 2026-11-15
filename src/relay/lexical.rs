//! Composed lexical routes: term statistics, BM25 pages, and the dictionaries.

use tonic::{Request, Response, Status};

use crate::metrics::Route;
use crate::pb::{
    Bm25PhraseQueryRequest, Bm25QueryRequest, Bm25QueryResponse, TermStatsRequest,
    TermStatsResponse,
};
use crate::stats_identity::StatsClaim;

use super::fanout::{echo_known, fan_out, fan_out_claimed, same_requests, shares_only};
use super::merge_dictionary::{merge_prefix_expansions, merge_suggest_terms};
use super::merge_lexical::{merge_bm25_responses, merge_term_stats, refuse_bm25_aggregates};
use super::{grpc_timeout, RelayService};

impl RelayService {
    pub(super) async fn handle_term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::metrics::timed(Route::TermStats, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            crate::visibility::validate_stats_request(&req)?;
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let (pinned, frozen, children) = self.pin_children()?;
            let requests = same_requests(&req, children.len());
            let shares = shares_only(
                fan_out(
                    &frozen,
                    &children,
                    requests,
                    timeout,
                    "term stats",
                    |mut link, request| async move { link.term_stats(request).await },
                )
                .await?,
            );
            let mut merged = merge_term_stats(&req, &shares)?;
            merged.stats_incarnation = self.stats_incarnation.bytes()?;
            self.still_current(&pinned, "TermStats")?;
            let epochs = shares
                .iter()
                .map(|s| StatsClaim::required(s.stats_epoch, &s.stats_incarnation))
                .collect::<Result<_, _>>()?;
            merged.stats_epoch = self.allocate_token(&pinned, children, epochs)?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        crate::metrics::timed(Route::ExpandTermPrefix, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let (pinned, frozen, children) = self.pin_children()?;
            let requests = same_requests(&req, children.len());
            let shares = shares_only(
                fan_out(
                    &frozen,
                    &children,
                    requests,
                    timeout,
                    "prefix expansion",
                    |mut link, request| async move { link.expand_term_prefix(request).await },
                )
                .await?,
            );
            let known = echo_known(&scope, &shares, |s| {
                (&s.visibility_fingerprint, &s.visibility_columns_known)
            })?;
            let mut merged = merge_prefix_expansions(req.cap, &shares);
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = known;
            self.still_current(&pinned, "ExpandTermPrefix")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        crate::metrics::timed(Route::SuggestTerms, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let (pinned, frozen, children) = self.pin_children()?;
            let requests = same_requests(&req, children.len());
            let shares = shares_only(
                fan_out(
                    &frozen,
                    &children,
                    requests,
                    timeout,
                    "dictionary scan",
                    |mut link, request| async move { link.suggest_terms(request).await },
                )
                .await?,
            );
            let known = echo_known(&scope, &shares, |s| {
                (&s.visibility_fingerprint, &s.visibility_columns_known)
            })?;
            let mut merged = merge_suggest_terms(req.max_scan, &shares)?;
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = known;
            self.still_current(&pinned, "SuggestTerms")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25Query, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            refuse_bm25_aggregates("Bm25Query", &req.stats_fields, &req.cardinality_fields)?;
            let opened = self.open_children(
                "Bm25Query",
                None,
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            let shares = fan_out_claimed(
                &opened,
                &req,
                |child, claim| {
                    child.expected_stats_epoch = claim.epoch;
                    child.expected_stats_incarnation = claim.incarnation();
                },
                timeout,
                "bm25 query",
                |mut link, request| async move { link.bm25_query(request).await },
            )
            .await?;
            let merged = merge_bm25_responses(&req, shares)?;
            self.still_current(&opened.pinned, "Bm25Query")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_bm25_phrase_query(
        &self,
        request: Request<Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25PhraseQuery, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let query = req
                .query
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Bm25PhraseQuery: query is absent"))?;
            refuse_bm25_aggregates(
                "Bm25PhraseQuery",
                &query.stats_fields,
                &query.cardinality_fields,
            )?;
            let opened = self.open_children(
                "Bm25PhraseQuery",
                None,
                query.expected_stats_epoch,
                &query.expected_stats_incarnation,
            )?;
            let shares = fan_out_claimed(
                &opened,
                &req,
                |child, claim| {
                    if let Some(q) = child.query.as_mut() {
                        q.expected_stats_epoch = claim.epoch;
                        q.expected_stats_incarnation = claim.incarnation();
                    }
                },
                timeout,
                "bm25 phrase query",
                |mut link, request| async move { link.bm25_phrase_query(request).await },
            )
            .await?;
            let merged = merge_bm25_responses(query, shares)?;
            self.still_current(&opened.pinned, "Bm25PhraseQuery")?;
            Ok(Response::new(merged))
        })
        .await
    }
}
