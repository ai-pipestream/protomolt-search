//! The rescoring routes: each candidate id goes to the child whose slot
//! range holds it, the children score their own, and the relay merges
//! the hits in the order a node answers in with one read receipt.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::metrics::Route;
use crate::pb::{Bm25RescoreRequest, Bm25RescoreResponse};

use super::fanout::{fan_out, read_metadata, shares_only, Opened};
use super::merge_util::{add_count, by_score_then_id, merge_known, route_ids};
use super::{grpc_timeout, RelayService};

/// The children opened for a routed route and the candidate ids sorted
/// into the child that owns each. An id in no child's range is another
/// shard's and is dropped, as a node drops an id outside its own range.
struct Routed {
    opened: Opened,
    by_child: Vec<Vec<u64>>,
}

impl RelayService {
    /// Open the children under the parent's claim, read their slot
    /// ranges from their health, and route `ids` by them.
    async fn open_routed(
        &self,
        route: &str,
        visibility: Option<&crate::pb::DocumentVisibility>,
        claim: (u64, &[u8]),
        timeout: Option<Duration>,
        ids: &[u64],
    ) -> Result<Routed, Status> {
        let opened = self.open_children(route, visibility, claim.0, claim.1)?;
        let ranges = self
            .ranges_of(&opened.frozen, &opened.children, timeout)
            .await?;
        let by_child = route_ids(&ranges, ids)?;
        Ok(Routed { opened, by_child })
    }

    /// The relay's receipt over the children's, under the parent's claim.
    fn routed_receipt(
        &self,
        opened: &Opened,
        visibility: Option<&crate::pb::DocumentVisibility>,
        receipts: &[crate::pb::VectorReadReceipt],
    ) -> Result<crate::pb::VectorReadReceipt, Status> {
        self.read_receipt(
            &opened.pinned,
            opened.children.clone(),
            visibility,
            Some(&opened.claims),
            receipts,
        )
    }

    pub(super) async fn handle_vector_rescore(
        &self,
        request: Request<crate::pb::VectorRescoreRequest>,
    ) -> Result<Response<crate::pb::VectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::VectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let claim = (
                req.expected_stats_epoch,
                &req.expected_stats_incarnation[..],
            );
            let visibility = req.visibility.as_ref();
            let Routed { opened, by_child } = self
                .open_routed(
                    "VectorRescore",
                    visibility,
                    claim,
                    timeout,
                    &req.candidate_ids,
                )
                .await?;
            let requests = by_child
                .into_iter()
                .zip(&opened.claims)
                .map(|(candidate_ids, claim)| {
                    Some(crate::pb::VectorRescoreRequest {
                        vector: req.vector.clone(),
                        candidate_ids,
                        field: req.field.clone(),
                        visibility: req.visibility.clone(),
                        expected_stats_epoch: claim.epoch,
                        expected_stats_incarnation: claim.incarnation(),
                    })
                })
                .collect();
            let shares = fan_out(
                &opened.frozen,
                &opened.children,
                requests,
                timeout,
                "vector rescore",
                |mut link, request| async move { link.vector_rescore(request).await },
            )
            .await?;
            let mut receipts = Vec::with_capacity(shares.len());
            let mut binding = None;
            let mut hits = Vec::new();
            for share in shares_only(shares) {
                let bound = share.vector_binding.as_ref();
                crate::vector_read::check_binding(&req.field, bound, &mut binding)?;
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            // The order one node answers in, over the union of the children's.
            hits.sort_by(|a, b| by_score_then_id((a.score, a.doc_id), (b.score, b.doc_id)));
            self.still_current(&opened.pinned, "VectorRescore")?;
            let receipt = self.routed_receipt(&opened, visibility, &receipts)?;
            Ok(Response::new(crate::pb::VectorRescoreResponse {
                hits,
                vector_binding: binding.flatten(),
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_exact_vector_rescore(
        &self,
        request: Request<crate::pb::ExactVectorRescoreRequest>,
    ) -> Result<Response<crate::pb::ExactVectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::ExactVectorRescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            // The root sends every candidate to every shard and each
            // shard answers for the ids it owns; only a child that owns
            // some is asked for them.
            let mut requested = req.candidate_ids.clone();
            let mut seen = HashSet::with_capacity(requested.len());
            requested.retain(|id| seen.insert(*id));
            let claim = (
                req.expected_stats_epoch,
                &req.expected_stats_incarnation[..],
            );
            let visibility = req.visibility.as_ref();
            let Routed { opened, by_child } = self
                .open_routed("ExactVectorRescore", visibility, claim, timeout, &requested)
                .await?;
            let requests = by_child
                .into_iter()
                .zip(&opened.claims)
                .map(|(candidate_ids, claim)| {
                    Some(crate::pb::ExactVectorRescoreRequest {
                        vector: req.vector.clone(),
                        candidate_ids,
                        field: req.field.clone(),
                        visibility: req.visibility.clone(),
                        expected_stats_epoch: claim.epoch,
                        expected_stats_incarnation: claim.incarnation(),
                        max_logical_bytes: req.max_logical_bytes,
                    })
                })
                .collect();
            let shares = fan_out(
                &opened.frozen,
                &opened.children,
                requests,
                timeout,
                "exact vector rescore",
                |mut link, request| async move { link.exact_vector_rescore(request).await },
            )
            .await?;
            let (mut merged, receipts, binding) =
                merge_exact_shares(&req.field, &requested, shares)?;
            self.still_current(&opened.pinned, "ExactVectorRescore")?;
            let receipt = self.routed_receipt(&opened, visibility, &receipts)?;
            merged.vector_binding = binding.flatten();
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_rescore_matryoshka(
        &self,
        request: Request<crate::pb::RescoreMatryoshkaRequest>,
    ) -> Result<Response<crate::pb::RescoreMatryoshkaResponse>, Status> {
        crate::metrics::timed(Route::RescoreMatryoshka, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let claim = (
                req.expected_stats_epoch,
                &req.expected_stats_incarnation[..],
            );
            let visibility = req.visibility.as_ref();
            let Routed { opened, by_child } = self
                .open_routed(
                    "RescoreMatryoshka",
                    visibility,
                    claim,
                    timeout,
                    &req.candidate_ids,
                )
                .await?;
            let requests = by_child
                .into_iter()
                .zip(&opened.claims)
                .map(|(candidate_ids, claim)| {
                    Some(crate::pb::RescoreMatryoshkaRequest {
                        query: req.query.clone(),
                        target_dim: req.target_dim,
                        candidate_ids,
                        visibility: req.visibility.clone(),
                        expected_stats_epoch: claim.epoch,
                        expected_stats_incarnation: claim.incarnation(),
                    })
                })
                .collect();
            let shares = fan_out(
                &opened.frozen,
                &opened.children,
                requests,
                timeout,
                "rescore matryoshka",
                |mut link, request| async move { link.rescore_matryoshka(request).await },
            )
            .await?;
            let mut receipts = Vec::with_capacity(shares.len());
            let mut hits = Vec::new();
            for share in shares_only(shares) {
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            hits.sort_by(|a, b| by_score_then_id((a.score, a.doc_id), (b.score, b.doc_id)));
            self.still_current(&opened.pinned, "RescoreMatryoshka")?;
            let receipt = self.routed_receipt(&opened, visibility, &receipts)?;
            Ok(Response::new(crate::pb::RescoreMatryoshkaResponse {
                hits,
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        crate::metrics::timed(Route::Bm25Rescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let claim = (
                req.expected_stats_epoch,
                &req.expected_stats_incarnation[..],
            );
            let visibility = req.visibility.as_ref();
            let Routed { opened, by_child } = self
                .open_routed(
                    "Bm25Rescore",
                    visibility,
                    claim,
                    timeout,
                    &req.candidate_ids,
                )
                .await?;
            let requested: Vec<HashSet<u64>> = by_child
                .iter()
                .map(|ids| ids.iter().copied().collect())
                .collect();
            let requests = by_child
                .into_iter()
                .zip(&opened.claims)
                .map(|(candidate_ids, claim)| {
                    let mut child_req = req.clone();
                    child_req.candidate_ids = candidate_ids;
                    child_req.expected_stats_epoch = claim.epoch;
                    child_req.expected_stats_incarnation = claim.incarnation();
                    Some(child_req)
                })
                .collect();
            let shares = fan_out(
                &opened.frozen,
                &opened.children,
                requests,
                timeout,
                "bm25 rescore",
                |mut link, request| async move { link.bm25_rescore(request).await },
            )
            .await?;
            let (hits, stage_known, receipts) = merge_bm25_rescores(&requested, shares)?;
            self.still_current(&opened.pinned, "Bm25Rescore")?;
            let receipt = self.routed_receipt(&opened, visibility, &receipts)?;
            Ok(Response::new(Bm25RescoreResponse {
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                hits,
                stage_columns_known: stage_known
                    .unwrap_or_else(|| vec![false; req.score_stages.len()]),
            }))
        })
        .await
    }
}

type ExactMerge = (
    crate::pb::ExactVectorRescoreResponse,
    Vec<crate::pb::VectorReadReceipt>,
    Option<Option<crate::pb::MappedVectorBinding>>,
);

/// The exact rescores as one: byte and page counts summed with a check,
/// one row per owned id in request order, an id two children answer a
/// sign the slot ranges overlap.
fn merge_exact_shares(
    field: &str,
    requested: &[u64],
    shares: Vec<(usize, crate::pb::ExactVectorRescoreResponse)>,
) -> Result<ExactMerge, Status> {
    let mut receipts = Vec::with_capacity(shares.len());
    let mut binding = None;
    let mut merged = crate::pb::ExactVectorRescoreResponse::default();
    let mut by_id = HashMap::new();
    for (i, share) in shares {
        crate::vector_read::check_binding(field, share.vector_binding.as_ref(), &mut binding)?;
        receipts.push(read_metadata(&share));
        add_count(
            &mut merged.logical_bytes,
            share.logical_bytes,
            i,
            "logical bytes",
        )?;
        add_count(
            &mut merged.pages_touched,
            share.pages_touched,
            i,
            "page count",
        )?;
        merged.tasks = merged.tasks.saturating_add(share.tasks);
        for hit in share.hits {
            if by_id.insert(hit.doc_id, hit).is_some() {
                return Err(Status::failed_precondition(
                    "relay: exact vector rescore candidate answered by more than one \
                     child; slot ranges overlap",
                ));
            }
        }
    }
    merged.hits = requested.iter().filter_map(|id| by_id.remove(id)).collect();
    Ok((merged, receipts, binding))
}

type Bm25RescoreMerge = (
    Vec<crate::pb::Bm25Hit>,
    Option<Vec<bool>>,
    Vec<crate::pb::VectorReadReceipt>,
);

/// The BM25 rescores as one: every hit one the child was asked for,
/// once, with a finite score; the stage flags ORed; ids ascending.
fn merge_bm25_rescores(
    requested: &[HashSet<u64>],
    shares: Vec<(usize, Bm25RescoreResponse)>,
) -> Result<Bm25RescoreMerge, Status> {
    let mut hits = Vec::new();
    let mut stage_known: Option<Vec<bool>> = None;
    let mut receipts = Vec::with_capacity(shares.len());
    for (i, share) in shares {
        merge_known(
            "stage-column",
            i,
            &mut stage_known,
            &share.stage_columns_known,
        )?;
        let mut seen = HashSet::new();
        for hit in &share.hits {
            if !requested[i].contains(&hit.doc_id)
                || !hit.score.is_finite()
                || !seen.insert(hit.doc_id)
            {
                return Err(Status::failed_precondition(
                    "relay: BM25 child returned an unrequested, duplicate or nonfinite score",
                ));
            }
        }
        receipts.push(read_metadata(&share));
        hits.extend(share.hits);
    }
    hits.sort_by_key(|hit| hit.doc_id);
    Ok((hits, stage_known, receipts))
}
