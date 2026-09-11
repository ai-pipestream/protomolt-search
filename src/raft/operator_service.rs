//! The Raft operator service: member status, membership changes and
//! image verification over the member's listener, under cluster trust
//! (docs/raft-hosting.md, "Operator surface").
//!
//! Trust is the cluster-membership check every control route applies: a
//! client certificate the listener verified against the cluster CA. The
//! operator's certificate does not have to be in the peer directory;
//! that directory binds Raft peers, and the operator is not one.
//!
//! Every mutation passes its error through unchanged: the host's wording
//! on a non-leader, on an unregistered certificate and on an unknown
//! learner, the library's membership error mapped as the host maps it.
//! The service does not retry, does not forward and adds no wording of
//! its own on top of the host's. `GetMemberStatus` reads through the
//! host's existing read accessors only and holds no guard across an
//! await.
use super::types::{log_id_to_proto, membership_to_proto, vote_to_proto};
use super::RaftHost;
use crate::control_plane::cluster_membership;
use crate::metrics::{self, Route};
use crate::pb::raft_operator_service_server::{RaftOperatorService, RaftOperatorServiceServer};
use crate::pb::{
    AddLearnerRequest, GetMemberStatusRequest, MemberStatus, MembershipChange, PeerRejectionSample,
    PromoteLearnersRequest, PublishedImageVerification, RemoveMemberRequest, SnapshotBuild,
    SnapshotRejectionSample, SnapshotRejectionSummary, VerifyPublishedImageRequest,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use tonic::{Request, Response, Status};

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    out
}

/// One member's status from the host's read accessors, for the route and
/// for the `raft-bootstrap` and `raft-status` CLIs.
pub fn member_status(host: &RaftHost) -> Result<MemberStatus, Status> {
    let store = host.store()?;
    let identity = store.identity();
    let readings = host.metrics();
    let metrics = readings.borrow();
    let now = std::time::Instant::now();
    Ok(MemberStatus {
        node_id: host.node_id(),
        group_id: hex(&identity.group_id),
        authority_incarnation: hex(&identity.authority_incarnation),
        advertised_addr: host.advertised_addr().map(str::to_string),
        believed_leader: host.believed_leader(),
        leads: metrics.state == openraft::ServerState::Leader,
        vote: Some(vote_to_proto(&metrics.vote)),
        applied: host.applied_position()?,
        last_log_index: metrics.last_log_index,
        membership: Some(membership_to_proto(metrics.membership_config.membership())),
        awaiting_snapshot: host.awaiting_snapshot()?,
        last_snapshot_build: host.last_snapshot_build().map(|build| SnapshotBuild {
            generation: build.generation,
            bytes: build.bytes,
            receiver_bound: build.receiver_bound,
            last_log_id: build.last_log_id.as_ref().map(log_id_to_proto),
            over_receiver_bound: build.over_receiver_bound(),
        }),
        peer_rejections: host
            .peer_rejections()
            .iter()
            .map(|(node, rejection)| PeerRejectionSample {
                node_id: *node,
                action: rejection.action.to_string(),
                code: format!("{:?}", rejection.code),
                message: rejection.message.clone(),
                count: rejection.count,
                age_ms: now
                    .saturating_duration_since(rejection.at)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            })
            .collect(),
        snapshot_rejections: Some(SnapshotRejectionSummary {
            count: host.snapshot_rejections().count(),
            last: host
                .snapshot_rejections()
                .last()
                .map(|last| SnapshotRejectionSample {
                    from: last.from,
                    announced: last.announced,
                    bound: last.bound,
                }),
        }),
    })
}

fn membership_change(host: &RaftHost) -> Result<MembershipChange, Status> {
    Ok(MembershipChange {
        membership: Some(membership_to_proto(
            host.metrics().borrow().membership_config.membership(),
        )),
        applied: host.applied_position()?,
    })
}

struct Inner {
    host: Arc<RaftHost>,
    require_client_cert: bool,
}

/// The operator service: status and membership over the member's host.
#[derive(Clone)]
pub struct RaftOperatorServiceImpl {
    inner: Arc<Inner>,
}

impl RaftOperatorServiceImpl {
    pub fn new(host: Arc<RaftHost>) -> Self {
        Self {
            inner: Arc::new(Inner {
                host,
                require_client_cert: false,
            }),
        }
    }

    /// Demand a client certificate on every call, as the listeners that
    /// verify against the cluster CA do.
    pub fn with_client_cert_required(self, required: bool) -> Self {
        Self {
            inner: Arc::new(Inner {
                host: Arc::clone(&self.inner.host),
                require_client_cert: required,
            }),
        }
    }

    /// Use this builder when registering the service so tonic enforces
    /// the message-size bound during decoding as on every listener.
    pub fn into_server(self) -> RaftOperatorServiceServer<Self> {
        RaftOperatorServiceServer::new(self)
    }

    fn trust<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if cluster_membership(request, self.inner.require_client_cert) {
            return Ok(());
        }
        Err(Status::unauthenticated(
            "raft operator routes require a client certificate from the cluster CA; a Bearer [REDACTED] is not membership",
        ))
    }

    async fn status(
        &self,
        request: Request<GetMemberStatusRequest>,
    ) -> Result<Response<MemberStatus>, Status> {
        self.trust(&request)?;
        member_status(&self.inner.host).map(Response::new)
    }

    async fn add_learner(
        &self,
        request: Request<AddLearnerRequest>,
    ) -> Result<Response<MembershipChange>, Status> {
        self.trust(&request)?;
        let AddLearnerRequest { node_id, addr } = request.into_inner();
        self.inner.host.add_learner(node_id, &addr).await?;
        membership_change(&self.inner.host).map(Response::new)
    }

    async fn promote(
        &self,
        request: Request<PromoteLearnersRequest>,
    ) -> Result<Response<MembershipChange>, Status> {
        self.trust(&request)?;
        let learners: BTreeSet<u64> = request.into_inner().learner_ids.into_iter().collect();
        self.inner.host.promote(learners).await?;
        membership_change(&self.inner.host).map(Response::new)
    }

    async fn remove(
        &self,
        request: Request<RemoveMemberRequest>,
    ) -> Result<Response<MembershipChange>, Status> {
        self.trust(&request)?;
        let node_id = request.into_inner().node_id;
        self.inner.host.remove_member(node_id).await?;
        membership_change(&self.inner.host).map(Response::new)
    }

    async fn verify(
        &self,
        request: Request<VerifyPublishedImageRequest>,
    ) -> Result<Response<PublishedImageVerification>, Status> {
        self.trust(&request)?;
        let generation = self.inner.host.verify_published_image()?;
        Ok(Response::new(PublishedImageVerification { generation }))
    }
}

#[tonic::async_trait]
impl RaftOperatorService for RaftOperatorServiceImpl {
    async fn get_member_status(
        &self,
        request: Request<GetMemberStatusRequest>,
    ) -> Result<Response<MemberStatus>, Status> {
        metrics::timed(Route::GetMemberStatus, request, |request| {
            self.status(request)
        })
        .await
    }

    async fn add_learner(
        &self,
        request: Request<AddLearnerRequest>,
    ) -> Result<Response<MembershipChange>, Status> {
        metrics::timed(Route::AddLearner, request, |request| {
            self.add_learner(request)
        })
        .await
    }

    async fn promote_learners(
        &self,
        request: Request<PromoteLearnersRequest>,
    ) -> Result<Response<MembershipChange>, Status> {
        metrics::timed(Route::PromoteLearners, request, |request| {
            self.promote(request)
        })
        .await
    }

    async fn remove_member(
        &self,
        request: Request<RemoveMemberRequest>,
    ) -> Result<Response<MembershipChange>, Status> {
        metrics::timed(Route::RemoveMember, request, |request| self.remove(request)).await
    }

    async fn verify_published_image(
        &self,
        request: Request<VerifyPublishedImageRequest>,
    ) -> Result<Response<PublishedImageVerification>, Status> {
        metrics::timed(Route::VerifyPublishedImage, request, |request| {
            self.verify(request)
        })
        .await
    }
}
