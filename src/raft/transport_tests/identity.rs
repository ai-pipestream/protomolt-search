//! Peer identity: the certificate, the group, the node, the timing
//! agreement, and fault injection, all refused by name.

use super::*;

pub(super) type Client = RaftTransportClient<tonic::transport::Channel>;

/// A vote request from `client` that the listener refuses.
pub(super) async fn refused(client: &Client, request: RaftVoteRequest) -> tonic::Status {
    client.clone().vote(timed(request)).await.unwrap_err()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_identity_binds_certificate_group_and_node() {
    let group = identity(9);
    let directory = directory(&group, &[1, 2, 3]);
    let dir = Directory::new("identity-1");
    let host = RaftHost::bootstrap_cluster(
        &dir.0,
        &group,
        1,
        &policy(),
        &limits(),
        &cluster_config(),
        transport(1, &directory),
    )
    .await
    .unwrap();
    let addr = host.listen_addr().unwrap();

    // A registered member speaking as itself is answered by node 1.
    let node2 = raw_client(addr, "node-2").await.unwrap();
    let reply = node2
        .clone()
        .vote(timed(vote_request(Some(header(&group, 2, 1)))))
        .await
        .unwrap()
        .into_inner();
    let responder = reply.header.unwrap();
    assert_eq!((responder.from_node_id, responder.to_node_id), (1, 2));
    assert_eq!(responder.group.as_ref(), Some(&group));
    header_refusals(&node2, &group).await;
    fault_refusals(&host, &node2, &group).await;
    timing_refusals(&node2, &group).await;
    stranger_refusals(addr, &group).await;
    membership_refusals(&host, &directory).await;
    host.shutdown().await.unwrap();
}

/// The same certificate cannot speak for node 3, reach node 9 or name
/// another group; an unversioned or missing header is malformed.
pub(super) async fn header_refusals(node2: &Client, group: &SourceAuthorityIdentity) {
    let error = refused(node2, vote_request(Some(header(group, 3, 1)))).await;
    assert!(error.message().contains("registered to node 2"), "{error}");
    assert_reason(
        &error,
        Code::PermissionDenied,
        super::super::reasons::TRANSPORT_IDENTITY_MISMATCH,
    );
    let error = refused(node2, vote_request(Some(header(group, 2, 9)))).await;
    assert!(error.message().contains("addressed to node 9"), "{error}");
    assert_reason(
        &error,
        Code::PermissionDenied,
        super::super::reasons::TRANSPORT_MISADDRESSED,
    );
    let error = refused(node2, vote_request(Some(header(&identity(8), 2, 1)))).await;
    assert_reason(
        &error,
        Code::PermissionDenied,
        super::super::reasons::TRANSPORT_WRONG_GROUP,
    );
    let mut stale = header(group, 2, 1);
    stale.protocol_version = 2;
    let error = refused(node2, vote_request(Some(stale))).await;
    assert_reason(
        &error,
        Code::InvalidArgument,
        super::super::reasons::TRANSPORT_BAD_HEADER,
    );
    let error = refused(node2, vote_request(None)).await;
    assert_reason(
        &error,
        Code::InvalidArgument,
        super::super::reasons::TRANSPORT_BAD_HEADER,
    );
}

/// Fault injection answers in front of the library too: a peer this node
/// isolates, and one it is deaf to, are refused by name, and heal
/// restores the answer.
pub(super) async fn fault_refusals(
    host: &RaftHost,
    node2: &Client,
    group: &SourceAuthorityIdentity,
) {
    host.isolate([2]);
    let error = refused(node2, vote_request(Some(header(group, 2, 1)))).await;
    assert_reason(
        &error,
        Code::Unavailable,
        super::super::reasons::TRANSPORT_ISOLATED,
    );
    host.heal();
    host.isolate_inbound([2]);
    let error = refused(node2, vote_request(Some(header(group, 2, 1)))).await;
    assert_reason(
        &error,
        Code::Unavailable,
        super::super::reasons::TRANSPORT_NOT_HEARD,
    );
    host.heal();
    let reply = node2
        .clone()
        .vote(timed(vote_request(Some(header(group, 2, 1)))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.header.unwrap().to_node_id, 2);
}

/// A peer whose election or lease values differ, or that presents none,
/// is refused: the lease argument depends on every member holding the
/// same values.
pub(super) async fn timing_refusals(node2: &Client, group: &SourceAuthorityIdentity) {
    let error = node2
        .clone()
        .vote(vote_request(Some(header(group, 2, 1))))
        .await
        .unwrap_err();
    assert_reason(
        &error,
        Code::FailedPrecondition,
        super::super::reasons::TRANSPORT_TIMING_MISMATCH,
    );
    let mut other_timing = tonic::Request::new(vote_request(Some(header(group, 2, 1))));
    other_timing.metadata_mut().insert(
        super::super::transport::TIMING_METADATA,
        "v1:heartbeat=50:election=150-300:lease=120:skew=50"
            .parse()
            .unwrap(),
    );
    let error = node2.clone().vote(other_timing).await.unwrap_err();
    assert_reason(
        &error,
        Code::FailedPrecondition,
        super::super::reasons::TRANSPORT_TIMING_MISMATCH,
    );
}

/// A certificate the cluster CA issued but nobody registered is not a
/// member, whatever node id it claims, and a certificate of another CA
/// does not complete the handshake.
pub(super) async fn stranger_refusals(addr: std::net::SocketAddr, group: &SourceAuthorityIdentity) {
    let node4 = raw_client(addr, "node-4").await.unwrap();
    let error = refused(&node4, vote_request(Some(header(group, 4, 1)))).await;
    assert_eq!(error.code(), Code::Unauthenticated, "{error}");
    let error = refused(&node4, vote_request(Some(header(group, 2, 1)))).await;
    assert_eq!(error.code(), Code::Unauthenticated, "{error}");
    if let Ok(stranger) = raw_client(addr, "stranger").await {
        let error = refused(&stranger, vote_request(Some(header(group, 2, 1)))).await;
        // The handshake fails after the lazy connect: the stream is cut
        // before any service sees the call.
        assert!(
            matches!(
                error.code(),
                Code::Unavailable | Code::Unknown | Code::Cancelled | Code::Internal
            ),
            "{error}"
        );
    }
}

/// A node without a registered certificate cannot be added, a registered
/// node with no dial address and a promotion naming no learner are
/// refused by name before the library sees them, and the directory
/// refuses to rebind a certificate or a node.
pub(super) async fn membership_refusals(host: &RaftHost, directory: &PeerDirectory) {
    let error = host.add_learner(4, "127.0.0.1:1").await.unwrap_err();
    assert_reason(
        &error,
        Code::FailedPrecondition,
        super::super::reasons::MEMBERSHIP_UNREGISTERED_CERTIFICATE,
    );
    let error = host.add_learner(2, "").await.unwrap_err();
    assert_reason(
        &error,
        Code::InvalidArgument,
        super::super::reasons::MEMBERSHIP_BAD_ADDRESS,
    );
    let error = host.promote(BTreeSet::new()).await.unwrap_err();
    assert_reason(
        &error,
        Code::InvalidArgument,
        super::super::reasons::MEMBERSHIP_NO_LEARNERS,
    );
    let error = directory
        .register(5, certificate_sha256(&pem("node-2.pem")).unwrap())
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists, "{error}");
    let error = directory
        .register(2, certificate_sha256(&pem("node-4.pem")).unwrap())
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists, "{error}");
}
