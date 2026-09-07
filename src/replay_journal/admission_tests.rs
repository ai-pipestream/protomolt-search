use super::*;

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "replay-admission-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("journal.redb")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn binding() -> ReplayStreamBinding {
    ReplayStreamBinding {
        contract_version: 1,
        workspace: "work".into(),
        collection: "docs".into(),
        source_shard_id: "source".into(),
        source_history_id: vec![1; 16],
        source_write_epoch: 2,
        target_shard_id: "target".into(),
        target_history_id: vec![3; 16],
        target_write_epoch: 4,
        topology_revision: 5,
        assignment_id: vec![6; 16],
        baseline_sha256: vec![7; 32],
        index_contract_sha256: vec![8; 32],
        ..Default::default()
    }
}
fn policy(revision: u64) -> ReplayAdmissionPolicy {
    let b = binding();
    ReplayAdmissionPolicy {
        contract_version: 1,
        authority_id: vec![9; 16],
        revision,
        binding_sha256: binding_digest(&b).unwrap(),
        source_certificate_sha256: vec![vec![10; 32]],
        source_history_id: b.source_history_id,
        target_history_id: b.target_history_id,
        source_write_epoch: b.source_write_epoch,
        target_write_epoch: b.target_write_epoch,
        source_residency: 1,
        target_residency: 1,
        enabled: true,
    }
}
fn frame() -> ReplayJournalFrame {
    seal_frame(
        &binding(),
        1,
        binding_digest(&binding()).unwrap(),
        vec![8, 1, 0x30, 1, 0x22, 0],
    )
    .unwrap()
}

#[test]
fn policy_is_required_for_peers_and_cannot_be_bypassed_locally() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    assert_eq!(
        journal
            .accept_inner(&frame(), Some(&[10; 32]))
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    journal.publish_authorization(&policy(1)).unwrap();
    assert_eq!(journal.head().unwrap().format_version, AUTHORIZED_FORMAT);
    assert_eq!(
        journal.accept(&frame()).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        journal
            .accept_inner(&frame(), Some(&[11; 32]))
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
    assert!(
        journal
            .accept_inner(&frame(), Some(&[10; 32]))
            .unwrap()
            .durable
    );
}

#[test]
fn policy_retries_are_exact_and_rollback_or_authority_replacement_refuses() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    journal.publish_authorization(&policy(2)).unwrap();
    journal.publish_authorization(&policy(2)).unwrap();
    let mut conflict = policy(2);
    conflict.enabled = false;
    assert_eq!(
        journal.publish_authorization(&conflict).unwrap_err().code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(
        journal
            .publish_authorization(&policy(1))
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut foreign = policy(3);
    foreign.authority_id[0] ^= 1;
    assert_eq!(
        journal.publish_authorization(&foreign).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    foreign = policy(3);
    foreign.binding_sha256[0] ^= 1;
    assert_eq!(
        journal.publish_authorization(&foreign).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    journal.accept_inner(&frame(), Some(&[10; 32])).unwrap();
}

#[test]
fn current_fences_and_device_residency_revoke_even_exact_retries_after_restart() {
    for case in 0..9 {
        let dir = Directory::new();
        let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
        journal.publish_authorization(&policy(1)).unwrap();
        let accepted = journal.accept_inner(&frame(), Some(&[10; 32])).unwrap();
        let mut denied = policy(2);
        match case {
            0 => denied.enabled = false,
            1 => denied.source_history_id[0] ^= 1,
            2 => denied.target_history_id[0] ^= 1,
            3 => denied.source_write_epoch += 1,
            4 => denied.target_write_epoch += 1,
            5 => denied.source_residency = 2,
            6 => denied.target_residency = 2,
            7 => denied.source_certificate_sha256.clear(),
            8 => denied.source_certificate_sha256 = vec![vec![11; 32]],
            _ => unreachable!(),
        }
        journal.publish_authorization(&denied).unwrap();
        drop(journal);
        let journal = ReplayJournal::open(&dir.path(), binding()).unwrap();
        assert_eq!(
            journal
                .accept_inner(&frame(), Some(&[10; 32]))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied,
            "case {case}"
        );
        assert_eq!(journal.head().unwrap().accepted_sequence, 1);
        if (1..=6).contains(&case) {
            assert!(journal.head().unwrap().admission_fenced);
            assert_eq!(
                journal
                    .publish_authorization(&policy(3))
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
        } else {
            journal.publish_authorization(&policy(3)).unwrap();
            assert_eq!(
                journal.accept_inner(&frame(), Some(&[10; 32])).unwrap(),
                ReplayJournalReceipt {
                    replayed: true,
                    ..accepted.clone()
                }
            );
        }
    }
}

#[test]
fn unrelated_policy_revision_does_not_invalidate_an_unchanged_assignment() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    journal.publish_authorization(&policy(1)).unwrap();
    journal.publish_authorization(&policy(100)).unwrap();
    let receipt = journal.accept_inner(&frame(), Some(&[10; 32])).unwrap();
    assert!(receipt.accepted && !receipt.searchable);
    assert_eq!(
        journal.head().unwrap().binding.unwrap().topology_revision,
        5
    );
}

#[test]
fn invalid_publication_cannot_replace_an_existing_revocation() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let mut denied = policy(1);
    denied.enabled = false;
    journal.publish_authorization(&denied).unwrap();
    for case in 0..7 {
        let mut invalid = policy(2);
        match case {
            0 => invalid.contract_version = 2,
            1 => invalid.source_residency = 0,
            2 => invalid.target_residency = 3,
            3 => invalid.source_write_epoch = 0,
            4 => invalid.source_history_id.clear(),
            5 => invalid.source_certificate_sha256.push(vec![10; 32]),
            6 => invalid.source_certificate_sha256[0].clear(),
            _ => unreachable!(),
        }
        assert_eq!(
            journal.publish_authorization(&invalid).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            journal
                .accept_inner(&frame(), Some(&[10; 32]))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}

#[test]
fn old_local_history_cannot_be_relabelled_as_authorized() {
    let dir = Directory::new();
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    journal.accept(&frame()).unwrap();
    assert_eq!(
        journal
            .publish_authorization(&policy(1))
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(journal.head().unwrap().format_version, FORMAT);
}

#[test]
fn missing_policy_or_format_downgrade_never_restores_local_access() {
    for case in 0..2 {
        let dir = Directory::new();
        let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
        journal.publish_authorization(&policy(1)).unwrap();
        drop(journal);
        let db = Database::open(dir.path()).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut meta = tx.open_table(META).unwrap();
            if case == 0 {
                meta.remove("admission").unwrap();
            } else {
                let mut header: ReplayJournalHeader =
                    decode(meta.get("header").unwrap().unwrap().value()).unwrap();
                header.format_version = FORMAT;
                meta.insert("header", header.encode_to_vec().as_slice())
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        drop(db);
        assert_eq!(
            ReplayJournal::open(&dir.path(), binding())
                .err()
                .unwrap()
                .code(),
            tonic::Code::DataLoss
        );
    }
}

#[test]
fn committed_revocation_fences_all_concurrent_deliveries() {
    let dir = Directory::new();
    let journal = std::sync::Arc::new(ReplayJournal::create(&dir.path(), binding()).unwrap());
    journal.publish_authorization(&policy(1)).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(9));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let journal = journal.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                journal.accept_inner(&frame(), Some(&[10; 32]))
            })
        })
        .collect();
    let mut revoked = policy(2);
    revoked.enabled = false;
    journal.publish_authorization(&revoked).unwrap();
    barrier.wait();
    for t in threads {
        assert_eq!(
            t.join().unwrap().unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn transport_certificate_is_required_and_ca_membership_is_not_a_grant() {
    use crate::{
        node::{NodeConfig, NodeServiceImpl},
        pb::{
            node_service_client::NodeServiceClient, node_service_server::NodeServiceServer,
            HealthRequest,
        },
        security::{ClientTls, ServerTls},
    };
    let dir = Directory::new();
    let journal = std::sync::Arc::new(ReplayJournal::create(&dir.path(), binding()).unwrap());
    let mut fake = tonic::Request::new(frame());
    fake.metadata_mut()
        .insert("authorization", "Bearer claimed-authority".parse().unwrap());
    fake.metadata_mut().insert(
        "x-peer-certificate-sha256",
        "claimed-certificate".parse().unwrap(),
    );
    assert_eq!(
        journal.accept_authenticated(&fake).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    let mut grant = policy(1); // Deliberately grants a different certificate.
    journal.publish_authorization(&grant).unwrap();
    let pem = |name: &str| {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/certs")
                .join(name),
        )
        .unwrap()
    };
    let tls = ServerTls {
        cert_pem: pem("server.pem"),
        key_pem: pem("server.key.pem"),
        client_ca_pem: Some(pem("ca.pem")),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let receiver = journal.clone();
    // A fixture interceptor supplies a fixed frame after TLS verification. No
    // production replay RPC is exposed by this test or by the journal module.
    let service = NodeServiceServer::with_interceptor(
        NodeServiceImpl::new(None, NodeConfig::default()),
        move |request: tonic::Request<()>| {
            let (metadata, extensions, _) = request.into_parts();
            let request = tonic::Request::from_parts(metadata, extensions, frame());
            receiver.accept_authenticated(&request)?;
            let (metadata, extensions, _) = request.into_parts();
            Ok(tonic::Request::from_parts(metadata, extensions, ()))
        },
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tls.server_config(true))
            .unwrap()
            .add_service(service)
            .serve_with_incoming(crate::harness::nodelay_incoming(listener)),
    );
    let client_tls = ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: Some((pem("client.pem"), pem("client.key.pem"))),
        domain: Some("localhost".into()),
    };
    let endpoint = crate::security::apply_client_tls(
        tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
            .unwrap()
            .timeout(std::time::Duration::from_secs(5)),
        Some(&client_tls),
    )
    .unwrap();
    let mut client = NodeServiceClient::new(endpoint.connect().await.unwrap());
    assert_eq!(
        client
            .health(HealthRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
    grant.revision = 2;
    // SHA-256 of tests/certs/client.pem's DER, independently computed.
    grant.source_certificate_sha256 = vec![vec![
        105, 67, 234, 227, 207, 215, 184, 118, 225, 134, 1, 237, 182, 86, 236, 85, 89, 130, 15, 19,
        52, 225, 159, 190, 135, 83, 48, 119, 190, 22, 228, 230,
    ]];
    journal.publish_authorization(&grant).unwrap();
    client.health(HealthRequest::default()).await.unwrap();
    client.health(HealthRequest::default()).await.unwrap();
    assert_eq!(journal.head().unwrap().accepted_sequence, 1);
    grant.revision = 3;
    grant.enabled = false;
    journal.publish_authorization(&grant).unwrap();
    // Reusing the live authenticated channel must not cache a former grant.
    assert_eq!(
        client
            .health(HealthRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    server.abort();
    let _ = server.await;
}
