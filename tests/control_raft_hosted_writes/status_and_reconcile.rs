//! Publication-status and startup reconciliation over the hosted Raft fixture.

use super::*;
use pipestream_search::node::{ManagedTarget, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::{DocumentPublicationTargetState, GetDocumentPublicationStatusRequest};

fn status_target() -> ManagedTarget {
    ManagedTarget {
        node: Arc::new(NodeServiceImpl::new(None, NodeConfig::default())),
        index_key: b"managed-index\0v1".to_vec(),
        stage: Default::default(),
    }
}

async fn hosted_service(
    cluster: &mut Voters,
    leader: u64,
    dir: &kit::TestDir,
) -> (
    Arc<RaftHost>,
    Arc<ActiveManagedCatalog>,
    HostedDocumentWriteService,
) {
    let catalog = Arc::new(bridge_active(cluster, leader, dir, "hosted-prepare").await);
    let host = take_host(cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap()
    .with_publication_targets(kit::COLLECTION, vec![status_target()])
    .unwrap();
    (host, catalog, service)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_status_reads_the_journal_under_ingest() {
    let _serial = serial();
    let config = slow_lease_config();
    let mut cluster = raft_kit::three_voters_with("hosted-status", &hosted_policy(), &config).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-status-catalog");
    let (host, _catalog, service) = hosted_service(&mut cluster, leader, &dir).await;
    let (mut client, server) = serve(service).await;
    let status = client
        .get_document_publication_status(auth(
            GetDocumentPublicationStatusRequest {
                collection: kit::COLLECTION.into(),
                accepted_sequence: 1,
                index_keys: vec![b"managed-index\0v1".to_vec()],
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!status.history_id.is_empty());
    assert_eq!(
        status.targets[0].state,
        DocumentPublicationTargetState::Pending as i32
    );
    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_driver_settles_unconfirmed_writes_after_failover() {
    let _serial = serial();
    let config = slow_lease_config();
    let lease = Duration::from_millis(config.admission_lease_ms);
    let mut cluster = raft_kit::three_voters_with("hosted-driver", &hosted_policy(), &config).await;
    let leader = cluster.leader().await;
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    let dir = kit::TestDir::new("hosted-driver-catalog");
    let (host, catalog, service) = hosted_service(&mut cluster, leader, &dir).await;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let driver = service
        .clone()
        .spawn_reconciler("alice".into(), shutdown_rx);
    let (mut client, server) = serve(service).await;
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    catalog.arm_postcommit_pause(lease * 3);
    let request = network_write(target.history_id, b"hosted-driver", b"driver-op", 0);
    let call = {
        let mut client = client.clone();
        tokio::spawn(async move { client.accept_document(auth(request, "alice")).await })
    };
    tokio::task::yield_now().await;
    host.isolate(others.iter().copied());
    for other in &others {
        let rest: Vec<u64> = cluster.nodes().into_iter().filter(|n| n != other).collect();
        cluster.host(*other).isolate(rest);
    }
    assert_eq!(call.await.unwrap().unwrap_err().code(), Code::Unavailable);
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    lead_again(&cluster, &host, &others).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pending = host
                .with_admission("alice", |admission| {
                    catalog.unconfirmed_operations(admission, 10)
                })
                .await
                .unwrap();
            if pending.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown_tx.send(true).unwrap();
    driver.await.unwrap().unwrap();
    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}
