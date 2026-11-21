//! Follower catch-up of logged derived values: index-declared columns and
//! pre-materialized NLP columns replay byte for byte under the WAL replay
//! mark, and only there.

use crate::pair::{appended, key, Pair};
use pipestream_search::authorization::PolicyAuthority;
use pipestream_search::derived::Declaration;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::wal::wal_record::Op;
use pipestream_search::pb::{
    self, node_service_client::NodeServiceClient, DerivedColumn, DerivedColumns, DerivedDisclosure,
    MaterializeKind,
};
use pipestream_search::replication::{
    encode_replay_first_id, encode_stable_keys, STABLE_KEYS_METADATA, WAL_REPLAY_METADATA,
};
use pipestream_search::security::{PrincipalConfig, Principals};
use pipestream_search::values::fnv1a64_bytes;
use std::sync::Arc;

/// The NLP materialization id a client that ran NLP upstream stamps.
const NLP_MARK: &str = "v1:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

fn declaration() -> Arc<Declaration> {
    let column = |name: &str, kind: MaterializeKind, expression: &str| DerivedColumn {
        name: name.into(),
        expression: expression.into(),
        kind: kind as i32,
        disclosure: DerivedDisclosure::Inputs as i32,
    };
    Arc::new(
        Declaration::compile(&DerivedColumns {
            columns: vec![
                column(
                    "key_bucket",
                    MaterializeKind::U64,
                    "hash.fnv64(stable_key()) % 64u",
                ),
                column("usd_cents", MaterializeKind::F64, "money['USD'] * 100.0"),
            ],
        })
        .unwrap(),
    )
}

/// A document carrying an upstream NLP money column and its mark.
fn priced(i: usize) -> pb::AddDocumentsRequest {
    pb::AddDocumentsRequest {
        text: format!("invoice {i} paid in dollars"),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        map_numerics: vec![pb::MapNumericEntry {
            field: "money".into(),
            key: "USD".into(),
            value: i as f64 + 0.25,
        }],
        nlp_materialization: NLP_MARK.into(),
        ..Default::default()
    }
}

fn unsigned(doc: &pb::AddDocumentsRequest, field: &str) -> Option<u64> {
    doc.unsigned_integers
        .iter()
        .find(|value| value.field == field)
        .map(|value| value.value)
}

fn numeric(doc: &pb::AddDocumentsRequest, field: &str) -> Option<f64> {
    doc.numerics
        .iter()
        .find(|value| value.field == field)
        .map(|value| value.value)
}

#[tokio::test]
async fn logged_derived_and_nlp_columns_replay_byte_for_byte_in_two_streams() {
    const ROWS: usize = 520;
    let declaration = declaration();
    let mut pair = Pair::start_derived("derived", declaration.clone()).await;
    let keys: Vec<Vec<u8>> = (0..ROWS).map(key).collect();
    pair.keyed((0..ROWS).map(priced).collect(), &keys).await;
    let start = pair.cursor.clone();
    pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddDocuments", "AddDocuments"]);
    let replicated = appended(&pair.cursor.replica).await;
    assert_eq!(replicated.len(), ROWS);
    for (row, op) in replicated.into_iter().enumerate() {
        let Op::AddDocuments(add) = op else {
            panic!("row {row} is not a document record")
        };
        assert_eq!(add.stable_routing_keys, [key(row)]);
        let doc = &add.documents[0];
        assert_eq!(doc.derived_fingerprint, declaration.fingerprint());
        assert_eq!(doc.nlp_materialization, NLP_MARK);
        assert_eq!(
            unsigned(doc, "key_bucket"),
            Some(fnv1a64_bytes(&key(row)) % 64),
            "row {row}"
        );
        assert_eq!(numeric(doc, "usd_cents"), Some((row as f64 + 0.25) * 100.0));
    }
}

fn replay_request<S>(
    stream: S,
    key: &[u8],
    first_id: Option<u64>,
    principal: Option<&str>,
) -> tonic::Request<S> {
    let mut request = tonic::Request::new(stream);
    let keys = encode_stable_keys(&[key.to_vec()]).unwrap();
    let metadata = request.metadata_mut();
    metadata.insert_bin(
        STABLE_KEYS_METADATA,
        tonic::metadata::MetadataValue::from_bytes(&keys),
    );
    if let Some(first_id) = first_id {
        metadata.insert_bin(
            WAL_REPLAY_METADATA,
            tonic::metadata::MetadataValue::from_bytes(&encode_replay_first_id(first_id)),
        );
    }
    if let Some(principal) = principal {
        metadata.insert(
            "authorization",
            format!("Bearer {principal}-token-0123456789")
                .parse()
                .unwrap(),
        );
    }
    request
}

/// Send `doc` straight to `addr` under `key`, marked as WAL replay from
/// `first_id` when one is given.
async fn send(
    addr: &str,
    doc: pb::AddDocumentsRequest,
    key: &[u8],
    first_id: Option<u64>,
    principal: Option<&str>,
) -> Result<pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let request = replay_request(tokio_stream::iter([doc]), key, first_id, principal);
    client.add_documents(request).await.map(|r| r.into_inner())
}

#[tokio::test]
async fn a_logged_row_outside_a_replay_stream_or_off_the_tip_is_refused() {
    let mut pair = Pair::start_derived("derived-refused", declaration()).await;
    pair.keyed(vec![priced(0), priced(1)], &[key(0), key(1)])
        .await;
    pair.source.flush(pb::FlushRequest {}).await.unwrap();
    let Some(Op::AddDocuments(logged)) = appended(&pair.cursor.primary).await.into_iter().next()
    else {
        panic!("the primary logged no document")
    };
    let row = logged.documents[0].clone();
    let replica = pair.cursor.replica.clone();
    let forged = send(&replica, row.clone(), &key(0), None, None)
        .await
        .unwrap_err();
    assert_eq!(forged.code(), tonic::Code::InvalidArgument);
    assert!(
        forged.message().contains("stamps derived fingerprint"),
        "{forged}"
    );
    let off_tip = send(&replica, row.clone(), &key(0), Some(1), None)
        .await
        .unwrap_err();
    assert_eq!(off_tip.code(), tonic::Code::FailedPrecondition);
    assert!(
        off_tip
            .message()
            .contains("WAL replay starts at document 1"),
        "{off_tip}"
    );
    let mut health = NodeServiceClient::connect(replica.clone()).await.unwrap();
    let before = health
        .health(pb::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(before.document_slots, 0, "no refused row was allocated");
    let start = pair.cursor.clone();
    pair.sync(&start).await;
}

fn access_policy() -> pb::AccessPolicy {
    let grants = [
        ("writer", pb::AccessAction::Ingest),
        ("admin", pb::AccessAction::Admin),
    ]
    .map(|(principal, action)| pb::CollectionGrant {
        principal: principal.into(),
        workspace: "workspace".into(),
        collection: "a".into(),
        actions: vec![action as i32],
        ..Default::default()
    });
    pb::AccessPolicy {
        format_version: 3,
        revision: 1,
        resources: vec![pb::CollectionResource {
            workspace: "workspace".into(),
            collection: "a".into(),
        }],
        grants: grants.into(),
    }
}

fn authorized_service(declaration: Arc<Declaration>) -> (NodeServiceImpl, Arc<PolicyAuthority>) {
    let authority = Arc::new(PolicyAuthority::new(access_policy()).unwrap());
    let principals = Principals::from_configs(&["writer", "admin"].map(|name| PrincipalConfig {
        name: name.into(),
        token: format!("{name}-token-0123456789"),
        ..Default::default()
    }))
    .unwrap()
    .with_authorizer(authority.clone());
    let node = NodeServiceImpl::new(
        None,
        NodeConfig {
            collection: "a".into(),
            analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            map_numeric_fields: vec!["money".into()],
            derived: Some(declaration),
            ..Default::default()
        },
    )
    .with_principals(Arc::new(principals));
    (node, authority)
}

type ServerHandle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

async fn authorized_node(
    declaration: Arc<Declaration>,
) -> (String, ServerHandle, Arc<PolicyAuthority>) {
    let (node, authority) = authorized_service(declaration);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (addr, server, authority)
}

fn cert(name: &str) -> Vec<u8> {
    std::fs::read(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/certs")
            .join(name),
    )
    .unwrap()
}

async fn authorized_member_node(declaration: Arc<Declaration>) -> (String, ServerHandle) {
    let (node, _) = authorized_service(declaration);
    let tls = pipestream_search::security::ServerTls {
        cert_pem: cert("server.pem"),
        key_pem: cert("server.key.pem"),
        client_ca_pem: Some(cert("ca.pem")),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("https://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tls.server_config(true))
            .unwrap()
            .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (addr, server)
}

async fn logged_row(pair: &mut Pair, tag: &str) -> pb::AddDocumentsRequest {
    pair.keyed(vec![priced(0)], &[key(0)]).await;
    pair.source.flush(pb::FlushRequest {}).await.unwrap();
    let ops = appended(&pair.cursor.primary).await;
    let Some(Op::AddDocuments(add)) = ops.into_iter().next() else {
        panic!("{tag}: primary logged no document")
    };
    add.documents[0].clone()
}

#[tokio::test]
async fn a_non_derived_primary_row_fails_closed_on_a_derived_replica() {
    let mut pair = Pair::start("non-derived-primary", false).await;
    let row = logged_row(&mut pair, "non-derived").await;
    assert!(row.derived_fingerprint.is_empty());
    let (addr, server, _) = authorized_node(declaration()).await;
    let error = send(&addr, row, &key(0), Some(0), Some("admin"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    server.abort();
}

#[tokio::test]
async fn a_logged_materialize_spec_is_refused_without_evaluation() {
    let declaration = declaration();
    let mut pair = Pair::start_derived("logged-materialize", declaration.clone()).await;
    let mut row = logged_row(&mut pair, "logged materialize").await;
    row.materialize = Some(pb::MaterializeSpec {
        columns: vec![pb::MaterializedColumn {
            name: "extra".into(),
            expression: "1.0".into(),
            kind: MaterializeKind::F64 as i32,
        }],
    });
    let (addr, server, _) = authorized_node(declaration).await;
    let error = send(&addr, row, &key(0), Some(0), Some("admin"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(
        error.message().contains("not a materialize spec"),
        "{error}"
    );
    server.abort();
}

#[tokio::test]
async fn wal_replay_requires_admin_for_bearer_principals() {
    let declaration = declaration();
    let mut pair = Pair::start_derived("replay-authorization", declaration.clone()).await;
    let row = logged_row(&mut pair, "replay authorization").await;
    let (addr, server, _) = authorized_node(declaration).await;
    let anonymous = send(&addr, row.clone(), &key(0), Some(0), None)
        .await
        .unwrap_err();
    assert_eq!(
        anonymous.code(),
        tonic::Code::Unauthenticated,
        "{anonymous}"
    );
    let denied = send(&addr, row.clone(), &key(0), Some(0), Some("writer"))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied, "{denied}");
    let admin = send(&addr, row, &key(0), Some(0), Some("admin"))
        .await
        .unwrap();
    assert_eq!(admin.added, 1);
    server.abort();
}

#[tokio::test]
async fn wal_replay_accepts_a_verified_member_without_a_bearer_token() {
    let declaration = declaration();
    let mut pair = Pair::start_derived("replay-member", declaration.clone()).await;
    let row = logged_row(&mut pair, "replay member").await;
    let (addr, server) = authorized_member_node(declaration).await;
    let tls = pipestream_search::security::ClientTls {
        ca_pem: cert("ca.pem"),
        identity_pem: Some((cert("client.pem"), cert("client.key.pem"))),
        domain: Some("localhost".into()),
    };
    let endpoint = pipestream_search::security::apply_client_tls(
        tonic::transport::Endpoint::from_shared(addr).unwrap(),
        Some(&tls),
    )
    .unwrap();
    let mut client = NodeServiceClient::new(endpoint.connect().await.unwrap());
    let response = client
        .add_documents(replay_request(
            tokio_stream::iter([row]),
            &key(0),
            Some(0),
            None,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.added, 1);
    server.abort();
}

#[tokio::test]
async fn wal_replay_rechecks_permission_for_every_streamed_row() {
    let declaration = declaration();
    let mut pair = Pair::start_derived("replay-revocation", declaration.clone()).await;
    pair.keyed(vec![priced(0), priced(1)], &[key(0), key(1)])
        .await;
    pair.source.flush(pb::FlushRequest {}).await.unwrap();
    let rows: Vec<_> = appended(&pair.cursor.primary)
        .await
        .into_iter()
        .map(|op| match op {
            Op::AddDocuments(add) => add.documents[0].clone(),
            other => panic!("unexpected record {other:?}"),
        })
        .collect();
    let (addr, server, authority) = authorized_node(declaration).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut request = replay_request(stream, &key(0), Some(0), Some("admin"));
    let keys = encode_stable_keys(&[key(0), key(1)]).unwrap();
    request.metadata_mut().insert_bin(
        STABLE_KEYS_METADATA,
        tonic::metadata::MetadataValue::from_bytes(&keys),
    );
    let mut replay = client.clone();
    let call = tokio::spawn(async move { replay.add_documents(request).await });
    tx.send(rows[0].clone()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let health = client
                .health(pb::HealthRequest {})
                .await
                .unwrap()
                .into_inner();
            if health.document_slots == 1 {
                break;
            }
            assert_eq!(health.document_slots, 0);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut revoked = access_policy();
    revoked.revision = 2;
    revoked.grants.clear();
    authority.replace(revoked).unwrap();
    tx.send(rows[1].clone()).await.unwrap();
    drop(tx);
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied, "{error}");
    let health = client
        .health(pb::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        health.document_slots, 1,
        "the revoked second row was refused"
    );
    server.abort();
}
