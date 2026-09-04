//! The security surface (`docs/security.md`): TLS and mTLS on the
//! listeners, the coordinator's membership toward its nodes, bearer
//! principals and their quotas on the public surface, cluster-control
//! membership, the metrics listener alongside TLS, the embedded crate's
//! dependency gate, and the configuration refusals.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::mock::{start_mock_analysis, start_mock_analysis_delayed};
use pipestream_search::collections::CollectionSet;
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::cluster_control_client::ClusterControlClient;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, GetClusterPlanRequest, HealthRequest,
    RoutedIngestMappedRequest, RoutedMappedDocument,
};
use pipestream_search::security::{
    ClientTls, MeteredIngest, PrincipalConfig, Principals, ServerTls, ToolClient, UdpKey,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request};

fn pem(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/certs")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn server_tls(with_client_ca: bool) -> ServerTls {
    ServerTls {
        cert_pem: pem("server.pem"),
        key_pem: pem("server.key.pem"),
        client_ca_pem: with_client_ca.then(|| pem("ca.pem")),
    }
}

fn client_tls(identity: Option<(&str, &str)>) -> ClientTls {
    ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: identity.map(|(cert, key)| (pem(cert), pem(key))),
        domain: Some("localhost".to_string()),
    }
}

/// An endpoint over TLS with the given client material; `None` is a
/// plaintext endpoint.
fn endpoint(addr: &str, tls: Option<&ClientTls>) -> Endpoint {
    let endpoint = Endpoint::from_shared(addr.to_string()).unwrap();
    pipestream_search::security::apply_client_tls(endpoint, tls).unwrap()
}

type Handle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

/// A node behind a TLS listener that demands a client certificate.
async fn serve_node(config: NodeConfig, tls: &ServerTls) -> (String, Handle) {
    let node = NodeServiceImpl::new(None, config);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        Server::builder()
            .tls_config(tls.server_config(true))
            .unwrap()
            .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    (format!("https://{addr}"), handle)
}

/// A coordinator surface (and optionally cluster control) behind a TLS
/// listener that accepts a client certificate when offered.
async fn serve_coordinator(
    set: CollectionSet,
    control: Option<ClusterControlService>,
    tls: &ServerTls,
) -> (String, Handle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let max = pipestream_search::MAX_MESSAGE_BYTES;
    let handle = tokio::spawn(
        Server::builder()
            .tls_config(tls.server_config(false))
            .unwrap()
            .add_optional_service(control.map(|c| c.into_server(max)))
            .add_service(set.into_server(max))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    (format!("https://{addr}"), handle)
}

async fn ingest_over(endpoint: Endpoint, docs: &[&str]) -> u64 {
    let mut client = NodeServiceClient::new(endpoint.connect_lazy());
    let (tx, rx) = mpsc::channel(8);
    for text in docs {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added
}

fn bearer(req: Bm25SearchRequest, token: Option<&str>) -> Request<Bm25SearchRequest> {
    let mut request = Request::new(req);
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}

fn search(text: &str, k: u32) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.to_string(),
        k,
        ..Default::default()
    }
}

const CONSOLE: &str = "console-token-0123456789abcdef";
const BATCH: &str = "batch-token-0123456789abcdef";

fn principals() -> Arc<Principals> {
    Arc::new(
        Principals::from_configs(&[
            PrincipalConfig {
                name: "console".into(),
                token: CONSOLE.into(),
                max_k: 5,
                concurrency: 1,
                ingest_docs_per_sec: 2,
            },
            PrincipalConfig {
                name: "batch".into(),
                token: BATCH.into(),
                ..Default::default()
            },
        ])
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tls_node_admits_the_cluster_ca_and_rejects_the_rest() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, handle) = serve_node(
        NodeConfig {
            analysis_addr: Some(analysis),
            ..Default::default()
        },
        &server_tls(true),
    )
    .await;
    // Plaintext to a TLS listener: refused at the transport.
    let plain = format!("http://{}", addr.trim_start_matches("https://"));
    let mut client = NodeServiceClient::new(endpoint(&plain, None).connect_lazy());
    assert!(client.health(HealthRequest {}).await.is_err());
    // The cluster CA but no identity: the listener demands a certificate.
    let mut client =
        NodeServiceClient::new(endpoint(&addr, Some(&client_tls(None))).connect_lazy());
    assert!(client.health(HealthRequest {}).await.is_err());
    // An identity from another CA.
    let foreign = client_tls(Some(("other-client.pem", "other-client.key.pem")));
    let mut client = NodeServiceClient::new(endpoint(&addr, Some(&foreign)).connect_lazy());
    assert!(client.health(HealthRequest {}).await.is_err());
    // A member: served.
    let member = client_tls(Some(("client.pem", "client.key.pem")));
    let mut client = NodeServiceClient::new(endpoint(&addr, Some(&member)).connect_lazy());
    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.bm25_docs, 0);
    assert_eq!(
        ingest_over(endpoint(&addr, Some(&member)), &["court one"]).await,
        1
    );
    handle.abort();
    mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_coordinator_presents_its_identity_and_serves_bearer_clients() {
    // A delayed unary analysis keeps a query in flight long enough for
    // the concurrency quota to be observed; ingest streams are not delayed.
    let (analysis, mock, _probe) = start_mock_analysis_delayed(Duration::from_millis(900)).await;
    let (node, node_handle) = serve_node(
        NodeConfig {
            analysis_addr: Some(analysis.clone()),
            ..Default::default()
        },
        &server_tls(true),
    )
    .await;
    let member = client_tls(Some(("client.pem", "client.key.pem")));
    let docs: Vec<String> = (0..8).map(|i| format!("court opinion {i}")).collect();
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    assert_eq!(ingest_over(endpoint(&node, Some(&member)), &docs).await, 8);

    // The coordinator: its channels to the node carry the member identity.
    // The address is handed over as the configuration normalizes it
    // (`http://`); the scheme follows the material, not the address.
    let plain_addr = node.replacen("https://", "http://", 1);
    let coordinator = CoordinatorServiceImpl::new(vec![plain_addr])
        .with_bm25(Some(analysis), Default::default())
        .with_client_tls(member.clone());
    let set = CollectionSet::single(coordinator).with_principals(principals());
    let tls = server_tls(true);
    let (public, public_handle) = serve_coordinator(set, None, &tls).await;
    // A public client: TLS with the CA, no client certificate, a bearer.
    let anon = client_tls(None);
    let mut client = SearchServiceClient::new(endpoint(&public, Some(&anon)).connect_lazy());

    let error = client
        .bm25_search(bearer(search("court", 3), None))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    assert!(
        error.message().contains("missing authorization"),
        "{}",
        error.message()
    );
    let error = client
        .bm25_search(bearer(search("court", 3), Some("nope-nope-nope-nope-nope")))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    assert!(
        error.message().contains("not recognized"),
        "{}",
        error.message()
    );
    let hits = client
        .bm25_search(bearer(search("court", 3), Some(BATCH)))
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 3, "served end to end over TLS in both hops");

    // max_k: over the cap refuses by name; k = 0 keeps its meaning (the
    // coordinator default) and is judged as that value, never rewritten.
    let error = client
        .bm25_search(bearer(search("court", 6), Some(CONSOLE)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error.message().contains("k=6 exceeds its max_k=5"),
        "{}",
        error.message()
    );
    let error = client
        .bm25_search(bearer(search("court", 0), Some(CONSOLE)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error.message().contains("k is unset") && error.message().contains("max_k=5"),
        "{}",
        error.message()
    );
    let unlimited = client
        .bm25_search(bearer(search("court", 0), Some(BATCH)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unlimited.hits.len(), 8);

    // Concurrency 1: while one console request is in flight, a second
    // refuses by name; another principal is unaffected; afterwards the
    // slot is free again.
    let mut slow = SearchServiceClient::new(endpoint(&public, Some(&anon)).connect_lazy());
    let in_flight = tokio::spawn(async move {
        slow.bm25_search(bearer(search("court", 1), Some(CONSOLE)))
            .await
            .map(|r| r.into_inner().hits.len())
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    let error = client
        .bm25_search(bearer(search("court", 1), Some(CONSOLE)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error.message().contains("concurrency limit of 1"),
        "{}",
        error.message()
    );
    assert_eq!(
        client
            .bm25_search(bearer(search("court", 1), Some(BATCH)))
            .await
            .unwrap()
            .into_inner()
            .hits
            .len(),
        1
    );
    assert_eq!(in_flight.await.unwrap().unwrap(), 1);
    assert_eq!(
        client
            .bm25_search(bearer(search("court", 1), Some(CONSOLE)))
            .await
            .unwrap()
            .into_inner()
            .hits
            .len(),
        1
    );
    public_handle.abort();
    node_handle.abort();
    mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tool_reaches_a_tls_fleet_from_its_flags() {
    // The verifier, the driver, and the console take the same flags
    // (docs/security.md): --tls-ca and the client identity for the node
    // listeners, --bearer-token-file for the coordinator. Here the tool's
    // client type reaches a TLS node with the identity and a TLS
    // coordinator with the bearer; a file token and a literal both load.
    let (analysis, _mock) = start_mock_analysis().await;
    let (node, node_handle) = serve_node(
        NodeConfig {
            analysis_addr: Some(analysis.clone()),
            ..Default::default()
        },
        &server_tls(true),
    )
    .await;
    let dir = tempdir("tool-client");
    let token_file = dir.join("bearer.token");
    std::fs::write(&token_file, format!("{BATCH}\n")).unwrap();
    let certs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/certs");
    let args = |extra: &[String]| -> Vec<String> {
        let mut args = vec![
            "tool".to_string(),
            "--k=3".to_string(),
            format!("--tls-ca={}", certs.join("ca.pem").display()),
            format!("--tls-client-cert={}", certs.join("client.pem").display()),
            format!(
                "--tls-client-key={}",
                certs.join("client.key.pem").display()
            ),
            "--tls-domain=localhost".to_string(),
            // A listener flag the tool does not take is left alone.
            "--tls-cert=/nowhere/server.pem".to_string(),
        ];
        args.extend_from_slice(extra);
        args
    };
    let member = ToolClient::from_args(args(&[format!(
        "--bearer-token-file={}",
        token_file.display()
    )]))
    .unwrap();
    assert!(member.is_tls() && member.has_bearer());
    // A bare host:port dials https under TLS; a schemed address is rewritten.
    assert_eq!(member.url("127.0.0.1:1"), "https://127.0.0.1:1");
    assert_eq!(member.url("http://127.0.0.1:1/"), "https://127.0.0.1:1");
    assert_eq!(
        ToolClient::from_args(vec!["tool".to_string()])
            .unwrap()
            .url("127.0.0.1:1"),
        "http://127.0.0.1:1"
    );

    // The node listener demands the identity: the tool's channel carries it.
    let mut node_client = NodeServiceClient::new(member.connect(&node).await.unwrap());
    let health = node_client
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.num_vectors, 0);

    // The coordinator's public surface takes the bearer from the interceptor.
    let coordinator = CoordinatorServiceImpl::new(vec![node.clone()])
        .with_bm25(Some(analysis), Default::default())
        .with_client_tls(client_tls(Some(("client.pem", "client.key.pem"))));
    let set = CollectionSet::single(coordinator).with_principals(principals());
    let (public, public_handle) = serve_coordinator(set, None, &server_tls(true)).await;
    let mut client = SearchServiceClient::with_interceptor(
        member.connect(&public).await.unwrap(),
        member.bearer(),
    );
    let hits = client
        .bm25_search(search("court", 3))
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert!(hits.is_empty(), "an empty node answers with no hits");
    // Without a token the same client type is refused by name.
    let anon = ToolClient::from_args(args(&[])).unwrap();
    assert!(!anon.has_bearer());
    let mut client = SearchServiceClient::with_interceptor(
        member.connect(&public).await.unwrap(),
        anon.bearer(),
    );
    let error = client.bm25_search(search("court", 3)).await.unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    // A literal token works the same; a short one and a client identity
    // without a CA are refused before any connection.
    assert!(
        ToolClient::from_args(args(&[format!("--bearer-token={BATCH}")]))
            .unwrap()
            .has_bearer()
    );
    assert!(
        ToolClient::from_args(args(&["--bearer-token=short".to_string()]))
            .unwrap_err()
            .contains("at least 16 bytes")
    );
    assert!(ToolClient::from_args(vec![
        "tool".to_string(),
        "--tls-client-cert=/nowhere/client.pem".to_string()
    ])
    .unwrap_err()
    .contains("need --tls-ca"));

    public_handle.abort();
    node_handle.abort();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ingest_meter_ends_the_stream_at_the_rate_and_never_trims() {
    let principals = principals();
    let mut md = tonic::metadata::MetadataMap::new();
    md.insert(
        "authorization",
        format!("Bearer {CONSOLE}").parse().unwrap(),
    );
    let console = principals.authenticate(&md).unwrap();
    let document = || RoutedIngestMappedRequest {
        payload: Some(
            pipestream_search::pb::routed_ingest_mapped_request::Payload::Document(
                RoutedMappedDocument::default(),
            ),
        ),
    };
    let bind = RoutedIngestMappedRequest {
        payload: Some(
            pipestream_search::pb::routed_ingest_mapped_request::Payload::Bind(Default::default()),
        ),
    };
    // Two documents per second with one second of burst: the bind is
    // free, two documents pass, the third ends the stream by name.
    let inbound = tokio_stream::iter(
        vec![bind, document(), document(), document(), document()]
            .into_iter()
            .map(Ok::<_, tonic::Status>),
    );
    let mut metered = MeteredIngest::new(inbound, Some(console));
    let mut outcomes = Vec::new();
    while let Some(item) = metered.next().await {
        outcomes.push(
            item.map(|_| ())
                .map_err(|s| (s.code(), s.message().to_string())),
        );
    }
    assert_eq!(
        outcomes.len(),
        4,
        "bind, two documents, one refusal, then the end"
    );
    assert!(outcomes[..3].iter().all(Result::is_ok));
    let (code, message) = outcomes[3].clone().unwrap_err();
    assert_eq!(code, Code::ResourceExhausted);
    assert!(message.contains("exceeds its rate of 2"), "{message}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_control_demands_membership_and_metrics_stay_reachable() {
    let (analysis, mock) = start_mock_analysis().await;
    let (node, node_handle) = serve_node(
        NodeConfig {
            analysis_addr: Some(analysis.clone()),
            ..Default::default()
        },
        &server_tls(true),
    )
    .await;
    let member = client_tls(Some(("client.pem", "client.key.pem")));
    let coordinator = CoordinatorServiceImpl::new(vec![node.clone()])
        .with_bm25(Some(analysis), Default::default())
        .with_client_tls(member.clone());
    let control = ClusterControlService::new(DurableControlPlane::in_memory(ControlPolicy {
        lease_ms: 60_000,
        replication_factor: 1,
        split_rows: 1_000_000,
        merge_rows: 1_000,
        compact_segments: 8,
        compact_tombstone_ppm: 100_000,
        history_limit: 8,
    }))
    .with_client_cert_required(true);
    let (public, public_handle) = serve_coordinator(
        CollectionSet::single(coordinator),
        Some(control),
        &server_tls(true),
    )
    .await;
    // Without a client certificate: the call is refused by name, and a
    // bearer token would not change that.
    let anon = client_tls(None);
    let mut client = ClusterControlClient::new(endpoint(&public, Some(&anon)).connect_lazy());
    let error = client
        .get_cluster_plan(GetClusterPlanRequest::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    assert!(
        error.message().contains("not membership"),
        "{}",
        error.message()
    );
    // With one: served.
    let mut client = ClusterControlClient::new(endpoint(&public, Some(&member)).connect_lazy());
    let plan = client
        .get_cluster_plan(GetClusterPlanRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(plan.topology_generation, 0);
    // The metrics exporter is a plain HTTP listener of its own.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_addr = listener.local_addr().unwrap();
    tokio::spawn(pipestream_search::metrics::serve(listener, Vec::new()));
    let reply = tokio::task::spawn_blocking(move || {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(metrics_addr).unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: metrics\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).unwrap();
        reply
    })
    .await
    .unwrap();
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    public_handle.abort();
    node_handle.abort();
    mock.abort();
}

#[test]
fn the_embedded_runtime_carries_no_tls_stack() {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "tree",
            "--locked",
            "-e",
            "normal",
            "--prefix",
            "none",
            "-p",
            "protomolt-search-embedded",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo tree");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tree = String::from_utf8_lossy(&output.stdout);
    let offending: Vec<&str> = tree
        .lines()
        .filter(|line| {
            [
                "rustls",
                "tokio-rustls",
                "aws-lc-rs",
                "aws-lc-sys",
                "ring",
                "webpki",
            ]
            .iter()
            .any(|name| line.starts_with(&format!("{name} v")))
        })
        .collect();
    assert!(
        offending.is_empty(),
        "TLS crates in the embedded tree: {offending:?}"
    );
    assert!(tree.lines().any(|l| l.starts_with("pipestream-search v")));
}

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("security-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn configuration_refuses_plaintext_off_loopback_and_incomplete_material() {
    let dir = tempdir("config");
    let certs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/certs");
    let write = |name: &str, body: &str| {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path.display().to_string()
    };
    let parse = |file: &str| pipestream_search::config::parse(&[format!("--config={file}")]);
    let node_off_loopback =
        "role = \"node\"\n[[shards]]\nlisten = \"0.0.0.0:59300\"\nindex = \"/tmp/x.tv\"\n";
    let error = parse(&write("plain.toml", node_off_loopback)).unwrap_err();
    assert!(error.contains("--allow-plaintext"), "{error}");
    let cfg = parse(&write(
        "allowed.toml",
        &format!("allow_plaintext = true\n{node_off_loopback}"),
    ))
    .unwrap();
    assert!(cfg.allow_plaintext && cfg.tls.is_none());
    let loopback = parse(&write(
        "loop.toml",
        "role = \"node\"\n[[shards]]\nlisten = \"127.0.0.1:0\"\nindex = \"/tmp/x.tv\"\n",
    ))
    .unwrap();
    assert!(!loopback.allow_plaintext);

    let cert = certs.join("server.pem").display().to_string();
    let key = certs.join("server.key.pem").display().to_string();
    let ca = certs.join("ca.pem").display().to_string();
    let error = parse(&write(
        "half.toml",
        &format!("role = \"node\"\ntls_cert = \"{cert}\"\n[[shards]]\nlisten = \"127.0.0.1:0\"\nindex = \"/tmp/x.tv\"\n"),
    ))
    .unwrap_err();
    assert!(error.contains("both --tls-cert and --tls-key"), "{error}");
    let error = parse(&write(
        "node-no-ca.toml",
        &format!("role = \"node\"\ntls_cert = \"{cert}\"\ntls_key = \"{key}\"\n[[shards]]\nlisten = \"0.0.0.0:59300\"\nindex = \"/tmp/x.tv\"\n"),
    ))
    .unwrap_err();
    assert!(error.contains("--tls-client-ca"), "{error}");
    let error = parse(&write(
        "coord-no-ca.toml",
        &format!("role = \"coordinator\"\nnodes = [\"10.0.0.1:1\"]\ntls_cert = \"{cert}\"\ntls_key = \"{key}\"\ntls_client_ca = \"{ca}\"\n"),
    ))
    .unwrap_err();
    assert!(error.contains("--tls-ca"), "{error}");
    let error = parse(&write(
        "contradiction.toml",
        &format!("role = \"coordinator\"\nnodes = [\"10.0.0.1:1\"]\ntls_cert = \"{cert}\"\ntls_key = \"{key}\"\ntls_client_ca = \"{ca}\"\ntls_ca = \"{ca}\"\nallow_plaintext = true\n"),
    ))
    .unwrap_err();
    assert!(error.contains("contradicts"), "{error}");

    // The complete material, principals, and a UDP key all load.
    let tokens = write(
        "tokens.toml",
        "[[principals]]\nname = \"console\"\ntoken = \"console-token-0123456789\"\nmax_k = 10\n",
    );
    let udp = write("udp.key", "000102030405060708090a0b0c0d0e0f\n");
    let client_cert = certs.join("client.pem").display().to_string();
    let client_key = certs.join("client.key.pem").display().to_string();
    let cfg = parse(&write(
        "full.toml",
        &format!(
            "role = \"both\"\nnodes = [\"10.0.0.1:1\"]\ntls_cert = \"{cert}\"\ntls_key = \"{key}\"\n\
             tls_client_ca = \"{ca}\"\ntls_ca = \"{ca}\"\ntls_client_cert = \"{client_cert}\"\n\
             tls_client_key = \"{client_key}\"\ntls_domain = \"localhost\"\n\
             bearer_tokens = \"{tokens}\"\nudp_hmac_key = \"{udp}\"\n\
             [[shards]]\nlisten = \"0.0.0.0:59300\"\nindex = \"/tmp/x.tv\"\n"
        ),
    ))
    .unwrap();
    assert!(cfg.tls.as_ref().is_some_and(|t| t.client_ca_pem.is_some()));
    assert!(cfg
        .client_tls
        .as_ref()
        .is_some_and(|t| t.identity_pem.is_some() && t.domain.as_deref() == Some("localhost")));
    assert_eq!(cfg.principals.as_ref().map(|p| p.len()), Some(1));
    assert!(cfg.udp_hmac_key.is_some());
    assert_eq!(
        UdpKey::from_bytes(b"000102030405060708090a0b0c0d0e0f")
            .unwrap()
            .tag(b"x"),
        cfg.udp_hmac_key.as_ref().unwrap().tag(b"x")
    );
    let _ = std::fs::remove_dir_all(&dir);
}
