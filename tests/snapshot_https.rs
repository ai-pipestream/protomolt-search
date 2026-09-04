//! The `url` snapshot source over HTTPS (`docs/snapshots.md`): the node's
//! fetcher trusts the cluster CA installed with `--tls-ca` beside the
//! public roots, so a repository behind an internal certificate is
//! reachable, with the same `Range` resume and bearer as plain HTTP. Its
//! own test binary, because the CA is installed process-wide and every
//! node channel in the process then runs TLS.

mod common;

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common::files::{FileServer, DROP_AFTER};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::install_snapshot_from_request::Source;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, FlushRequest, HealthRequest, InstallSnapshotFromRequest,
};
use pipestream_search::security::{install_client_tls, ClientTls, ServerTls};
use pipestream_search::snapshot::{export_snapshot, install_snapshot_from};
use pipestream_search::snapshot_repository::RepositoryManifest;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tonic::Code;

fn pem(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/certs")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("snapshot_https_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn client_tls() -> ClientTls {
    ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: Some((pem("client.pem"), pem("client.key.pem"))),
        domain: Some("localhost".to_string()),
    }
}

/// A TLS node listener from the test certificates (mTLS, like the
/// serving binary's node listeners).
async fn serve_tls_node(node: NodeServiceImpl) -> String {
    let tls = ServerTls {
        cert_pem: pem("server.pem"),
        key_pem: pem("server.key.pem"),
        client_ca_pem: Some(pem("ca.pem")),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tls.server_config(true))
            .unwrap()
            .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    format!("https://127.0.0.1:{}", addr.port())
}

/// The file server behind TLS with the given identity from the test
/// certificates.
async fn serve_tls_files(server: Arc<FileServer>, cert: &str, key: &str) -> u16 {
    use rustls_pki_types::pem::PemObject;
    let certs: Vec<rustls_pki_types::CertificateDer<'static>> =
        rustls_pki_types::CertificateDer::pem_slice_iter(&pem(cert))
            .collect::<Result<_, _>>()
            .unwrap();
    let key = rustls_pki_types::PrivateKeyDer::from_pem_slice(&pem(key)).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let server = Arc::clone(&server);
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(stream) = acceptor.accept(stream).await {
                    server.handle(stream).await;
                }
            });
        }
    });
    port
}

fn config(index_path: PathBuf) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::SingleImage,
        wal: true,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_https_source_trusts_the_cluster_ca_and_resumes_with_range() {
    install_client_tls(Some(client_tls()));
    let dir = tempdir("https");
    let source =
        serve_tls_node(NodeServiceImpl::open(config(dir.join("source.tv")), None, false).unwrap())
            .await;
    let endpoint = Endpoint::from_shared(source.clone())
        .unwrap()
        .tls_config(client_tls().client_config())
        .unwrap();
    let mut client = NodeServiceClient::new(endpoint.connect_lazy());
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        for i in 0..400 {
            tx.send(AddDocumentsRequest {
                text: format!(
                    "document {i} about the court and the appeal number {}",
                    i * 7
                ),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
    });
    assert_eq!(
        client
            .add_documents(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner()
            .added,
        400
    );
    let corpus = pipestream_search::harness::unit_vectors(400, 128, 0x5EED_CA11);
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        tx.send(AddVectorsRequest {
            vectors: corpus,
            dim: 128,
        })
        .await
        .unwrap();
    });
    assert_eq!(
        client
            .add_vectors(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner()
            .added,
        400
    );
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    let repo_dir = dir.join("repo");
    let export = export_snapshot(&source, &repo_dir).await.unwrap();
    let manifest = RepositoryManifest::from_pb(export.manifest.as_ref().unwrap()).unwrap();
    assert!(
        manifest
            .artifacts
            .iter()
            .any(|a| a.bytes as usize > DROP_AFTER),
        "the fixture must exceed the drop point"
    );
    let server = FileServer::new(repo_dir.clone(), Some("nas-token"), true);
    let port = serve_tls_files(Arc::clone(&server), "server.pem", "server.key.pem").await;

    let target =
        serve_tls_node(NodeServiceImpl::open(config(dir.join("target.tv")), None, false).unwrap())
            .await;
    // Under the cluster CA and the certificate's name: the manifest and
    // every artifact come over TLS, the interrupted one resumes.
    let installed = install_snapshot_from(
        &target,
        InstallSnapshotFromRequest {
            source: Some(Source::Url(format!("https://localhost:{port}"))),
            expected_manifest_sha256: export.manifest_sha256.clone(),
            bearer_token: "nas-token".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(installed.num_documents, 400);
    assert!(server.range_requests.load(Ordering::Relaxed) >= 1);
    let health = NodeServiceClient::new(
        Endpoint::from_shared(target.clone())
            .unwrap()
            .tls_config(client_tls().client_config())
            .unwrap()
            .connect_lazy(),
    )
    .health(HealthRequest {})
    .await
    .unwrap()
    .into_inner();
    assert_eq!(health.document_slots, 400);
    // A repository under a certificate from another CA fails the
    // handshake, and the refusal names the certificate, not a status.
    let foreign = FileServer::new(repo_dir.clone(), None, false);
    let foreign_port = serve_tls_files(foreign, "other-client.pem", "other-client.key.pem").await;
    let error = install_snapshot_from(
        &target,
        InstallSnapshotFromRequest {
            source: Some(Source::Url(format!("https://localhost:{foreign_port}"))),
            expected_manifest_sha256: String::new(),
            bearer_token: String::new(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    let message = error.message().to_ascii_lowercase();
    assert!(
        !message.contains("http ") && message.contains("certificate"),
        "{}",
        error.message()
    );
    // The wrong bearer over TLS is the same 401 as over plain HTTP.
    let error = install_snapshot_from(
        &target,
        InstallSnapshotFromRequest {
            source: Some(Source::Url(format!("https://localhost:{port}"))),
            expected_manifest_sha256: String::new(),
            bearer_token: "wrong".into(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    let _ = std::fs::remove_dir_all(&dir);
}
