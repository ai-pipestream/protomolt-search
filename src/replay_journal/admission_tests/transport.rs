//! Admission over a verified mTLS transport.

use super::*;
use crate::{
    node::{NodeConfig, NodeServiceImpl},
    pb::{
        node_service_client::NodeServiceClient, node_service_server::NodeServiceServer,
        HealthRequest,
    },
    security::{ClientTls, ServerTls},
};
use std::sync::Arc;
use tonic::transport::Channel;

type Server = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

fn pem(name: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/certs")
            .join(name),
    )
    .unwrap()
}

/// A node behind mTLS whose every request first offers a fixed frame to
/// `receiver`. A fixture interceptor supplies the frame after TLS
/// verification. No production replay RPC is exposed by this test or by the
/// journal module.
async fn serve_gated(receiver: Arc<ReplayJournal>) -> (std::net::SocketAddr, Server) {
    let tls = ServerTls {
        cert_pem: pem("server.pem"),
        key_pem: pem("server.key.pem"),
        client_ca_pem: Some(pem("ca.pem")),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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
    (addr, server)
}

/// A client presenting `tests/certs/client.pem`.
async fn mtls_client(addr: std::net::SocketAddr) -> NodeServiceClient<Channel> {
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
    NodeServiceClient::new(endpoint.connect().await.unwrap())
}

async fn refusal(client: &mut NodeServiceClient<Channel>) -> tonic::Code {
    client
        .health(HealthRequest::default())
        .await
        .unwrap_err()
        .code()
}

#[tokio::test]
async fn transport_certificate_is_required_and_ca_membership_is_not_a_grant() {
    let dir = Directory::new();
    let journal = Arc::new(ReplayJournal::create(&dir.path(), binding()).unwrap());
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
    let (addr, server) = serve_gated(journal.clone()).await;
    let mut client = mtls_client(addr).await;
    assert_eq!(refusal(&mut client).await, tonic::Code::PermissionDenied);
    assert_eq!(journal.head().unwrap().accepted_sequence, 0);
    grant.revision = 2;
    // SHA-256 of tests/certs/client.pem's DER, independently computed.
    grant.source_certificate_sha256 = vec![vec![
        105, 67, 234, 227, 207, 215, 184, 118, 225, 134, 1, 237, 182, 86, 236, 85, 89, 130, 15, 19,
        52, 225, 159, 190, 135, 83, 48, 119, 190, 22, 228, 230,
    ]];
    let _session = grant_live(&journal, &grant);
    client.health(HealthRequest::default()).await.unwrap();
    client.health(HealthRequest::default()).await.unwrap();
    assert_eq!(journal.head().unwrap().accepted_sequence, 1);
    journal.suspend_authorization().unwrap();
    assert_eq!(refusal(&mut client).await, tonic::Code::Unavailable);
    let _replacement = grant_live(&journal, &grant);
    client.health(HealthRequest::default()).await.unwrap();
    grant.revision = 3;
    grant.enabled = false;
    journal.publish_authorization(&grant).unwrap();
    // Reusing the live authenticated channel must not cache a former grant.
    assert_eq!(refusal(&mut client).await, tonic::Code::PermissionDenied);
    server.abort();
    let _ = server.await;
}
