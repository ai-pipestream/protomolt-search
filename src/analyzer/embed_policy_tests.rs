use super::*;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn refreshed_budget_preserves_remote_configuration() {
    let mut config = turbo_client::RemoteEndpointConfig::new("https://embed.example")
        .with_bearer_token("token")
        .with_priority(turbo_client::RemotePriority::Bulk);
    #[cfg(feature = "tls")]
    {
        config.tls = Some(turbo_client::RemoteTlsConfig {
            server_ca_pem: b"ca".to_vec(),
            client_identity_pem: Some((b"cert".to_vec(), b"key".to_vec())),
            domain_name: Some("embed.example".into()),
        });
    }
    config.default_timeout = Some(Duration::from_secs(10));
    let client = turbo_client::RemoteClient::new(config.clone());
    let deadline = EmbedDeadline::new(config.default_timeout).unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    let request = metadata::request(&client, "model", deadline).unwrap();
    assert_eq!(request.metadata().get("grpc-timeout").unwrap(), "4000000u");
    assert_eq!(
        request.metadata().get("authorization").unwrap(),
        "Bearer token"
    );
    assert_eq!(
        request.metadata().get("x-turbo-priority").unwrap(),
        "interactive"
    );
    assert_eq!(client.config().default_timeout, config.default_timeout);
    assert!(same_endpoint_identity(client.config(), &config));
    assert_eq!(client.config().default_priority, config.default_priority);
    config.default_timeout = None;
    let unlimited = turbo_client::RemoteClient::new(config);
    let request =
        metadata::request(&unlimited, "model", EmbedDeadline::new(None).unwrap()).unwrap();
    assert!(!request.metadata().contains_key("grpc-timeout"));
}
