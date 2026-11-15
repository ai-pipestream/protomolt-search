//! Deadline, security, and priority-lane policy for `turbo-kserve` calls.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::Status;

/// Converts an absolute Unix-epoch microsecond `finish_deadline_us` into a
/// remaining [`Duration`] for outgoing `grpc-timeout` propagation.
pub fn remaining_deadline_duration(finish_deadline_us: u64) -> Result<Option<Duration>, Status> {
    if finish_deadline_us == 0 {
        return Ok(None);
    }
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    if finish_deadline_us <= now_us {
        return Err(Status::deadline_exceeded(
            "request deadline expired before dispatching to turbo-kserve",
        ));
    }
    Ok(Some(Duration::from_micros(finish_deadline_us - now_us)))
}

/// Client mTLS and server CA material for `turbo-kserve` channels.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MtlsClientMaterial {
    /// PEM-encoded root CA bundle used to verify `turbo-kserve`.
    pub server_ca_pem: Vec<u8>,
    /// Optional `(client_cert_pem, client_key_pem)` pair for mutual TLS.
    pub client_identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    /// Optional explicit SNI / SAN server domain override.
    pub domain_name: Option<String>,
}

/// Remote `turbo-kserve` security, priority lane, and deadline policy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KserveCallPolicy {
    /// Bearer credential (`authorization: Bearer <token>`).
    pub bearer_token: Option<String>,
    /// Optional PEM-encoded server CA and client mTLS identity.
    pub mtls: Option<MtlsClientMaterial>,
}

impl KserveCallPolicy {
    /// Builds a [`turbo_client::RemoteEndpointConfig`] for `endpoint` with the
    /// requested priority lane and optional per-call timeout.
    #[cfg(feature = "net")]
    pub fn endpoint_config(
        &self,
        endpoint: &str,
        priority: turbo_client::RemotePriority,
        timeout: Option<Duration>,
    ) -> turbo_client::RemoteEndpointConfig {
        let mut cfg = turbo_client::RemoteEndpointConfig::new(endpoint).with_priority(priority);
        if let Some(tok) = &self.bearer_token {
            cfg = cfg.with_bearer_token(tok.clone());
        }
        if let Some(dur) = timeout {
            cfg = cfg.with_timeout(dur);
        }
        #[cfg(feature = "tls")]
        if let Some(mtls) = &self.mtls {
            cfg.tls = Some(turbo_client::RemoteTlsConfig {
                server_ca_pem: mtls.server_ca_pem.clone(),
                client_identity_pem: mtls.client_identity_pem.clone(),
                domain_name: mtls.domain_name.clone(),
            });
        }
        cfg
    }
}
