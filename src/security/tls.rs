//! TLS material for listeners and cluster-internal channels, and the
//! process-wide client material the library's own channels consult.

use std::path::Path;

/// A listener's identity and, for cluster-internal listeners, the CA
/// client certificates must chain to.
#[derive(Clone)]
pub struct ServerTls {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    /// The CA that issues cluster-member client certificates. Node
    /// listeners require a client certificate from it; the coordinator
    /// listener accepts one and lets cluster control demand it.
    pub client_ca_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTls")
            .field("cert_pem", &self.cert_pem.len())
            .field("key_pem", &"<redacted>")
            .field("client_ca_pem", &self.client_ca_pem.as_ref().map(Vec::len))
            .finish()
    }
}

impl ServerTls {
    pub fn load(cert: &Path, key: &Path, client_ca: Option<&Path>) -> Result<Self, String> {
        Ok(ServerTls {
            cert_pem: read_pem(cert, "certificate")?,
            key_pem: read_pem(key, "private key")?,
            client_ca_pem: client_ca
                .map(|path| read_pem(path, "client CA"))
                .transpose()?,
        })
    }

    /// The tonic configuration: with `require_client` the listener
    /// refuses connections without a client certificate from the CA
    /// (node listeners); without it the certificate is accepted when
    /// offered and checked per call by cluster control.
    #[cfg(feature = "tls")]
    pub fn server_config(&self, require_client: bool) -> tonic::transport::ServerTlsConfig {
        let mut config = tonic::transport::ServerTlsConfig::new().identity(
            tonic::transport::Identity::from_pem(&self.cert_pem, &self.key_pem),
        );
        if let Some(ca) = &self.client_ca_pem {
            config = config
                .client_ca_root(tonic::transport::Certificate::from_pem(ca))
                .client_auth_optional(!require_client);
        }
        config
    }
}

/// What a cluster-internal client presents and trusts.
#[derive(Clone)]
pub struct ClientTls {
    pub ca_pem: Vec<u8>,
    /// The client certificate and key: this process's membership.
    pub identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    /// The name to verify server certificates against when the address
    /// is an IP literal the certificate names differently.
    pub domain: Option<String>,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTls")
            .field("ca_pem", &self.ca_pem.len())
            .field("identity", &self.identity_pem.is_some())
            .field("domain", &self.domain)
            .finish()
    }
}

impl ClientTls {
    pub fn load(
        ca: &Path,
        cert: Option<&Path>,
        key: Option<&Path>,
        domain: Option<String>,
    ) -> Result<Self, String> {
        let identity_pem = match (cert, key) {
            (Some(cert), Some(key)) => Some((
                read_pem(cert, "client certificate")?,
                read_pem(key, "client private key")?,
            )),
            (None, None) => None,
            _ => {
                return Err(
                    "a client identity needs both --tls-client-cert and --tls-client-key"
                        .to_string(),
                )
            }
        };
        Ok(ClientTls {
            ca_pem: read_pem(ca, "CA")?,
            identity_pem,
            domain,
        })
    }

    #[cfg(feature = "tls")]
    pub fn client_config(&self) -> tonic::transport::ClientTlsConfig {
        let mut config = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(&self.ca_pem));
        if let Some((cert, key)) = &self.identity_pem {
            config = config.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        if let Some(domain) = &self.domain {
            config = config.domain_name(domain.clone());
        }
        config
    }
}

fn read_pem(path: &Path, what: &str) -> Result<Vec<u8>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {what} {}: {e}", path.display()))?;
    if !bytes.windows(10).any(|w| w == b"-----BEGIN") {
        return Err(format!(
            "{what} {} is not PEM (no BEGIN block)",
            path.display()
        ));
    }
    Ok(bytes)
}

/// The process-wide client TLS material for cluster-internal channels
/// opened outside the coordinator (replica sync, snapshot install, the
/// calibration tools). The coordinator carries its own copy
/// explicitly; this is the fallback those helpers consult.
static PROCESS_CLIENT_TLS: std::sync::OnceLock<Option<ClientTls>> = std::sync::OnceLock::new();

/// Install the process-wide client TLS material once (from `main`).
pub fn install_client_tls(tls: Option<ClientTls>) {
    let _ = PROCESS_CLIENT_TLS.set(tls);
}

/// The installed process-wide client TLS material, if any.
pub fn process_client_tls() -> Option<&'static ClientTls> {
    PROCESS_CLIENT_TLS.get().and_then(Option::as_ref)
}

/// Process-wide client bearer token presented to authenticated `turbo-kserve`
/// and coordinator endpoints when no per-call override is supplied.
static PROCESS_CLIENT_BEARER: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

/// Install the process-wide client bearer token once (from `main` or
/// [`ToolClient::install`](super::ToolClient::install)).
pub fn install_client_bearer_token(token: Option<String>) {
    let normalized = token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let _ = PROCESS_CLIENT_BEARER.set(normalized);
}

/// Resolve the active client bearer token from installed process state,
/// `TURBO_BEARER_TOKEN_FILE`, or `TURBO_BEARER_TOKEN`.
pub fn process_client_bearer_token() -> Option<String> {
    if let Some(installed) = PROCESS_CLIENT_BEARER.get().and_then(Option::clone) {
        return Some(installed);
    }
    if let Ok(path) = std::env::var("TURBO_BEARER_TOKEN_FILE") {
        let trimmed_path = path.trim();
        if !trimmed_path.is_empty() {
            if let Ok(contents) = std::fs::read_to_string(trimmed_path) {
                let tok = contents.trim();
                if !tok.is_empty() {
                    return Some(tok.to_string());
                }
            }
        }
    }
    std::env::var("TURBO_BEARER_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The URL a client dials for `addr` under `tls`: `https://` with TLS
/// material, `http://` without, whatever scheme `addr` carried (a bare
/// `host:port` gets one). `tonic` applies a channel's TLS only to an
/// `https` URI, so an address normalized to `http://` (the `--nodes`
/// list, a shard map) would otherwise dial a TLS listener in plaintext.
pub fn secure_url(addr: &str, tls: Option<&ClientTls>) -> String {
    let bare = addr
        .split_once("://")
        .map_or(addr, |(_, rest)| rest)
        .trim_end_matches('/');
    let scheme = if tls.is_some() { "https" } else { "http" };
    format!("{scheme}://{bare}")
}

/// [`secure_url`] under the process-wide client TLS material.
pub fn process_secure_url(addr: &str) -> String {
    secure_url(addr, process_client_tls())
}

/// Apply the process-wide client TLS material to an endpoint, when
/// installed; a plaintext endpoint otherwise.
#[cfg(feature = "net")]
pub fn secure_endpoint(
    endpoint: tonic::transport::Endpoint,
) -> Result<tonic::transport::Endpoint, String> {
    apply_client_tls(endpoint, process_client_tls())
}

/// Apply the given client TLS material to an endpoint.
#[cfg(feature = "net")]
pub fn apply_client_tls(
    endpoint: tonic::transport::Endpoint,
    tls: Option<&ClientTls>,
) -> Result<tonic::transport::Endpoint, String> {
    match tls {
        None => Ok(endpoint),
        #[cfg(feature = "tls")]
        Some(tls) => endpoint
            .tls_config(tls.client_config())
            .map_err(|e| format!("client TLS: {e}")),
        #[cfg(not(feature = "tls"))]
        Some(_) => Err("this build has no TLS support (feature `tls` is off)".to_string()),
    }
}

/// Whether a listen address is loopback: the only place plaintext gRPC
/// and unsigned UDP are accepted without an explicit opt-in.
pub fn is_loopback(addr: &std::net::SocketAddr) -> bool {
    addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_url_follows_the_tls_material_not_the_address() {
        let tls = ClientTls {
            ca_pem: b"-----BEGIN CERTIFICATE-----".to_vec(),
            identity_pem: None,
            domain: None,
        };
        for addr in [
            "10.0.0.1:19300",
            "http://10.0.0.1:19300",
            "https://10.0.0.1:19300/",
        ] {
            assert_eq!(secure_url(addr, Some(&tls)), "https://10.0.0.1:19300");
            assert_eq!(secure_url(addr, None), "http://10.0.0.1:19300");
        }
    }
}
