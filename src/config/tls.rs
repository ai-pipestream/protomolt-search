//! The security surface: listener TLS, the outbound client material, bearer
//! principals, and the UDP datagram key. Files are read here so a missing
//! certificate refuses at startup, not at the first connection.

use std::path::PathBuf;
use std::sync::Arc;

use super::file::FileConfig;
use super::option::Options;
use crate::security::{ClientTls, Principals, ServerTls, UdpKey};

/// Every security option a serving process resolves.
pub(super) struct Security {
    pub(super) tls: Option<ServerTls>,
    pub(super) client_tls: Option<ClientTls>,
    pub(super) allow_plaintext: bool,
    pub(super) principals: Option<Arc<Principals>>,
    pub(super) udp_hmac_key: Option<UdpKey>,
}

fn path(o: Options, flag: &str, file: Option<&str>) -> Option<PathBuf> {
    o.text(flag, file).map(PathBuf::from)
}

/// Listener TLS: a certificate and key, plus the cluster CA client
/// certificates chain to.
fn server_tls(o: Options, file: &FileConfig) -> Result<Option<ServerTls>, String> {
    let cert = path(o, "tls-cert", file.tls_cert.as_deref());
    let key = path(o, "tls-key", file.tls_key.as_deref());
    let client_ca = path(o, "tls-client-ca", file.tls_client_ca.as_deref());
    match (cert, key) {
        (Some(cert), Some(key)) => Ok(Some(ServerTls::load(&cert, &key, client_ca.as_deref())?)),
        (None, None) if client_ca.is_some() => {
            Err("--tls-client-ca needs --tls-cert and --tls-key".to_string())
        }
        (None, None) => Ok(None),
        _ => Err("TLS needs both --tls-cert and --tls-key".to_string()),
    }
}

/// What this process presents and trusts on cluster-internal channels.
fn client_tls(o: Options, file: &FileConfig) -> Result<Option<ClientTls>, String> {
    let ca = path(o, "tls-ca", file.tls_ca.as_deref());
    let cert = path(o, "tls-client-cert", file.tls_client_cert.as_deref());
    let key = path(o, "tls-client-key", file.tls_client_key.as_deref());
    let domain = o.text("tls-domain", file.tls_domain.as_deref());
    match ca {
        Some(ca) => Ok(Some(ClientTls::load(
            &ca,
            cert.as_deref(),
            key.as_deref(),
            domain,
        )?)),
        None if cert.is_some() || key.is_some() || domain.is_some() => {
            Err("--tls-client-cert / --tls-client-key / --tls-domain need --tls-ca".to_string())
        }
        None => Ok(None),
    }
}

/// The listener TLS and the client material, the pair the local directory
/// commands need as well as the serving roles.
pub(super) fn parse_tls(
    o: Options,
    file: &FileConfig,
) -> Result<(Option<ServerTls>, Option<ClientTls>), String> {
    Ok((server_tls(o, file)?, client_tls(o, file)?))
}

pub(super) fn resolve_security(o: Options, file: &FileConfig) -> Result<Security, String> {
    let (tls, client_tls) = parse_tls(o, file)?;
    let principals = path(o, "bearer-tokens", file.bearer_tokens.as_deref())
        .map(|path| Principals::load(&path).map(Arc::new))
        .transpose()?;
    let udp_hmac_key = path(o, "udp-hmac-key", file.udp_hmac_key.as_deref())
        .map(|path| UdpKey::load(&path))
        .transpose()?;
    Ok(Security {
        tls,
        client_tls,
        allow_plaintext: o.switch_or_bare("allow-plaintext", file.allow_plaintext, false),
        principals,
        udp_hmac_key,
    })
}
