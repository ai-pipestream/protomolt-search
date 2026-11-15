//! TLS material for the listeners and the outbound client.

use std::path::PathBuf;

use super::file::FileConfig;
use super::option::Options;

/// The security surface (`docs/security.md`): the listener TLS and the
/// client material. Files are read here so a missing certificate
/// refuses at startup, not at the first connection.
pub(super) fn parse_tls(
    o: Options<'_>,
    file: &FileConfig,
) -> Result<
    (
        Option<crate::security::ServerTls>,
        Option<crate::security::ClientTls>,
    ),
    String,
> {
    let path = |flag: &str, file_value: Option<&str>| o.text(flag, file_value).map(PathBuf::from);
    let tls_cert = path("tls-cert", file.tls_cert.as_deref());
    let tls_key = path("tls-key", file.tls_key.as_deref());
    let tls_client_ca = path("tls-client-ca", file.tls_client_ca.as_deref());
    let tls = match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => Some(crate::security::ServerTls::load(
            &cert,
            &key,
            tls_client_ca.as_deref(),
        )?),
        (None, None) => {
            if tls_client_ca.is_some() {
                return Err("--tls-client-ca needs --tls-cert and --tls-key".to_string());
            }
            None
        }
        _ => return Err("TLS needs both --tls-cert and --tls-key".to_string()),
    };
    let tls_ca = path("tls-ca", file.tls_ca.as_deref());
    let tls_client_cert = path("tls-client-cert", file.tls_client_cert.as_deref());
    let tls_client_key = path("tls-client-key", file.tls_client_key.as_deref());
    let tls_domain = o.text("tls-domain", file.tls_domain.as_deref());
    let client_tls = match tls_ca {
        Some(ca) => Some(crate::security::ClientTls::load(
            &ca,
            tls_client_cert.as_deref(),
            tls_client_key.as_deref(),
            tls_domain,
        )?),
        None => {
            if tls_client_cert.is_some() || tls_client_key.is_some() || tls_domain.is_some() {
                return Err(
                    "--tls-client-cert / --tls-client-key / --tls-domain need --tls-ca".to_string(),
                );
            }
            None
        }
    };
    Ok((tls, client_tls))
}

/// TLS material, the plaintext override, bearer principals, and the UDP key.
pub(super) struct Security {
    pub(super) tls: Option<crate::security::ServerTls>,
    pub(super) client_tls: Option<crate::security::ClientTls>,
    pub(super) allow_plaintext: bool,
    pub(super) principals: Option<std::sync::Arc<crate::security::Principals>>,
    pub(super) udp_hmac_key: Option<crate::security::UdpKey>,
}

pub(super) fn resolve_security(o: Options<'_>, file: &FileConfig) -> Result<Security, String> {
    let (tls, client_tls) = parse_tls(o, file)?;
    Ok(Security {
        tls,
        client_tls,
        allow_plaintext: o.raised("allow-plaintext", file.allow_plaintext),
        principals: o
            .text("bearer-tokens", file.bearer_tokens.as_deref())
            .map(|path| {
                crate::security::Principals::load(&PathBuf::from(path)).map(std::sync::Arc::new)
            })
            .transpose()?,
        udp_hmac_key: o
            .text("udp-hmac-key", file.udp_hmac_key.as_deref())
            .map(|path| crate::security::UdpKey::load(&PathBuf::from(path)))
            .transpose()?,
    })
}
