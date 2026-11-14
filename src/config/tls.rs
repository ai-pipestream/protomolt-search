use super::*;

/// The security surface (`docs/security.md`): the listener TLS and the
/// client material. Files are read here so a missing certificate
/// refuses at startup, not at the first connection.
pub(super) fn parse_tls(
    args: &[String],
    file: &FileConfig,
) -> Result<
    (
        Option<crate::security::ServerTls>,
        Option<crate::security::ClientTls>,
    ),
    String,
> {
    // The security surface (docs/security.md). Files are read here so a
    // missing certificate refuses at startup, not at the first connection.
    let path_opt = |key: &str, env: &str, file_value: Option<&str>| {
        opt(args, key, env, file_value).map(PathBuf::from)
    };
    let tls_cert = path_opt(
        "tls-cert",
        "PIPESTREAM_SEARCH_TLS_CERT",
        file.tls_cert.as_deref(),
    );
    let tls_key = path_opt(
        "tls-key",
        "PIPESTREAM_SEARCH_TLS_KEY",
        file.tls_key.as_deref(),
    );
    let tls_client_ca = path_opt(
        "tls-client-ca",
        "PIPESTREAM_SEARCH_TLS_CLIENT_CA",
        file.tls_client_ca.as_deref(),
    );
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
    let tls_ca = path_opt("tls-ca", "PIPESTREAM_SEARCH_TLS_CA", file.tls_ca.as_deref());
    let tls_client_cert = path_opt(
        "tls-client-cert",
        "PIPESTREAM_SEARCH_TLS_CLIENT_CERT",
        file.tls_client_cert.as_deref(),
    );
    let tls_client_key = path_opt(
        "tls-client-key",
        "PIPESTREAM_SEARCH_TLS_CLIENT_KEY",
        file.tls_client_key.as_deref(),
    );
    let tls_domain = opt(
        args,
        "tls-domain",
        "PIPESTREAM_SEARCH_TLS_DOMAIN",
        file.tls_domain.as_deref(),
    );
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
