//! The HTTP(S) repository source: the artifact request and the client
//! that sends it.

use tonic::Status;

/// The URL of `file` under the repository `base` (with or without a
/// trailing slash).
pub fn artifact_url(base: &str, file: &str) -> String {
    format!("{}/{file}", base.trim_end_matches('/'))
}

/// The GET request for one artifact: `Range: bytes=<from>-` resumes a
/// partial download, `Authorization: Bearer <token>` when a token is
/// given. Refuses a URL that is not http or https, or one without a
/// host, by name.
pub fn build_get(
    url: &str,
    range_from: Option<u64>,
    bearer: &str,
) -> Result<hyper::Request<http_body_util::Empty<bytes::Bytes>>, String> {
    let uri: hyper::Uri = url.parse().map_err(|e| {
        format!("repository url {url:?}: {e}; expected http://host/path or https://host/path")
    })?;
    match uri.scheme_str() {
        Some("http") | Some("https") => {}
        Some(other) => {
            return Err(format!(
                "repository url {url:?} has scheme {other:?}; http and https are the sources"
            ))
        }
        None => return Err(format!("repository url {url:?} has no scheme")),
    }
    let host = uri
        .host()
        .ok_or_else(|| format!("repository url {url:?} has no host"))?;
    let host_header = match uri.port_u16() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    let mut request = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri(uri.clone())
        .header(hyper::header::HOST, host_header)
        .header(hyper::header::USER_AGENT, "pipestream-search-snapshot/1");
    if let Some(from) = range_from {
        request = request.header(hyper::header::RANGE, format!("bytes={from}-"));
    }
    if !bearer.is_empty() {
        if bearer.bytes().any(|b| b < 0x20 || b == 0x7f) {
            return Err("bearer token contains control characters".to_string());
        }
        request = request.header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"));
    }
    request
        .body(http_body_util::Empty::new())
        .map_err(|e| format!("build request for {url:?}: {e}"))
}

/// The HTTP client behind the `url` source: HTTP/1.1 over hyper-util's
/// connector, and HTTPS through hyper-rustls when the build has `tls`.
pub(super) enum Fetcher {
    Plain(
        hyper_util::client::legacy::Client<
            hyper_util::client::legacy::connect::HttpConnector,
            http_body_util::Empty<bytes::Bytes>,
        >,
    ),
    #[cfg(feature = "tls")]
    Tls(
        hyper_util::client::legacy::Client<
            hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
            http_body_util::Empty<bytes::Bytes>,
        >,
    ),
}

impl Fetcher {
    /// A client for `url`'s scheme. HTTPS trusts the public web roots and
    /// the cluster CA installed with `--tls-ca`, when any.
    pub(super) fn for_url(url: &str) -> Result<Self, Status> {
        let executor = hyper_util::rt::TokioExecutor::new();
        if url.starts_with("https://") {
            #[cfg(feature = "tls")]
            {
                let connector = https_connector().map_err(Status::failed_precondition)?;
                return Ok(Fetcher::Tls(
                    hyper_util::client::legacy::Client::builder(executor).build(connector),
                ));
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(Status::failed_precondition(format!(
                    "repository url {url:?} is https but this build has no TLS support \
                     (feature `tls` is off)"
                )));
            }
        }
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.enforce_http(true);
        Ok(Fetcher::Plain(
            hyper_util::client::legacy::Client::builder(executor).build(connector),
        ))
    }

    pub(super) async fn request(
        &self,
        request: hyper::Request<http_body_util::Empty<bytes::Bytes>>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, String> {
        match self {
            Fetcher::Plain(client) => client.request(request).await.map_err(error_chain),
            #[cfg(feature = "tls")]
            Fetcher::Tls(client) => client.request(request).await.map_err(error_chain),
        }
    }
}

/// An error with its sources, so a refusal names the cause (the TLS
/// alert, the reset) and not only hyper's category.
fn error_chain(error: impl std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// The HTTPS connector: rustls with `ring` (the provider tonic links),
/// the web's roots plus the cluster CA from `--tls-ca`, HTTP/1.1.
#[cfg(feature = "tls")]
fn https_connector(
) -> Result<hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, String>
{
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(tls) = crate::security::process_client_tls() {
        use rustls_pki_types::pem::PemObject;
        for cert in rustls_pki_types::CertificateDer::pem_slice_iter(&tls.ca_pem) {
            let cert = cert.map_err(|e| format!("cluster CA PEM: {e:?}"))?;
            roots
                .add(cert)
                .map_err(|e| format!("cluster CA certificate: {e}"))?;
        }
    }
    let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("TLS protocol versions: {e}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_or_http()
        .enable_http1()
        .build())
}

/// The `Content-Range` start of a 206 response, when well-formed
/// (`bytes <start>-<end>/<total>`).
pub(super) fn content_range_start(headers: &hyper::HeaderMap) -> Option<u64> {
    let value = headers.get(hyper::header::CONTENT_RANGE)?.to_str().ok()?;
    let rest = value.trim().strip_prefix("bytes ")?;
    let (start, _) = rest.split_once('-')?;
    start.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_builder_names_its_refusals_and_sets_the_headers() {
        let request = build_get("http://nas.local:8080/repo/x", Some(1024), "s3cret").unwrap();
        assert_eq!(request.method(), hyper::Method::GET);
        assert_eq!(request.uri().path(), "/repo/x");
        assert_eq!(request.headers()[hyper::header::HOST], "nas.local:8080");
        assert_eq!(request.headers()[hyper::header::RANGE], "bytes=1024-");
        assert_eq!(
            request.headers()[hyper::header::AUTHORIZATION],
            "Bearer s3cret"
        );
        let plain = build_get("https://nas.local/repo/x", None, "").unwrap();
        assert_eq!(plain.headers()[hyper::header::HOST], "nas.local");
        assert!(plain.headers().get(hyper::header::RANGE).is_none());
        assert!(plain.headers().get(hyper::header::AUTHORIZATION).is_none());
        assert!(build_get("ftp://nas/x", None, "")
            .unwrap_err()
            .contains("scheme \"ftp\""));
        assert!(build_get("nas/x", None, "")
            .unwrap_err()
            .contains("expected http://host/path"));
        assert!(build_get("http:///x", None, "")
            .unwrap_err()
            .contains("host"));
        assert!(build_get("http://nas/x", None, "bad\ntoken")
            .unwrap_err()
            .contains("control characters"));
        assert_eq!(
            artifact_url("http://nas/repo/", "catalog/segments.json"),
            "http://nas/repo/catalog/segments.json"
        );
        assert_eq!(
            artifact_url("http://nas/repo", "vector.index"),
            "http://nas/repo/vector.index"
        );
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::CONTENT_RANGE,
            "bytes 1024-2047/2048".parse().unwrap(),
        );
        assert_eq!(content_range_start(&headers), Some(1024));
        headers.insert(hyper::header::CONTENT_RANGE, "items 1-2".parse().unwrap());
        assert_eq!(content_range_start(&headers), None);
    }

    #[cfg(feature = "tls")]
    #[test]
    fn the_https_connector_builds_over_the_ring_provider() {
        https_connector().unwrap();
    }
}
