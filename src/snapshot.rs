//! Client side of `NodeService.InstallSnapshot`: push a centrally built
//! shard image (opaque provider bytes plus optional exact-vector and BM25
//! sidecars) to a node
//! over one client stream. This is the bulk-load path for pre-computed corpora:
//! build compatible provider images once, then push each artifact to its shard
//! owner instead of re-embedding per node.
//! See the proto comments on the RPC for the install rules.
//!
//! The second half is the snapshot repository's network sources
//! (`docs/snapshots.md`): the node-side fetchers behind
//! `InstallSnapshotFrom` for a peer's `StreamSnapshot` and for an HTTP(S)
//! repository, plus the client helpers for `ExportSnapshot` and
//! `InstallSnapshotFrom`.

use std::path::{Path, PathBuf};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::{
    snapshot_chunk, ExportSnapshotRequest, ExportSnapshotResponse, InstallSnapshotFromRequest,
    InstallSnapshotResponse, SnapshotChunk, SnapshotManifest, StreamSnapshotRequest,
};
use crate::snapshot_repository::{self as repo, RepositoryManifest};

/// Snapshot payload chunk size. 1 MiB keeps messages far under
/// [`crate::MAX_MESSAGE_BYTES`] while amortizing per-message overhead.
pub const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

/// Stream the provider image at `vector_path` (and the `.bm25` sidecar at
/// `bm25_path`, when given) to the node at `addr`, returning the node's
/// install report.
pub async fn install_snapshot(
    addr: &str,
    vector_path: &Path,
    bm25_path: Option<&Path>,
) -> Result<InstallSnapshotResponse, Status> {
    install_snapshot_with_exact(addr, vector_path, None, bm25_path).await
}

/// Stream a complete product generation. The exact-vector sidecar, when
/// present, is sent between the provider image and BM25 bytes.
pub async fn install_snapshot_with_exact(
    addr: &str,
    vector_path: &Path,
    exact_vector_path: Option<&Path>,
    bm25_path: Option<&Path>,
) -> Result<InstallSnapshotResponse, Status> {
    install_snapshot_generation(addr, vector_path, exact_vector_path, bm25_path, None).await
}

/// Stream all files in a product generation, including its optional
/// live-row overlay.
pub async fn install_snapshot_generation(
    addr: &str,
    vector_path: &Path,
    exact_vector_path: Option<&Path>,
    bm25_path: Option<&Path>,
    live_docs_path: Option<&Path>,
) -> Result<InstallSnapshotResponse, Status> {
    let tv_bytes = std::fs::metadata(vector_path)
        .map_err(|e| Status::internal(format!("stat {}: {e}", vector_path.display())))?
        .len();
    let exact_vector_bytes = match exact_vector_path {
        Some(p) => std::fs::metadata(p)
            .map_err(|e| Status::internal(format!("stat {}: {e}", p.display())))?
            .len(),
        None => 0,
    };
    let bm25_bytes = match bm25_path {
        Some(p) => std::fs::metadata(p)
            .map_err(|e| Status::internal(format!("stat {}: {e}", p.display())))?
            .len(),
        None => 0,
    };
    let live_docs_bytes = match live_docs_path {
        Some(p) => std::fs::metadata(p)
            .map_err(|e| Status::internal(format!("stat {}: {e}", p.display())))?
            .len(),
        None => 0,
    };
    let paths: Vec<PathBuf> = [
        Some(vector_path),
        exact_vector_path,
        bm25_path,
        live_docs_path,
    ]
    .into_iter()
    .flatten()
    .map(Path::to_path_buf)
    .collect();

    let (tx, rx) = mpsc::channel::<SnapshotChunk>(2);
    tokio::spawn(async move {
        let manifest = SnapshotChunk {
            payload: Some(snapshot_chunk::Payload::Manifest(SnapshotManifest {
                vector_bytes: tv_bytes,
                bm25_bytes,
                exact_vector_bytes,
                live_docs_bytes,
            })),
        };
        if tx.send(manifest).await.is_err() {
            return;
        }
        for path in paths {
            if !send_file(&tx, &path).await {
                // A local read failure ends the stream early; the node
                // reports it as a truncated snapshot.
                return;
            }
        }
    });

    let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
        .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?;
    let endpoint =
        crate::security::secure_endpoint(endpoint).map_err(Status::failed_precondition)?;
    let mut client = NodeServiceClient::new(
        endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?,
    );
    Ok(client
        .install_snapshot(ReceiverStream::new(rx))
        .await?
        .into_inner())
}

/// Read `path` in [`SNAPSHOT_CHUNK_BYTES`] chunks onto the stream.
/// Returns false when the receiver hung up or the file could not be
/// read (the server then sees a truncated snapshot).
async fn send_file(tx: &mpsc::Sender<SnapshotChunk>, path: &Path) -> bool {
    use tokio::io::AsyncReadExt;
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return false;
    };
    let mut buf = vec![0u8; SNAPSHOT_CHUNK_BYTES];
    loop {
        match file.read(&mut buf).await {
            Ok(0) => return true,
            Ok(n) => {
                let chunk = SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Data(buf[..n].to_vec())),
                };
                if tx.send(chunk).await.is_err() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
}

// ---------------------------------------------------------------------
// Snapshot repository sources (docs/snapshots.md)
// ---------------------------------------------------------------------

/// A channel to a node under the process-wide client TLS material.
async fn node_client(addr: &str) -> Result<NodeServiceClient<tonic::transport::Channel>, Status> {
    let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
        .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?
        .initial_stream_window_size(crate::H2_STREAM_WINDOW)
        .initial_connection_window_size(crate::H2_CONN_WINDOW);
    let endpoint =
        crate::security::secure_endpoint(endpoint).map_err(Status::failed_precondition)?;
    Ok(NodeServiceClient::new(
        endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?,
    )
    .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
    .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
}

/// Ask the node at `addr` to export its shard into `directory` (a path
/// the NODE can write).
pub async fn export_snapshot(
    addr: &str,
    directory: &Path,
) -> Result<ExportSnapshotResponse, Status> {
    Ok(node_client(addr)
        .await?
        .export_snapshot(ExportSnapshotRequest {
            directory: directory.display().to_string(),
        })
        .await?
        .into_inner())
}

/// Ask the node at `addr` to install a snapshot it fetches itself.
pub async fn install_snapshot_from(
    addr: &str,
    request: InstallSnapshotFromRequest,
) -> Result<InstallSnapshotResponse, Status> {
    Ok(node_client(addr)
        .await?
        .install_snapshot_from(request)
        .await?
        .into_inner())
}

/// Write the manifest file into the staging directory, creating it.
async fn stage_manifest(tmp_dir: &Path, bytes: &[u8]) -> Result<(), Status> {
    tokio::fs::create_dir_all(tmp_dir)
        .await
        .map_err(|e| Status::internal(format!("create staging {}: {e}", tmp_dir.display())))?;
    let path = tmp_dir.join(repo::MANIFEST_FILE);
    tokio::fs::write(&path, bytes)
        .await
        .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))
}

/// Pull a peer node's `StreamSnapshot` into the staging directory: the
/// manifest frame first, then every artifact's bytes in manifest order.
/// Returns the manifest and the SHA-256 of its canonical bytes. Byte
/// counts are checked against the manifest as they arrive; the digests
/// are checked by the install.
pub async fn stage_from_peer(
    peer_addr: &str,
    tmp_dir: &Path,
) -> Result<(RepositoryManifest, String), Status> {
    use tokio::io::AsyncWriteExt;
    let peer_addr = crate::config::normalize_addr(peer_addr.to_string());
    let mut stream = node_client(&peer_addr)
        .await?
        .stream_snapshot(StreamSnapshotRequest {})
        .await
        .map_err(|e| Status::new(e.code(), format!("peer {peer_addr}: {}", e.message())))?
        .into_inner();
    let manifest = match stream.message().await? {
        Some(SnapshotChunk {
            payload: Some(snapshot_chunk::Payload::Repository(manifest)),
        }) => RepositoryManifest::from_pb(&manifest).map_err(|e| {
            Status::invalid_argument(format!("peer {peer_addr} sent an invalid manifest: {e}"))
        })?,
        _ => {
            return Err(Status::invalid_argument(format!(
                "peer {peer_addr}: the first StreamSnapshot frame must be the repository manifest"
            )))
        }
    };
    let encoded = manifest.encode();
    let sha = crate::sha256::hex_digest(&encoded);
    stage_manifest(tmp_dir, &encoded).await?;
    let io_err = |what: &Path, e: std::io::Error| {
        Status::internal(format!("snapshot receive {}: {e}", what.display()))
    };
    let mut pending: Vec<u8> = Vec::new();
    let mut ended = false;
    for artifact in &manifest.artifacts {
        let path = tmp_dir.join(&artifact.file);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| io_err(parent, e))?;
        }
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(|e| io_err(&path, e))?;
        let mut written = 0u64;
        while written < artifact.bytes {
            if pending.is_empty() {
                match stream.message().await? {
                    Some(SnapshotChunk {
                        payload: Some(snapshot_chunk::Payload::Data(data)),
                    }) => pending = data,
                    Some(_) => {
                        return Err(Status::invalid_argument(format!(
                            "peer {peer_addr}: a frame after the manifest must carry data"
                        )))
                    }
                    None => {
                        ended = true;
                        break;
                    }
                }
            }
            let take = (artifact.bytes - written).min(pending.len() as u64) as usize;
            file.write_all(&pending[..take])
                .await
                .map_err(|e| io_err(&path, e))?;
            pending.drain(..take);
            written += take as u64;
        }
        file.sync_all().await.map_err(|e| io_err(&path, e))?;
        if written != artifact.bytes {
            return Err(Status::invalid_argument(format!(
                "truncated snapshot from peer {peer_addr}: artifact {:?} received {written} of \
                 {} bytes",
                artifact.file, artifact.bytes
            )));
        }
    }
    if !pending.is_empty() {
        return Err(Status::invalid_argument(format!(
            "peer {peer_addr} sent more data than its manifest declares"
        )));
    }
    if !ended {
        match stream.message().await? {
            None => {}
            Some(SnapshotChunk {
                payload: Some(snapshot_chunk::Payload::Data(data)),
            }) if data.is_empty() => {}
            Some(_) => {
                return Err(Status::invalid_argument(format!(
                    "peer {peer_addr} sent more data than its manifest declares"
                )))
            }
        }
    }
    Ok((manifest, sha))
}

// ---------------------------------------------------------------------
// HTTP(S) repository source
// ---------------------------------------------------------------------

/// GET attempts per artifact before the install refuses: an interrupted
/// body resumes with a `Range` request from the bytes already staged.
const HTTP_ATTEMPTS: u32 = 8;
/// The largest manifest the client accepts.
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

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
enum Fetcher {
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
    fn for_url(url: &str) -> Result<Self, Status> {
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

    async fn request(
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
fn content_range_start(headers: &hyper::HeaderMap) -> Option<u64> {
    let value = headers.get(hyper::header::CONTENT_RANGE)?.to_str().ok()?;
    let rest = value.trim().strip_prefix("bytes ")?;
    let (start, _) = rest.split_once('-')?;
    start.trim().parse().ok()
}

/// GET `url` into `destination`, resuming from the bytes already there
/// with a `Range` request when the transfer breaks. Sizes and digests are
/// verified by the install; this only moves bytes and names HTTP
/// refusals.
async fn fetch_artifact(
    fetcher: &Fetcher,
    url: &str,
    bearer: &str,
    destination: &Path,
    expected_bytes: u64,
) -> Result<(), Status> {
    use http_body_util::BodyExt;
    use tokio::io::AsyncWriteExt;
    let io_err =
        |e: std::io::Error| Status::internal(format!("stage {}: {e}", destination.display()));
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(io_err)?;
    }
    let mut last_error = String::new();
    for attempt in 1..=HTTP_ATTEMPTS {
        let have = match tokio::fs::metadata(destination).await {
            Ok(meta) => meta.len(),
            Err(_) => 0,
        };
        if have >= expected_bytes && (have > 0 || expected_bytes == 0) {
            if have == 0 {
                tokio::fs::File::create(destination).await.map_err(io_err)?;
            }
            return Ok(());
        }
        let resume = (have > 0).then_some(have);
        let request = build_get(url, resume, bearer).map_err(Status::invalid_argument)?;
        let response = match fetcher.request(request).await {
            Ok(response) => response,
            Err(e) => {
                last_error = format!("GET {url}: {e}");
                continue;
            }
        };
        let status = response.status();
        let (open_mode_append, skip) = match (status.as_u16(), resume) {
            (200, _) => (false, 0u64),
            (206, Some(from)) => match content_range_start(response.headers()) {
                Some(start) if start == from => (true, 0),
                Some(start) if start < from => (true, from - start),
                other => {
                    return Err(Status::invalid_argument(format!(
                        "GET {url}: 206 with Content-Range start {other:?}, expected {from}"
                    )))
                }
            },
            (401, _) | (403, _) => {
                return Err(Status::unauthenticated(format!(
                    "GET {url}: HTTP {status}; the repository refused the request \
                     (bearer_token missing or wrong?)"
                )))
            }
            (404, _) => {
                return Err(Status::not_found(format!(
                    "GET {url}: HTTP 404; the repository has no such artifact"
                )))
            }
            _ => {
                return Err(Status::unavailable(format!(
                    "GET {url}: HTTP {status}; the repository did not serve the artifact"
                )))
            }
        };
        let mut file = if open_mode_append {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(destination)
                .await
                .map_err(io_err)?
        } else {
            tokio::fs::File::create(destination).await.map_err(io_err)?
        };
        let mut body = response.into_body();
        let mut to_skip = skip;
        let mut broke = false;
        while let Some(frame) = body.frame().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(e) => {
                    last_error = format!("GET {url} body (attempt {attempt}): {e}");
                    broke = true;
                    break;
                }
            };
            if let Some(data) = frame.data_ref() {
                let mut data: &[u8] = data;
                if to_skip > 0 {
                    let drop = (to_skip.min(data.len() as u64)) as usize;
                    data = &data[drop..];
                    to_skip -= drop as u64;
                }
                file.write_all(data).await.map_err(io_err)?;
            }
        }
        file.sync_all().await.map_err(io_err)?;
        if !broke {
            return Ok(());
        }
    }
    Err(Status::unavailable(format!(
        "artifact {} did not complete after {HTTP_ATTEMPTS} attempts: {last_error}",
        destination.display()
    )))
}

/// Fetch a repository over HTTP(S) into the staging directory: the
/// manifest, then every artifact it names. Returns the manifest and the
/// SHA-256 of the manifest bytes as served.
pub async fn stage_from_url(
    url: &str,
    bearer: &str,
    tmp_dir: &Path,
) -> Result<(RepositoryManifest, String), Status> {
    use http_body_util::BodyExt;
    let fetcher = Fetcher::for_url(url)?;
    let manifest_url = artifact_url(url, repo::MANIFEST_FILE);
    let request = build_get(&manifest_url, None, bearer).map_err(Status::invalid_argument)?;
    let response = fetcher
        .request(request)
        .await
        .map_err(|e| Status::unavailable(format!("GET {manifest_url}: {e}")))?;
    let status = response.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(Status::unauthenticated(format!(
            "GET {manifest_url}: HTTP {status}; the repository refused the request \
             (bearer_token missing or wrong?)"
        )));
    }
    if !status.is_success() {
        return Err(Status::unavailable(format!(
            "GET {manifest_url}: HTTP {status}; the repository did not serve the manifest"
        )));
    }
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame =
            frame.map_err(|e| Status::unavailable(format!("GET {manifest_url} body: {e}")))?;
        if let Some(data) = frame.data_ref() {
            if bytes.len() + data.len() > MAX_MANIFEST_BYTES {
                return Err(Status::invalid_argument(format!(
                    "GET {manifest_url}: manifest exceeds {MAX_MANIFEST_BYTES} bytes"
                )));
            }
            bytes.extend_from_slice(data);
        }
    }
    let manifest = RepositoryManifest::parse(&bytes)
        .map_err(|e| Status::invalid_argument(format!("GET {manifest_url}: {e}")))?;
    let sha = crate::sha256::hex_digest(&bytes);
    stage_manifest(tmp_dir, &bytes).await?;
    for artifact in &manifest.artifacts {
        fetch_artifact(
            &fetcher,
            &artifact_url(url, &artifact.file),
            bearer,
            &tmp_dir.join(&artifact.file),
            artifact.bytes,
        )
        .await?;
    }
    Ok((manifest, sha))
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
