//! Stage a repository fetched over HTTP(S): the manifest, then every
//! artifact it names, resuming interrupted bodies.

use std::path::Path;

use tonic::Status;

use super::http::{artifact_url, build_get, content_range_start, Fetcher};
use super::peer::stage_manifest;
use crate::snapshot_repository::{self as repo, RepositoryManifest};

/// GET attempts per artifact before the install refuses: an interrupted
/// body resumes with a `Range` request from the bytes already staged.
const HTTP_ATTEMPTS: u32 = 8;
/// The largest manifest the client accepts.
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

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
    let io_err = stage_error(destination);
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(&io_err)?;
    }
    let mut last_error = String::new();
    for attempt in 1..=HTTP_ATTEMPTS {
        let have = match tokio::fs::metadata(destination).await {
            Ok(meta) => meta.len(),
            Err(_) => 0,
        };
        if have >= expected_bytes && (have > 0 || expected_bytes == 0) {
            if have == 0 {
                tokio::fs::File::create(destination)
                    .await
                    .map_err(&io_err)?;
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
        let (open_mode_append, skip) = resume_plan(url, &response, resume)?;
        let mut file = if open_mode_append {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(destination)
                .await
                .map_err(&io_err)?
        } else {
            tokio::fs::File::create(destination)
                .await
                .map_err(&io_err)?
        };
        let broke = write_body(response.into_body(), &mut file, skip, &io_err).await?;
        file.sync_all().await.map_err(&io_err)?;
        match broke {
            None => return Ok(()),
            Some(e) => last_error = format!("GET {url} body (attempt {attempt}): {e}"),
        }
    }
    Err(Status::unavailable(format!(
        "artifact {} did not complete after {HTTP_ATTEMPTS} attempts: {last_error}",
        destination.display()
    )))
}

fn stage_error(destination: &Path) -> impl Fn(std::io::Error) -> Status + '_ {
    move |e| Status::internal(format!("stage {}: {e}", destination.display()))
}

/// How a response joins the bytes already staged: whether the body is
/// appended to them, and how many leading body bytes repeat them. A
/// status that serves no artifact bytes is the named refusal.
fn resume_plan(
    url: &str,
    response: &hyper::Response<hyper::body::Incoming>,
    resume: Option<u64>,
) -> Result<(bool, u64), Status> {
    let status = response.status();
    Ok(match (status.as_u16(), resume) {
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
    })
}

/// Write a response body to `file` after dropping its first `skip`
/// bytes. Returns the transport error when the body broke part-way; the
/// bytes written before it stay staged for the next attempt.
async fn write_body(
    mut body: hyper::body::Incoming,
    file: &mut tokio::fs::File,
    skip: u64,
    io_err: &impl Fn(std::io::Error) -> Status,
) -> Result<Option<hyper::Error>, Status> {
    use http_body_util::BodyExt;
    use tokio::io::AsyncWriteExt;
    let mut to_skip = skip;
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(e) => return Ok(Some(e)),
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
    Ok(None)
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
    let sha = turbo_sha256::sha256(&bytes).to_hex();
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
