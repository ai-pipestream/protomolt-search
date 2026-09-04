//! A minimal HTTP/1.1 file server for the snapshot repository tests: GET
//! with `Range: bytes=N-` (206 plus `Content-Range`), a bearer check
//! when one is configured, one request per connection, generic over the
//! stream so the same server runs plain and under TLS. With
//! `drop_first`, the first full GET of an artifact declares its whole
//! length and closes after 64 KiB, so the client has to resume with a
//! Range request.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

pub const DROP_AFTER: usize = 64 * 1024;

pub struct FileServer {
    pub root: PathBuf,
    pub bearer: Option<String>,
    pub drop_first: AtomicBool,
    pub range_requests: AtomicU64,
    pub requests: AtomicU64,
}

impl FileServer {
    pub fn new(root: PathBuf, bearer: Option<&str>, drop_first: bool) -> Arc<Self> {
        Arc::new(FileServer {
            root,
            bearer: bearer.map(str::to_string),
            drop_first: AtomicBool::new(drop_first),
            range_requests: AtomicU64::new(0),
            requests: AtomicU64::new(0),
        })
    }

    /// Serve one request on `stream`, then close it.
    pub async fn handle<S: AsyncRead + AsyncWrite + Unpin>(&self, mut stream: S) {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                return;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).into_owned();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or("");
        let mut parts = request_line.split(' ');
        let method = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("/");
        let mut range = None;
        let mut authorization = None;
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                match name.trim().to_ascii_lowercase().as_str() {
                    "range" => range = Some(value.trim().to_string()),
                    "authorization" => authorization = Some(value.trim().to_string()),
                    _ => {}
                }
            }
        }
        self.requests.fetch_add(1, Ordering::Relaxed);
        let reply = |status: &str, body: &[u8]| {
            format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .collect::<Vec<u8>>()
        };
        if method != "GET" {
            let _ = stream
                .write_all(&reply("405 Method Not Allowed", b""))
                .await;
            return;
        }
        if let Some(expected) = &self.bearer {
            if authorization.as_deref() != Some(&format!("Bearer {expected}")) {
                let _ = stream.write_all(&reply("401 Unauthorized", b"no")).await;
                return;
            }
        }
        let file = self.root.join(path.trim_start_matches('/'));
        let Ok(bytes) = std::fs::read(&file) else {
            let _ = stream.write_all(&reply("404 Not Found", b"")).await;
            return;
        };
        let is_artifact = !path.ends_with("snapshot-manifest.json");
        match range {
            Some(range) => {
                self.range_requests.fetch_add(1, Ordering::Relaxed);
                let from: usize = range
                    .strip_prefix("bytes=")
                    .and_then(|r| r.strip_suffix('-'))
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0);
                let tail = &bytes[from.min(bytes.len())..];
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {from}-{}/{}\r\nConnection: close\r\n\r\n",
                    tail.len(),
                    bytes.len().saturating_sub(1),
                    bytes.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(tail).await;
            }
            None if is_artifact
                && bytes.len() > DROP_AFTER
                && self.drop_first.swap(false, Ordering::Relaxed) =>
            {
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    bytes.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&bytes[..DROP_AFTER]).await;
                let _ = stream.flush().await;
            }
            None => {
                let _ = stream.write_all(&reply("200 OK", &bytes)).await;
            }
        }
        let _ = stream.shutdown().await;
    }
}

/// Serve `server` plain on a loopback listener; returns `http://host:port`.
pub async fn serve_plain(server: Arc<FileServer>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let server = Arc::clone(&server);
            tokio::spawn(async move { server.handle(stream).await });
        }
    });
    format!("http://{addr}")
}
