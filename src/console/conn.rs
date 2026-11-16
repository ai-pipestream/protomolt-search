//! One client connection: HTTP/1.1 requests read one after another from
//! the same socket under read deadlines, answered in order, until either
//! side closes. Request bodies are framed by `Content-Length` or
//! `Transfer-Encoding: chunked`.

use super::*;

/// Read deadlines and size caps for one connection.
#[derive(Clone, Copy, Debug)]
pub(super) struct Limits {
    /// How long an open connection may wait for its next request.
    pub(super) idle: Duration,
    /// How long a request head may take once its first byte arrived.
    pub(super) header: Duration,
    /// How long a request body may take once its head is read.
    pub(super) body: Duration,
    pub(super) max_head: usize,
    pub(super) max_body: usize,
}

/// The limits the console serves with.
pub(super) const LIMITS: Limits = Limits {
    idle: Duration::from_secs(30),
    header: Duration::from_secs(10),
    body: Duration::from_secs(30),
    max_head: 64 << 10,
    max_body: MAX_BODY_BYTES,
};

/// The longest chunk-size or trailer line a chunked body may carry.
const MAX_CHUNK_LINE: usize = 4096;

/// A socket and the bytes read from it but not consumed yet, which may
/// hold the start of the next pipelined request.
pub(super) struct Conn<S> {
    pub(super) stream: S,
    buf: Vec<u8>,
}

/// What the connection yields next.
pub(super) enum Next {
    Request(HttpRequest),
    /// A request that cannot be served: answer it, then close.
    Reject(Reply),
    /// The client closed, or stayed idle past [`Limits::idle`].
    Closed,
}

enum Framing {
    Length(usize),
    Chunked,
}

struct Head {
    method: String,
    target: String,
    keep_alive: bool,
    framing: Framing,
    headers: Headers,
}

fn bad(status: u16, message: &str) -> Reply {
    Reply::text(status, "text/plain", message)
}

impl<S: AsyncRead + Unpin> Conn<S> {
    pub(super) fn new(stream: S) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(4096),
        }
    }

    /// Reads more bytes into the buffer; `false` at end of stream.
    async fn fill(&mut self) -> std::io::Result<bool> {
        let mut chunk = [0u8; 8192];
        let n = self.stream.read(&mut chunk).await?;
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(n > 0)
    }

    /// Takes bytes up to and including the next `delimiter`, refusing a
    /// run longer than `max` bytes without one.
    async fn take_through(
        &mut self,
        delimiter: &[u8],
        max: usize,
        what: &str,
    ) -> std::io::Result<Result<Vec<u8>, Reply>> {
        loop {
            let found = find_subslice(&self.buf, delimiter).map(|pos| pos + delimiter.len());
            if let Some(end) = found.filter(|&end| end <= max) {
                return Ok(Ok(self.buf.drain(..end).collect()));
            }
            if found.is_some() || self.buf.len() > max {
                let status = if what == "head" { 431 } else { 400 };
                return Ok(Err(bad(status, &format!("request {what} too large"))));
            }
            if !self.fill().await? {
                return Ok(Err(bad(
                    400,
                    &format!("connection closed inside the {what}"),
                )));
            }
        }
    }

    /// Takes exactly `n` bytes.
    async fn take(&mut self, n: usize) -> std::io::Result<Result<Vec<u8>, Reply>> {
        while self.buf.len() < n {
            if !self.fill().await? {
                return Ok(Err(bad(400, "connection closed inside the body")));
            }
        }
        Ok(Ok(self.buf.drain(..n).collect()))
    }

    /// Decodes a chunked body, trailer fields read and dropped.
    async fn chunked(&mut self, max_body: usize) -> std::io::Result<Result<Vec<u8>, Reply>> {
        let mut body = Vec::new();
        loop {
            let line = match self
                .take_through(b"\r\n", MAX_CHUNK_LINE, "chunk size")
                .await?
            {
                Ok(line) => line,
                Err(reply) => return Ok(Err(reply)),
            };
            let Some(size) = chunk_size(&line) else {
                return Ok(Err(bad(400, "malformed chunk size")));
            };
            if size == 0 {
                return Ok(self.trailers().await?.map(|()| body));
            }
            if size > max_body.saturating_sub(body.len()) {
                return Ok(Err(bad(413, "body too large")));
            }
            let mut data = match self.take(size + 2).await? {
                Ok(data) => data,
                Err(reply) => return Ok(Err(reply)),
            };
            if !data.ends_with(b"\r\n") {
                return Ok(Err(bad(400, "chunk data not followed by CRLF")));
            }
            data.truncate(size);
            body.extend_from_slice(&data);
        }
    }

    /// Reads trailer fields through the blank line that ends the body.
    async fn trailers(&mut self) -> std::io::Result<Result<(), Reply>> {
        loop {
            match self
                .take_through(b"\r\n", MAX_CHUNK_LINE, "trailer")
                .await?
            {
                Ok(line) if line == b"\r\n" => return Ok(Ok(())),
                Ok(_) => {}
                Err(reply) => return Ok(Err(reply)),
            }
        }
    }

    async fn body(
        &mut self,
        framing: &Framing,
        max: usize,
    ) -> std::io::Result<Result<Vec<u8>, Reply>> {
        match framing {
            Framing::Length(n) => self.take(*n).await,
            Framing::Chunked => self.chunked(max).await,
        }
    }
}

/// The size on a chunk-size line, extensions after `;` ignored.
fn chunk_size(line: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(line).ok()?.trim_end_matches("\r\n");
    let digits = text.split(';').next()?.trim();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    usize::from_str_radix(digits, 16).ok()
}

/// Reads the next request. Every refusal closes the connection after its
/// reply, because the framing of whatever follows is unknown.
pub(super) async fn read_request<S: AsyncRead + Unpin>(
    conn: &mut Conn<S>,
    limits: &Limits,
) -> std::io::Result<Next> {
    if conn.buf.is_empty() {
        match tokio::time::timeout(limits.idle, conn.fill()).await {
            Err(_) | Ok(Ok(false)) => return Ok(Next::Closed),
            Ok(Err(e)) => return Err(e),
            Ok(Ok(true)) => {}
        }
    }
    // Empty lines before a request line are skipped (RFC 9112, 2.2).
    while conn.buf.starts_with(b"\r\n") {
        conn.buf.drain(..2);
    }
    let read_head = conn.take_through(b"\r\n\r\n", limits.max_head, "head");
    let head = match tokio::time::timeout(limits.header, read_head).await {
        Err(_) => return Ok(Next::Reject(bad(408, "request head timed out"))),
        Ok(head) => match head?.and_then(|bytes| parse_head(&bytes, limits.max_body)) {
            Ok(head) => head,
            Err(reply) => return Ok(Next::Reject(reply)),
        },
    };
    let read_body = conn.body(&head.framing, limits.max_body);
    let body = match tokio::time::timeout(limits.body, read_body).await {
        Err(_) => return Ok(Next::Reject(bad(408, "request body timed out"))),
        Ok(body) => match body? {
            Ok(body) => body,
            Err(reply) => return Ok(Next::Reject(reply)),
        },
    };
    let (path, query) = match head.target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (head.target, Vec::new()),
    };
    Ok(Next::Request(HttpRequest {
        method: head.method,
        path,
        query,
        body,
        keep_alive: head.keep_alive,
        headers: head.headers,
    }))
}

/// The request line and the headers this server acts on. HTTP/1.1 keeps
/// the connection unless `Connection: close`; HTTP/1.0 closes it unless
/// `Connection: keep-alive`. A request carrying both `Content-Length` and
/// `Transfer-Encoding`, conflicting lengths, or a coding other than
/// `chunked` is refused.
fn parse_head(bytes: &[u8], max_body: usize) -> Result<Head, Reply> {
    let text = String::from_utf8_lossy(bytes);
    let mut lines = text.lines();
    let request_line = lines.find(|l| !l.is_empty()).unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad(400, "malformed request line"));
    };
    let http10 = match version {
        "HTTP/1.1" => false,
        "HTTP/1.0" => true,
        _ => return Err(bad(505, "only HTTP/1.0 and HTTP/1.1 are served")),
    };
    let (mut connection, mut lengths, mut codings) = (Vec::new(), Vec::new(), Vec::new());
    let mut headers = Headers::default();
    for (name, value) in lines.filter_map(|l| l.split_once(':')) {
        let list = value.split(',').map(|v| v.trim().to_ascii_lowercase());
        match name.trim().to_ascii_lowercase().as_str() {
            "connection" => connection.extend(list),
            "content-length" => lengths.extend(list),
            "transfer-encoding" => codings.extend(list),
            "host" => headers.host = Some(value.trim().to_ascii_lowercase()),
            "origin" => headers.origin = Some(value.trim().to_ascii_lowercase()),
            "content-type" => headers.content_type = media_type(value),
            _ => {}
        }
    }
    let keep_alive = if http10 {
        connection.iter().any(|t| t == "keep-alive")
    } else {
        !connection.iter().any(|t| t == "close")
    };
    Ok(Head {
        method: method.to_string(),
        target: target.to_string(),
        keep_alive,
        framing: framing(&lengths, &codings, max_body)?,
        headers,
    })
}

/// A `Content-Type` value without its parameters, lowercased.
fn media_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn framing(lengths: &[String], codings: &[String], max_body: usize) -> Result<Framing, Reply> {
    if !codings.is_empty() {
        if !lengths.is_empty() {
            return Err(bad(400, "both Content-Length and Transfer-Encoding"));
        }
        if codings != ["chunked"] {
            return Err(bad(501, "only the chunked transfer coding is supported"));
        }
        return Ok(Framing::Chunked);
    }
    let mut parsed = lengths.iter().map(|v| v.parse::<usize>());
    let length = match parsed.next() {
        None => 0,
        Some(Ok(n)) if parsed.all(|other| other == Ok(n)) => n,
        Some(_) => return Err(bad(400, "invalid Content-Length")),
    };
    if length > max_body {
        return Err(bad(413, "body too large"));
    }
    Ok(Framing::Length(length))
}

/// Serves one connection: requests in order, each reply flagged with
/// whether the connection stays open. A stream (server-sent events or a
/// Connect stream) is the last response on its connection.
pub(super) async fn handle_conn(stream: TcpStream, ctx: Arc<Ctx>) -> std::io::Result<()> {
    let _ = stream.set_nodelay(true);
    let mut conn = Conn::new(stream);
    loop {
        let request = match read_request(&mut conn, &LIMITS).await? {
            Next::Request(request) => request,
            Next::Reject(reply) => return write_reply(&mut conn.stream, reply, false).await,
            Next::Closed => return Ok(()),
        };
        let segments: Vec<&str> = request.path.split('/').filter(|s| !s.is_empty()).collect();
        let out = &mut conn.stream;
        let connect_stream = request.headers.content_type == CONNECT_STREAM_TYPE;
        match (request.method.as_str(), segments.as_slice()) {
            ("GET" | "POST", ["api", "stream", service, method]) if ctx.raw_explorer => {
                return stream_rpc(out, &ctx, service, method, &request).await;
            }
            ("POST", ["bff", "v1", "sse", method]) => {
                return bff_sse(out, &ctx, &request, method).await;
            }
            ("POST", ["bff", "v1", BFF_SERVICE, method]) if connect_stream => {
                return bff_connect_stream(out, &ctx, &request, method).await;
            }
            _ => {
                let reply = route(&ctx, &request, &segments).await;
                write_reply(out, reply, request.keep_alive).await?;
                if !request.keep_alive {
                    return Ok(());
                }
            }
        }
    }
}
