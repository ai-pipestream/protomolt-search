//! The hand-rolled HTTP/1.1 side: requests, replies, the router, and the
//! static UI assets.

use super::*;

/// One HTTP response.
#[derive(Debug)]
pub(super) struct Reply {
    pub(super) status: u16,
    pub(super) content_type: &'static str,
    pub(super) body: Vec<u8>,
}

impl Reply {
    pub(super) fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: value.to_string().into_bytes(),
        }
    }

    pub(super) fn json_bytes(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
        }
    }

    pub(super) fn error(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self::json(status, json!({ "error": message.into(), "code": code }))
    }

    pub(super) fn text(status: u16, content_type: &'static str, body: &str) -> Self {
        Self {
            status,
            content_type,
            body: body.as_bytes().to_vec(),
        }
    }
}

pub(super) struct HttpRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) query: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
}

pub(super) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Percent-decoding with `+` as space, by hand (no regex, no crate).
pub(super) fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = &s[i + 1..i + 3];
                match u8::from_str_radix(hex, 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(super) fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (url_decode(k), url_decode(v)),
            None => (url_decode(kv), String::new()),
        })
        .collect()
}

pub(super) fn param<'a>(query: &'a [(String, String)], key: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

pub(super) async fn read_request(
    stream: &mut TcpStream,
) -> std::io::Result<Result<HttpRequest, Reply>> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(Err(Reply::text(400, "text/plain", "empty request")));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > MAX_BODY_BYTES {
            return Ok(Err(Reply::text(431, "text/plain", "headers too large")));
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (target.clone(), Vec::new()),
    };
    let content_length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Ok(Err(Reply::text(413, "text/plain", "body too large")));
    }
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Ok(Ok(HttpRequest {
        method,
        path,
        query,
        body,
    }))
}

pub(super) fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        _ => "Error",
    }
}

pub(super) async fn write_reply(stream: &mut TcpStream, reply: Reply) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        reply.status,
        reason(reply.status),
        reply.content_type,
        reply.body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&reply.body).await?;
    stream.flush().await
}

pub(super) async fn handle_conn(mut stream: TcpStream, ctx: Arc<Ctx>) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await? {
        Ok(request) => request,
        Err(reply) => return write_reply(&mut stream, reply).await,
    };
    let segments: Vec<&str> = request.path.split('/').filter(|s| !s.is_empty()).collect();
    match (request.method.as_str(), segments.as_slice()) {
        ("GET", ["api", "stream", service, method])
        | ("POST", ["api", "stream", service, method]) => {
            stream_rpc(&mut stream, &ctx, service, method, &request).await
        }
        _ => {
            let reply = route(&ctx, &request, &segments).await;
            write_reply(&mut stream, reply).await
        }
    }
}

pub(super) fn asset(path: &str) -> Option<(&'static str, &'static str)> {
    Some(match path {
        "/" | "/index.html" => ("text/html; charset=utf-8", INDEX_HTML),
        "/dashboard" | "/dashboard.html" => ("text/html; charset=utf-8", DASHBOARD_HTML),
        "/explorer" | "/explorer.html" => ("text/html; charset=utf-8", EXPLORER_HTML),
        "/explorer.js" => ("text/javascript; charset=utf-8", EXPLORER_JS),
        "/app.css" => ("text/css; charset=utf-8", APP_CSS),
        "/common.js" => ("text/javascript; charset=utf-8", COMMON_JS),
        "/search.js" => ("text/javascript; charset=utf-8", SEARCH_JS),
        "/dashboard.js" => ("text/javascript; charset=utf-8", DASHBOARD_JS),
        "/query-state.js" => ("text/javascript; charset=utf-8", QUERY_STATE_JS),
        "/sparkline.js" => ("text/javascript; charset=utf-8", SPARKLINE_JS),
        _ => return None,
    })
}

pub(super) async fn route(ctx: &Ctx, request: &HttpRequest, segments: &[&str]) -> Reply {
    match (request.method.as_str(), segments) {
        ("GET", []) | ("GET", [_]) if asset(&request.path).is_some() => {
            let (content_type, body) = asset(&request.path).expect("checked");
            Reply::text(200, content_type, body)
        }
        ("GET", ["api", "health"]) => match health(ctx).await {
            Ok(v) => Reply::json(200, v),
            Err(e) => Reply::error(502, "UNAVAILABLE", e),
        },
        ("GET", ["api", "config"]) => Reply::json(200, config_json(ctx)),
        ("GET", ["api", "describe", service, method]) => match describe(service, method) {
            Ok(v) => Reply::json(200, v),
            Err(reply) => reply,
        },
        ("POST", ["api", "embed"]) => embed(ctx, &request.body).await,
        ("POST", ["api", "documents"]) => documents(ctx, &request.body).await,
        ("POST", ["api", "rpc", service, method]) => unary_rpc(ctx, service, method, request).await,
        ("GET", ["api", "rpc", ..]) => Reply::error(
            405,
            "INVALID_ARGUMENT",
            "POST the request message as proto3 JSON to /api/rpc/<Service>/<Method>",
        ),
        _ => Reply::error(404, "NOT_FOUND", format!("no route for {}", request.path)),
    }
}

pub(super) fn config_json(ctx: &Ctx) -> Value {
    let methods: Vec<Value> = exposed_methods()
        .into_iter()
        .map(|(service, method, streaming)| {
            json!({ "service": service, "method": method, "server_streaming": streaming })
        })
        .collect();
    let spec = crate::analyzer::body_spec();
    json!({
        "coordinator": ctx.coordinator,
        "nodes": ctx.nodes,
        "analysis": ctx.analysis.is_some(),
        "tls": ctx.security.is_tls(),
        "bearer": ctx.security.has_bearer(),
        "methods": methods,
        "body_spec": {
            "tokenizer": spec.tokenizer,
            "stemmer": spec.stemmer,
            "term_vector_mode": spec.term_vector_mode,
            "term_vector_source": spec.term_vector_source,
            "char_filters": spec.char_filters,
        },
    })
}
