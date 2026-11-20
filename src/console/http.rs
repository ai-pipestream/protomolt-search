//! The hand-rolled HTTP/1.1 side: requests, replies, and the router.

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
    /// Whether the connection stays open after the reply.
    pub(super) keep_alive: bool,
    pub(super) headers: Headers,
}

/// The request headers the routes act on, lowercased.
#[derive(Debug, Default)]
pub(super) struct Headers {
    pub(super) host: Option<String>,
    pub(super) origin: Option<String>,
    /// The media type without parameters; empty when absent.
    pub(super) content_type: String,
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

pub(super) fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        499 => "Client Closed Request",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        _ => "Error",
    }
}

/// One reply, head and body in a single write under the stream write
/// deadline. `keep_alive` says whether the connection stays open after it.
pub(super) async fn write_reply(
    stream: &mut TcpStream,
    reply: Reply,
    keep_alive: bool,
) -> std::io::Result<()> {
    let connection = if keep_alive {
        format!(
            "keep-alive\r\nKeep-Alive: timeout={}",
            LIMITS.idle.as_secs()
        )
    } else {
        "close".to_string()
    };
    let mut bytes = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: {connection}\r\n\r\n",
        reply.status,
        reason(reply.status),
        reply.content_type,
        reply.body.len()
    )
    .into_bytes();
    bytes.extend_from_slice(&reply.body);
    write_frame(stream, &[&bytes]).await
}

/// Why an operator route answers 404 on a console without the raw explorer.
const RAW_OFF: &str = "the raw explorer is off: the browser surface is /bff/v1; an operator \
    on loopback enables /api, /explorer and /dashboard with --enable-raw-explorer";

pub(super) async fn route(ctx: &Arc<Ctx>, request: &HttpRequest, segments: &[&str]) -> Reply {
    let raw = matches!(segments.first(), Some(&"api"))
        || asset_name(&request.path).is_some_and(is_raw_page);
    if raw && !ctx.raw_explorer {
        return Reply::error(404, "NOT_FOUND", RAW_OFF);
    }
    if raw && request.headers.origin.is_some() && guard_origin(request).is_err() {
        return Reply::error(
            403,
            "PERMISSION_DENIED",
            "a cross-origin request to the raw explorer",
        );
    }
    match (request.method.as_str(), segments) {
        ("GET", []) | ("GET", [_]) if asset_name(&request.path).is_some() => {
            asset_reply(ctx.ui_dir.as_deref(), &request.path).unwrap_or_else(|| {
                Reply::error(404, "NOT_FOUND", format!("no route for {}", request.path))
            })
        }
        ("POST", ["bff", "v1", service, method]) => bff_unary(ctx, request, service, method).await,
        (_, ["bff", ..]) => BffError::new(
            Code::NotFound,
            format!("POST Connect calls to /bff/v1/{BFF_SERVICE}/<Method>"),
        )
        .reply(),
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
