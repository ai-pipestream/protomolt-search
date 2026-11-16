//! The transcoded RPC routes: unary JSON calls and server-streamed methods
//! as server-sent events.

use super::*;

pub(super) async fn unary_rpc(
    ctx: &Ctx,
    service: &str,
    method: &str,
    request: &HttpRequest,
) -> Reply {
    let method = match resolve_method(service, method) {
        Ok(m) => m,
        Err(reply) => return reply,
    };
    if method.is_server_streaming() {
        return Reply::error(
            400,
            "INVALID_ARGUMENT",
            format!(
                "{service}.{} is server-streaming: use /api/stream/{service}/{}",
                method.name(),
                method.name()
            ),
        );
    }
    let addr = match ctx.target(param(&request.query, "target")) {
        Ok(addr) => addr,
        Err(reply) => return reply,
    };
    let bytes = match encode_request(&method, &request.body) {
        Ok(b) => b,
        Err(reply) => return reply,
    };
    let mut grpc = match raw_grpc(ctx, &addr) {
        Ok(g) => g,
        Err(reply) => return reply,
    };
    if let Err(e) = grpc.ready().await {
        return Reply::error(502, "UNAVAILABLE", format!("{addr}: {e}"));
    }
    let response = match grpc
        .unary(Request::new(bytes), method_path(&method), RawCodec)
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => return call_status_reply(&status, &method, &addr),
    };
    match decode_response(&method, response) {
        Ok(json) => Reply::json_bytes(json),
        Err(reply) => reply,
    }
}

/// The request message for a streaming call: the POST body, else the
/// `request` query parameter (JSON), else the scalar query parameters
/// as top-level fields (`interval_ms=1000`).
pub(super) fn streaming_request_json(request: &HttpRequest) -> Vec<u8> {
    if !request.body.iter().all(u8::is_ascii_whitespace) {
        return request.body.clone();
    }
    if let Some(json) = param(&request.query, "request") {
        return json.as_bytes().to_vec();
    }
    let mut object = serde_json::Map::new();
    for (k, v) in &request.query {
        if k == "target" {
            continue;
        }
        let value = if let Ok(n) = v.parse::<i64>() {
            Value::from(n)
        } else if let Ok(f) = v.parse::<f64>() {
            Value::from(f)
        } else if v == "true" || v == "false" {
            Value::Bool(v == "true")
        } else {
            Value::String(v.clone())
        };
        object.insert(k.clone(), value);
    }
    Value::Object(object).to_string().into_bytes()
}

pub(super) async fn stream_rpc(
    stream: &mut TcpStream,
    ctx: &Ctx,
    service: &str,
    method: &str,
    request: &HttpRequest,
) -> std::io::Result<()> {
    let method = match resolve_method(service, method) {
        Ok(m) => m,
        Err(reply) => return write_reply(stream, reply, false).await,
    };
    if !method.is_server_streaming() {
        return write_reply(
            stream,
            Reply::error(
                400,
                "INVALID_ARGUMENT",
                format!(
                    "{service}.{} is unary: use POST /api/rpc/{service}/{}",
                    method.name(),
                    method.name()
                ),
            ),
            false,
        )
        .await;
    }
    let addr = match ctx.target(param(&request.query, "target")) {
        Ok(addr) => addr,
        Err(reply) => return write_reply(stream, reply, false).await,
    };
    let bytes = match encode_request(&method, &streaming_request_json(request)) {
        Ok(b) => b,
        Err(reply) => return write_reply(stream, reply, false).await,
    };
    let mut grpc = match raw_grpc(ctx, &addr) {
        Ok(g) => g,
        Err(reply) => return write_reply(stream, reply, false).await,
    };
    if let Err(e) = grpc.ready().await {
        return write_reply(
            stream,
            Reply::error(502, "UNAVAILABLE", format!("{addr}: {e}")),
            false,
        )
        .await;
    }
    let mut messages = match grpc
        .server_streaming(Request::new(bytes), method_path(&method), RawCodec)
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return write_reply(stream, call_status_reply(&status, &method, &addr), false).await
        }
    };
    let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
    write_frame(stream, &[head]).await?;
    loop {
        match messages.message().await {
            Ok(Some(bytes)) => match decode_response(&method, bytes) {
                Ok(json) => write_frame(stream, &[b"data: ", &json, b"\n\n"]).await?,
                Err(reply) => {
                    return write_frame(stream, &[b"event: error\ndata: ", &reply.body, b"\n\n"])
                        .await;
                }
            },
            Ok(None) => return write_frame(stream, &[b"event: end\ndata: {}\n\n"]).await,
            Err(status) => {
                let error = json!({ "error": status.message(), "code": code_name(status.code()) })
                    .to_string();
                return write_frame(
                    stream,
                    &[b"event: error\ndata: ", error.as_bytes(), b"\n\n"],
                )
                .await;
            }
        }
    }
}

/// One server-sent frame, written and flushed within
/// [`STREAM_WRITE_DEADLINE`]. A client that holds its connection open
/// without reading would otherwise park this task on the write for
/// good; the gRPC stream behind it then goes unread and its data holds
/// flow-control window on the channel every other call shares.
pub(super) async fn write_frame<W: AsyncWrite + Unpin>(
    stream: &mut W,
    parts: &[&[u8]],
) -> std::io::Result<()> {
    let write = async {
        for part in parts {
            stream.write_all(part).await?;
        }
        stream.flush().await
    };
    match tokio::time::timeout(STREAM_WRITE_DEADLINE, write).await {
        Ok(written) => written,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "the stream's client stopped reading",
        )),
    }
}
