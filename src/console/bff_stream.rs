//! BFF server streams: the Connect protocol's enveloped stream, and the
//! same stream as server-sent events with `: keepalive` comments.

use tokio_stream::{Stream, StreamExt};

use super::*;

/// The envelope flag of a stream's last message.
pub(super) const END_STREAM: u8 = 0x02;
/// The envelope flag of a compressed message.
const COMPRESSED: u8 = 0x01;
/// The comment a server-sent stream carries while it waits.
pub(super) const KEEPALIVE: &[u8] = b": keepalive\n\n";

/// The single message of a Connect streaming request body.
pub(super) fn request_envelope(body: &[u8]) -> Result<&[u8], BffError> {
    let invalid = |m: &str| Err(BffError::new(Code::InvalidArgument, m));
    let Some((&flags, rest)) = body.split_first() else {
        return invalid("a Connect stream request is one envelope");
    };
    if flags & COMPRESSED != 0 {
        return Err(BffError::new(
            Code::Unimplemented,
            "compressed envelopes are not supported",
        ));
    }
    if flags != 0 || rest.len() < 4 {
        return invalid("a Connect stream request is one uncompressed envelope");
    }
    let (length, message) = rest.split_at(4);
    let length = u32::from_be_bytes(length.try_into().expect("four bytes")) as usize;
    if length != message.len() {
        return invalid("the envelope length does not match the request body");
    }
    Ok(message)
}

/// One Connect envelope.
pub(super) fn envelope(flags: u8, message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + message.len());
    out.push(flags);
    out.extend_from_slice(&(message.len() as u32).to_be_bytes());
    out.extend_from_slice(message);
    out
}

/// The stream's end: `{}`, or the error that ended it.
pub(super) fn end_stream(error: Option<&BffError>) -> Vec<u8> {
    let body = match error {
        Some(e) => json!({ "error": e.json() }),
        None => json!({}),
    };
    envelope(END_STREAM, body.to_string().as_bytes())
}

/// A served streaming method, resolved and checked; nothing is called yet.
fn streaming_method(
    ctx: &Ctx,
    request: &HttpRequest,
    name: &str,
) -> Result<MethodDescriptor, BffError> {
    guard(ctx, request)?;
    let (method, op) = bff_method(name)?;
    if !method.is_server_streaming() || op != Op::Upstream {
        let message = format!("{name} is unary: call it as application/json");
        return Err(BffError::new(Code::InvalidArgument, message));
    }
    Ok(method)
}

type Upstream = tonic::Streaming<Bytes>;

/// Opens the coordinator's stream for `method` with the JSON request.
async fn open_upstream(
    ctx: &Ctx,
    method: &MethodDescriptor,
    json: &[u8],
) -> Result<Upstream, BffError> {
    let input = decode_input(method, json)?;
    let mut grpc = coordinator_grpc(ctx).await?;
    let call = Request::new(Bytes::from(input.encode_to_vec()));
    grpc.server_streaming(call, upstream_path(method), RawCodec)
        .await
        .map(|r| r.into_inner())
        .map_err(|s| BffError::upstream(&s))
}

/// Each upstream message as proto3 JSON of `method`'s output.
fn json_messages(
    method: MethodDescriptor,
    upstream: Upstream,
) -> impl Stream<Item = Result<Vec<u8>, BffError>> {
    upstream.map(move |next| {
        let bytes = next.map_err(|s| BffError::upstream(&s))?;
        message_bytes_to_json(&method.output(), &bytes)
            .map_err(|e| BffError::new(Code::Internal, e))
    })
}

/// `POST /bff/v1/<BFF_SERVICE>/<Method>` as `application/connect+json`.
/// A request the protocol refuses answers with an HTTP error; once the
/// call is accepted, the answer is 200 and any failure is the end-stream
/// message's error.
pub(super) async fn bff_connect_stream(
    stream: &mut TcpStream,
    ctx: &Ctx,
    request: &HttpRequest,
    name: &str,
) -> std::io::Result<()> {
    let accepted = streaming_method(ctx, request, name)
        .and_then(|method| request_envelope(&request.body).map(|json| (method, json)));
    let (method, json) = match accepted {
        Ok(accepted) => accepted,
        Err(e) => return write_reply(stream, e.reply(), false).await,
    };
    let head = b"HTTP/1.1 200 OK\r\nContent-Type: application/connect+json\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
    write_frame(stream, &[head]).await?;
    let mut messages = match open_upstream(ctx, &method, json).await {
        Ok(upstream) => Box::pin(json_messages(method, upstream)),
        Err(e) => return write_frame(stream, &[&end_stream(Some(&e))]).await,
    };
    while let Some(next) = messages.next().await {
        match next {
            Ok(json) => write_frame(stream, &[&envelope(0, &json)]).await?,
            Err(e) => return write_frame(stream, &[&end_stream(Some(&e))]).await,
        }
    }
    write_frame(stream, &[&end_stream(None)]).await
}

/// `POST /bff/v1/sse/<Method>` with the request as `application/json`: the
/// stream as `message` events, then one `end` or `error` event.
pub(super) async fn bff_sse(
    stream: &mut TcpStream,
    ctx: &Ctx,
    request: &HttpRequest,
    name: &str,
) -> std::io::Result<()> {
    let method = match streaming_method(ctx, request, name) {
        Ok(method) if request.headers.content_type == "application/json" => method,
        Ok(_) => {
            let refused = BffError::new(
                Code::InvalidArgument,
                "an SSE request body is application/json",
            );
            return write_reply(
                stream,
                Reply {
                    status: 415,
                    ..refused.reply()
                },
                false,
            )
            .await;
        }
        Err(e) => return write_reply(stream, e.reply(), false).await,
    };
    let upstream = match open_upstream(ctx, &method, &request.body).await {
        Ok(upstream) => upstream,
        Err(e) => return write_reply(stream, e.reply(), false).await,
    };
    let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nX-Accel-Buffering: no\r\nConnection: close\r\n\r\n";
    write_frame(stream, &[head]).await?;
    sse_pump(stream, ctx.sse_heartbeat, json_messages(method, upstream)).await
}

/// Writes `messages` as server-sent events: a `: keepalive` comment at
/// once and after every `period` without a message, so a proxy that
/// drops idle connections keeps this one.
pub(super) async fn sse_pump<W, S>(
    out: &mut W,
    period: Duration,
    messages: S,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    S: Stream<Item = Result<Vec<u8>, BffError>>,
{
    let mut messages = std::pin::pin!(messages);
    write_frame(out, &[KEEPALIVE]).await?;
    let mut beat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    loop {
        tokio::select! {
            biased;
            next = messages.next() => match next {
                Some(Ok(json)) => write_frame(out, &[b"event: message\ndata: ", &json, b"\n\n"]).await?,
                Some(Err(e)) => {
                    let error = e.json().to_string();
                    return write_frame(out, &[b"event: error\ndata: ", error.as_bytes(), b"\n\n"]).await;
                }
                None => return write_frame(out, &[b"event: end\ndata: {}\n\n"]).await,
            },
            _ = beat.tick() => write_frame(out, &[KEEPALIVE]).await?,
        }
    }
}
