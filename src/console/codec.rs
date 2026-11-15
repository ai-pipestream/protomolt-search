//! The raw-bytes gRPC codec and the JSON/proto transcoding around it, with
//! the gRPC-to-HTTP status mapping.

use super::*;

/// A codec that moves message bytes untouched: the facade encodes and
/// decodes through the descriptor, so the transport only frames.
#[derive(Default)]
pub(super) struct RawCodec;

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

pub(super) struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

pub(super) struct RawDecoder;

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

pub(super) fn method_path(method: &MethodDescriptor) -> PathAndQuery {
    let path = format!("/{}/{}", method.parent_service().full_name(), method.name());
    PathAndQuery::from_maybe_shared(path).expect("a proto method name is a valid path")
}

pub(super) type RawGrpc = tonic::client::Grpc<InterceptedService<Channel, Bearer>>;

pub(super) fn raw_grpc(ctx: &Ctx, addr: &str) -> Result<RawGrpc, Reply> {
    let channel = ctx
        .channel(addr)
        .map_err(|e| Reply::error(502, "UNAVAILABLE", e))?;
    Ok(
        tonic::client::Grpc::new(InterceptedService::new(channel, ctx.security.bearer()))
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES),
    )
}

/// proto3 JSON -> message bytes for `message`. An empty document is
/// the empty message. Shared with the `raft-bootstrap` CLI's `--policy`
/// and `--limits` files, so the console and the CLI reject the same
/// documents the same way.
pub fn json_to_message_bytes(message: &MessageDescriptor, json: &[u8]) -> Result<Vec<u8>, String> {
    let body: &[u8] = if json.iter().all(u8::is_ascii_whitespace) {
        b"{}"
    } else {
        json
    };
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let decoded = DynamicMessage::deserialize(message.clone(), &mut deserializer)
        .map_err(|e| format!("request JSON for {}: {e}", message.full_name()))?;
    Ok(decoded.encode_to_vec())
}

/// Message bytes -> proto3 JSON for `message`. Every field is rendered,
/// defaults included (what grpcurl calls `-emit-defaults`): a document
/// id of 0 or a score of 0 is a value a reader needs to see, not an
/// omission. Shared with the `raft-*` CLI's status printing, which is
/// the console's rendering of the same bytes.
pub fn message_bytes_to_json(message: &MessageDescriptor, bytes: &[u8]) -> Result<Vec<u8>, String> {
    let decoded = DynamicMessage::decode(message.clone(), bytes)
        .map_err(|e| format!("response bytes for {}: {e}", message.full_name()))?;
    let mut out = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut out);
    decoded
        .serialize_with_options(
            &mut serializer,
            &SerializeOptions::new().skip_default_fields(false),
        )
        .map_err(|e| format!("response JSON: {e}"))?;
    Ok(out)
}

/// proto3 JSON -> message bytes for `method`'s input. An empty body is
/// the empty message.
pub(super) fn encode_request(method: &MethodDescriptor, json: &[u8]) -> Result<Bytes, Reply> {
    json_to_message_bytes(&method.input(), json)
        .map(Bytes::from)
        .map_err(|e| Reply::error(400, "INVALID_ARGUMENT", e))
}

/// Message bytes -> proto3 JSON for `method`'s output.
pub(super) fn decode_response(method: &MethodDescriptor, bytes: Bytes) -> Result<Vec<u8>, Reply> {
    message_bytes_to_json(&method.output(), &bytes).map_err(|e| Reply::error(502, "INTERNAL", e))
}

/// The doc's status-to-HTTP table.
pub(super) fn http_status(code: Code) -> u16 {
    match code {
        Code::InvalidArgument => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::ResourceExhausted => 429,
        Code::Unimplemented => 501,
        _ => 502,
    }
}

pub(super) fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

pub(super) fn status_reply(status: &Status) -> Reply {
    Reply::error(
        http_status(status.code()),
        code_name(status.code()),
        status.message().to_string(),
    )
}

/// A status from a call, with an empty UNIMPLEMENTED (what a server
/// answers for a service it does not register) given words.
pub(super) fn call_status_reply(status: &Status, method: &MethodDescriptor, addr: &str) -> Reply {
    if status.code() == Code::Unimplemented && status.message().is_empty() {
        return Reply::error(
            501,
            "UNIMPLEMENTED",
            format!(
                "{}.{} is not served by {addr}",
                method.parent_service().name(),
                method.name()
            ),
        );
    }
    status_reply(status)
}
