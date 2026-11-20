//! `ConsoleBffService`, the browser's only surface: the method table, the
//! request guard, Connect errors, and unary calls.

use std::net::IpAddr;

use prost_reflect::DynamicMessage;

use super::*;

pub(super) const BFF_SERVICE: &str = "ai.protomolt.console.bff.v1.ConsoleBffService";
/// The Connect streaming media type with JSON messages.
pub(super) const CONNECT_STREAM_TYPE: &str = "application/connect+json";
/// The BFF service's descriptor (`ui/scripts/gen.sh` writes it from
/// `ui/proto`); its imports resolve against [`descriptor_pool`].
const BFF_SET: &[u8] = include_bytes!("console_bff.binpb");

/// What a BFF method does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Op {
    /// The coordinator's `SearchService` method of the same name and types.
    Upstream,
    Config,
    Health,
    Collections,
    Describe,
    PreviewDocuments,
    PreviewEmbedding,
}

/// The whole browser surface. A method of the descriptor that is absent
/// here is not served.
pub(super) const METHODS: &[(&str, Op)] = &[
    ("Query", Op::Upstream),
    ("QueryStream", Op::Upstream),
    ("VariantSearch", Op::Upstream),
    ("Suggest", Op::Upstream),
    ("TermSuggest", Op::Upstream),
    ("ListCollections", Op::Collections),
    ("DescribeMethod", Op::Describe),
    ("GetConsoleConfig", Op::Config),
    ("HealthSummary", Op::Health),
    ("PreviewDocuments", Op::PreviewDocuments),
    ("PreviewEmbedding", Op::PreviewEmbedding),
];

/// The search descriptors plus the BFF service.
pub(super) fn bff_pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let mut pool = descriptor_pool().clone();
        pool.decode_file_descriptor_set(BFF_SET)
            .expect("the BFF descriptor resolves against the search protos");
        pool
    })
}

/// A served BFF method and what it does.
pub(super) fn bff_method(name: &str) -> Result<(MethodDescriptor, Op), BffError> {
    let not_served = || {
        BffError::new(
            Code::NotFound,
            format!("{BFF_SERVICE} has no method {name:?}"),
        )
    };
    let op = METHODS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, op)| *op)
        .ok_or_else(not_served)?;
    let method = bff_pool()
        .get_service_by_name(BFF_SERVICE)
        .and_then(|s| s.methods().find(|m| m.name() == name))
        .ok_or_else(not_served)?;
    Ok((method, op))
}

/// A Connect error: its code, the message the browser sees, and the HTTP
/// status when the protocol, not the code, decides it.
#[derive(Debug)]
pub(super) struct BffError {
    pub(super) code: Code,
    pub(super) message: String,
    http: Option<u16>,
}

impl BffError {
    pub(super) fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            http: None,
        }
    }

    fn media(message: impl Into<String>) -> Self {
        Self {
            http: Some(415),
            ..Self::new(Code::InvalidArgument, message)
        }
    }

    /// An upstream status. Codes that describe the cluster rather than the
    /// request get a generic message, so no address or internal detail
    /// reaches the browser.
    pub(super) fn upstream(status: &Status) -> Self {
        let code = status.code();
        let internal = matches!(
            code,
            Code::Unknown
                | Code::Internal
                | Code::Unavailable
                | Code::DataLoss
                | Code::DeadlineExceeded
                | Code::Cancelled
                | Code::Aborted
        );
        if internal {
            let message = format!(
                "the search cluster could not complete the call ({})",
                connect_code(code)
            );
            return Self::new(code, message);
        }
        Self::new(code, status.message())
    }

    pub(super) fn json(&self) -> Value {
        json!({ "code": connect_code(self.code), "message": self.message })
    }

    pub(super) fn reply(&self) -> Reply {
        Reply {
            status: self.http.unwrap_or_else(|| connect_http_status(self.code)),
            content_type: "application/json",
            body: self.json().to_string().into_bytes(),
        }
    }
}

/// The Connect protocol's code names.
pub(super) fn connect_code(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "canceled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// The Connect protocol's code-to-HTTP table.
fn connect_http_status(code: Code) -> u16 {
    match code {
        Code::Ok => 200,
        Code::Cancelled => 499,
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => 400,
        Code::DeadlineExceeded => 504,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::Aborted => 409,
        Code::PermissionDenied => 403,
        Code::ResourceExhausted => 429,
        Code::Unimplemented => 501,
        Code::Unavailable => 503,
        Code::Unauthenticated => 401,
        Code::Unknown | Code::Internal | Code::DataLoss => 500,
    }
}

/// Refuses what a page on another origin or a rebound DNS name could send,
/// and the explorer's `target` routing.
pub(super) fn guard(ctx: &Ctx, request: &HttpRequest) -> Result<(), BffError> {
    let denied = |m: String| Err(BffError::new(Code::PermissionDenied, m));
    if param(&request.query, "target").is_some() {
        return denied("the BFF calls the coordinator only; `target` is not accepted".into());
    }
    let host = request.headers.host.as_deref().unwrap_or_default();
    if ctx.loopback && !is_loopback_host(host) {
        return denied(format!(
            "Host {host:?} is not a loopback name of this console"
        ));
    }
    guard_origin(request)
}

/// A request with an `Origin` must come from a page this console served.
pub(super) fn guard_origin(request: &HttpRequest) -> Result<(), BffError> {
    let host = request.headers.host.as_deref().unwrap_or_default();
    match request.headers.origin.as_deref() {
        Some(origin) if origin.split_once("://").map(|(_, authority)| authority) != Some(host) => {
            let message = format!("Origin {origin:?} is not this console's");
            Err(BffError::new(Code::PermissionDenied, message))
        }
        _ => Ok(()),
    }
}

/// `localhost` or a loopback IP, with or without a port.
pub(super) fn is_loopback_host(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or_default(),
        None => host.rsplit_once(':').map_or(host, |(name, _)| name),
    };
    name == "localhost" || name.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The coordinator path of an [`Op::Upstream`] method.
pub(super) fn upstream_path(method: &MethodDescriptor) -> PathAndQuery {
    PathAndQuery::from_maybe_shared(format!("/{PACKAGE}.SearchService/{}", method.name()))
        .expect("a proto method name is a valid path")
}

/// proto3 JSON -> the message `method` takes.
pub(super) fn decode_input(
    method: &MethodDescriptor,
    json: &[u8],
) -> Result<DynamicMessage, BffError> {
    let bytes = json_to_message_bytes(&method.input(), json)
        .map_err(|e| BffError::new(Code::InvalidArgument, e))?;
    DynamicMessage::decode(method.input(), bytes.as_slice())
        .map_err(|e| BffError::new(Code::Internal, e.to_string()))
}

/// A coordinator connection for one call.
pub(super) async fn coordinator_grpc(ctx: &Ctx) -> Result<RawGrpc, BffError> {
    let unavailable = || BffError::upstream(&Status::unavailable(""));
    let mut grpc = raw_grpc(ctx, &ctx.coordinator).map_err(|_| unavailable())?;
    grpc.ready().await.map_err(|_| unavailable())?;
    Ok(grpc)
}

/// `POST /bff/v1/<BFF_SERVICE>/<Method>`: one Connect unary call.
pub(super) async fn bff_unary(
    ctx: &Arc<Ctx>,
    request: &HttpRequest,
    service: &str,
    method: &str,
) -> Reply {
    match unary_json(ctx, request, service, method).await {
        Ok(json) => Reply::json_bytes(json),
        Err(e) => e.reply(),
    }
}

async fn unary_json(
    ctx: &Arc<Ctx>,
    request: &HttpRequest,
    service: &str,
    name: &str,
) -> Result<Vec<u8>, BffError> {
    guard(ctx, request)?;
    if service != BFF_SERVICE {
        let message = format!("{service} is not served; the browser surface is {BFF_SERVICE}");
        return Err(BffError::new(Code::NotFound, message));
    }
    let (method, op) = bff_method(name)?;
    if method.is_server_streaming() {
        return Err(BffError::media(format!(
            "{name} is server-streaming: send {CONNECT_STREAM_TYPE}"
        )));
    }
    if request.headers.content_type != "application/json" {
        return Err(BffError::media("Connect unary calls are application/json"));
    }
    let input = decode_input(&method, &request.body)?;
    let output = match op {
        Op::Upstream => {
            let mut grpc = coordinator_grpc(ctx).await?;
            let call = Request::new(Bytes::from(input.encode_to_vec()));
            grpc.unary(call, upstream_path(&method), RawCodec)
                .await
                .map_err(|s| BffError::upstream(&s))?
                .into_inner()
                .to_vec()
        }
        op => {
            let value = own_op(ctx, op, &input).await?;
            json_to_message_bytes(&method.output(), value.to_string().as_bytes())
                .map_err(|e| BffError::new(Code::Internal, e))?
        }
    };
    message_bytes_to_json(&method.output(), &output).map_err(|e| BffError::new(Code::Internal, e))
}
