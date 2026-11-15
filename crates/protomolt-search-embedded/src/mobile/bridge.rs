//! The bridge's shared plumbing: the handle registry, the runtime, the
//! error envelope, and the encode/decode helpers every operation uses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use prost::Message;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::ReceiverStream;

use pipestream_search::embedded::{EmbeddedError, EmbeddedSearch};
use pipestream_search::pb::mobile::{
    mobile_response, MobileError, MobileErrorCode, MobileResponse,
};
use pipestream_search::pb::QueryStreamResponse;

pub(super) type QueryReceiver =
    pipestream_search::metrics::Timed<ReceiverStream<Result<QueryStreamResponse, tonic::Status>>>;

pub(super) struct OpenStream {
    pub(super) owner: u64,
    pub(super) receiver: Option<QueryReceiver>,
    pub(super) cancel: tokio::sync::watch::Sender<bool>,
}

pub(super) struct Registry {
    pub(super) next: u64,
    pub(super) searches: HashMap<u64, Arc<EmbeddedSearch>>,
    pub(super) streams: HashMap<u64, OpenStream>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            next: 1,
            searches: HashMap::new(),
            streams: HashMap::new(),
        }
    }
}

impl Registry {
    pub(super) fn allocate(&mut self) -> Result<u64, BridgeError> {
        let handle = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| BridgeError::internal("mobile handle space exhausted"))?;
        Ok(handle)
    }
}

#[derive(Debug)]
pub(super) struct BridgeError {
    pub(super) code: MobileErrorCode,
    pub(super) message: String,
}

impl BridgeError {
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: MobileErrorCode::InvalidArgument,
            message: message.into(),
        }
    }

    pub(super) fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: MobileErrorCode::NotFound,
            message: message.into(),
        }
    }

    pub(super) fn internal(message: impl Into<String>) -> Self {
        Self {
            code: MobileErrorCode::Internal,
            message: message.into(),
        }
    }
}

impl From<EmbeddedError> for BridgeError {
    fn from(error: EmbeddedError) -> Self {
        let code = match error {
            EmbeddedError::InvalidConfig(_) => MobileErrorCode::InvalidArgument,
            EmbeddedError::ExistingData(_) => MobileErrorCode::AlreadyExists,
            EmbeddedError::OpenShard { .. } => MobileErrorCode::FailedPrecondition,
            EmbeddedError::Rpc(ref status) => mobile_code(status.code()),
        };
        Self {
            code,
            message: error.to_string(),
        }
    }
}

impl From<tonic::Status> for BridgeError {
    fn from(status: tonic::Status) -> Self {
        Self {
            code: mobile_code(status.code()),
            message: status.message().to_string(),
        }
    }
}

pub(super) fn mobile_code(code: tonic::Code) -> MobileErrorCode {
    match code {
        tonic::Code::InvalidArgument => MobileErrorCode::InvalidArgument,
        tonic::Code::NotFound => MobileErrorCode::NotFound,
        tonic::Code::AlreadyExists => MobileErrorCode::AlreadyExists,
        tonic::Code::FailedPrecondition => MobileErrorCode::FailedPrecondition,
        tonic::Code::Cancelled => MobileErrorCode::Cancelled,
        tonic::Code::Aborted => MobileErrorCode::Aborted,
        tonic::Code::ResourceExhausted => MobileErrorCode::ResourceExhausted,
        tonic::Code::DataLoss => MobileErrorCode::DataLoss,
        _ => MobileErrorCode::Internal,
    }
}

pub(super) fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

pub(super) fn runtime() -> Result<&'static Runtime, BridgeError> {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let candidate = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("protomolt-mobile")
        .build()
        .map_err(|error| BridgeError::internal(format!("create mobile runtime: {error}")))?;
    let _ = RUNTIME.set(candidate);
    RUNTIME
        .get()
        .ok_or_else(|| BridgeError::internal("mobile runtime initialization raced"))
}

pub(super) fn lock_registry() -> Result<std::sync::MutexGuard<'static, Registry>, BridgeError> {
    registry()
        .lock()
        .map_err(|_| BridgeError::internal("mobile handle registry is poisoned"))
}

pub(super) fn decode<M: Message + Default>(bytes: &[u8], what: &str) -> Result<M, BridgeError> {
    M::decode(bytes).map_err(|error| BridgeError::invalid(format!("decode {what}: {error}")))
}

pub(super) fn encoded<M: Message>(message: &M) -> Vec<u8> {
    message.encode_to_vec()
}

pub(super) fn success<M: Message>(message: &M) -> Vec<u8> {
    MobileResponse {
        outcome: Some(mobile_response::Outcome::Payload(encoded(message))),
    }
    .encode_to_vec()
}

pub(super) fn failure(error: BridgeError) -> Vec<u8> {
    MobileResponse {
        outcome: Some(mobile_response::Outcome::Error(MobileError {
            code: error.code as i32,
            message: error.message,
        })),
    }
    .encode_to_vec()
}

pub(super) fn response(operation: impl FnOnce() -> Result<Vec<u8>, BridgeError>) -> Vec<u8> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => failure(error),
        Err(_) => failure(BridgeError::internal("panic inside mobile bridge")),
    }
}
