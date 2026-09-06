//! Stable byte-oriented ABI used by the Android AAR and Apple XCFramework.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use prost::Message;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use pipestream_search::embedded::{
    EmbeddedDocumentCatalogConfig, EmbeddedError, EmbeddedSearch, EmbeddedSearchConfig,
    EmbeddedShardConfig,
};
use pipestream_search::pb::mobile::{
    mobile_response, MobileCloseResponse, MobileError, MobileErrorCode, MobileFlushResponse,
    MobileIngestMappedBatch, MobileOpenRequest, MobileOpenResponse, MobileQueryStreamNextResponse,
    MobileQueryStreamOpenResponse, MobileResponse, MobileShardConfig,
};
use pipestream_search::pb::{
    AcceptDocumentRequest, DescribeSchemaRequest, PlanIndexRequest, QueryRequest, QueryResponse,
    QueryStreamRequest, QueryStreamResponse, ReadAcceptedDocumentsRequest,
};

type QueryReceiver =
    pipestream_search::metrics::Timed<ReceiverStream<Result<QueryStreamResponse, tonic::Status>>>;

struct OpenStream {
    owner: u64,
    receiver: Option<QueryReceiver>,
    cancel: tokio::sync::watch::Sender<bool>,
}

struct Registry {
    next: u64,
    searches: HashMap<u64, Arc<EmbeddedSearch>>,
    streams: HashMap<u64, OpenStream>,
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
    fn allocate(&mut self) -> Result<u64, BridgeError> {
        let handle = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| BridgeError::internal("mobile handle space exhausted"))?;
        Ok(handle)
    }
}

#[derive(Debug)]
struct BridgeError {
    code: MobileErrorCode,
    message: String,
}

impl BridgeError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: MobileErrorCode::InvalidArgument,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: MobileErrorCode::NotFound,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
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

fn mobile_code(code: tonic::Code) -> MobileErrorCode {
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

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

fn runtime() -> Result<&'static Runtime, BridgeError> {
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

fn lock_registry() -> Result<std::sync::MutexGuard<'static, Registry>, BridgeError> {
    registry()
        .lock()
        .map_err(|_| BridgeError::internal("mobile handle registry is poisoned"))
}

fn decode<M: Message + Default>(bytes: &[u8], what: &str) -> Result<M, BridgeError> {
    M::decode(bytes).map_err(|error| BridgeError::invalid(format!("decode {what}: {error}")))
}

fn encoded<M: Message>(message: &M) -> Vec<u8> {
    message.encode_to_vec()
}

fn success<M: Message>(message: &M) -> Vec<u8> {
    MobileResponse {
        outcome: Some(mobile_response::Outcome::Payload(encoded(message))),
    }
    .encode_to_vec()
}

fn failure(error: BridgeError) -> Vec<u8> {
    MobileResponse {
        outcome: Some(mobile_response::Outcome::Error(MobileError {
            code: error.code as i32,
            message: error.message,
        })),
    }
    .encode_to_vec()
}

fn response(operation: impl FnOnce() -> Result<Vec<u8>, BridgeError>) -> Vec<u8> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => failure(error),
        Err(_) => failure(BridgeError::internal("panic inside mobile bridge")),
    }
}

fn node_config(config: MobileShardConfig) -> Result<EmbeddedShardConfig, BridgeError> {
    if config.in_memory && !config.index_path.is_empty() {
        return Err(BridgeError::invalid(
            "mobile shard cannot set both in_memory and index_path",
        ));
    }
    let mut shard = if config.in_memory {
        EmbeddedShardConfig::in_memory(config.slot_offset)
    } else if config.index_path.is_empty() {
        return Err(BridgeError::invalid(
            "mobile persisted shard requires index_path",
        ));
    } else {
        EmbeddedShardConfig::persistent(PathBuf::from(config.index_path), config.slot_offset)
    };
    shard.allow_missing_bm25 = config.allow_missing_bm25;

    let node = &mut shard.node;
    if let Some(value) = config.vector_backend {
        node.vector_backend = value;
    }
    if let Some(value) = config.bit_width {
        node.bit_width = value as usize;
    }
    if !config.bm25_fields.is_empty() {
        node.bm25_fields = config.bm25_fields;
    }
    node.facet_fields = config.facet_fields;
    node.numeric_fields = config.numeric_fields;
    node.map_facet_fields = config.map_facet_fields;
    node.map_numeric_fields = config.map_numeric_fields;
    node.integer_fields = config.integer_fields;
    node.unsigned_integer_fields = config.unsigned_integer_fields;
    node.geo_fields = config.geo_fields;
    if let Some(value) = config.wal {
        node.wal = value;
    }
    if let Some(value) = config.wal_buckets {
        node.wal_buckets = value;
    }
    if let Some(value) = config.vocab {
        node.vocab = value;
    }
    if let Some(value) = config.vocab_window_docs {
        node.vocab_window_docs = value;
    }
    if let Some(value) = config.vocab_top_k {
        node.vocab_top_k = value as usize;
    }
    if let Some(value) = config.chunk_blocks {
        node.chunk_blocks = value as usize;
    }
    if let Some(value) = config.share_floors {
        node.share_floors = value;
    }
    if let Some(value) = config.block_max {
        node.block_max = value;
    }
    if let Some(value) = config.floor_delta {
        node.floor_delta = value;
    }
    if let Some(value) = config.floor_warmup_chunks {
        node.floor_warmup_chunks = value;
    }
    if let Some(value) = config.floor_min_interval_ms {
        node.floor_min_interval_ms = value;
    }
    if let Some(value) = config.coalesce {
        node.coalesce = value;
    }
    if let Some(value) = config.scan_parallel {
        node.scan_parallel = value as usize;
    }
    if let Some(value) = config.rerank_parallel {
        node.rerank_parallel = value as usize;
    }
    Ok(shard)
}

fn embedded_config(request: MobileOpenRequest) -> Result<EmbeddedSearchConfig, BridgeError> {
    if request.shards.is_empty() {
        return Err(BridgeError::invalid(
            "mobile open request requires at least one shard",
        ));
    }
    let shards = request
        .shards
        .into_iter()
        .map(node_config)
        .collect::<Result<Vec<_>, _>>()?;
    let mut config = EmbeddedSearchConfig::new(shards);
    if let Some(catalog) = request.document_catalog {
        if catalog.in_memory != catalog.path.is_empty() {
            return Err(BridgeError::invalid(
                "document catalog path must be empty exactly when in_memory is true",
            ));
        }
        for shard in &mut config.shards {
            shard.node.collection = catalog.collection.clone();
        }
        config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
            collection: catalog.collection,
            path: (!catalog.in_memory).then(|| PathBuf::from(catalog.path)),
        });
    }
    if let Some(value) = request.bm25_k1 {
        config.bm25_params.k1 = value;
    }
    if let Some(value) = request.bm25_b {
        config.bm25_params.b = value;
    }
    if let Some(value) = request.stream_search {
        config.stream_search = value;
    }
    if let Some(value) = request.bm25_stream {
        config.bm25_stream = value;
    }
    if let Some(value) = request.max_k {
        config.max_k = value;
    }
    if let Some(value) = request.max_rerank_bytes {
        config.max_rerank_bytes = value;
    }
    config.topology_generation = request.topology_generation;
    Ok(config)
}

fn search(handle: u64) -> Result<Arc<EmbeddedSearch>, BridgeError> {
    lock_registry()?
        .searches
        .get(&handle)
        .cloned()
        .ok_or_else(|| BridgeError::not_found(format!("unknown search handle {handle}")))
}

fn open_bytes(input: &[u8], create: bool) -> Vec<u8> {
    response(|| {
        let request = decode::<MobileOpenRequest>(input, "MobileOpenRequest")?;
        let config = embedded_config(request)?;
        let runtime = runtime()?;
        let search = if create {
            runtime.block_on(EmbeddedSearch::create(config))?
        } else {
            runtime.block_on(EmbeddedSearch::open(config))?
        };
        if search.allows_network() {
            return Err(BridgeError::internal(
                "embedded runtime unexpectedly permits network access",
            ));
        }
        let shard_count = search.shard_count() as u32;
        let mut registry = lock_registry()?;
        let handle = registry.allocate()?;
        registry.searches.insert(handle, Arc::new(search));
        Ok(success(&MobileOpenResponse {
            handle,
            shard_count,
            no_egress: true,
        }))
    })
}

fn ingest_mapped_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let batch = decode::<MobileIngestMappedBatch>(input, "MobileIngestMappedBatch")?;
        let search = search(handle)?;
        let result =
            runtime()?.block_on(search.ingest_mapped(batch.shard as usize, batch.requests))?;
        Ok(success(&result))
    })
}

fn accept_document_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<AcceptDocumentRequest>(input, "AcceptDocumentRequest")?;
        let result = search(handle)?.accept_document(&request)?;
        Ok(success(&result))
    })
}

fn describe_schema_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<DescribeSchemaRequest>(input, "DescribeSchemaRequest")?;
        let search = search(handle)?;
        let result = runtime()?.block_on(search.describe_schema(request))?;
        Ok(success(&result))
    })
}

fn plan_index_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<PlanIndexRequest>(input, "PlanIndexRequest")?;
        let search = search(handle)?;
        let result = runtime()?.block_on(search.plan_index(request))?;
        Ok(success(&result))
    })
}

fn query_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<QueryRequest>(input, "QueryRequest")?;
        let search = search(handle)?;
        let result: QueryResponse = runtime()?.block_on(search.query(request))?;
        Ok(success(&result))
    })
}

fn read_accepted_documents_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request =
            decode::<ReadAcceptedDocumentsRequest>(input, "ReadAcceptedDocumentsRequest")?;
        let result = search(handle)?.read_accepted_documents(&request)?;
        Ok(success(&result))
    })
}

fn query_stream_open_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<QueryStreamRequest>(input, "QueryStreamRequest")?;
        let search = search(handle)?;
        let receiver = runtime()?.block_on(search.query_stream(request))?;
        let mut registry = lock_registry()?;
        if !registry.searches.contains_key(&handle) {
            return Err(BridgeError::not_found(format!(
                "unknown search handle {handle}"
            )));
        }
        let stream_handle = registry.allocate()?;
        registry.streams.insert(
            stream_handle,
            OpenStream {
                owner: handle,
                receiver: Some(receiver),
                cancel: tokio::sync::watch::channel(false).0,
            },
        );
        Ok(success(&MobileQueryStreamOpenResponse { stream_handle }))
    })
}

fn query_stream_next_bytes(stream_handle: u64) -> Vec<u8> {
    response(|| {
        let (mut receiver, mut cancel) = {
            let mut registry = lock_registry()?;
            let open = registry.streams.get_mut(&stream_handle).ok_or_else(|| {
                BridgeError::not_found(format!("unknown stream handle {stream_handle}"))
            })?;
            let receiver = open.receiver.take().ok_or_else(|| BridgeError {
                code: MobileErrorCode::FailedPrecondition,
                message: "a read is already pending on this stream".into(),
            })?;
            (receiver, open.cancel.subscribe())
        };
        let item = runtime()?.block_on(async {
            tokio::select! {
                biased;
                _ = cancel.changed() => None,
                item = receiver.next() => Some(item),
            }
        });
        let mut registry = lock_registry()?;
        if !registry.streams.contains_key(&stream_handle) || item.is_none() {
            return Err(BridgeError {
                code: MobileErrorCode::Cancelled,
                message: "mobile query stream closed".into(),
            });
        }
        match item.unwrap() {
            Some(Ok(item)) => {
                registry.streams.get_mut(&stream_handle).unwrap().receiver = Some(receiver);
                Ok(success(&MobileQueryStreamNextResponse {
                    response: Some(item),
                    end: false,
                }))
            }
            Some(Err(status)) => {
                registry.streams.remove(&stream_handle);
                Err(status.into())
            }
            None => {
                registry.streams.remove(&stream_handle);
                Ok(success(&MobileQueryStreamNextResponse {
                    response: None,
                    end: true,
                }))
            }
        }
    })
}

fn query_stream_close_bytes(stream_handle: u64) -> Vec<u8> {
    response(|| {
        let stream = lock_registry()?.streams.remove(&stream_handle);
        let closed = stream.is_some();
        if let Some(stream) = stream {
            let _ = stream.cancel.send(true);
        }
        Ok(success(&MobileCloseResponse { closed }))
    })
}

fn flush_bytes(handle: u64) -> Vec<u8> {
    response(|| {
        let search = search(handle)?;
        let shards = runtime()?.block_on(search.flush_all())?;
        Ok(success(&MobileFlushResponse { shards }))
    })
}

fn close_bytes(handle: u64) -> Vec<u8> {
    response(|| {
        let mut registry = lock_registry()?;
        let closed = registry.searches.remove(&handle).is_some();
        registry.streams.retain(|_, stream| {
            if stream.owner == handle {
                let _ = stream.cancel.send(true);
                false
            } else {
                true
            }
        });
        Ok(success(&MobileCloseResponse { closed }))
    })
}

/// Owned result bytes returned by the C ABI. Release exactly once with
/// `protomolt_search_buffer_free`.
#[repr(C)]
pub struct MobileBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

impl From<Vec<u8>> for MobileBuffer {
    fn from(mut bytes: Vec<u8>) -> Self {
        let buffer = Self {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
            capacity: bytes.capacity(),
        };
        std::mem::forget(bytes);
        buffer
    }
}

unsafe fn input<'a>(data: *const u8, len: usize) -> Result<&'a [u8], BridgeError> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(BridgeError::invalid(
            "null input pointer with nonzero length",
        ));
    }
    // SAFETY: the caller contract requires `data` to remain readable for
    // `len` bytes for the duration of this synchronous call.
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

fn ffi_input(data: *const u8, len: usize, op: impl FnOnce(&[u8]) -> Vec<u8>) -> MobileBuffer {
    let bytes = match unsafe { input(data, len) } {
        Ok(input) => op(input),
        Err(error) => failure(error),
    };
    bytes.into()
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_open(
    request: *const u8,
    request_len: usize,
    create: u8,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| open_bytes(bytes, create != 0))
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_ingest_mapped(
    handle: u64,
    request: *const u8,
    request_len: usize,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| {
        ingest_mapped_bytes(handle, bytes)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_accept_document(
    handle: u64,
    request: *const u8,
    request_len: usize,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| {
        accept_document_bytes(handle, bytes)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_describe_schema(
    handle: u64,
    request: *const u8,
    request_len: usize,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| {
        describe_schema_bytes(handle, bytes)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_read_accepted_documents(
    handle: u64,
    request: *const u8,
    request_len: usize,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| {
        read_accepted_documents_bytes(handle, bytes)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_plan_index(
    handle: u64,
    request: *const u8,
    request_len: usize,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| {
        plan_index_bytes(handle, bytes)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_query(
    handle: u64,
    request: *const u8,
    request_len: usize,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| query_bytes(handle, bytes))
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_query_stream_open(
    handle: u64,
    request: *const u8,
    request_len: usize,
) -> MobileBuffer {
    ffi_input(request, request_len, |bytes| {
        query_stream_open_bytes(handle, bytes)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_query_stream_next(stream_handle: u64) -> MobileBuffer {
    query_stream_next_bytes(stream_handle).into()
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_query_stream_close(stream_handle: u64) -> MobileBuffer {
    query_stream_close_bytes(stream_handle).into()
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_flush(handle: u64) -> MobileBuffer {
    flush_bytes(handle).into()
}

#[unsafe(no_mangle)]
pub extern "C" fn protomolt_search_close(handle: u64) -> MobileBuffer {
    close_bytes(handle).into()
}

/// # Safety
///
/// `buffer` must be an owned value returned by this library and must not have
/// been freed previously.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn protomolt_search_buffer_free(buffer: MobileBuffer) {
    if buffer.data.is_null() {
        return;
    }
    // SAFETY: guaranteed by the function's caller contract.
    drop(unsafe { Vec::from_raw_parts(buffer.data, buffer.len, buffer.capacity) });
}

#[cfg(target_os = "android")]
mod android {
    use super::*;
    use jni::errors::ThrowRuntimeExAndDefault;
    use jni::objects::{JByteArray, JClass};
    use jni::sys::{jboolean, jlong};
    use jni::EnvUnowned;

    fn with_input<'caller>(
        mut env: EnvUnowned<'caller>,
        input: JByteArray<'caller>,
        operation: impl FnOnce(&[u8]) -> Vec<u8>,
    ) -> JByteArray<'caller> {
        env.with_env(|env| -> Result<_, jni::errors::Error> {
            let input = env.convert_byte_array(&input)?;
            env.byte_array_from_slice(&operation(&input))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
    }

    fn without_input<'caller>(
        mut env: EnvUnowned<'caller>,
        operation: impl FnOnce() -> Vec<u8>,
    ) -> JByteArray<'caller> {
        env.with_env(|env| -> Result<_, jni::errors::Error> {
            env.byte_array_from_slice(&operation())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeOpen<'caller>(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        request: JByteArray<'caller>,
        create: jboolean,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| open_bytes(bytes, create))
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeIngestMapped<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
        request: JByteArray<'caller>,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| {
            ingest_mapped_bytes(handle as u64, bytes)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeAcceptDocument<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
        request: JByteArray<'caller>,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| {
            accept_document_bytes(handle as u64, bytes)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeDescribeSchema<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
        request: JByteArray<'caller>,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| {
            describe_schema_bytes(handle as u64, bytes)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativePlanIndex<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
        request: JByteArray<'caller>,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| plan_index_bytes(handle as u64, bytes))
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeReadAcceptedDocuments<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
        request: JByteArray<'caller>,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| {
            read_accepted_documents_bytes(handle as u64, bytes)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQuery<'caller>(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
        request: JByteArray<'caller>,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| query_bytes(handle as u64, bytes))
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQueryStreamOpen<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
        request: JByteArray<'caller>,
    ) -> JByteArray<'caller> {
        with_input(env, request, |bytes| {
            query_stream_open_bytes(handle as u64, bytes)
        })
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQueryStreamNext<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        stream_handle: jlong,
    ) -> JByteArray<'caller> {
        without_input(env, || query_stream_next_bytes(stream_handle as u64))
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQueryStreamClose<
        'caller,
    >(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        stream_handle: jlong,
    ) -> JByteArray<'caller> {
        without_input(env, || query_stream_close_bytes(stream_handle as u64))
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeFlush<'caller>(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
    ) -> JByteArray<'caller> {
        without_input(env, || flush_bytes(handle as u64))
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeClose<'caller>(
        env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        handle: jlong,
    ) -> JByteArray<'caller> {
        without_input(env, || close_bytes(handle as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipestream_search::analyzer::body_spec;
    use pipestream_search::pb::{
        ingest_mapped_request, query_stream_response, search_query, selection_query,
        IngestMappedRequest, LexicalQuery, MappedBind, SearchQuery, SelectionQuery,
    };
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn outcome(bytes: &[u8]) -> mobile_response::Outcome {
        MobileResponse::decode(bytes)
            .expect("response envelope")
            .outcome
            .expect("response outcome")
    }

    #[test]
    fn mobile_unsigned_column_declarations_survive_the_wire_bridge() {
        let request = MobileShardConfig {
            in_memory: true,
            integer_fields: vec!["signed".into()],
            unsigned_integer_fields: vec!["counter".into()],
            ..Default::default()
        };
        let decoded = MobileShardConfig::decode(request.encode_to_vec().as_slice()).unwrap();
        let configured = node_config(decoded).unwrap();
        assert_eq!(configured.node.integer_fields, ["signed"]);
        assert_eq!(configured.node.unsigned_integer_fields, ["counter"]);
        let old = MobileShardConfig::decode([0x10u8, 0x01].as_slice()).unwrap();
        assert!(node_config(old)
            .unwrap()
            .node
            .unsigned_integer_fields
            .is_empty());
    }

    #[test]
    fn mobile_schema_description_needs_no_index_rows_or_source_catalog() {
        let opened: MobileOpenResponse = payload(&open_bytes(
            &MobileOpenRequest {
                shards: vec![MobileShardConfig {
                    in_memory: true,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode_to_vec(),
            true,
        ));
        assert!(opened.no_egress);
        let request = DescribeSchemaRequest {
            descriptor_set: include_bytes!("../../../tests/fixtures/schema-report/source-only.bin")
                .to_vec(),
            message_type: "source_report.AllShapes".into(),
            ..Default::default()
        };
        let expected = pipestream_search::mapping::describe_schema(
            &request.descriptor_set,
            &request.message_type,
        )
        .unwrap();
        let bytes = request.encode_to_vec();
        let buffer = protomolt_search_describe_schema(opened.handle, bytes.as_ptr(), bytes.len());
        let actual: pipestream_search::pb::DescribeSchemaResponse =
            payload(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
        unsafe { protomolt_search_buffer_free(buffer) };
        assert_eq!(actual, expected);
        let _: MobileCloseResponse = payload(&close_bytes(opened.handle));
        let mobile_response::Outcome::Error(error) =
            outcome(&describe_schema_bytes(opened.handle, &bytes))
        else {
            panic!("closed schema handle remained accessible");
        };
        assert_eq!(error.code, MobileErrorCode::NotFound as i32);
    }

    #[test]
    fn mobile_document_acceptance_preserves_retry_and_conflict_codes() {
        use pipestream_search::pb::mobile::MobileDocumentCatalogConfig;
        use pipestream_search::pb::{
            accept_document_request::Mutation, DocumentWriteReceipt, ProtobufSource,
        };
        let opened: MobileOpenResponse = payload(&open_bytes(
            &MobileOpenRequest {
                shards: vec![MobileShardConfig {
                    in_memory: true,
                    ..Default::default()
                }],
                document_catalog: Some(MobileDocumentCatalogConfig {
                    collection: "private".into(),
                    in_memory: true,
                    ..Default::default()
                }),
                ..Default::default()
            }
            .encode_to_vec(),
            true,
        ));
        let mut request = AcceptDocumentRequest {
            contract_version: 1,
            document_key: b"source".to_vec(),
            operation_id: b"create".to_vec(),
            expected_version: Some(0),
            mutation: Some(Mutation::Source(ProtobufSource {
                descriptor_set: record_descriptor(),
                message_type: "private.v1.Record".into(),
                payload: vec![],
            })),
        };
        let bytes = request.encode_to_vec();
        let buffer = protomolt_search_accept_document(opened.handle, bytes.as_ptr(), bytes.len());
        let receipt: DocumentWriteReceipt =
            payload(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
        // The buffer was returned by the C ABI above and is freed exactly once.
        unsafe { protomolt_search_buffer_free(buffer) };
        assert!(receipt.accepted && !receipt.durable && !receipt.searchable && !receipt.replayed);
        let retry: DocumentWriteReceipt = payload(&accept_document_bytes(opened.handle, &bytes));
        assert!(retry.replayed);
        assert_eq!(retry.version, receipt.version);
        let mut history_request = ReadAcceptedDocumentsRequest {
            limit: 10,
            max_bytes: 1024 * 1024,
            ..Default::default()
        };
        let history: pipestream_search::pb::ReadAcceptedDocumentsResponse = payload(
            &read_accepted_documents_bytes(opened.handle, &history_request.encode_to_vec()),
        );
        assert!(history.complete);
        assert_eq!(history.documents.len(), 1);
        assert_eq!(history.documents[0].document_key, request.document_key);
        history_request.max_bytes = 1;
        let mobile_response::Outcome::Error(error) = outcome(&read_accepted_documents_bytes(
            opened.handle,
            &history_request.encode_to_vec(),
        )) else {
            panic!("oversized source must fail");
        };
        assert_eq!(error.code(), MobileErrorCode::ResourceExhausted);
        request.operation_id = b"stale".to_vec();
        let mobile_response::Outcome::Error(error) = outcome(&accept_document_bytes(
            opened.handle,
            &request.encode_to_vec(),
        )) else {
            panic!("stale version must fail");
        };
        assert_eq!(error.code(), MobileErrorCode::Aborted);
        let closed: MobileCloseResponse = payload(&close_bytes(opened.handle));
        assert!(closed.closed);
    }

    fn payload<M: Message + Default>(bytes: &[u8]) -> M {
        let payload = match outcome(bytes) {
            mobile_response::Outcome::Payload(payload) => payload,
            mobile_response::Outcome::Error(error) => {
                panic!(
                    "mobile operation failed: {:?}: {}",
                    error.code(),
                    error.message
                )
            }
        };
        M::decode(payload.as_slice()).expect("operation payload")
    }

    fn lexical_request() -> QueryRequest {
        QueryRequest {
            request_id: "mobile-abi".into(),
            k: 10,
            selection_k: 10,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "lexical".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "zebra".into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }
    }

    fn scalar(name: &str, number: i32, typ: Type, label: Label) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(label as i32),
            r#type: Some(typ as i32),
            ..Default::default()
        }
    }

    fn record_descriptor() -> Vec<u8> {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("private.proto".into()),
                package: Some("private.v1".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Record".into()),
                    field: vec![
                        scalar("id", 1, Type::String, Label::Optional),
                        scalar("body", 2, Type::String, Label::Optional),
                        scalar("embedding", 3, Type::Float, Label::Repeated),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn varint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    fn string_field(out: &mut Vec<u8>, field: u64, value: &str) {
        varint(out, field << 3 | 2);
        varint(out, value.len() as u64);
        out.extend_from_slice(value.as_bytes());
    }

    fn record_document() -> Vec<u8> {
        let mut out = Vec::new();
        string_field(&mut out, 1, "mobile-1");
        string_field(&mut out, 2, "private mobile zebra");
        varint(&mut out, 3 << 3 | 2);
        let embedding = [0.125_f32; 32];
        varint(&mut out, (embedding.len() * 4) as u64);
        for value in embedding {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn tree_bytes(path: &std::path::Path) -> u64 {
        let Ok(metadata) = std::fs::metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }
        std::fs::read_dir(path)
            .expect("read test index tree")
            .map(|entry| tree_bytes(&entry.expect("test index entry").path()))
            .sum()
    }

    fn blocked_read_is_cancelled(close_owner: bool) {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let receiver = runtime()
            .unwrap()
            .block_on(pipestream_search::metrics::timed_stream(
                pipestream_search::metrics::Route::QueryStream,
                tonic::Request::new(()),
                |_| async { Ok(tonic::Response::new(ReceiverStream::new(receiver))) },
            ))
            .unwrap()
            .into_inner();
        let opened: MobileOpenResponse = payload(&open_bytes(
            &MobileOpenRequest {
                shards: vec![MobileShardConfig {
                    in_memory: true,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode_to_vec(),
            false,
        ));
        let owner = opened.handle;
        let stream_handle = {
            let mut registry = lock_registry().unwrap();
            let stream_handle = registry.allocate().unwrap();
            registry.streams.insert(
                stream_handle,
                OpenStream {
                    owner,
                    receiver: Some(receiver),
                    cancel: tokio::sync::watch::channel(false).0,
                },
            );
            stream_handle
        };
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            done_tx
                .send(query_stream_next_bytes(stream_handle))
                .unwrap();
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if lock_registry().unwrap().streams[&stream_handle]
                .receiver
                .is_none()
            {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "reader did not start");
            std::thread::yield_now();
        }
        let mobile_response::Outcome::Error(error) =
            outcome(&query_stream_next_bytes(stream_handle))
        else {
            panic!("concurrent read unexpectedly succeeded");
        };
        assert_eq!(error.code(), MobileErrorCode::FailedPrecondition);
        if close_owner {
            let closed: MobileCloseResponse = payload(&close_bytes(owner));
            assert!(closed.closed);
        } else {
            let closed: MobileCloseResponse = payload(&query_stream_close_bytes(stream_handle));
            assert!(closed.closed);
        }
        let bytes = done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("close must wake a blocked read");
        reader.join().unwrap();
        let mobile_response::Outcome::Error(error) = outcome(&bytes) else {
            panic!("closed read unexpectedly succeeded");
        };
        assert_eq!(error.code(), MobileErrorCode::Cancelled);
        assert!(!lock_registry()
            .unwrap()
            .streams
            .contains_key(&stream_handle));
        assert!(sender.is_closed(), "close must drop the query receiver");
        let closed: MobileCloseResponse = payload(&query_stream_close_bytes(stream_handle));
        assert!(!closed.closed);
        let owner_closed: MobileCloseResponse = payload(&close_bytes(owner));
        assert_eq!(owner_closed.closed, !close_owner);
    }

    #[test]
    fn closing_stream_wakes_pending_read_without_restoring_handle() {
        blocked_read_is_cancelled(false);
    }

    #[test]
    fn closing_owner_wakes_pending_read_without_restoring_handle() {
        blocked_read_is_cancelled(true);
    }

    #[test]
    fn malformed_request_is_an_encoded_error() {
        let mobile_response::Outcome::Error(error) = outcome(&open_bytes(&[0xff], false)) else {
            panic!("malformed input unexpectedly succeeded");
        };
        assert_eq!(error.code(), MobileErrorCode::InvalidArgument);
        assert!(error.message.contains("decode MobileOpenRequest"));
    }

    #[test]
    fn empty_open_request_is_refused() {
        let bytes = MobileOpenRequest::default().encode_to_vec();
        let mobile_response::Outcome::Error(error) = outcome(&open_bytes(&bytes, false)) else {
            panic!("empty open unexpectedly succeeded");
        };
        assert_eq!(error.code(), MobileErrorCode::InvalidArgument);
        assert!(error.message.contains("at least one shard"));
    }

    #[test]
    fn unknown_handle_is_not_found() {
        let request = QueryRequest::default().encode_to_vec();
        let mobile_response::Outcome::Error(error) = outcome(&query_bytes(u64::MAX, &request))
        else {
            panic!("unknown handle unexpectedly succeeded");
        };
        assert_eq!(error.code(), MobileErrorCode::NotFound);
    }

    #[test]
    fn c_buffer_round_trip() {
        let expected = open_bytes(&[0xff], false);
        let buffer = protomolt_search_open([0xff].as_ptr(), 1, 0);
        // SAFETY: the returned buffer remains owned until the matching free.
        let actual = unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) };
        assert_eq!(actual, expected);
        // SAFETY: this buffer came from the bridge and has not been freed.
        unsafe { protomolt_search_buffer_free(buffer) };
    }

    #[test]
    fn lifecycle_stream_persistence_and_no_egress_cross_the_byte_abi() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "protomolt-mobile-abi-{}-{nonce}",
            std::process::id()
        ));
        let index = root.join("private.tv");
        let request = MobileOpenRequest {
            shards: vec![MobileShardConfig {
                index_path: index.to_string_lossy().into_owned(),
                facet_fields: vec!["id".into()],
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();

        let opened: MobileOpenResponse = payload(&open_bytes(&request, true));
        assert_eq!(opened.shard_count, 1);
        assert!(opened.no_egress);

        let descriptor_set = record_descriptor();
        let planning = PlanIndexRequest {
            descriptor_set: descriptor_set.clone(),
            message_type: "private.v1.Record".into(),
            ..Default::default()
        }
        .encode_to_vec();
        let buffer = protomolt_search_plan_index(opened.handle, planning.as_ptr(), planning.len());
        let planned: pipestream_search::pb::PlanIndexResponse =
            payload(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
        unsafe { protomolt_search_buffer_free(buffer) };
        let plan = planned.plan.expect("mobile fixture plan");
        let ingest = MobileIngestMappedBatch {
            shard: 0,
            requests: vec![
                IngestMappedRequest {
                    payload: Some(ingest_mapped_request::Payload::Bind(MappedBind {
                        collection: String::new(),
                        descriptor_set,
                        message_type: "private.v1.Record".into(),
                        expected_fingerprint: plan.fingerprint,
                        body_path: "body".into(),
                        analysis: Some(body_spec()),
                        materialize: None,
                        field_analysis: Vec::new(),
                    })),
                },
                IngestMappedRequest {
                    payload: Some(ingest_mapped_request::Payload::Document(record_document())),
                },
            ],
        };
        let ingested: pipestream_search::pb::IngestMappedResponse =
            payload(&ingest_mapped_bytes(opened.handle, &ingest.encode_to_vec()));
        assert_eq!(ingested.added, 1);

        let flushed: MobileFlushResponse = payload(&flush_bytes(opened.handle));
        assert_eq!(flushed.shards.len(), 1);

        let query = lexical_request();
        let queried: QueryResponse = payload(&query_bytes(opened.handle, &query.encode_to_vec()));
        assert_eq!(queried.hits.len(), 1);

        let stream_request = QueryStreamRequest {
            collection: String::new(),
            query: Some(query),
            timeout_ms: 0,
        };
        let stream: MobileQueryStreamOpenResponse = payload(&query_stream_open_bytes(
            opened.handle,
            &stream_request.encode_to_vec(),
        ));
        let mut completions = 0;
        loop {
            let next: MobileQueryStreamNextResponse =
                payload(&query_stream_next_bytes(stream.stream_handle));
            if next.end {
                break;
            }
            if matches!(
                next.response.and_then(|response| response.payload),
                Some(query_stream_response::Payload::Completion(_))
            ) {
                completions += 1;
            }
        }
        assert_eq!(completions, 1);

        let flushed: MobileFlushResponse = payload(&flush_bytes(opened.handle));
        assert_eq!(flushed.shards.len(), 1);
        let closed: MobileCloseResponse = payload(&close_bytes(opened.handle));
        assert!(closed.closed);

        let disk_bytes = tree_bytes(&root);
        assert!(
            disk_bytes > 0,
            "persistent open must create durable WAL state"
        );
        assert!(
            disk_bytes < 1024 * 1024,
            "empty mobile generation exceeded the 1 MiB lifecycle-test budget: {disk_bytes}"
        );

        let reopened: MobileOpenResponse = payload(&open_bytes(&request, false));
        assert!(reopened.no_egress);
        let closed: MobileCloseResponse = payload(&close_bytes(reopened.handle));
        assert!(closed.closed);

        let mobile_response::Outcome::Error(error) = outcome(&open_bytes(&request, true)) else {
            panic!("create overwrote an existing mobile generation");
        };
        assert_eq!(error.code(), MobileErrorCode::AlreadyExists);
        std::fs::remove_dir_all(root).expect("remove mobile ABI fixture");
    }
}
