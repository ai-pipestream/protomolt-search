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

#[cfg(test)]
mod tests;
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
    node.map_integer_fields = config.map_integer_fields;
    node.map_unsigned_integer_fields = config.map_unsigned_integer_fields;
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
