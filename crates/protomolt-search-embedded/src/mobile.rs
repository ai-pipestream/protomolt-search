//! Stable byte-oriented ABI used by the Android AAR and Apple XCFramework.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use prost::Message;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use pipestream_search::embedded::{
    EmbeddedError, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
};
use pipestream_search::pb::mobile::{
    mobile_response, MobileCloseResponse, MobileError, MobileErrorCode, MobileFlushResponse,
    MobileIngestMappedBatch, MobileOpenRequest, MobileOpenResponse, MobileQueryStreamNextResponse,
    MobileQueryStreamOpenResponse, MobileResponse, MobileShardConfig,
};
use pipestream_search::pb::{QueryRequest, QueryResponse, QueryStreamRequest, QueryStreamResponse};

type QueryReceiver = ReceiverStream<Result<QueryStreamResponse, tonic::Status>>;

struct OpenStream {
    owner: u64,
    receiver: QueryReceiver,
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

fn query_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<QueryRequest>(input, "QueryRequest")?;
        let search = search(handle)?;
        let result: QueryResponse = runtime()?.block_on(search.query(request))?;
        Ok(success(&result))
    })
}

fn query_stream_open_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<QueryStreamRequest>(input, "QueryStreamRequest")?;
        let search = search(handle)?;
        let receiver = runtime()?.block_on(search.query_stream(request))?;
        let mut registry = lock_registry()?;
        let stream_handle = registry.allocate()?;
        registry.streams.insert(
            stream_handle,
            OpenStream {
                owner: handle,
                receiver,
            },
        );
        Ok(success(&MobileQueryStreamOpenResponse { stream_handle }))
    })
}

fn query_stream_next_bytes(stream_handle: u64) -> Vec<u8> {
    response(|| {
        let mut open = lock_registry()?
            .streams
            .remove(&stream_handle)
            .ok_or_else(|| {
                BridgeError::not_found(format!("unknown stream handle {stream_handle}"))
            })?;
        match runtime()?.block_on(open.receiver.next()) {
            Some(Ok(item)) => {
                lock_registry()?.streams.insert(stream_handle, open);
                Ok(success(&MobileQueryStreamNextResponse {
                    response: Some(item),
                    end: false,
                }))
            }
            Some(Err(status)) => Err(status.into()),
            None => Ok(success(&MobileQueryStreamNextResponse {
                response: None,
                end: true,
            })),
        }
    })
}

fn query_stream_close_bytes(stream_handle: u64) -> Vec<u8> {
    response(|| {
        let closed = lock_registry()?.streams.remove(&stream_handle).is_some();
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
        registry.streams.retain(|_, stream| stream.owner != handle);
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
        let plan = pipestream_search::mapping::derive_plan(&descriptor_set, "private.v1.Record")
            .expect("derive mobile fixture plan");
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
