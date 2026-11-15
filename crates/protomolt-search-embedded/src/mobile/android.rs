//! The JNI entry points the Android AAR binds to.

use super::ops::*;
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
    env.with_env(|env| -> Result<_, jni::errors::Error> { env.byte_array_from_slice(&operation()) })
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
pub extern "system" fn Java_ai_pipestream_search_mobile_ProtomoltSearch_nativePlanIndex<'caller>(
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
