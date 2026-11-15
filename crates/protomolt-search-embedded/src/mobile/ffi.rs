//! The C ABI: owned result buffers and one exported entry point per
//! operation.

use super::bridge::{failure, BridgeError};
use super::ops::*;

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

pub(super) unsafe fn input<'a>(data: *const u8, len: usize) -> Result<&'a [u8], BridgeError> {
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

pub(super) fn ffi_input(
    data: *const u8,
    len: usize,
    op: impl FnOnce(&[u8]) -> Vec<u8>,
) -> MobileBuffer {
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
