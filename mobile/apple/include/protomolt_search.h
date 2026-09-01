#ifndef PROTOMOLT_SEARCH_H
#define PROTOMOLT_SEARCH_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ProtomoltSearchBuffer {
    uint8_t *data;
    size_t len;
    size_t capacity;
} ProtomoltSearchBuffer;

// Each returned buffer contains one encoded
// ai.pipestream.search.mobile.v1.MobileResponse and must be released once.
ProtomoltSearchBuffer protomolt_search_open(
    const uint8_t *request,
    size_t request_len,
    uint8_t create);
ProtomoltSearchBuffer protomolt_search_ingest_mapped(
    uint64_t handle,
    const uint8_t *request,
    size_t request_len);
ProtomoltSearchBuffer protomolt_search_query(
    uint64_t handle,
    const uint8_t *request,
    size_t request_len);
ProtomoltSearchBuffer protomolt_search_query_stream_open(
    uint64_t handle,
    const uint8_t *request,
    size_t request_len);
ProtomoltSearchBuffer protomolt_search_query_stream_next(uint64_t stream_handle);
ProtomoltSearchBuffer protomolt_search_query_stream_close(uint64_t stream_handle);
ProtomoltSearchBuffer protomolt_search_flush(uint64_t handle);
ProtomoltSearchBuffer protomolt_search_close(uint64_t handle);
void protomolt_search_buffer_free(ProtomoltSearchBuffer buffer);

#ifdef __cplusplus
}
#endif

#endif
