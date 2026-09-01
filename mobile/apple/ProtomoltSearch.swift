import Foundation
import ProtomoltSearch

/// Data-only Swift facade over the XCFramework's stable C ABI. Inputs and
/// outputs are encoded protobuf messages from mobile.proto/search.proto.
public enum ProtomoltSearchMobile {
    private static func consume(_ buffer: ProtomoltSearchBuffer) -> Data {
        defer { protomolt_search_buffer_free(buffer) }
        guard let bytes = buffer.data, buffer.len > 0 else { return Data() }
        return Data(bytes: bytes, count: buffer.len)
    }

    private static func call(
        _ request: Data,
        _ operation: (UnsafePointer<UInt8>?, Int) -> ProtomoltSearchBuffer
    ) -> Data {
        request.withUnsafeBytes { raw in
            consume(operation(raw.bindMemory(to: UInt8.self).baseAddress, request.count))
        }
    }

    public static func open(_ request: Data, create: Bool) -> Data {
        call(request) { bytes, count in
            protomolt_search_open(bytes, count, create ? 1 : 0)
        }
    }

    public static func ingestMapped(handle: UInt64, request: Data) -> Data {
        call(request) { protomolt_search_ingest_mapped(handle, $0, $1) }
    }

    public static func query(handle: UInt64, request: Data) -> Data {
        call(request) { protomolt_search_query(handle, $0, $1) }
    }

    public static func openQueryStream(handle: UInt64, request: Data) -> Data {
        call(request) { protomolt_search_query_stream_open(handle, $0, $1) }
    }

    public static func nextQueryStream(streamHandle: UInt64) -> Data {
        consume(protomolt_search_query_stream_next(streamHandle))
    }

    public static func closeQueryStream(streamHandle: UInt64) -> Data {
        consume(protomolt_search_query_stream_close(streamHandle))
    }

    public static func flush(handle: UInt64) -> Data {
        consume(protomolt_search_flush(handle))
    }

    public static func close(handle: UInt64) -> Data {
        consume(protomolt_search_close(handle))
    }
}
