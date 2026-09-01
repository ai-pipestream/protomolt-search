import Darwin
import Foundation
import ProtomoltSearch
import XCTest

final class ProtomoltSearchDeviceTests: XCTestCase {
    private let fingerprint = "e08ddb9a6de98aecd3861a430ca26774cbb990511f8c1445f98ee12f85f13d4a"
    private let diskBudget: UInt64 = 64 * 1024 * 1024

    func testPersistenceBackgroundDiskAndNoEgress() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("protomolt-apple-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let request = openRequest(root.appendingPathComponent("private.tv").path)
        let socketsBefore = socketDescriptors()
        var handle = try open(request, create: true)

        try ingest(handle)
        try flush(handle)
        XCTAssertEqual(try query(handle).fieldCount(2), 1)
        try stream(handle)

        let bytes = try treeBytes(root)
        XCTAssertGreaterThan(bytes, 0)
        XCTAssertLessThan(bytes, diskBudget)

        // The host's background transition is flush + close. Reopening must
        // attach to the same durable generation without a network service.
        try close(handle)
        handle = try open(request, create: false)
        XCTAssertEqual(try query(handle).fieldCount(2), 1)
        try close(handle)

        let refusal = try Envelope(consume(request) {
            protomolt_search_open($0, $1, 1)
        }, allowError: true)
        XCTAssertNotNil(refusal.error)
        XCTAssertEqual(socketsBefore, socketDescriptors())
    }

    func testQueryPowerAndStorageProbe() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("protomolt-power-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let handle = try open(openRequest(root.appendingPathComponent("private.tv").path), create: true)
        defer { _ = try? close(handle) }
        try ingest(handle)
        try flush(handle)
        let encoded = lexicalQuery()

        measure(metrics: [XCTCPUMetric(), XCTClockMetric(), XCTStorageMetric()]) {
            for _ in 0..<100 {
                _ = try! Envelope(consume(encoded) {
                    protomolt_search_query(handle, $0, $1)
                }).payload
            }
        }
    }

    private func open(_ request: Data, create: Bool) throws -> UInt64 {
        let envelope = try Envelope(consume(request) {
            protomolt_search_open($0, $1, create ? 1 : 0)
        })
        let payload = try XCTUnwrap(envelope.payload)
        XCTAssertEqual(payload.firstVarint(3), 1, "mobile runtime did not certify no-egress")
        return try XCTUnwrap(payload.firstVarint(1))
    }

    private func ingest(_ handle: UInt64) throws {
        let bind = Wire.message(1, descriptorSet())
            + Wire.string(2, "private.v1.Record")
            + Wire.string(3, fingerprint)
            + Wire.string(4, "body")
            + Wire.message(5, bodySpec())
        let request = Wire.message(1, bind)
        let documentRequest = Wire.message(2, document())
        let batch = Wire.message(2, request) + Wire.message(2, documentRequest)
        let envelope = try Envelope(consume(batch) {
            protomolt_search_ingest_mapped(handle, $0, $1)
        })
        XCTAssertEqual(try XCTUnwrap(envelope.payload).firstVarint(1), 1)
    }

    private func flush(_ handle: UInt64) throws {
        let envelope = try Envelope(consume(protomolt_search_flush(handle)))
        XCTAssertNotNil(envelope.payload)
    }

    private func query(_ handle: UInt64) throws -> Data {
        let envelope = try Envelope(consume(lexicalQuery()) {
            protomolt_search_query(handle, $0, $1)
        })
        return try XCTUnwrap(envelope.payload)
    }

    private func stream(_ handle: UInt64) throws {
        let request = Wire.message(1, lexicalQuery())
        let opened = try Envelope(consume(request) {
            protomolt_search_query_stream_open(handle, $0, $1)
        })
        let streamHandle = try XCTUnwrap(try XCTUnwrap(opened.payload).firstVarint(1))
        var completions = 0
        while true {
            let next = try Envelope(consume(protomolt_search_query_stream_next(streamHandle)))
            let payload = try XCTUnwrap(next.payload)
            if payload.firstVarint(2) == 1 { break }
            if let response = payload.firstBytes(1), response.firstBytes(2) != nil {
                completions += 1
            }
        }
        XCTAssertEqual(completions, 1)
    }

    private func close(_ handle: UInt64) throws {
        let envelope = try Envelope(consume(protomolt_search_close(handle)))
        XCTAssertEqual(try XCTUnwrap(envelope.payload).firstVarint(1), 1)
    }

    private func openRequest(_ path: String) -> Data {
        let shard = Wire.string(1, path) + Wire.string(8, "id")
        return Wire.message(1, shard)
    }

    private func lexicalQuery() -> Data {
        let lexical = Wire.string(1, "zebra") + Wire.message(2, bodySpec())
        let search = Wire.string(1, "lexical") + Wire.message(2, lexical)
        let selection = Wire.message(1, search)
        return Wire.string(1, "apple-device")
            + Wire.varintField(2, 10)
            + Wire.varintField(3, 10)
            + Wire.message(4, selection)
    }

    private func bodySpec() -> Data {
        Wire.varintField(1, 1)
            + Wire.varintField(2, 2)
            + Wire.varintField(3, 1)
            + Wire.varintField(4, 3)
            + Wire.varintField(5, 1)
            + Wire.varintField(5, 2)
            + Wire.varintField(5, 15)
            + Wire.varintField(5, 6)
    }

    private func descriptorSet() -> Data {
        func field(_ name: String, _ number: UInt64, _ label: UInt64, _ type: UInt64) -> Data {
            Wire.string(1, name)
                + Wire.varintField(3, number)
                + Wire.varintField(4, label)
                + Wire.varintField(5, type)
        }
        let record = Wire.string(1, "Record")
            + Wire.message(2, field("id", 1, 1, 9))
            + Wire.message(2, field("body", 2, 1, 9))
            + Wire.message(2, field("embedding", 3, 3, 2))
        let file = Wire.string(1, "private.proto")
            + Wire.string(2, "private.v1")
            + Wire.message(4, record)
        return Wire.message(1, file)
    }

    private func document() -> Data {
        var packed = Data()
        for _ in 0..<32 {
            var bits = Float(0.125).bitPattern.littleEndian
            withUnsafeBytes(of: &bits) { packed.append(contentsOf: $0) }
        }
        return Wire.string(1, "mobile-1")
            + Wire.string(2, "private mobile zebra")
            + Wire.message(3, packed)
    }

    private func consume(_ buffer: ProtomoltSearchBuffer) -> Data {
        defer { protomolt_search_buffer_free(buffer) }
        guard let bytes = buffer.data, buffer.len > 0 else { return Data() }
        return Data(bytes: bytes, count: buffer.len)
    }

    private func consume(
        _ request: Data,
        operation: (UnsafePointer<UInt8>?, Int) -> ProtomoltSearchBuffer
    ) -> Data {
        request.withUnsafeBytes { raw in
            consume(operation(raw.bindMemory(to: UInt8.self).baseAddress, request.count))
        }
    }

    private func socketDescriptors() -> Int {
        var sockets = 0
        for descriptor in 0..<getdtablesize() {
            var type: Int32 = 0
            var length = socklen_t(MemoryLayout<Int32>.size)
            if getsockopt(descriptor, SOL_SOCKET, SO_TYPE, &type, &length) == 0 {
                sockets += 1
            }
        }
        return sockets
    }

    private func treeBytes(_ root: URL) throws -> UInt64 {
        let keys: [URLResourceKey] = [.isRegularFileKey, .fileSizeKey]
        let files = try XCTUnwrap(FileManager.default.enumerator(
            at: root,
            includingPropertiesForKeys: keys
        ))
        var total: UInt64 = 0
        for case let file as URL in files {
            let values = try file.resourceValues(forKeys: Set(keys))
            if values.isRegularFile == true {
                total += UInt64(values.fileSize ?? 0)
            }
        }
        return total
    }
}

private struct Envelope {
    let payload: Data?
    let error: Data?

    init(_ encoded: Data, allowError: Bool = false) throws {
        payload = try encoded.firstBytes(1)
        error = try encoded.firstBytes(2)
        if payload == nil && error == nil {
            throw WireError.malformed
        }
        if let error, !allowError {
            let message = try error.firstBytes(2).map { String(decoding: $0, as: UTF8.self) } ?? ""
            XCTFail("mobile bridge error: \(message)")
        }
    }
}

private enum WireError: Error {
    case malformed
}

private enum Wire {
    static func varint(_ value: UInt64) -> Data {
        var value = value
        var bytes = Data()
        repeat {
            var byte = UInt8(value & 0x7f)
            value >>= 7
            if value != 0 { byte |= 0x80 }
            bytes.append(byte)
        } while value != 0
        return bytes
    }

    static func varintField(_ number: UInt64, _ value: UInt64) -> Data {
        varint(number << 3) + varint(value)
    }

    static func string(_ number: UInt64, _ value: String) -> Data {
        message(number, Data(value.utf8))
    }

    static func message(_ number: UInt64, _ value: Data) -> Data {
        varint(number << 3 | 2) + varint(UInt64(value.count)) + value
    }
}

private extension Data {
    func fields() throws -> [(UInt64, UInt64, Data)] {
        var cursor = startIndex
        var result: [(UInt64, UInt64, Data)] = []
        func readVarint() throws -> UInt64 {
            var value: UInt64 = 0
            var shift: UInt64 = 0
            while cursor < endIndex && shift < 70 {
                let byte = self[cursor]
                cursor = index(after: cursor)
                value |= UInt64(byte & 0x7f) << shift
                if byte & 0x80 == 0 { return value }
                shift += 7
            }
            throw WireError.malformed
        }
        while cursor < endIndex {
            let tag = try readVarint()
            let number = tag >> 3
            let wire = tag & 7
            if wire == 0 {
                let value = try readVarint()
                result.append((number, wire, Wire.varint(value)))
            } else if wire == 2 {
                let length = try Int(readVarint())
                guard length >= 0, distance(from: cursor, to: endIndex) >= length else {
                    throw WireError.malformed
                }
                let end = index(cursor, offsetBy: length)
                result.append((number, wire, Data(self[cursor..<end])))
                cursor = end
            } else {
                throw WireError.malformed
            }
        }
        return result
    }

    func firstBytes(_ number: UInt64) throws -> Data? {
        try fields().first { $0.0 == number && $0.1 == 2 }.map { $0.2 }
    }

    func firstVarint(_ number: UInt64) -> UInt64? {
        guard let parsed = try? fields(),
              let field = parsed.first(where: { $0.0 == number && $0.1 == 0 }) else {
            return nil
        }
        let bytes = field.2
        var value: UInt64 = 0
        var shift: UInt64 = 0
        for byte in bytes {
            value |= UInt64(byte & 0x7f) << shift
            if byte & 0x80 == 0 { return value }
            shift += 7
        }
        return nil
    }

    func fieldCount(_ number: UInt64) -> Int {
        (try? fields().filter { $0.0 == number }.count) ?? 0
    }
}
