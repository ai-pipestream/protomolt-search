# Unsigned mapping fixtures

`descriptor.bin` comes from `unsigned.proto` with libprotoc 25.1 and includes
imports. Regenerate from the repository root:

```sh
protoc -Iproto -Itests/fixtures/unsigned-mapping --include_imports \
  --descriptor_set_out=tests/fixtures/unsigned-mapping/descriptor.bin \
  tests/fixtures/unsigned-mapping/unsigned.proto
```

The tests encode `Record` with prost-derived Rust messages independently of the
reflection-based projection decoder. Cases cover all unsigned encodings,
optional/implicit presence, oneofs, merged messages, nested fields, repeated and
map source-only values, explicit signed conversion, and chunk identity fields.
The ambiguous fixtures must refuse planning while remaining describable as
source schemas.

`legacy-fingerprint.txt` records `unsigned_mapping.Record` derived by the mapper
at `d0a1716`. It was obtained by running that revision's `mapping.rs` and
`schema_report.rs` against this descriptor, adding only unreachable match arms
so the old source could compile against the newly extended enums. That mapper
produces signed kinds/families; the current mapper's unsigned plan must differ.
Unrelated message declarations do not participate in the root's fingerprint.

The lifecycle test runs real mapped gRPC ingest, checks exact unsigned key
filters, reopens and compacts both disk layouts, and verifies original payload
bytes (including repeated/map values and an unknown field). Legacy mapped
ingest does not publish catalog identities; this test does not claim that
broader identity and write-receipt work is complete. Unsupported materialization
reads must refuse before ingest acknowledges a row; unsigned absence must not
bypass the declared-type check.
