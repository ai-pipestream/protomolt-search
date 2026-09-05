# Integer keyword fixtures

`descriptor.bin` is a complete descriptor set compiled from `integers.proto`
with libprotoc 25.1. It includes the vendored ProtoMolt indexing hints.
The tests construct descriptor-validated protobuf values, encode them, and pass
those bytes through mapped extraction and real gRPC ingest.

Regenerate from the repository root:

```sh
protoc -I proto -I . --include_imports \
  --descriptor_set_out=tests/fixtures/integer-keywords/descriptor.bin \
  tests/fixtures/integer-keywords/integers.proto
```

The fields cover all ten protobuf integer encodings as explicit keywords,
optional zero versus absence, signed and unsigned parent IDs, and a separate
numeric field that still reports the current signed-column range restriction.
Query checks use decimal-string equality because that is the keyword contract;
they do not claim unsigned numeric range support.
