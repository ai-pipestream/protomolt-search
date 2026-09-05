# ProtoMolt indexing-hint fixture

`descriptor.bin` is a complete `FileDescriptorSet` for
`annotated_record.proto`, including the vendored ProtoMolt indexing-hints
contract and `google/protobuf/descriptor.proto`. It proves that a real
descriptor carrying ProtoMolt's field-option annotations plans fields whose
names cannot be inferred as an ID or vector.

Regenerate from the repository root with libprotoc 25.1:

```bash
protoc -I proto -I . --include_imports \
  --descriptor_set_out=tests/fixtures/protomolt-hints/descriptor.bin \
  tests/fixtures/protomolt-hints/annotated_record.proto
```

The fixture is generated only when the checked-in proto or vendored contract
changes. Cargo tests read the checked-in bytes and require no runtime generator.
