# Schema-report fixture

Generated with libprotoc 25.1 from the checked-in schema and vendored ProtoMolt
hints. From the repository root:

```sh
protoc -I proto -I . --include_imports \
  --descriptor_set_out=tests/fixtures/schema-report/descriptor.bin \
  tests/fixtures/schema-report/record.proto
```

The descriptor exercises skipped fields and messages, recursive/repeated/map
references, explicit and implicit presence, oneofs and well-known messages.
The report must enumerate the reachable type graph independently of the
projection walk's depth and skip rules.
