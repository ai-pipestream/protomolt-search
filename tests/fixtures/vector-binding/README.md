# Vector binding descriptors

These descriptors exercise explicit ProtoMolt vector naming, scalar/vector name
collisions, and reserved text/lineage names. Regenerate from the repository root:

```sh
protoc -I proto -I . --include_imports \
  --descriptor_set_out=tests/fixtures/vector-binding/descriptor.bin \
  tests/fixtures/vector-binding/vector-binding.proto
```

Tests read the checked-in descriptor bytes without invoking protoc at runtime.
