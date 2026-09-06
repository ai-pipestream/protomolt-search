# Wrapper mapping conformance fixture

Regenerate from the search repository root with protoc and its standard imports:

```sh
protoc -Iproto -Itests/fixtures/wrapper-mapping --include_imports \
  --descriptor_set_out=tests/fixtures/wrapper-mapping/descriptor.bin \
  tests/fixtures/wrapper-mapping/wrappers.proto
```

The fixture covers all nine standard scalar wrapper messages, type/name/role
hints, oneof replacement, nested values, repeated and map preservation, source-only
projections and invalid identity plans. tests/wrapper_mapping.rs compares
extraction with generated prost wrapper messages and exercises report inputs,
descriptor refusals, native embedded body analysis and stored lifecycle behavior.

The legacy fingerprint asserted by that test belongs to the schema-report
Record fixture at commit 5db4065, before wrapper fields moved from `.value`
paths to their declared paths. Its old fingerprint was measured before the
implementation change; it must not be substituted as the new binding.
