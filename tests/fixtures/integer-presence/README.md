# Integer presence fixtures

`legacy-kind4.bm25` was generated with the unmodified writer at search commit
`816c2791f79965c6d00158ec31ecf35f02c691bf`. It is a v8 integrity container
holding a legacy kind-4 integer column: rows contain -7, absent, zero and
`i64::MAX`. Keep it unchanged to prove the previous absence encoding still
loads and rewrites correctly. `generate.rs.txt` records the exact generator;
run it as an integration test at that commit to reproduce the bytes.

`descriptor.bin` is compiled with libprotoc 25.1:

```sh
protoc -I proto -I . --include_imports \
  --descriptor_set_out=tests/fixtures/integer-presence/descriptor.bin \
  tests/fixtures/integer-presence/presence.proto
```

The optional int64 field distinguishes full-domain numeric values from missing
fields. The gRPC test also materializes the value, reopens flushed shards, and replays
the WAL through compaction on both layouts before querying it again.
