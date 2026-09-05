# Protobuf semantic fixtures

`descriptor.bin` is compiled from `closed.proto` and `open.proto` with protoc
25.1. `cases.json` records original wire bytes, initialization validity and the
fields visible to Google's Python protobuf 6.33.5 runtime. Field numbers are
object keys, enum values are numbers, and bytes are hex. Unknown fields are not
part of the projected value tree. No runtime dependency on Python is added.

The fixtures cover enum openness, oneofs, message/group merging, required fields
in unindexed messages, map entry replacement and registered extensions. The
enum's defining file controls openness, including an open enum imported by a
proto2 message. See [protobuf enum semantics](https://protobuf.dev/programming-guides/enum/)
and [proto2 required fields](https://protobuf.dev/programming-guides/proto2/#specifying-field-cardinality).
Neither these fixtures nor the projection decoder establish byte preservation,
extension indexing, general map/list querying, MessageSet or Editions support.

To refresh, use an isolated Python environment with `protobuf==6.33.5` and run
`python generate.py` from this directory with protoc 25.1 on PATH. Review both
descriptor and case changes. Cargo tests use the committed files directly:

```sh
cargo test --lib protobuf::tests -- --test-threads=4
cargo test --test protobuf_semantics --test descriptor_mappings -- --test-threads=4
```
