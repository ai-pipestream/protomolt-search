# Protobuf semantic fixtures

`descriptor.bin` is compiled from `closed.proto` and `open.proto` with protoc
25.1. `cases.json` records original wire bytes, initialization validity and the
fields visible to Google's Python protobuf 6.33.5 upb runtime, except for the
explicit C++ map references described below. Field numbers are
object keys, enum values are numbers, and bytes are hex. Unknown fields are not
part of the projected value tree. No runtime dependency on Python is added.

The fixtures cover enum openness, oneofs, message/group merging, required fields
in unindexed messages, map entry replacement, registered extensions, and every
legal mismatched wire type for representative field shapes. Malformed skipped
lengths, fixed-width values, and group framing remain invalid. The
enum's defining file controls openness, including an open enum imported by a
proto2 message. See [protobuf enum semantics](https://protobuf.dev/programming-guides/enum/)
and [proto2 required fields](https://protobuf.dev/programming-guides/proto2/#specifying-field-cardinality).
Neither these fixtures nor the projection decoder establish byte preservation,
extension indexing, general map/list querying, MessageSet or Editions support.

## Proto2/proto3 scalar boundary profile

`wire-boundaries.json` adds 26 measured cases, 13 per syntax, without changing
any of the 117 established semantic cases. Each record retains the raw C++
`protoc 25.1` and Python protobuf 6.33.5 upb observations, the exact input hex,
and a separately reviewed product disposition. The seven accepted case names
and six refused case names form a closed set applied to each syntax; an unknown
case name has no implicit success default. Accepted expected fields come from
the pinned upb observation, while the product success/refusal choice does not
come from a Rust result. Fourteen syntax/case pairs are accepted and twelve are
refused. Eight refusals intentionally differ from both references: two proto2
invalid-UTF-8 cases and three overwide-varint cases under each syntax.

`generate-wire-boundaries.py` requires `protoc 25.1`, Python protobuf 6.33.5 and the upb
backend. It takes an explicit new output path and refuses to overwrite an
existing file. Regenerate to a separate file and review its diff against the
committed fixture; tests never invoke the generator or replace the fixture.
With `--compare EXISTING`, the generator compares the fresh parsed JSON after
normalizing only the date/time/PID prefix on `wire_format_lite.cc:603` stderr
for the two proto3 invalid-UTF-8 cases. Severity, file, line and diagnostic text,
and every other fixture field remain exact comparison inputs. Both the committed
fixture and the separately written refresh retain their raw observations.

Source retention is tested independently through the in-memory
`DocumentCatalog` API: all 26 original descriptor and payload byte strings are
retrieved exactly. This does not mean a failed mapped request is automatically
stored, and it makes no stream atomicity claim.

## Explicit map references

Seventeen synthetic map-entry cases carry `reference: cpp_25_1_text_parse`.
The [C++ generated-code contract](https://protobuf.dev/reference/cpp/cpp-generated/#parsing-unknown-values)
discards unknown fields inside an entry while retaining the entry's known or
default key/value. Unknown closed-enum values remain the separate whole-entry
unknown case covered by the original fixtures. Upb 6.33.5 instead omits entries
containing any unknown synthetic field; copying that behavior into projection
would silently lose known map values.

For the explicit case list, the generator runs C++ `protoc 25.1 --decode`,
parses its text into a fresh message with unknown numeric fields ignored, then
records the known field tree and checks initialization. Text parsing normalizes
duplicate map keys; initialization still refuses missing required fields inside
message values. Each record retains the exact C++ text and the upb binary
observation under `observations`. This is a declared reference choice backed by
independent parser output, not an automatic fallback when the implementation
fails a test. The other cases retain their upb reference. All 43 original
records are unchanged.

This normalization is only for test expectations. Production source retention
always stores the original bytes. Pure-Python protobuf is not substituted as a
blanket reference: the comparison exposed separate oneof and required-map-value
problems in that backend.

To refresh, use an isolated Python environment with `protobuf==6.33.5` and run
`PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=upb python generate.py` from this directory with protoc 25.1 on PATH. Review both
descriptor and case changes. Cargo tests use the committed files directly:

```sh
cargo test --lib protobuf::tests -- --test-threads=4
cargo test --test protobuf_semantics --test descriptor_mappings -- --test-threads=4
```
