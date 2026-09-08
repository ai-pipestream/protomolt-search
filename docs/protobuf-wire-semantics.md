# Protobuf wire-type semantics

Projection matches a field's number and its compatible wire type before changing
its value or presence. A well-formed occurrence with a known number but an
incompatible wire type is unknown data. It cannot replace a value, select a
oneof member, or satisfy a required field. This follows the
[reference parser's field dispatch](https://github.com/protocolbuffers/protobuf/blob/v33.5/src/google/protobuf/wire_format.cc).

Repeated packable fields accept both scalar and length-delimited packed values,
regardless of the descriptor's preferred encoding. Singular numeric fields do
not accept packed occurrences as values. Groups require their group wire type;
a length-delimited occurrence does not become that group. The same checks apply
to registered extensions and nested messages.

Skipping still validates framing. Truncated varints, fixed-width values and
lengths, unmatched group ends and recursion-limit violations remain errors.
Required-field validation runs on the merged known values, including unindexed
messages and message-valued map entries. An unknown occurrence cannot make an
uninitialized message valid.

## Maps and independent references

An unknown synthetic key/value occurrence leaves that component at its protobuf
default unless another recognized occurrence supplies it. An extra synthetic
field does not remove the entry. Duplicate entries then follow the normal
last-key-wins rule. An unknown closed-enum value is a separate case: the whole
entry remains unknown and does not replace a previous known entry.

Google's C++ contract and Python upb 6.33.5 differ on generic unknown fields
inside map entries. The projection follows the
[documented C++ map contract](https://protobuf.dev/reference/cpp/cpp-generated/#parsing-unknown-values).
The fixtures explicitly select the C++ reference for 17 new synthetic-entry
cases and retain both parser observations. The other cases keep their pinned
upb reference; this is not a blanket claim that either runtime defines every
protobuf edge case. See the [fixture generation contract](../tests/fixtures/protobuf-semantics/README.md).

## Measured proto2/proto3 scalar boundaries

A separate boundary fixture records 26 inputs, 13 under each syntax, observed
with C++ `protoc 25.1` and Python protobuf 6.33.5 using upb. Product
dispositions are explicit rather than inferred from either reference or from a
Rust run: 14 cases are accepted and 12 are refused. Accepted controls include
ASCII and multibyte UTF-8, canonical zero, a noncanonical ten-byte zero,
`uint64` maximum, `int64` minimum and ten-byte negative one. Their projected
signed and unsigned values are asserted exactly. These wire-width cases do not
change or test later `int32` narrowing.

Strings must be valid UTF-8 under both syntaxes. Varints whose final byte carries
payload above 64 bits and truncated varints are malformed. These rules match the
[proto2 string requirement](https://protobuf.dev/programming-guides/proto2/#scalar)
and the [ten-byte unsigned-64 varint domain](https://protobuf.dev/programming-guides/encoding/#varints).
They intentionally refuse eight inputs accepted by both measured references:
the two invalid-UTF-8 proto2 strings, plus three overwide final-byte varints
under each syntax. Proto3 invalid UTF-8 and truncated varints agree with the
reference refusals. For malformed recognized fields, errors name the innermost
known field or registered extension without including values; unknown framing
may have only enclosing or document context. The strict UTF-8 and
greater-than-64-bit refusal policy is unchanged; the fixture makes its
boundaries and diagnostics permanent.

The empty-secondary-mapped-text correction preserves the data-model boundary.
Mapped extraction and accepted-source prepared protobuf rows retain explicit
empty field presence. At `IngestSource::next`, only an empty mapped secondary
string is omitted from the actual secondary analyzer input. Original source
bytes remain unchanged. Plain ingest keeps its existing raw-empty refusal, and
the main body, facets, whitespace-only and stopword-only values keep their
existing rules.

## Preservation and compatibility

The original descriptor and payload are retained independently of the projection
decoder. Skipping an unknown occurrence does not remove it from source storage.
The mapped-ingest regression checks the exact incoming bytes after flush while
the existing binding/reopen checks continue to apply.

The correction accepts well-formed occurrences that the previous decoder
refused. It does not change previously accepted projections, plan derivation,
fingerprints, product protobuf definitions or stored formats. Existing indexes
need no rebuild for this correction. A node that still has the old decoder can
continue to refuse these newly accepted documents; update it before sending
such raw mapped input. Broader shape support, source routing and remote
permission enforcement remain unfinished.

## Verification

Both empty-secondary ingestion paths reproduced their rejection before the fix.
Malformed UTF-8 and overwide-varint tests also failed before field context was
added to their errors. An initial attempt
omitted the value during mapping and failed the existing optional-presence
regression; it was reverted and the boundary moved without weakening that test.
A staged fixture initially dropped unknown options, so its reflected edit was
corrected before reproducing the staging rejection.

Focused validation then passed 6 library and 94 integration tests: 14
descriptor, 7 projection, 21 staging, 24 mapped, 8 multi-field wire and 20
semantic tests. The existing optional-presence test passed. All 646 input hashes
remained stable in an 8 GiB, swap-disabled scope that reached the cap, recorded
99 `memory.max` events and zero OOM events.

The pinned reference generator reproduced all 26 observations with only the
two protoc timestamp/PID prefixes normalized for comparison. Both raw outputs
were retained. Nine comparator tests and the command-line same-path and
existing-output refusals passed; the reference scope peaked at 33,472,512 bytes
with zero swap/OOM and 647 unchanged input hashes.

The full gate at `e9854f3` passed 680 library tests, all 148 integration targets
(887 reported passes and one existing ignored live-OpenNLP test), 13 embedded
tests and two IVF tests. The nine reference-comparator tests also passed.
All five Android/iOS compilation checks, tests/examples compilation, formatting,
vendored-proto checks and diff checks passed. All 21 product/vendored protobuf
files and the original `cases.json` and `descriptor.bin` fixtures are
byte-identical to `3575772`.

The driver verified a clean, unchanged HEAD and identical hashes for all 647
files before and after validation. The full scope reached its 8 GiB cap,
recorded 39,518 memory-limit events and four socket-throttling events, with zero
swap and zero OOM events. Another task's release build ran concurrently for part
of the gate; these are correctness results, not a latency or fleet measurement.
This validates the scalar-boundary and empty-field checkpoint; source routing
and remote permission enforcement remain unfinished.

The focused gate passes 117 reference cases within the protobuf unit test, all
20 public semantic tests and the mapped-ingest binding/reopen regression. The
two public regressions and the ingest regression reproduced semantic failures
before the production change. An initial public test used `unwrap_err` on a
non-Debug success type; after correcting that test compilation error, both
public failures were reproduced before applying the fix.

The focused gate passed with unchanged runtime/fixture hashes in an 8 GiB,
swap-disabled scope: 5.25 GiB peak, zero swap and zero OOM events.

The full gate at `5710605` passed 667 library tests and the first 102 integration
targets, then stopped on a schema-report fixture assertion: the added double
field made the count 19 rather than 18. Commit `de96f0b` changes only that
assertion. A manifest comparison verified every other tracked file against the
initial run before resuming at the affected integration group. The original
failure logs were preserved; the resumed run passed with unchanged file hashes.
Combined reported results are 1,555 passed, zero failed and one existing ignored
test across the library, 146 integration targets, embedded package and IVF
adapter. This is combined evidence across the test-only correction, not a claim
that the initial invocation succeeded.

All five mobile target checks, tests/examples compilation, formatting, vendored
proto checks and the product-proto compatibility gate passed. Product protobuf
files are byte-identical to `5cd1039`. Both full validation scopes reached their
8 GiB memory cap with zero swap and zero OOM events. This validates the decoder
checkpoint; it does not establish the entire foundations goal or a fleet
deployment.
