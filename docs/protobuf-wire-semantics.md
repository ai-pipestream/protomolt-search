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

The focused gate passes 117 reference cases within the protobuf unit test, all
20 public semantic tests and the mapped-ingest binding/reopen regression. The
two public regressions and the ingest regression reproduced semantic failures
before the production change. An initial public test used `unwrap_err` on a
non-Debug success type; after correcting that test compilation error, both
public failures were reproduced before applying the fix.

The focused gate passed with unchanged runtime/fixture hashes in an 8 GiB,
swap-disabled scope: 5.25 GiB peak, zero swap and zero OOM events. Full-suite
validation is recorded separately when complete; these focused checks do not
establish the entire foundations goal or a fleet deployment.
