# Protobuf map projection

An explicit `IndexDefinition` can now project a protobuf map as one physical
map column. The projection path ends at the map field. Its kind describes the
entry values; it does not flatten the synthetic entry message into unrelated
key and value columns. Ordinary `AddDocuments` map entries and descriptor-driven
`IngestMapped` use the same storage and query path.

## Supported projections

- `KEYWORD` accepts string, boolean, enum and all signed/unsigned integer value
  types. Strings remain exact, booleans become `true` or `false`, integers become
  exact decimal strings, and enums use their first declared alias. Unknown
  open-enum numbers become decimal strings. The plan reports `MAP_FACET` and
  the schema report reports `MAP_STRING_FACET`.
- `BOOLEAN` requires boolean map values and uses the same string facet plane.
- `FLOAT` or `DOUBLE` accepts float/double map values. Float values widen to
  f64; double values retain their numeric value. Non-finite projected values
  refuse during extraction. The plan reports `MAP_F64` and the schema report
  reports `MAP_FLOATING_POINT`.

For example, for `map<string, string> labels = 4`, this policy creates the
query column `attrs`:

```textproto
projections {
  field_numbers: 4
  kind: MAPPED_KIND_KEYWORD
  column_name: "attrs"
}
```

The complete current row-index definition still needs its explicit document ID,
vector and body projections. At node bind, declare `attrs` in
`--map-facet-fields`; floating maps use `--map-numeric-fields`. Mobile and Rust
configurations have the corresponding lists. A missing declaration refuses
before the binding or rows are applied.

Unhinted maps retain their existing source-only inference. Explicit descriptor
KEYWORD/BOOLEAN/FLOAT/DOUBLE hints use the same map rules as an explicit index
definition. Adding a projection changes the plan fingerprint and requires a
new compatible index generation; a persisted binding cannot be changed in
place. Existing plans without these new map projections retain their
fingerprints.

## Keys, defaults and repeated wire entries

All twelve permitted protobuf map-key types are supported. String keys retain
their exact contents, including empty strings. Boolean keys use `true` and
`false`; signed and unsigned integer keys use canonical decimal strings with
no leading plus or zero padding. Signed and unsigned extremes remain exact.
The source descriptor binds the key type, so these encodings cannot collide
between different key types within one map. Different key types produce
different schema fingerprints.

Map selectors use the canonical string key. For example, a uint64 key equal to
its maximum is selected as `weights['18446744073709551615']`. This addresses
one entry; it does not turn numeric key ranges into an ordered numeric index.
Integer values explicitly projected as KEYWORD also have string comparison
and ordering semantics, rather than numeric arithmetic semantics.

The protobuf decoder resolves map entries before projection:

- Omitted key and value fields within a present entry use their protobuf
  defaults. A zero-length entry is a present default key/value pair.
- A later entry for a key replaces the earlier entry, including when the later
  entry omits its value and therefore supplies the default.
- An unknown closed-enum value makes the entire map entry unknown. It does not
  replace an earlier known entry for the same key. Unknown open-enum values
  remain values.
- A missing entry stays absent. Empty string values and numeric zero remain
  present values.

Projection sorts the resolved entries by canonical key before submitting
unique materialized entries. This makes extraction independent of hash-map
iteration order. It never rewrites the retained source: duplicate wire entries,
unknown fields and the producer's exact payload remain in the original bytes.
The ordinary `AddDocuments` API still rejects duplicate keys because its entries
are already materialized values, rather than raw protobuf wire occurrences.

## Scope and query behavior

A map inside an explicit CHUNKS scope projects independently for each chunk.
A singular parent occurrence and a chunk occurrence retain their own path and
column assignment. Traversal into a map's synthetic entry fields is refused;
that would discard the relationship between each key and its value.

Schema reports mark the map field as a VALUE with its map query representation.
The synthetic `key` and `value` fields are INPUTs with `value_path` pointing to
that map projection. They are not independent dotted query fields. Constraints
record key conversion, default/duplicate handling and value conversion.

Existing map filters, presence tests, value projections, scoring, facet counts,
range facets and expression-based aggregates address these columns. The explicit
empty-key selector rules in [map columns](map-columns.md) apply. Authorization
continues to name the physical field; defining a projection grants no access.

## Remaining work and compatibility

Exact numeric int64/uint64 map projections and message-valued map projections
are not implemented. The integer-map feature branch now provides low-level
storage and document transport; see [its status](integer-map-storage.md). Integer maps can be explicitly projected as KEYWORD when string
semantics are intended; numeric projections refuse instead of rounding through
f64. Bytes, message values and arbitrary repeated/nested values retain their
source-only disposition until their storage and query contracts are implemented.
This increment does not remove the row index's ID/vector/body requirements,
publish catalog identity, implement remote authorization, or complete the three
search foundations.

The wire changes add two ColumnFamily values and two MappedQueryRepresentation
values. Existing fields and enum numbers remain unchanged. Clients must
recognize the returned map families/representations and inspect the acknowledged
index definition. Older planners cannot derive these value projections; binding
to an older node refuses rather than dropping a requested map column. Existing
map storage formats and the durable explicit-policy format are unchanged.

`tests/map_projection.rs` covers all key types and schema fingerprint separation,
default and duplicate entries, open/closed enums, signed zero from explicit wire
bytes, finite numeric validation, schema dispositions, chunk separation and
named unsupported-shape refusals. RPC tests cover planning, missing node column
declarations, ingestion, direct and nested-relay queries, both storage layouts,
reopen, durable rebind refusal and compaction that renumbers rows while retaining
the exact source payload. Signed-zero source fidelity is separate from numeric
query equality and from default elision by protobuf client encoders.

Validation against main `73a420b` passed 507 library tests, 743 integration tests
across 127 targets, 12 embedded tests and two IVF tests (1,264 total; one existing
live OpenNLP test ignored). All five Android/iOS Rust compile checks,
test/example compilation, formatting and vendored-proto checks passed. Descriptor
comparison confirms only the four additive map enum values, with all preceding
wire declarations unchanged. Validation used two build jobs and four test
threads. No fleet deployment was performed.
