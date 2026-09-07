# Map sorting and collapse

Status: implemented on `feat/integer-map-storage-2026-09`. QuerySort and
CollapseSpec accept an explicit protobuf MapRead, including an empty key.
Signed and unsigned integer entries retain their full-width values through
selection, relay composition, pagination and group identity. String maps can
also sort and collapse; floating maps can sort. See
[integer map queries](integer-map-queries.md) for filters and expressions.

## Protobuf selectors

Exactly one of `column` or `map` must be set. The map's column is nonempty;
its key is a literal string, including empty strings, quotes and Unicode.
Keys are not parsed as CEL. Integer-keyed source maps use the canonical string
keys described in [map projection](map-projection.md).

```textproto
sort {
  map { column: "counts" key: "" }
  descending: true
}
sort { column: "title" }
```

A collapse uses the same selector:

```textproto
collapse {
  map { column: "counts" key: "" }
  inner_hits: 3
}
```

`QuerySort.map`, `CollapseSpec.map` and the internal `BrowseSort.map` use tag 3.
Existing column and direction/inner-hit fields retain their numbers. The sort
and collapse shape restrictions are unchanged: sort orders a browse or one
lexical leaf's exact membership; collapse groups a scored candidate pool by its
existing relevance order. Sort and collapse do not combine in one request.

A document without any requested sort entry is excluded from the sorted set.
A document without its collapse entry forms no group. A present zero or empty
string is an ordinary value. Every selector must resolve its column and key on
at least one contributing shard; an unseen key is a named error. A child that
lacks the key or column contributes no sorted rows.

## Exact ordering and composition

Integer order uses the existing order-preserving bits and typed unsigned keys.
Neither an i64 nor a u64 map value passes through double for comparison, cursor
boundaries or group identity. `QueryHit.sort_values` carries the original typed
value. The legacy numeric `sort_key` display remains lossy for large integers;
it is not the comparison key. String ordering compares UTF-8 bytes.

Each node declares the type of every sort column even when no rows qualify.
Map types remain concrete when a key is unknown or the configured node has no
documents. Coordinators and relays compare those declarations independently of
key-known flags, rejecting signed/unsigned disagreements even in empty children.
Relays check row types and acknowledgements before truncating child pages.

`BrowseShardResponse.sort_contract_version` at tag 16 must be at least 1 on
**every** consulted child when any map selector is present. A zero acknowledgement
refuses even if that child returned no rows or another child resolved the key.
Relays advertise version 1 only after checking each child. This prevents older
nodes from dropping an unknown map selector and contributing a silent omission.
Legacy scalar sorts do not require the new acknowledgement.

A valid public map selector has an empty legacy `column`. Older public Query
decoders reject that missing column. Collapse fetches use the typed value
operator and require `FetchValuesResponse.typed_map_contract_version` at tag 11
to be at least 1 on every node or relay. Earlier empty nodes could bypass
expression validation and return unknown types; the acknowledgement rejects
that shortcut. Empty fetches now resolve against configured families, and
collapse checks agreed types even without candidate rows. Floating map keys
refuse as group identities even when no rows qualify. No route number or
existing field number changes, and the mapped-plan fingerprint is unchanged.

## Permissions and lifecycle

Sorting and collapse disclose their keys. They require both USE and DISCLOSE
on the physical map column, checked before statistics or shard reads. Choosing
a different key does not change the field grant. Denied streaming requests
emit no hits or successful partial response.

The same map-key dictionary translation runs in mutable, sealed, reopened and
compacted stores. Tests use keys inserted after sealing, both storage layouts,
nested and sibling relays, signed/unsigned extremes, adjacent integers above
2^53, duplicate group keys and missing entries. Pagination is checked against
one complete ordered response, with changed selectors refusing old cursors.
Compaction tests delete a row and verify new queries after row renumbering;
these tests do not establish cursor continuity across compaction.

This increment completes direct map sorting/collapse, not the remaining
integer-map bounded scoring, exact range facets or column-statistics work.
The broader remote authorization and transactional identity/durability goals
remain open.

## Validation, 2026-09-07

With main `1af6936` incorporated, the completed increment passed 508 library
tests, 769 integration tests across 135 targets, 12 embedded tests and two
IVF-provider tests: **1,291 passed, zero failed**. The existing live OpenNLP
conformance test remains ignored. All five Android/iOS compile targets passed,
plus tests/examples compilation, formatting, vendored-proto verification and
whitespace checks.

The full validation ran in one systemd scope with `MemoryMax=8G` and
`MemorySwapMax=0`; peak usage including file cache was 8 GiB. All 359 recorded
source and fixture inputs remained unchanged during the final run. Descriptor
comparison against `51f25dd` confirms existing declarations are unchanged:
three map selectors and two response acknowledgements were added.

Two failures were reproduced before their fixes: a valid map sort rejected as
an empty column, and collapse accepting incompatible unsigned/signed map
declarations when one node had no document store. Tests now cover both paths
through nested relays. A simulated older gRPC fetch peer proves that an empty
response without the typed-map acknowledgement cannot be combined with a
current peer's values. The lifecycle fixture selects a quoted key whose ordinal
changes after a later key is inserted, and verifies fresh results after deletion,
compaction and reopening. These are local tests and cross-compilation checks;
no fleet deployment or performance measurement was performed.
