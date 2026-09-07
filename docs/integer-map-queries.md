# Exact integer map queries

Status: implemented on `feat/integer-map-storage-2026-09` for filters, key
presence, typed value expressions, materialization and expression-based
aggregates. [Direct map sorting/collapse](integer-map-order.md) is also
implemented. Bounded score stages, exact range facets and column statistics
remain unfinished. See
[map projection](map-projection.md) and [storage](integer-map-storage.md).

## Protobuf operators and older nodes

FilterExpr has `typed_map_number` at tag 15 and `typed_map_has_key` at tag 16.
ValueExpr has `typed_map` at tag 16. The numeric predicate reuses
MapNumberPredicate and its typed FilterBound values; the value read reuses
MapRead. Column names and literal string keys identify entries, including the
empty key. Integer-keyed source maps use their canonical string keys.

These operators include signed and unsigned map planes. The original
FilterExpr `map_number` continues to resolve only floating maps; `map_has_key`
continues to resolve string and floating maps. The original ValueExpr `map`
retains its string/float scope. Existing protobuf field and enum numbers are
unchanged.

New CEL compilation emits the typed operators for numeric map comparisons,
map-key presence and map value reads, including string/float reads. An older
protobuf decoder sees an unset expression, which query validation rejects.
This prevents a mixed cluster from treating an integer-map column on an older
node as unknown and silently omitting its rows while another node answers.
Map queries emitted by the new compiler therefore require updated node and
relay decoders; legacy protobuf operators remain usable for their original
families. Candidate fetches also require the typed-map response acknowledgement
described in [map order](integer-map-order.md#exact-ordering-and-composition),
including from empty nodes that previously skipped expression validation.
These checks use request/response contracts, not health probes.

## Exact values and missing entries

Examples for an unsigned map named `counts` and a signed map named `offsets`:

```cel
counts[''] == 18446744073709551615u
counts[''] > 9007199254740992u
offsets[''] < -9223372036854775807
'' in counts
!('' in counts)
counts[''] + 1u
double(counts[''])
```

Comparisons use the scalar integer bound rules. Integer literals never round
through double; finite double bounds are compared in their exact mathematical
relation to the integer domain. Exclusive bounds outside a type's range do not
wrap. A missing entry produces UNKNOWN in comparisons, including under NOT.
Presence is total: false for a missing key or column, true for a present zero.

The existing known-name rule still applies: a numeric-map comparison or value
projection must resolve its column and key on at least one shard. Presence
requires only that some shard has the column, so asking whether an unseen key
is absent is valid. A child without the column contributes missing values.

Value expressions retain `int` or `uint`. Arithmetic requires matching types;
conversion to double is explicit. Overflow and division by zero yield absence,
matching the scalar expression contract. Materialization uses the same typed
inputs and validates output column types before accepting documents.

An integer map with no entries still has a static value type. Projection
metadata preserves that type even when the requested key is absent. Conflicting
signed/unsigned declarations across shards fail, including through relays,
instead of accepting whichever shard returns a value first.

## Composition and permissions

Heap, sealed, reopened and segmented reads use the same exact storage getters.
Segmented reads translate map-key ordinals independently for each segment and
tail. Introducing a key that sorts before an older segment's keys cannot change
what a compiled read returns. Integer-map leaves currently do not prune segments
from summary metadata; they evaluate rows without an unsupported pruning guess.

Expression aggregates use the existing integer folds: COUNT, CARDINALITY,
SUM, MIN, MAX and exact percentiles preserve their typed semantics. SUM uses
wide internal accumulation and rejects a result outside its output type.
Statistical folds still require explicit double conversion. Relays compose the
same typed partials and validate projection types and known-name metadata.

Field grants address the physical map column. Filtering requires USE;
projection and aggregate disclosure also require DISCLOSE. Aliasing or applying
arithmetic does not bypass those checks. Document-visibility predicates use the
same exact numeric map comparisons. A denied streaming projection emits no hits.

Schema reports now expose MAP_SIGNED_INTEGER (9) and MAP_UNSIGNED_INTEGER (10).
Their constraints distinguish the implemented query operations from the
remaining score, facet and statistics work. This does not complete the
broader network authorization, catalog identity or durability goals.

## Validation, 2026-09-07

On the feature branch with main `3d0f109` incorporated, this increment passed
508 library tests, 761 integration tests across 132 targets, 12 embedded tests
and two IVF-provider tests: 1,283 passed, zero failed. The existing live OpenNLP
conformance test remains ignored. All five Android/iOS compile targets passed,
along with tests/examples compilation, formatting, vendored-proto verification
and whitespace checks.

The entire validation ran in one systemd scope with `MemoryMax=8G` and
`MemorySwapMax=0`; its peak including file cache was 8 GiB. All 356 recorded
source inputs remained unchanged during validation. Descriptor comparison
against `a223ab8` confirms that existing declarations are unchanged: three
oneof operators and two query-representation enum values were added.

Regression tests distinguish adjacent integers above 2^53 and the signed and
unsigned extremes, compare placement predicates with served filters, and cover
missing keys/columns, overflow, materialization, typed aggregate folds, ordinal
translation during sealing, reopening, nested relays and sibling relay groups.
Empty signed/unsigned columns still reject conflicting projection types.
Permission tests cover USE versus DISCLOSE, aliases, arithmetic, streaming
refusal and exact document-visibility boundaries. A legacy protobuf decoder
fixture verifies rejection of the new operators; this is not a deployment test
against an older fleet binary.
