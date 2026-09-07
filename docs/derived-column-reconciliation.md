# Derived-column reconciliation, 2026-09-07

Review of `sea-of-slop` design note
`design-notes/cel-value-columns-2026-09-07.md` at `ac2c282`, against search main
`1af6936` and `feat/integer-map-storage-2026-09` at `51f25dd` plus the map-order
increment. The proposed declaration, typed inputs, immutable backfill and
partition-binding sequence can proceed with the corrections below. This note
coordinates contracts; it does not implement derived columns or authorize a
fleet operation.

## Declaration and recovery

Define DerivedColumns in `ai.protomolt.search.v1` as the canonical contract.
TOML and command-line files configure that protobuf declaration. Its canonical
fingerprint needs the declared output order, names, exact kinds, expressions,
input bindings and evaluator/function versions. Calendar timezone and boundary
rules and the hash's byte encoding must be fixed by that contract. Define
input dependency ordering, and reject cycles if derived inputs are supported.

Keeping WAL replay value-based is correct. Keeping all WAL metadata unchanged
is insufficient: a standalone replay needs the declaration fingerprint and
complete ordered column definitions, even when no row has a value. Version-gate
new recovery metadata so older readers refuse rather than erase the declaration.
Check declaration compatibility before appending records or serving a restored
index. Incomplete source columns in a backfill are a named refusal; do not infer
that a missing declaration is an optional per-document absence.

The current typed column-table allocations include kind 14 for the explicit
index binding, 15 for i64 maps and 16 for u64 maps. Kind 6 is the legacy binding,
not the current complete binding. Use a distinct verified-free kind for derived
metadata, preserve kinds 12–16, and carry it through heap/spill writers, readers,
segmented catalogs, compaction, snapshot/replication and segment transplant.
The mapped-plan binding and derived declaration should compose without either
one replacing the other.

Direct and routed ingest must enforce the declaration before placement or row
allocation. A client cannot bypass derivation by supplying its own value under
a derived column, omitting MaterializeSpec, or echoing only a fingerprint.
Define validation of already-materialized routed records separately from
internal WAL replay. Name collisions, incompatible inputs and output overflow
must fail before an ambiguous value can become the shard key.

## Typed inputs and hashing

The feature branch already materializes exact signed and unsigned map inputs:
`NumericTypes`, `IngestEnv`, `apply_materialize`, and the typed map ValueExpr
operator. Preserve these paths when adding timestamps, facet strings and bytes.
Unsigned operations require unsigned literals, for example `% 64u`; do not
coerce the full u64 domain through f64. Pin the missing-value and malformed-
timestamp rules, including a missing result used as a placement key.

Reuse `coordinator::stable_routing_hash`: it already implements FNV-1a over
opaque stable-key bytes. `route_stable_key_in` selects a configured inclusive
hash range within a placement leaf. `% n` is a different partition function,
so comparing it to current routing is not a valid equivalence test. Preserve
the range scheme or declare a separately versioned bucket policy and rebuild
under it. Bucket count and routing policy belong to the durable declaration.

The historical `reshard::fnv1a64`/`bucket_of` contract hashes eight little-endian
row-ID bytes and uses high bits to route WAL buckets. Leave that contract intact.
The stable key is not a generation-local row ID. Backfill must prove that the
source retains the same opaque key bytes; imported public identity or a
reconstructed row number is not an implicit substitute.

## Permissions and ownership

Derived columns need explicit USE/DISCLOSE policy, including values revealed
through ordering, groups, statistics, Explain and partition diagnostics.
Hashing an input does not grant permission to disclose it. An explicit policy
may disclose a derived result while withholding its inputs, but that is a
policy decision rather than an automatic property of the hash.

Shared files extend beyond the original note's list: `src/values.rs`,
`src/cel.rs`, materialization in `src/node.rs`, `src/postings.rs`, WAL and
reshard metadata, `src/schema_report.rs`, and index-definition reporting.
FetchValues needs no new protocol for ordinary stored derived columns.

The earlier feature checkpoints already allocate `ValueExpr.typed_map` at tag
16, `FilterExpr.typed_map_number` at 15 and `typed_map_has_key` at 16, plus
ColumnFamily MAP_I64/MAP_U64 at 9/10 and the corresponding map query
representations at 9/10. Calendar/hash additions must not reuse those numbers
even while this feature branch is separate from main.

The map-order increment adds `QuerySort.map`, `CollapseSpec.map` and
`BrowseSort.map` at tag 3, and `BrowseShardResponse.sort_contract_version` at
16. The empty-node type fix also adds
`FetchValuesResponse.typed_map_contract_version` at tag 11, checked on every
child of a typed-map fetch, and resolves empty fetches against configured
families. Preserve these selectors and per-child checks during reconciliation.
The route table is unchanged. The existing map operators and column families
remain under `ai.protomolt.search.v1`.

Declaration/persistence can start independently. Reconcile against the current
feature checkpoint before changing the shared evaluator, and keep the fleet
proof after the source contract, durability checks and backfill verification.

The following scoring increment allocates `ScoreStage.typed_map_op` at tag 10,
reusing MapScoreOperation for f64/i64/u64 inputs. Preserve its distinction from
the legacy f64-only selectors and its double conversion at score evaluation.
This changes no routes, value-expression tags, storage formats or recovery
metadata.
