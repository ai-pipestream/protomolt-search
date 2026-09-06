# Search foundations

Foundation work began on `feat/search-foundations` from `PRE_ASTRA`.
Current implementation branch: `feat/protobuf-unsigned-numerics-2026-09`.
This tracks the full requested foundation. Individual passing increments do not
establish completion of the three workstreams.

## Completion requirements

| Requirement | Required evidence | Current state |
|---|---|---|
| Faithful protobuf decoding and compatible index binding | Generated-runtime differential fixtures for presence, oneofs, merges, scalar encodings, unknown values and schema evolution | Oneof, presence, merged messages, int32, enum openness, required fields and groups corrected; v3 includes reachable extensions; coverage expanding |
| Every protobuf shape has an explicit preservation, indexing and query disposition | Typed index definition and exhaustive descriptor/field support report; no silent omission | Mapped plans report their graph and projection/query dispositions; DescribeSchema also reports source-only graphs; configurable index definitions remain |
| Original payload and descriptor identity survive storage and replay | Byte equality after restart, snapshots, replication, compaction and resharding, including unknown fields | Row-bearing sources survive image/WAL lifecycle byte-for-byte; the catalog retains zero-row sources across restart; catalog backup and publication remain |
| Complete scalar, repeated, map, nested and well-known-type semantics | Projection and query conformance across supported syntax/edition and shape combinations | Incomplete; existing column-family restrictions remain |
| Workspace and collection grants separate read, ingest and administration | Denial tests on every public and node entry point, default collection resolution and direct access | Public search and coordinator diagnostics enforce revisioned protobuf capabilities; direct node/cluster-control policy enforcement remains |
| Document and field grants cover retrieval and disclosure | Selection, statistics, suggestions, facets, highlights, projections, source fetch, caches and cursors tested under distinct and revoked policies | Document grants enforce private-shard BM25 selection, statistics, caches, suggestions and prefix expansion; field use/disclosure grants cover these routes; broader query and network enforcement remains |
| Stable document and chunk identity | Exact key lookup and returned identity unchanged through compaction, replay and resharding | Imported identities persist through image/WAL lifecycle, node fetch and lexical results; catalog publication and the other result routes remain |
| Conditional writes and persistent idempotency | Concurrent version conflicts, repeated requests, key reuse with different payload, disconnected acknowledgment and restart tests | Collection-wide local source authority implemented; server routing and projection transactions remain |
| Accepted, searchable and durable receipts | API states tied to actual transaction publication and persisted recovery boundaries, crash tests at each boundary | Local source acceptance has durable/volatile receipts and abrupt-process-exit coverage; searchable publication remains |

## Design constraints

The public contracts belong in the product's protobuf package. Keep descriptor
vocabulary owned by ProtoMolt, product projection and policy owned by search,
and vector distribution owned by the interchangeable backends. The embedded
library must retain its no-network dependency boundary.

Preservation, projection and querying are separate contracts. An original message
must not lose bytes because a field is not indexed. The index definition must
record every field's disposition and reject unsupported requested operations.
Descriptor content identity, decoder semantics and projection identity must be
separately inspectable. Physical postings and vector blocks can remain packed
and memory mapped.

Authorization uses the ecosystem's workspace authority through a replaceable
provider, not a second user directory. A resolved request context binds subject,
workspace, collection, action and policy revision. Mandatory document selection
and field grants cannot be overridden by user CEL. Cache/cursor identity and
stream revocation must use that context. A trusted internal connection alone
must not permit a public principal to bypass the policy through a node route.

The write transaction owns the exact stable key, source version, conditional
precondition, original payload, projection, idempotency record and receipt.
Publishing a replacement must expose one logical version, not both the appended
row and its predecessor. Compaction may change storage slots but cannot change
this identity or discard the deduplication history required by the contract.
Define acknowledgment and retention rules explicitly; fsync and replicated
acknowledgment are separate capabilities. A phone retaining sole ownership never
promises durability from a remote copy.

## Decoder increment

`src/mapping.rs` now validates descriptors with `prost-reflect` and decodes the
whole message before projecting into columns. This resolves oneof selection and
submessage merging before any leaf lands, including a oneof alternative that has
no indexed column. Projection retains explicitly present empty strings and zero
values. Implicit-presence scalars project their protobuf defaults consistently
whether omitted or explicitly encoded; an absent explicit-presence field remains
missing. An absent enclosing message does not invent child values.

The v3 plan fingerprint includes the reachable wire schema as well as the
projection. Field numbers, scalar encodings, cardinality, syntax, defaults,
oneof membership, enum declarations and map-entry shape participate. Descriptor
file order, unrelated files and source comments do not. The original descriptor
content hash remains separate.

This is an index compatibility change. Existing v1/v2 mapped generations remain
readable, but a new bind derives v3 and refuses to append into a v1/v2 binding.
Rebuild mapped data from original protobuf sources into a new generation. Do not
rewrite stored fingerprints or replay reduced columns as proof of corrected
extraction. Unmapped generations do not acquire a new mapping or require a
rebuild from this change alone.

The local catalog now retains zero-row originals, and standalone schema
inspection reports graphs that cannot form an index plan. Remaining protobuf
work includes unsigned columns, repeated/nested correlation, extension indexing,
well-known-type projection and Editions. Index planning refuses reachable
MessageSet types; source-only inspection can describe them.
The decoder dependency does not itself prove those contracts. Its behavior must
be covered or adapted by the conformance suite before support is claimed.

Integer keyword extraction now distinguishes signed and unsigned descriptor
types. All ten protobuf integer encodings render exact decimal facets over their
full domain, including optional zero, `i64::MIN` and `u64::MAX`. Keyword parent
IDs retain their integer bit pattern instead of acquiring string hashes. This
removes an erroneous i64 coercion from keyword ingest and its schema report;
numeric u64 columns remain unfinished. Existing accepted values and v3 plan
fingerprints are unchanged. `tests/integer_keywords.rs` reproduces the previous
refusal and covers extraction, ID reduction, CEL string selection, projections
and persisted source/column bytes.

Validation on 2026-09-05: 420 library tests, 500 integration tests across 84
targets and 10 embedded tests passed, with one existing sidecar test ignored.
The first integration pass had a transport-readiness failure during fixture
ingest in `ltr::lexical_boost_on_a_dense_leaf_carries_its_own_analysis`; the
entire 16-test LTR target passed on retry. The cause was not established.
All five Android/iOS Rust target checks passed with the three existing relay
dead-code warnings. These are local checks, not fleet deployment evidence.

Signed numeric columns now preserve the entire i64 domain with a separate
presence bitmap in storage kind 10. Heap and spill writers agree byte-for-byte;
heap and mapped readers validate section geometry, canonical absence bytes,
bitmap padding and min/max. Legacy kind 4 still decodes its original sentinel
semantics and rewrites into kind 10. Older binaries refuse the new kind.
Mapped extraction and materialization preserve a real `i64::MIN`, including
through flush/reopen and WAL replay during compaction on both layouts.

I64 materialization now contributes a semantic-version marker to the binding
hash: old bindings can contain silently omitted MIN values and refuse new
appends until rebuilt from original documents. F64-only bindings and the v3
base mapping fingerprint remain unchanged. Startup still reconciles ordinary
shard WAL records to the flushed image; this increment does not add automatic
roll-forward recovery or stronger write receipts. Unsigned numeric columns,
other protobuf shapes and the remaining authorization/publication work remain
required.

The presence increment passed 420 library tests, 505 integration tests across
85 targets and 10 embedded tests on 2026-09-05, with one existing sidecar test
ignored and no failures in the final run. All five Android/iOS target checks
passed with the three existing relay dead-code warnings. Tests/examples
compilation, formatting, fixture regeneration and vendored-proto checks passed;
the public search descriptor remains byte-identical when source comments are
excluded. No fleet benchmark, rebuild or cutover was performed.

The unsigned-numeric branch adds a physical u64 family (storage kind 11),
explicit presence, exact unsigned bounds, and heap/spill/mapped access. Segment
reads preserve those values across a frozen seal, publication, continued tail
ingest and reopen; segment summaries keep unsigned ranges in `uint_columns`.
Schema checks reject a tail or segment with different ordered columns before
it can change the meaning of a column ordinal. Existing signed storage bytes
and protobuf contracts are unchanged. Storage alone does not establish mapped unsigned semantics. The following
increments add typed ingest, filtering, descriptor mapping and value expressions;
unsigned range facets and scoring remain unfinished.
See `tests/unsigned_columns.rs` and `docs/range-facets.md`.

The next increment connects `UnsignedIntegerValue` to ordinary protobuf ingest,
server/Rust/mobile configuration, all three write implementations and WAL
replay. Duplicate or incompatible column names refuse before ingest, including
for Rust callers that bypass the CLI. `tests/unsigned_ingest.rs` exercises zero,
absent values, values above 2^53 and i64::MAX, and u64::MAX over gRPC, flush,
reopen, repeated compaction and a two-child WAL split. It reproduced a
single-image compaction bug that dropped entirely absent column declarations;
both layouts now retain the live column tables. Offline splitting still needs
a durable declaration contract to retain columns absent from all input records.

Validation of the ingest increment on 2026-09-05: 421 library tests, 517
integration tests across 87 targets and 11 embedded tests passed, with one
existing sidecar test ignored. All five Android/iOS Rust target checks,
tests/examples compilation, formatting and vendored-proto checks passed.
The descriptor comparison against `ee2abb1` confirms that the only wire
changes are the unsigned value message, ingest field 26 and mobile config
field 28; existing declarations are unchanged. This branch includes the
relay-BM25 main checkpoint `4dedddb`. No fleet rebuild or deployment ran.

Unsigned numeric filters now preserve typed `FilterBound.uint` values from
CEL decimal/hexadecimal uint literals through protobuf transport, per-shard
resolution, placement evaluation and both topology and segment pruning.
Mixed signed/unsigned/double comparisons are exact, including beyond 2^53 and
at the domain boundaries. Empty and absent columns retain three-valued
semantics. Extreme exclusive floating bounds no longer overflow i128 during
signed normalization. `tests/unsigned_filters.rs` compares numeric behavior
with an independent IEEE integer-ratio oracle and fixed expected rows through
heap, reopened single-image and reopened segmented searches, monolithic and
distributed. These filter checks alone do not establish complete unsigned
protobuf semantics; the subsequent increments cover mapping, value expressions
and ordering, followed by exact scalar aggregation and percentiles. Unsigned
range facets and scoring remain unfinished.

Validation of the filter increment on 2026-09-05: 421 library tests, 521
integration tests across 88 targets and 11 embedded tests passed, with one
existing sidecar test ignored. All five Android/iOS Rust target checks passed
with the three existing relay dead-code warnings. Tests/examples compilation,
formatting and vendored-proto checks passed. A descriptor comparison against
`442ec30` confirms that only `FilterBound.uint` field 4 was added; existing
declarations remain unchanged. These are local checks on the unsigned-numerics
feature branch, not fleet validation.

The descriptor-mapping increment infers `UINT32`/`UINT64` kinds for all four
unsigned protobuf encodings and lands singular values in `U64`. Bind validates
the declared unsigned column table. The schema report distinguishes unsigned
query representation and names its remaining unsupported operations. The new
kind/family participates in the existing canonical hash; the checked-in legacy
fingerprint from mapper `d0a1716` is rejected, while the existing signed plan
fingerprint remains unchanged. Explicit signed hints retain checked i64
conversion. Ambiguous column names now refuse during planning, including
parent/chunk aliases and flattened paths with the same output name.

A regression test reproduced mapped ingest acknowledging an expression over an
unsigned input that the materialization environment did not represent. The
initial explicit refusal has now been replaced with typed uint evaluation and
U64 outputs. Node spec validation checks declared input types before ingest,
including absent optional inputs; shared coordinator materialization checks the
actual request types. Wrong target families and mixed numeric expressions refuse.

`tests/unsigned_mapping.rs` covers optional/implicit presence, oneof clearing,
merged nested messages, full unsigned parent and chunk keys, repeated/map source
preservation, signed hint refusal, old binding refusal, and mapped ingest,
reopen, exact key filtering and repeated compaction on both layouts. Legacy
mapped ingest still does not publish catalog `DocumentIdentity`; retaining
indexed key values and original bytes is not completion of the identity,
conditional-write or receipt contract. Unsigned range facets, scoring and the other protobuf shapes also remain required.

The unsigned ordering increment carries u64 columns and lineage keys through
sort values, distinct unsigned cursor components, collapse representatives and
inner hits. Sorted browse and candidate-scoped `FetchValues` publish scalar
metadata, including empty responses; coordinators refuse missing metadata,
incompatible shard types and rows that disagree with their declared type. The
native BM25 projection merge now applies the same validation, including streamed
completions and nested relay merges; zero-hit and empty-analysis queries still
check projection types. Sorted lexical queries now return requested projections;
a lifecycle test reproduced their previous silent omission. Tests restart pagination after
compaction and do not claim cursor validity across generation changes.

Validation of the ordering increment on 2026-09-05: 422 library tests, 533
integration tests across 91 targets and 11 embedded tests passed; one existing
sidecar test was ignored. All five Android/iOS Rust target checks passed with
three existing relay dead-code warnings. Tests/examples compilation, formatting,
vendored-proto and diff checks passed. Descriptor comparison against `35ea12d`
confirms four additive fields and one scalar-type enum, with existing
declarations unchanged. No persisted format or mapping fingerprint changed.
This remains feature-branch validation; this work does not update main or
deploy to the fleet.

Validation of the unsigned value-expression increment on 2026-09-05: 422
library tests, 530 integration tests across 90 targets and 11 embedded tests
passed; one existing sidecar test was ignored. All five Android/iOS Rust target
checks passed with the three existing relay dead-code warnings. Tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `41f27a9` confirms exactly three additive declarations: the uint literal,
typed uint projected result and U64 materialization kind. Existing declarations
are unchanged. This checkpoint remains on the unsigned feature branch; main and
the fleet were not changed.

Validation of the mapping increment on 2026-09-05: 422 library tests, 527
integration tests across 89 targets and 11 embedded tests passed; one existing
sidecar test was ignored. All five Android/iOS Rust target checks passed with
the three existing relay dead-code warnings. Tests/examples compilation,
formatting, fixture regeneration and vendored-proto checks passed. Descriptor
comparison against `d0a1716` confirms that the only wire additions are two
unsigned mapped kinds, the U64 column family and unsigned query representation;
existing declarations are unchanged. The materialization regression failed
before the input validation and passed afterward. These checks are local to
the unsigned feature branch; no fleet deployment or cutover was performed.

The unsigned storage foundation passed 420 library tests and 510 integration
tests across 86 targets on the `0effb06` base, with one existing sidecar test
ignored and no failures. After incorporating main's console changes through
`8275e56`, all three console tests, five unsigned storage tests, and ten
embedded tests passed. All five Android/iOS Rust target checks passed with
the three existing relay dead-code warnings. This is local validation of the
storage foundation, not evidence of public unsigned search or fleet cutover.

`tests/protobuf_semantics.rs` uses generated prost messages as differential
oracles and adversarial wire encodings. `tests/descriptor_mappings.rs` pins the
new fingerprint. `tests/mapped_ingest.rs` exercises binding, column landing,
restart, routed ingest, replication and resharding through the real handlers.

`src/protobuf.rs` intercepts closed-enum values before the reflected message
mutates, so an unknown number cannot erase a known value or change oneof
selection. Unknown closed-enum map values omit the entire entry from projection.
Open enums retain unknown numbers, rendered as decimal facet strings; declared
numbers retain the first alias name. Openness follows the enum's defining file,
including proto3 enums imported by proto2 messages. Protobuf framing and recursion
limits remain in prost; no parallel wire parser was added.

Required fields are validated after all message fragments merge. The check
includes unindexed repeated/map messages, groups and present extensions, while
inactive oneof members and absent optional messages do not invent requirements.
Singular groups now expand and project like singular messages. Registered
extensions and their reachable message/enum types participate in the v3 hash;
reordering files or extension declarations does not change it.

The checked-in fixtures under `tests/fixtures/protobuf-semantics/` compare merged
values and required-field validity against Google's protobuf 6.33.5 runtime.
They cover decoding, not original-source preservation: the private projection
decoder intentionally excludes closed-enum unknowns from its value tree. Its
output must never substitute for the original payload in future source storage.

## Capability increment

`authorization.proto` and `src/authorization.rs` supply exact workspace-bound
collection grants, an ecosystem authority adapter, revisioned decisions and
stream revocation. `CollectionSet` enforces separate search, ingest and admin
capabilities on every public RPC. Bearer configuration now requires an explicit
policy; credentials without grants deny access. [Security](security.md) records
the route table, migration, tests and remaining direct-node/document/field gaps.
This does not complete authorization or the overall foundation.

## Source storage increment

[Original protobuf storage](protobuf-source-storage.md) records the archive and
WAL formats, byte-preservation evidence and remaining zero-row catalog gap.
Mapped rows retain original producer bytes separately from projected values.
Descriptors and source payloads are interned within each image and WAL
generation; spill builders put payloads on disk. This does not yet establish
logical identity, complete shape support, source disclosure permissions or
transactional durability.

## Schema report increment

[Schema and projection report](schema-report.md) describes every reachable field
and registered extension for accepted plans, including skipped and recursive
shapes. Exact projection paths distinguish source-only occurrences, traversed
containers and query values; constraints expose current value-domain losses.
This does not supply the remaining scalar/nested/extension query implementations
or an independently configurable index definition.

`DescribeSchema` now inventories source-only graphs without requiring an index
plan, including empty messages and MessageSet. The source-only report applies no
indexing hints and promises no query projections. The collection-admin RPC and
embedded/mobile calls share the same implementation and graph contract.

Validation on 2026-09-05 encountered two 600-second stalls in
`single_image_shard_compacts_online`, despite successful isolated runs. Captured
stacks showed compaction waiting for asynchronous analysis while holding the
live shard's write lock, with ingest blocked on that lock. Cutover now prepares
and analyzes the tail unlocked, then checks the WAL generation and high
watermark under the lock before installing. An optimistic-only attempt then
failed under sustained writes by exhausting its 16 retries. Final preparation
now reserves commits through an asynchronous gate, allowing reads and analysis
to proceed while new commits wait. The WAL fence remains defensive coverage
against an uncoordinated mutation. A deterministic regression failed on the previous
implementation and now checks both read availability during analysis and
inclusion of a write committed during final preparation. This fixes the observed
lock cycle without extending the integration test's timeout.

The combined schema, namespace and compaction tree passed 356 library tests,
431 integration tests across 70 targets (one existing ignored test), and 10
embedded tests, including the no-network dependency gate. All five Android/iOS
Rust target checks, examples/tests compilation, Java wrapper and C header
compilation, protobuf bundle compilation, and vendored byte-identity checks
passed. These are local validation results, not device-run or fleet deployment
claims.

## Logical source authority increment

[Document writes](document-writes.md) records the collection-wide catalog,
conditional versions and persistent retry decisions. Embedded Rust and mobile
calls retain originals without requiring any index rows. Its path is independent
of shard geometry, and source acceptance never claims search visibility.
An ordered, fenced source feed is persisted with acceptance and exposed to local
projection consumers through Rust and mobile bridges. It does not publish rows
or advance searchable state. Format-1 catalogs upgrade their history index
transactionally, preserving original sources and retry decisions.
Legacy ingest, public result identity, atomic projection replacement, catalog
backup/migration, workspace binding and document/field grants remain unfinished.

## Row identity increment

`DocumentIdentity` and interned archive metadata retain exact document keys,
versions and chunk ordinals independently of physical rows. Node fetch and
lexical hits expose that metadata, and compaction tests verify it after row
renumbering and recovery. Simple lexical `Query` selection and its streamed
terminal response preserve the scored row's identity. Dense identity on the
product-owned node paths now travels with the scored snapshot through classic/
coalesced and streaming top-k, public dense `Query` and its terminal stream.
[Dense identity](dense-identity.md) describes the bounded winner-only exchange
and relay requirements. Remote-provider, streaming parent-collapse, hybrid,
Boolean, browse and provisional result identities remain.
Identity-bearing archives use format 2, and their WAL records require format 3.
This storage/import capability does not establish authority over versions on
legacy ingest or propagate identity through every query response. The publisher
must still attach identities from accepted versions and atomically control which
version's complete chunk set is visible.

The lexical result increment passed 356 library tests, 432 integration tests
(one existing ignored test), 10 embedded tests, all five mobile target checks,
examples/tests compilation, and the vendored-proto byte gate. The public tests
cover flat/fused BM25, both node delivery modes, rescoring and terminal streaming
with exact binary keys, large versions, absent identities and optional chunk
ordinals. Both compaction layouts verify lexical identities after renumbering,
reopening and replay.


Validation of the lexical projection type increment on 2026-09-05: 422 library
and 537 integration tests across 92 targets passed, plus 11 embedded tests;
one existing sidecar conformance test was ignored. The first integration run
stopped when `ltr::scorer_interplays_refuse_by_name` encountered a transport
error during fixture ingest, before query assertions. The entire six-target
group passed on an unchanged-source rerun, followed by all remaining groups.
All five Android/iOS Rust target checks passed with the three existing relay
dead-code warnings. Tests/examples compilation, formatting, vendored-proto
identity and whitespace checks passed. Descriptor comparison against `209f166`
verified exactly one additive field, `Bm25QueryResponse.projection_types = 13`,
and no changes to existing declarations. This checkpoint remains on the unsigned
feature branch; the broader shape, authorization and document-lifecycle goals
are not complete.


The unsigned aggregate increment adds exact u128 partial sums, typed uint
extrema and results, exact distinct unions, and unsigned nearest-rank
percentiles over filtered, grouped and query-pool selections. Sums outside u64
refuse with the exact total; statistical folds still require explicit double
conversion. Percentile ranks use integer arithmetic over the supplied IEEE
percentile and full u64 count. The schema report advertises these capabilities,
and the console displays uint values without narrowing them to JavaScript
numbers. Unsigned range facets and scoring remain unfinished; no new relay
aggregation routes are enabled. See `docs/aggregations.md` section 11.

Validation on 2026-09-05: 425 library tests, 538 integration tests across 93
targets and 11 embedded tests passed (974 total); one existing sidecar test was
ignored. The first integration attempt hit a transport error during fixture
ingest in `query_api::unsupported_shapes_refuse_by_name`, before query assertions.
Its full six-target group passed unchanged on rerun, followed by the remaining
groups. The final library run includes the updated capability-report wording.
This repeats the earlier fixture transport symptom; the address-keyed,
process-global analyzer channel cache is a follow-up for a deterministic runtime
lifecycle investigation, not an established cause yet.

All five Android/iOS Rust targets passed, with the three existing relay
dead-code warnings. Tests/examples compilation, formatting, vendored-proto
identity, console JavaScript syntax and whitespace checks passed. Descriptor
comparison against `d864208` verified exactly two uint result variants, five
unsigned partial fields and one UINT enum value, with existing declarations
unchanged. Stored formats and analyzer fingerprints are unchanged by this
increment. Work remains on the unsigned feature branch, and the broader
protobuf-shape, permission and document-lifecycle goals remain incomplete.


The analyzer-channel follow-up reproduced the transport failure without any
server restart or port reuse: a sidecar stayed healthy on one runtime while a
client runtime was destroyed and replaced. The first runtime analyzed twice;
the second failed immediately with `Service was not ready: transport error`.
The process-global address-only cache had retained a tonic channel whose worker
belonged to the retired runtime.

Channels now pool by runtime and address. A task owned by each runtime retains
its pool until shutdown, while the process registry holds only weak references.
Shutdown releases the cached channels even if that owner task was never polled.
Concurrent callers share creation under the pool lock. A caller outside a Tokio
runtime receives a named failed-precondition error. No request replay was added.
The manifest records Tokio 1.49 as the minimum for the stable runtime ID API;
the existing lockfile remains at 1.53.1. See `docs/native-analysis.md`.

Validation on 2026-09-05: the new fixed-endpoint runtime-replacement regression
failed on the previous implementation and passes after the ownership change.
A second regression closes one of two concurrent client runtimes while the
other continues to use the same sidecar. Two library tests cover pool release
and the outside-runtime refusal. The full suite passed without retries: 427
library tests, 540 integration tests across 94 targets, and 11 embedded tests
(978 total), with one existing sidecar conformance test ignored. All five
Android/iOS Rust target checks passed with three existing relay dead-code
warnings. Tests/examples compilation, formatting, vendored-proto identity,
whitespace checks, and descriptor comparison against `aa399e2` passed; the
search, schema-report and mobile descriptors are unchanged. This remains a
feature-branch checkpoint. The unsigned range-facet, scoring, wider protobuf
shape, permission and document-lifecycle work is still incomplete.


## Exact range-facet checkpoint (2026-09-05)

Range facets now preserve each stored value's numeric domain. `typed_edges`
accepts signed, unsigned and finite double bounds, with exact mixed-domain
ordering and fixed half-open intervals. This includes the full signed and
unsigned domains: the exclusive upper limits 2^63 and 2^64 are exactly
representable doubles. Empty/unset, exclusive, nonfinite, duplicate or
out-of-order edges refuse before a match set is read.

Legacy double edges also compare exactly against integers. Previously,
`i64::MAX` rounded up to 2^63 and disappeared from a bucket ending there;
the value now remains below the edge. Typed responses echo authoritative
`typed_from` and `typed_to` bounds alongside the old display doubles. A typed
integer interval can have coincident display doubles without being empty.

One interval implementation now serves node counting and both root and relay
merges. Every child must echo the requested column, key and interval list;
unknown children must return no buckets. Exact typed bounds cannot disappear
through a relay. Counts use checked addition, and only the root requires at
least one shard to resolve every column. Numeric families may differ between
shards because each interval has the same exact numeric meaning across them.

The independent test oracle uses doubled i128 values to represent both all
64-bit integers and half-valued doubles exactly. It covers matching, filtered
and empty sets; flat unary/streamed and nested relay queries; the fused lexical
route; and flush, reopen, compaction and subsequent reopen on both layouts.
Unit tests exercise malformed edges, corrupted child responses, unresolved
columns and count overflow. This adds three protobuf fields, with no stored
format or materialization fingerprint change. Existing descriptor declarations
are preserved. See `docs/range-facets.md` for client requirements.

The full objective remains open: unsigned score stages and full-width column
statistics, remaining protobuf shapes, permission enforcement across every
surface, and catalog-to-search identity and document-lifecycle integration
still need implementation and evidence.

Validation: 430 library tests, 542 integration tests across 95 targets, and
11 embedded tests passed (983 total), with one existing sidecar conformance
test ignored. The full suite needed no retries. All five Android/iOS Rust
target checks passed, with the three existing relay dead-code warnings.
Tests/examples compilation, formatting, vendored-proto byte identity and
whitespace checks passed. Descriptor comparison against `6622053` confirms
only `RangeFacetField.typed_edges` (4) and `RangeBucket.typed_from` (4) /
`typed_to` (5) were added; existing declarations are unchanged. This is a
feature-branch checkpoint, with no main merge or fleet operation.


## Unsigned scoring checkpoint (2026-09-05)

`ColumnRef::UnsignedInteger` connects exact u64 storage reads to score-chain
evaluation, explain inputs and contributions, and stored-value signal fetches.
Node resolution now obtains unsigned extrema from heap, mapped and segmented
stores and checks the inverted empty range before converting extrema to the
score scale. The availability report includes unsigned columns independently
of k or matches. A declared empty column stays known and contributes identity.

This uses the existing double-precision stage contract. Values and extrema
share one monotone conversion, so the same upper-bound proof applies to
unsigned columns. Source values remain exact; scores and explain inputs may
round adjacent large integers. The protobuf comments and schema report state
that distinction, and the report still names unsigned `stats_fields` as
unsupported. No wire declaration, stored format or fingerprint changes.

The regression suite uses decimal parsing as an independent conversion oracle,
checks contribution/evaluation agreement and bound dominance, and compares
seeded/unseeded pruning with exhaustive scoring over 3,000 documents. Real
nodes compare distributed scores with monolithic scores through nested relays,
streamed responses, explain output, owner-node FetchValues signals and compaction on both
layouts. A separate stable row projection identifies fixture documents after
row renumbering. Explain contribution comparisons respect protobuf's signed
zero normalization; nonzero contributions and final scores retain bitwise
checks.

Full-width column statistics, remaining protobuf shapes, field/document grants
across all operations, and catalog-to-query identity, retries and durability
remain part of the active objective.

Validation: 430 library tests, 544 integration tests across 96 targets, and
11 embedded tests passed (985 total), with one existing sidecar conformance
test ignored. The full suite needed no retries. All five Android/iOS Rust
target checks passed with the three existing relay dead-code warnings.
Tests/examples compilation, formatting, vendored-proto byte identity and
whitespace checks passed. The search, schema-report and mobile descriptors are
identical to `a914507`. This remains a feature-branch checkpoint; no main merge,
fleet deployment or corpus rebuild was performed.


## Typed column-statistics checkpoint (2026-09-05)

`ColumnStats` now declares its numeric type and retains signed/unsigned extrema
and a 128-bit exact sum in typed payloads. The width covers the full 64-bit
value/count domain. Exact sum plus count supplies a rational mean; the old
double fields remain approximate views with their existing fold order.
Node collection and root merging share validation and exact sum encoding.
Empty known columns retain type and payload, while unknown columns carry no
values. Both extrema must be consistent with the count and exact sum.

Root merging now rejects wrong field names, missing/mismatched type metadata,
impossible or malformed summaries, count overflow and nonfinite floating sums.
A concrete type mismatch refuses even with zero matches. This deliberately
replaces implicit mixed signed/double aggregation with a single declared
numeric family per field. Matching server/client builds are required. No
stored format or mapping/materialization fingerprint changed.

The new tests compare i128/u128 oracles against filtered and empty selections,
reverse shard order, both layouts, reopen and compaction. They distinguish a
declared empty column from an unknown column, verify signed and unsigned sums
beyond 64 bits, exercise maximum-count protobuf roundtrips, and reject malformed
partials and empty-match type conflicts. Existing double/signed summary tests
also pass. Relay and fused/phrase statistics retain their existing refusals.

The wider objective remains open: remaining protobuf shapes, complete grants
across all public/node/control surfaces, and catalog publication into search
with stable identity, conditional writes, idempotency and durability receipts
still require implementation and end-to-end evidence.

Validation: 433 library tests, 546 integration tests across 97 targets, and
11 embedded tests passed (990 total), with one existing sidecar conformance
test ignored. The full suite needed no retries. All five Android/iOS Rust
target checks passed with three existing relay dead-code warnings.
Tests/examples compilation, formatting, vendored-proto byte identity and
whitespace checks passed. Descriptor comparison against `13303dd` confirms
existing declarations are unchanged: ColumnStats adds value_type (8) and an
exact_integer oneof with signed (9) and unsigned (10), plus the two four-field exact
summary messages. This is a feature-branch checkpoint, with no main merge or
fleet operation.

Timestamp DATE projection now checks descriptor components before returning a
plan and validates the protobuf instant domain during extraction. Direct
TimestampValue ingest, placement extraction and WAL-derived resharding use the
same value validation. Original-byte preservation remains separate: an
incompatible named descriptor can still be inspected without an index plan.
Regression tests first reproduced acceptance of malformed Timestamp descriptors
and out-of-range mapped instants, then verified refusal alongside presence,
merged components, valid endpoints and negative-time microsecond flooring.
Valid plan fingerprints and stored representations are unchanged. See
[Timestamp projection validation](descriptor-mappings.md#timestamp-projection-validation-2026-09-05-feature-branch)
for recovery compatibility. This does not complete configurable well-known-type
projections or the other foundation requirements above.

Timestamp validation on 2026-09-05: 434 library tests, 548 integration tests
across 97 targets and 11 embedded tests passed, with one existing sidecar test
ignored and no retries. All five Android/iOS Rust target checks passed with the
three existing relay dead-code warnings. Tests/examples compilation, formatting,
vendored-proto identity and whitespace checks passed. Public protobuf
declarations are unchanged; comments now document the accepted instant range.
These are local checks, not on-device or fleet deployment evidence.


Scalar wrapper mappings now use their containing field paths, honoring type,
name and identity hints and preserving absent versus default-valued messages.
The report records component inputs explicitly in version 2. Empty facet strings
are distinct from absence through native ingest, public selection/projection and
persisted storage. Planning rejects unusable identity projections and ignored
well-known-component hints. Existing wrapper bindings require new plans and a
rebuild from source; the column formats are unchanged.

The wrapper lifecycle test exposed a mobile gap that the subsequent explicit
mapped-analysis checkpoint addresses: `MappedBind.field_analysis` supplies all
projected TEXT specifications, including non-body fields. The complete binding
persists through restart, sealing, compaction, resharding and replication.
Native validation runs before mutation, including absent fields. Legacy bindings
retain body-only/default semantics; converting them requires a new binding and
rebuild. The current query-analysis change carries the originating fingerprint
through every lexical scoring and membership route and requires a matching
nonzero identity for explicit bindings. Neither change silently substitutes the
body's analyzer for another field. See [mapped analysis](descriptor-mappings.md#explicit-mapped-analysis-2026-09-05-feature-branch).

Wrapper validation on 2026-09-05: 434 library tests, 559 integration tests
across 98 targets and 11 embedded tests passed, with one existing sidecar test
ignored and no retries in the final suite. All five Android/iOS Rust target
checks passed with the three existing relay dead-code warnings. The new native
embedded runtime test covers wrapped body analysis and scalar defaults; it does
not claim native non-body analysis support. Tests/examples compilation,
formatting, vendored-proto identity, fixture regeneration and whitespace checks
passed. Descriptor comparison confirms only additive INPUT=4 and
FieldProjection.value_path=7 declarations. No fleet deployment or reindex ran.

## Query analysis identity checkpoint (2026-09-05)

The node now enforces analyzer identity on flat and fused BM25, candidate
rescoring, both internal hybrid leg routes, lexical membership and lexical
sorting. The originating specification travels through coordinator fan-out,
Boolean planning/scoring, boosts and relays. Explicit mapped bindings reject
missing identities and mismatches even before the first row; optional fields
retain that rule after flush and reopen. Legacy unknown identities keep their
existing semantics. The six additive request fields change no index format.
Deploy matching clients and every coordinator/relay/node to enforce the contract
throughout the path; this is not a mixed-version capability negotiation.

`tests/query_analysis_identity.rs` checks identical terms under different
specifications, zero identities, k=0 requests, empty bound shards, optional-field
restart, all five hybrid modes, Boolean queries, sorting, boosts, cached-stat
reuse, and unary flat/fused BM25 plus streamed flat BM25 through two relay
levels. The existing multi-field error still explains that a mismatch scores
different term identities.

The full foundation objective remains open. Configurable projections for the
remaining protobuf shapes, document/field grants across all disclosure paths,
and catalog publication with stable identity, conditional writes, persistent
idempotency and accepted/searchable/durable receipts are not completed by this
checkpoint.

Validation: 439 library tests, 569 integration tests across 100 targets, and
11 embedded tests passed (1,019 total), with one existing sidecar conformance
test ignored. The final suite passed without retries. All five Android/iOS Rust
target checks passed with three existing relay dead-code warnings.
Tests/examples compilation, formatting, vendored-proto byte identity and
whitespace checks passed. Descriptor comparison against `47233a2` verifies
exactly six additive uint64 request fields; existing declarations and stored
formats are unchanged. These are local checks, not on-device or fleet results.


## Query cursor context checkpoint (2026-09-05)

Four regression tests reproduced unsigned cursors being accepted under another
principal, a later policy revision, changed query text and a different topology
generation while the boundary id/score remained unchanged. Public Query and
QueryStream now retain the server's AccessDecision and wrap internal boundaries
in integrity-protected protobuf envelopes. Request normalization permits only
observational trace/profile changes and equivalent collection/topology defaults;
query semantics and routing context remain bound. A mismatch refuses before
execution or stream creation, and streamed nested collections are validated.

Default keys are ephemeral per coordinator and shared by its clones. A library
host can supply a retained key; no new persistent signing-key store or CLI key
option is implied. Invalid, oversized, noncanonical and old unsigned tokens
refuse. Tokens do not freeze index data or bind every shard data mutation, so
this does not resolve compaction-safe pagination or the remaining stable-view
work. Document/field grants, scoped statistics and caches, and durable catalog
publication remain part of the full active objective.

Validation: 443 library tests, 579 integration tests across 101 targets and
11 embedded tests passed (1,033 total), with one existing sidecar conformance
test ignored. All five Android/iOS Rust target checks passed with three existing
relay dead-code warnings. Tests/examples compilation, formatting, vendored-proto
byte identity and whitespace checks passed. The descriptor comparison against
`836b9d8` confirms existing declarations are unchanged; only the three messages
in `query_cursor.proto` are added. No index format or fleet state changed.

The compaction fixtures retain one test key across coordinator instances so
they still exercise the data-boundary refusal. Embedded/network conformance
compares the full result apart from host-bound token bytes, verifies identical
second pages using each host's token, and checks cross-host refusal. The unsigned
ordering test inspects its typed boundary inside the new envelope and still
stitches pages exactly. These fixture updates preserve the original correctness
checks while accounting for the new cursor contract.

## Diagnostics capability checkpoint (2026-09-05)

Regression tests reproduced all six coordinator diagnostics routes accepting an
operator flag without an authority, or with administration of only one of two
workspaces. This exposed process-wide observations and allowed runtime changes
across collection boundaries. An idle metrics stream also survived revocation.

Diagnostics now requires the operator flag and an Admin decision for every
served collection. It validates the complete set before work, before each
collection update or shard fan-out, and before disclosure. Metrics streams hold
all resource permits, register every authority's revision channel, and recheck
before and after producer polling. Replacement suppresses both result and error
items and releases the idle snapshot producer without waiting for its next tick.
Previously applied knob mutations are not rolled back by concurrent revocation.

These endpoints remain operator views of the whole process. A library host must
supply its complete collection membership, including the ring and gauge sources;
the metrics registry is not tenant-scoped. Direct node/control membership and
document/field grants remain unfinished. In particular, the next search-policy
work must scope TermStats corpus counts and document frequencies as well as
selection: at this checkpoint TermStats carried only terms/fields, and
StatsCache keyed shares by node, field and epoch. The next increment below
adds the document-view contract and separates its cached shares. Filtering returned hits alone would still leak
restricted documents through ranking and statistics.

Validation: 444 library tests, 586 integration tests across 102 targets and
11 embedded tests passed (1,041 total), with one existing sidecar conformance
test ignored. The final suite passed without retries. All five Android/iOS Rust
target checks passed with three existing relay dead-code warnings per target.
Tests/examples compilation, formatting, vendored-proto byte identity and
whitespace checks passed. Descriptor comparison against `b634bba` confirms all
protobuf declarations are unchanged. These are local checks; no fleet service,
index generation, or main-branch state changed.

## Visibility statistics checkpoint (2026-09-06)

A raw gRPC probe carrying the proposed visibility field reproduced the previous
node silently returning three contributing documents where the restricted
reference corpus had two. `TermStats` now accepts a typed `DocumentVisibility`,
validates it before inspecting an empty or populated shard, and returns counts,
lengths and document frequencies over the view intersected with tombstones.
Its fingerprint echo lets a relay or cache refuse an older node that ignored
the request. The wire contract has one new message and three additive fields.

Nodes compute membership, statistics and data epoch under one read guard. Relay
levels carry the same view and OR its known-column flags; missing or mismatched
echoes refuse before merge. The statistics cache separates views, checks response
shapes, bounds scope churn and clears all scopes when a node's epoch changes.
Unrestricted callers retain their fast path and sparse tombstone-length
subtraction. Tests compare exact statistics and score bits with a physically
restricted corpus and cover both persisted layouts, deletes, compaction, reopen,
relay levels and cache contamination attempts.

This provides the statistics prerequisite, not public document or field grants.
Public queries still request unrestricted statistics. The authority, mandatory
selection, field-use/disclosure checks, suggestion dictionaries, source fetch,
RAG context and node delegation still need integration. The view fingerprint
is not a credential, a policy-decision cache key, or a replacement for the
existing data epoch protocol. See [document visibility](document-visibility.md)
for the wire identity, cold-request cost and exact boundary.

The fingerprint uses recursively ascending active field numbers, not the order
chosen by a generated encoder. An independently encoded unsigned oneof bound
with an exclusive flag exposed the ordering difference; normalization and fixed
wire/hash fixtures now pin the language-independent contract. The schema graph
check refuses to add undefined protobuf map or extension ordering unnoticed.

Validation: 448 library tests, 591 integration tests across 103 targets and
11 embedded tests passed (1,050 total), with one existing sidecar conformance
test ignored. The final suite passed without retries after the encoding fix.
All five Android/iOS Rust target checks passed with three existing relay
dead-code warnings per target. Tests/examples compilation, formatting,
vendored-proto byte identity and whitespace checks passed. Descriptor comparison
against `6631cb9` confirms exactly one new message and three additive fields;
existing declarations are unchanged. No index format or fleet state changed.


## Statistics lifetime checkpoint (2026-09-06)

The visibility cache audit reproduced another correctness gap: two distinct
node lifetimes at the same mutation count accepted each other's statistics.
The coordinator's retry also discarded its epoch check after fetching again.
Statistics and lexical membership now carry a 32-byte lifetime identity;
caches, relays and lexical scoring preserve it alongside the epoch. All retry
paths keep the newly fetched claim, and repeated change refuses. The same-address
regression retains a pooled connection and compares warm-cache scores with a
fresh coordinator, including through one and two relays. See
[statistics lifetimes](statistics-lifetimes.md) for the wire contract, upgrade
boundary and test scope.

This fixes a prerequisite for correct permission-scoped ranking. It does not
enable document/field grants or finish the durable identity/write requirements.


Validation: 454 library tests, 595 integration tests across 104 targets, and
11 embedded tests passed (1,060 total); one existing sidecar conformance test
remains ignored. All five Android/iOS Rust target checks, tests/examples build
checks, formatting and vendored-proto checks passed. Descriptor comparison
against `d260c92` verifies exactly six additive fields with existing declarations
unchanged. The network replacement and retry regressions pass, including direct,
one-relay and two-relay cache reuse. No fleet deployment or device runtime test
was performed; stored index formats are unchanged.


## Private-shard document grant checkpoint (2026-09-06)

Policy format 2 adds mandatory document visibility to collection search grants
and authority decisions. Private in-process BM25 execution now applies that view
to both scoring statistics and selection, including inline facets, projections,
highlights and explains. Cache lookup follows current authorization; physical
segment counters are explicitly redacted. The mobile package exposes an
`authorized_service` facade over its private nodes.

This is not completion of document/field authorization. Network-node delegation,
other public retrieval routes, dictionary prefixes, field grants and eventual RAG
context still require enforcement. Restricted uses of the uncertified routes and
deployments refuse before execution. See [document grants](document-grants.md).


Validation: 454 library tests, 601 integration tests across 105 targets, and
12 embedded tests passed (1,067 total); the existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `5e18438` confirms three additive grant/redaction fields and the
corresponding authorization import, with existing declarations unchanged.
These are local checks; no fleet deployment or device runtime test ran. Stored
index and WAL formats are unchanged.


## Document-scoped dictionary checkpoint (2026-09-06)

Private-shard `Suggest`, `TermSuggest` and BM25 prefix expansion now apply the
authority's document view before counting terms and postings. Hidden-only and
deleted-only terms do not consume expansion caps or influence suggestions.
Every internal dictionary response must echo the requested view. The posting
scan keeps bounded term storage through heap, mapped and merged segment cursors.

This extends the previous grant checkpoint; network delegation, other retrieval
routes, field grants, the remaining protobuf shapes and durable publication still
require work. See [document grants](document-grants.md#permission-scoped-dictionaries-2026-09-06)
for the execution cost and local test scope.


Validation: 454 library tests, 603 integration tests across 106 targets, and
12 embedded tests passed (1,069 total); one existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `2e30bdf` confirms exactly six additive dictionary visibility fields;
existing declarations are unchanged. These are local checks, with no fleet or
device-runtime validation. No stored index or WAL format changes were introduced.


## Private-shard field grant checkpoint (2026-09-06)

Policy format 3 adds exact indexed-field grants for query use and disclosure,
plus independent raw document-key disclosure. Private BM25 and dictionary routes
check field dependencies before statistics or fan-out. Automatic details can be
withheld with an explicit response flag; unauthorized explicit detail requests
refuse. Phrase planning does not implicitly grant a body's auxiliary bigram
column. See [field grants](field-grants.md) for semantics and coverage.

The complete goal remains open: broader queries and node delegation, remaining
protobuf shapes and indexing definitions, and durable source-to-search publication
with stable identity still require implementation and validation.

Validation: 454 library tests, 610 integration tests across 107 targets, and
12 embedded tests passed (1,076 total); one existing live-sidecar conformance
test remains ignored. All five Android/iOS Rust target checks, tests/examples
compilation, formatting and vendored-proto checks passed. Descriptor comparison
against `7e9496b` confirms exactly three additive fields, two field-policy
messages and one action enum; existing declarations are unchanged. These are
local checks, with no fleet deployment or device-runtime validation. Stored
index and WAL formats are unchanged.
