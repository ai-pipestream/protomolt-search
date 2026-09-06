# Schema and projection report

`PlanIndex` returns `MappedPlan.schema_report` for a successfully derived plan.
The report distinguishes original-byte preservation, mapped projection and the
query representation. It is an explanation of the current plan, not a new
indexing policy or a promise that every protobuf field type is queryable.

`DescribeSchema` accepts a complete descriptor set and a root type without
requiring a viable index plan. Its report inventories proto2/proto3 schemas,
including empty messages, all scalar types, groups, MessageSet, extensions,
recursive messages and well-known types. Every field is source-only, with empty
projection lists. No indexing hints are applied, so `excluded_by_hint` is false;
this does not override hints in a later `PlanIndex` call. MessageSet's wire-format
option is explicit in the report even though mapped ingest cannot decode it.
Malformed descriptors, missing imports, unknown roots and unsupported descriptor
syntax are errors. Dynamic payloads inside `Any` remain bytes here; this RPC
describes the declared graph, not types hidden in a document's payload.

Planning and inspection validate syntax before entering reflection. The pinned
descriptor library panics while constructing its unsupported-syntax error;
explicit validation returns `INVALID_ARGUMENT` instead. Currently accepted
syntax is absent (proto2), `proto2`, or `proto3`; editions are not supported by
this reflection contract.

The response includes the SHA-256 of the exact supplied descriptor bytes. This
is a content address, not a semantic plan fingerprint. The RPC is read-only,
needs no shard fanout and uses the same collection administration permission as
`PlanIndex`. Embedded Rust's `describe_schema`, Android's `nativeDescribeSchema`
and Swift's `describeSchema` provide the same report locally. The mobile calls
take a serialized `DescribeSchemaRequest` and return `DescribeSchemaResponse`
inside the usual `MobileResponse` envelope; call them off the UI thread.
Android's `nativePlanIndex` and Swift's `planIndex` also expose the existing
planner through `PlanIndexRequest`/`PlanIndexResponse`. A phone can derive and
review its plan locally, then pass the returned fingerprint to mapped ingest.

The schema is a finite graph. Each reachable message appears once with every
ordinary field and registered extension, including fields hidden by SKIP hints,
repeated-message boundaries or recursion in the projection walk. Field type
names point to other graph nodes. Map-entry messages, enum declarations and
openness, required/optional labels, defaults, presence and oneof declarations
remain visible. Message names and field numbers order the report; enum value
order retains alias/default meaning.

Each field lists exact root-relative paths and their field-number paths. VALUE
paths name the mapped column and its query representation. CONTAINER paths
identify traversal or a chunk scope, with no independently queryable message
value. Version 2 adds INPUT paths for wrapper `value` and Timestamp `seconds`
and `nanos` components. An INPUT names its consuming VALUE through `value_path`
and carries that output column name; its query representation is NONE. It does
not create an independently queryable dotted field. SOURCE_ONLY paths do not
extract values. All unlisted occurrences of a
field are source-only, including descendants reached through an unindexed
repeated message or a recursive occurrence beyond the listed paths. Thus a
message reused under two parents does not acquire a projection everywhere just
because one occurrence is indexed.

The query representation distinguishes analyzed text, string facets, signed and unsigned
integers, floating-point numbers and dense vectors. Constraints describe current
conversions and value-domain restrictions: finite numerics, f32 vectors, the
unsigned-to-i64 limit on signed numeric columns, string-rendered enums and
epoch-microsecond timestamp storage. Bind-time analyzer configuration,
materialization and authorization are separate contracts. The report grants no
access and adds no source-fetch route.

Explicit integer keywords use exact decimal strings across their full declared
signed or unsigned domain. In particular, `uint64` and `fixed64` keywords accept
values through `18446744073709551615`; they do not inherit the numeric i64 limit.
Their query representation remains `STRING_FACET`, so equality and sorting use
string semantics. Unhinted unsigned fields instead use `UNSIGNED_INTEGER`
query representation and full-domain u64 columns. Their constraints explicitly
report typed value expressions, sorting, collapse, exact aggregates and range
facets. Statistical folds require explicit double conversion. Score stages
convert unsigned inputs and extrema to double arithmetic. Signed and unsigned
column statistics retain exact extrema and 128-bit sums; their double summary
fields are approximate, and the exact mean is the sum divided by count.

Preservation means exact bytes in the retained original protobuf. Unknown fields
share that rule. `PlanIndex` reports that legacy mapped ingest requires at least
one row for source retention. `DescribeSchema` reports the logical catalog's
row-independent preservation contract, with
`requires_index_rows_for_preservation=false`. Describing does not configure a
catalog, accept a document, validate document payloads or acknowledge durability.
Source acceptance still uses the [document catalog](document-writes.md), including
for empty and zero-chunk sources.

The report is derived and excluded from the v3 projection fingerprint. Adding
report metadata does not change a binding; changing wrapper projection paths or
other projection/wire semantics does. See the wrapper migration in
[descriptor mappings](descriptor-mappings.md#scalar-wrappers-2026-09-05-feature-branch). Planning also validates the
extractor for each proposed value path, so a column family that cannot decode
its declared protobuf type refuses during planning rather than failing only at
bind. Rejected plans still return a status; clients can independently call
`DescribeSchema` to inspect their source graph. Configurable projections and
query implementations for currently source-only shapes remain unfinished.

`tests/schema_report.rs` uses protoc-generated schemas and the existing Google
protobuf semantics fixture. It checks fields hidden by projection boundaries,
recursive and map graphs, extensions, oneofs, defaults, enum openness, exact
projected occurrences, constraints and descriptor-file reorderings without
losing custom options. A second fixture covers all 18 protobuf field types,
empty messages, MessageSet and recursive extensions without search roles. RPC,
authorization and mobile bridge tests cover transport, administration grants,
revocation and local inspection without source or index rows. Existing
fingerprint and mapped-ingest tests pin binding and extraction behavior.
