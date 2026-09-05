# Schema and projection report

`PlanIndex` returns `MappedPlan.schema_report` for a successfully derived plan.
The report distinguishes original-byte preservation, mapped projection and the
query representation. It is an explanation of the current plan, not a new
indexing policy or a promise that every protobuf field type is queryable.

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
value. SOURCE_ONLY paths do not extract values. All unlisted occurrences of a
field are source-only, including descendants reached through an unindexed
repeated message or a recursive occurrence beyond the listed paths. Thus a
message reused under two parents does not acquire a projection everywhere just
because one occurrence is indexed.

The query representation distinguishes analyzed text, string facets, signed
integers, floating-point numbers and dense vectors. Constraints describe current
conversions and value-domain restrictions: finite numerics, f32 vectors, the
unsigned-to-i64 limit, the signed absence sentinel, string-rendered enums and
epoch-microsecond timestamp storage. Bind-time analyzer configuration,
materialization and authorization are separate contracts. The report grants no
access and adds no source-fetch route.

Preservation means exact bytes in the retained original protobuf. Unknown fields
share that rule. The report explicitly states the current limitation that node
source retention requires at least one mapped row. A zero-chunk source still
needs the logical document catalog described in [Search foundations](search-foundations.md).

The report is derived and excluded from the v3 projection fingerprint. Valid
existing mappings keep their identities; incompatible projection/wire changes
still require their existing migration checks. Planning also validates the
extractor for each proposed value path, so a column family that cannot decode
its declared protobuf type refuses during planning rather than failing only at
bind. Rejected plans still return a status, so independently describing schemas
that the current planner cannot bind remains future work.

`tests/schema_report.rs` uses protoc-generated schemas and the existing Google
protobuf semantics fixture. It checks fields hidden by projection boundaries,
recursive and map graphs, extensions, oneofs, defaults, enum openness, exact
projected occurrences, constraints and descriptor-file reorderings without
losing custom options. Existing fingerprint and mapped-ingest tests pin binding
and extraction behavior.
