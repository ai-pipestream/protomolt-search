# Field-aware vector membership and candidate scores

Vector membership, native candidate scoring and original-FP32 candidate scoring
now accept the exact indexed vector field name. Nodes resolve that name from the
[durable mapped binding](vector-binding-storage.md), under the same read guard
that protects membership or scores. A grant on `semantic` cannot authorize a
vector whose source path is `signal`, or a legacy unnamed vector plane, merely
because its dimension matches. A declared nonzero dimension must agree with any
present provider and FP32 stores.

The node returns the actual `MappedVectorBinding`, including on empty results.
Named reads refuse missing bindings. The coordinator checks the requested name,
validates the declaration and requires all responding shards to agree on the
complete binding. Empty names preserve legacy unrestricted reads; they cannot
satisfy field-restricted execution. Raw vector-only indexing still needs an
explicit field-definition contract.

## Selection and read consistency

All three operations carry the mandatory document view. Candidate scoring
removes out-of-range, deleted, duplicate and invisible rows before touching
vector payloads. A vector without document metadata cannot satisfy a restricted
view. FP32 logical-byte accounting and its per-shard limit apply to the visible,
live owned candidate set. FP32 candidate bookkeeping is proportional to the
requested owned slots rather than allocating a bitmap for the entire corpus;
the native provider retains its existing masked-search implementation.

Both scoring requests can require a physical stats epoch and incarnation. The
node checks that claim under its read guard before returning any response,
including an empty one. Responses carry the physical version, view fingerprint
and known-column flags. The coordinator validates those receipts before merging
scores, rejects unrequested IDs, duplicate ownership and nonfinite scores, and
requires the authority's referenced columns to exist somewhere in the admitted
shard set. Named/scoped native scoring contacts every shard even when some have
no candidate IDs, so empty shards participate in binding and view validation.

A coordinator scoring call without an admitted read set captures one first and
validates it after fan-out. Query execution uses its existing root read set.
This detects concurrent changes; it does not provide historical reads or MVCC.
Exact scoring retains its contract that every requested candidate must resolve
exactly once. An invisible or absent candidate produces no node score, and the
coordinator refuses an incomplete exact candidate set instead of returning a
partial result.

The [scan protocol integration](vector-scan-views.md) adds opt-in initial
receipts to classic and streaming vector scans. The coordinator admits every
participating shard before using candidates. Private Query requests with field
grants and no document view now use this boundary.

## Authorization boundary and compatibility

Private coordinator execution requires `Use` on the requested vector field.
`Disclose` alone is insufficient. This operation produces a score rather than
returning the stored vector. The document view is supplied by trusted planning;
it and the returned binding are not authorization credentials.

Private Query and QueryStream enforce field grants across selection, projection,
inner-hit disclosure and revocation; public dense clauses name their indexed
field. Document-restricted public queries remain gated pending the rest of their
selection and execution-metadata audit. The clustered native-vector
provider currently lacks this product-field receipt contract and refuses scoped
scoring. Network delegation and direct-node authorization remain unfinished.
These internal APIs must not be exposed as an alternate authorization path.

The wire change is twenty additive fields across six existing messages, with no
new routes. Existing field numbers, declarations and storage formats are
unchanged. A new coordinator requires scoring receipts from every product node;
an older node cannot silently serve as a scoped peer. Upgrade nodes and
coordinators together. This change alone does not require reindexing. A legacy
unnamed generation remains insufficient for named field grants; the mapped
binding and source-rebuild requirements are described in the storage note.

## Evidence

`tests/vector_field_reads.rs` uses real gRPC nodes in both single-image and
segmented layouts. It checks exact names versus source-path and body aliases,
public/private/deleted/vector-only rows, duplicate and foreign candidates,
filtering before FP32 byte limits, scoped/native score equality, complete empty
receipts and refusal after a deletion invalidates the admitted physical version.

The coordinator's `vector_field_read_tests` use two private nodes to verify field
Use versus Disclose, document selection, missing and incompatible bindings,
empty reads, missing version receipts and stale generations. They also assert
that restricted public Query remains gated until its remaining boundaries are
implemented.

Validation on 2026-09-06 passed 470 library tests, 649 integration tests across
114 targets and 12 embedded tests (1,131 total), with one existing live-sidecar
conformance test ignored. All five Android/iOS compile checks, tests/examples
compilation, formatting and vendored-proto identity checks passed. Descriptor
comparison against `7fddc56` confirms exactly twenty additive fields with all
existing declarations unchanged. Source, build and test hashes were unchanged
through validation. No fleet benchmark, deployment or device-runtime test ran.
