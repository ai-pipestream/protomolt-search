# Foundations and main reconciliation, 2026-09-05

This checkpoint reconciles the published foundations tip `11e8fae` with
main `7bb1ec0`, including the reserved placement-tree contract. It brings the
implemented foundations onto the shared development line; the remaining
work in [search foundations](search-foundations.md) is still unfinished.
The uncommitted immutable identity-snapshot experiment is saved separately
and is excluded from this checkpoint.

## Instructions for the placement branches and their reviewers

Fetch Forgejo and merge the new `forgejo/main` into each placement worktree
before continuing. Preserve that worktree's uncommitted changes first. Use a
normal merge, inspect the result, and run the relevant tests before publishing.
Do not replace a conflicted file wholesale with either parent.

- Keep Search-owned proto files under `proto/ai/protomolt/search/`, with
  packages `ai.protomolt.search.*`. Move additions from an older branch's
  `ai/pipestream/search` path into the corresponding current file. This includes
  diagnostics, partition and placement contracts. Update imports, generated
  binding names, console descriptor lookup, JavaScript and documented RPC paths.
  ProtoMolt-owned `ai.protomolt.proto.*` imports stay byte-identical to their
  owning repository; OpenNLP and TEI stay in their existing packages. The
  handwritten Android/JNI wrapper package is independent of protobuf naming.
- Main's `Bm25Hit.explain` keeps field 6; identity is field 7. Main's
  `QueryHit.sort_values` and `explain` keep 10 and 11; identity is 12.
  Regenerate clients, including clients made from the earlier foundations
  branch. Main's multi-key sort/cursor shape also supersedes the earlier branch.
  Check current field allocations before adding any new contract fields.
- Preserve main's route numbering through `PlanPlacement` (index 62).
  `DescribeSchema` is appended at index 63; the route table now has 64 entries.
  Append future routes after it in both the enum and parallel table.
- In `collections.rs`, authorization surrounds the current handler and its
  diagnostic recording. Keep the decision extension, the post-response
  policy check, and `AuthorizedStream`. `TermSuggest` requires Search;
  `PlanPlacement` and `DescribeSchema` require Admin. The existing cluster-wide
  diagnostics principal flag is separate from collection capabilities.
- Compaction retains main's partition replay, column declarations, stall
  detection and shard-lock helpers. Final tail analysis runs outside the live
  shard write lock. The mutation reservation is acquired before the seal lock;
  public writers wait asynchronously. Preserve the generation/watermark fence
  before installation, and carry original sources and identities through
  partition spill logs, replay, rewritten images and WAL.
- Keep main's `execute_browse` path and sorting/collapse behavior. Simple
  lexical results retain identity alongside explain; unsupported identity
  routes keep explicit absence. A row number is not stable product identity.
- Keep `metrics::snapshot` available in the embedded build. Only the HTTP
  metrics listener is gated on `net`; inserting diagnostics helpers between
  that function and its feature attribute had broken no-network compilation.
- Keep placement topology/fan-out changes in the coordinator and config layer,
  while preserving authorization and generation-bound cache/cursor checks.
  A compiler-clean merge alone does not establish these semantics.

After a placement branch lands, the other branch should merge the resulting
main again. Publish Forgejo first and GitHub second, using live remote checks.
Do not force-push or deploy the fleet as part of this reconciliation.

## Verification

The compiled-descriptor audit preserved all 1,516 pre-checkpoint main fields,
including their types and numbers, plus existing enum values and RPC signatures,
after normalizing the intentional Search namespace rename. The foundations
comparison additionally identified main's intentional single-key to multi-key
sort/cursor changes and the two identity field reallocations above.

Validation: 394 library tests, 472 integration tests across 78 targets, and
10 embedded tests passed (876 total; one existing integration test ignored).
All five Android/iOS Rust target checks, tests/examples compilation, formatting
and vendored-proto byte identity passed. The embedded dependency gate confirms
that phone builds link no network stack. Integration coverage includes partitioned
compaction/reopen, legacy identity absence, identity plus explain, the console
facade, diagnostics with explicit grants, and the new routes' authorization.

## Remaining foundations work

Descriptor inspection and source preservation do not mean every protobuf shape
has a complete index/query representation. Document/field grants, catalog-backed
atomic search publication, and identity on every query shape remain unfinished.
See [schema reports](schema-report.md), [document writes](document-writes.md),
and [security](security.md) for the implemented boundaries. Phone-owned shard
bytes remain local under the intended [device-shard contract](device-shards.md).
