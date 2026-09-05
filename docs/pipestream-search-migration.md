# Pipestream Search naming and provider migration

The search product is now backend-neutral and is named **Pipestream Search**.
This is an API and packaging migration, not a vector-format migration.

## New identities

| Surface | Current identity |
|---|---|
| Cargo package and server binary | `pipestream-search` |
| Rust crate | `pipestream_search` |
| gRPC package | `ai.protomolt.search.v1` |
| WAL protobuf package | `ai.protomolt.search.wal.v1` |
| Environment prefix | `PIPESTREAM_SEARCH_*` |
| Default vector provider | `embedded-turbovec` |
| Repository and local checkout | `protomolt-search` |

Regenerate clients from
[`search.proto`](../proto/ai/protomolt/search/v1/search.proto). The gRPC
package rename changes service paths, so old generated clients cannot call the
new server without regeneration.

## Preserved compatibility

- `TURBOVEC_*` environment variables remain fallback aliases. New names win
  when both are present.
- Existing configured `.tv` paths still load through `embedded-turbovec`.
- Snapshot recovery recognizes legacy `index.tv` and `index.tv.bm25` names;
  new generations use `vector.index`, optional `vectors.f32`, and
  `documents.bm25`.
- WAL manifests without generic provider fields are upgraded in memory from
  their legacy calibration fields. New manifests carry opaque provider state
  and retain the legacy embedded-adapter fields for older inspection tools.
- `GetCalibration`, `SetCalibration`, and `BroadcastCalibration` remain as
  compatibility adapters. New clients should use `GetVectorBackend`,
  `ConfigureVectorBackend`, and `BroadcastVectorBackend`.
- The current mapping fingerprint tag is `pipestream-search.plan.v3`.
  The foundations decoder changes require a source rebuild before binding
  v1/v2 mapped generations for new writes; the namespace reconciliation
  itself does not change the v3 algorithm. See [search foundations](search-foundations.md).

## Protocol namespace break

Search-owned protobuf packages moved from `ai.pipestream.search.*` to
`ai.protomolt.search.*`, including the public service, node API, mobile
bridge, WAL envelope, and storage contracts. This changes every gRPC full
method name and every `google.protobuf.Any` type URL that names one of these
messages. Regenerate every client binding from the current `search.proto`; an
old generated client cannot call the new server. There is no compatibility
alias for the retired protocol names.

Do not rewrite persisted user descriptor sets or retained original protobuf
sources to make this rename appear backward-compatible. They remain opaque
producer data. Review a generation's stored descriptors and Any payloads when
planning a cutover, and migrate only through a verified reader/rebuild path.

The main reconciliation also assigns `Bm25Hit.identity` field 7 and
`QueryHit.identity` field 12. Main retains `Bm25Hit.explain` at 6 and
`QueryHit.sort_values`/`explain` at 10/11. Clients generated from the earlier
foundations branch must regenerate too: their identity fields were 6 and 10.
Main's multi-key sort and cursor contracts replace the older single-key shapes.

## Operational consequence

No corpus rebuild is required solely for this refactor when the generation has
no Search-owned Any URLs or descriptor dependency that must be read by a
different client. The default adapter, TurboVec revision, encoded bytes, score
ordering, and mapping fingerprint algorithm are unchanged. A producer descriptor
that changes its imported type names is a schema change and can change its
mapping fingerprint. Deploying regenerated clients
is required. A rebuild is still required for an actual provider, scoring
fingerprint, analysis fingerprint, incompatible storage-format change, or a
verified descriptor/Any migration requirement.

The hosting-level rename is complete: the repository and canonical workspace
checkout are `protomolt-search`. The product, crate, and binary retain the
Pipestream Search identity; its protobuf protocol is now ProtoMolt-namespaced.
