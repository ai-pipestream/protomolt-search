# Pipestream Search naming and provider migration

The search product is now backend-neutral and is named **Pipestream Search**.
This is an API and packaging migration, not a vector-format migration.

## New identities

| Surface | Current identity |
|---|---|
| Cargo package and server binary | `pipestream-search` |
| Rust crate | `pipestream_search` |
| gRPC package | `ai.pipestream.search.v1` |
| WAL protobuf package | `ai.pipestream.search.wal.v1` |
| Environment prefix | `PIPESTREAM_SEARCH_*` |
| Default vector provider | `embedded-turbovec` |
| Repository and local checkout | `protomolt-search` |

Regenerate clients from
[`search.proto`](../proto/ai/pipestream/search/v1/search.proto). The gRPC
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
- The persisted mapping fingerprint tag remains `turbovec-search.plan.v1`.
  It is a frozen format identifier, not current branding, and retaining it
  avoids invalidating mapped generations whose canonical plan did not change.

## Operational consequence

No corpus rebuild is required solely for this refactor: the default adapter,
TurboVec revision, encoded bytes, score ordering, and mapping fingerprint are
unchanged. Deploying the renamed binary and regenerated clients is required.
A rebuild is still required for an actual provider, scoring fingerprint,
analysis fingerprint, or incompatible storage-format change.

The hosting-level rename is complete: the repository and canonical workspace
checkout are `protomolt-search`. The product, crate, binary, and protocol retain
the Pipestream Search identities above.
