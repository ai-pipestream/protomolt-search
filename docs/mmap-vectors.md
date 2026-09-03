# Mapped vector images and provider verification

Implemented on branch 2026-09-03. A sealed segment's vector image is served
from its file through a memory map, with the scores the heap path produces
bit for bit; and the coordinator refuses to score across a fleet whose
shards do not agree on one provider state, before any shard is asked.

## The engine patch

The mmap serving is the third patch on the TurboVec fork chain
(`turbovec-pipestream-s18`, `turbovec/src/mapped.rs`), on top of the seeded
search floor and the streaming collector. It changes nothing about the `.tv`
encoding: a v7 image stays the sync container upstream writes.
`TurboQuantIndex::load_mapped(path)` maps the file, parses the superblock
and the two commit headers (the same parse `load` runs, factored so both
share it), and leaves the block units on their pages. A search assembles the
chunks it scans on demand: each chunk gathers its blocks' code bytes and
scales from their units (the partial last block from the commit header),
applies the redo ops the loaded header carries — exactly as the loader does
— and runs the same stored-to-native layout transform, so the kernel reads
the same bytes it reads from a loaded index. The assembled chunks live in a
bounded least-recently-used cache (64 MiB by default), so resident memory
stays at the budget plus whatever the page cache keeps, never the image.
Top-k over a mapped image is the streaming scan's chunk loop with a global
heap per query, the batch's current k-th best seeded as the floor of the
next chunk (a true lower bound, so pruning is exact and ties at the floor
survive), and the kernel's own ordering rule. A mapped index is read-only:
`add`, `swap_remove`, `calibrate`, and `sync` refuse by name; `write` and
`to_bytes` materialize the layout in memory first, on request. A v5 or v6
file is refused with the conversion advice `load` gives: converting a file
forward is a file operation, not a corpus rebuild.

The fork's `tests/mapped_image.rs` pins mapped against loaded results bit
for bit (top-k, batched queries, masks, seeded floors, streaming), a synced
file with pending removal ops and a partial tail block, the read-only
refusals, `write` reproducing the loaded bytes, resident memory at open (a
20 MiB image maps for under an eighth of its size where a load costs the
image), and the legacy refusal. `cargo test -p turbovec --locked` on the
chain branch: 43 binaries, 512 tests, all green.

## In the product

`VectorIndex::load_mapped` opens an image mapped; `VectorIndex::is_mapped`
says so. A segment catalog opens its sealed images mapped by default
(`segments::VectorLoad::Mapped`); the tail and single-image shards stay
owned, because they take writes. `--vector-mmap=false` (config
`vector_mmap`) loads sealed images into memory instead. The exact FP32
sidecar was already mapped (`docs/exact-vectors.md`) and is untouched.

`tests/segment_layout.rs` pins a segmented shard served mapped against the
same shard served from memory (Search and Query hits and scores, bit for
bit), the mapped state of the opened set, and resident memory at open for
a sealed image large enough to measure.

## Provider verification

A fleet scores in one space or not at all. `fleet_vector_identity` asks
every shard for its `GetVectorBackend` descriptor and refuses, naming both
shards and both states, when provider kinds, scoring fingerprints (the
calibration is part of the fingerprint), or dimensions differ:

- `Search` runs it before any shard is asked (`Query`'s dense route already
  did, in its execution preflight); a mixed fleet refuses with
  `FAILED_PRECONDITION` before a hit is produced.
- Routed ingest runs it at bind time, with unconfigured shards tolerated:
  a fleet that already scores in two spaces takes no rows.
- `ClusterHealth` reports `provider_mismatch`: every distinct kind and
  fingerprint pair with the shards that serve it, empty when the fleet
  agrees.
- A snapshot install compares the image's descriptor with the shard's
  serving provider (kind from the live index or the WAL's locked backend,
  fingerprint from the live index) and refuses a foreign one by name; a
  seeded shard's calibration check stays as it was.

`tests/vector_backend.rs` pins the Search refusal, the health report, and
the ingest refusal on a two-shard fleet calibrated two ways;
`tests/snapshot.rs` pins the snapshot refusal.
