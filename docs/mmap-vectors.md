# Mapped vector images and provider verification

Implemented on branch 2026-09-03. A sealed segment's vector image is served
from its file through a memory map, with the scores the heap path produces
bit for bit; and the coordinator refuses to score across a fleet whose
shards do not agree on one provider state, before any shard is asked.

## The engine patch

The mmap serving is the third patch on the TurboVec fork chain
(`turbovec-pipestream-s20`, `turbovec/src/mapped.rs`), on top of the seeded
search floor and the streaming collector. It changes nothing about the `.tv`
encoding: a v7 image stays the sync container upstream writes.
The unsafe `TurboQuantIndex::load_mapped(path)` maps the file, parses the superblock
and the two commit headers (the same parse `load` runs, factored so both
share it), and leaves the block units on their pages. A search assembles the
chunks it scans on demand: each chunk gathers its blocks' code bytes and
scales from their units (the partial last block from the commit header),
applies the redo ops the loaded header carries — exactly as the loader does
— and runs the same stored-to-native layout transform, so the kernel reads
the same bytes it reads from a loaded index. Each chunk's ordered candidates
are merged into the ordered global result window in linear time with one
reused scratch buffer; the mapped path does not re-sort up to k candidates
after each chunk, because k=10,000 or more is a normal retrieval shape. The
assembled chunks live in a bounded least-recently-used cache (64 MiB by
default), so resident memory stays at the budget plus whatever the page cache
keeps, never the image.
Top-k over a mapped image is the streaming scan's chunk loop with a global
heap per query, the batch's current k-th best seeded as the floor of the
next chunk (a true lower bound, so pruning is exact and ties at the floor
survive), and the kernel's own ordering rule. The constructor requires the
backing storage to remain unchanged and untruncated for the mapping's lifetime;
the product discharges that requirement only at its sealed-segment boundary.
A mapped index is read-only:
`add`, `swap_remove`, `calibrate`, and `sync` refuse by name; `write` and
`to_bytes` materialize the layout in memory first, on request. A v5 or v6
file is refused with the conversion advice `load` gives: converting a file
forward is a file operation, not a corpus rebuild.

Since upstream 1.1 the fork's `main` is one patch commit on upstream
`main`, and an index of 32,768 vectors or more whose host kernels read the
bit-plane layout (aarch64, or x86 with AVX-512 VBMI + VNNI) is written as a
v8 image with bit-plane units. `load_mapped` serves those too: the chunk
assembler converts each unit's canonical, host-independent planes back to
sequential-blocked bytes (the converter's route) before the shared
transform, and the mapped scan keeps the classic kernels
(ai-pipestream/turbovec#8). Before that change such an image was refused,
so a large sealed segment written on those hosts could not open mapped.
`vector::tests::a_sealed_image_past_the_planes_threshold_serves_mapped`
pins it here at 2 and 4 bits.

The fork's `tests/mapped_image.rs` pins mapped against loaded results bit
for bit (top-k, oversized k, batched queries, masks, seeded floors, streaming), a synced
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
sidecar was already mapped and is untouched when serving. While a
persisted shard ingests, the sidecar builds on disk beside its final
path (`<sidecar>.building`): every appended row goes to that file, reads
serve rows by offset, the payload digest runs as rows arrive, and a flush
finalizes it in place (one header write, one rename), so an ingest of any
length holds none of its FP32 rows in heap. An in-memory shard keeps a
heap builder.

**Exactness since turbovec 1.1.** Upstream searches an image whose cache is
in the bit-plane layout (32,768 rows or more on those hosts, served from
memory: the tail, a single-image shard, or any sealed image under
`--vector-mmap=false`) in stages: a sign-plane shortlist, a ranking on the
lower planes, and an exact rescore of the best. Scores are exact for the ids
returned, but the id set is approximate, and a single query's shortlist
differs between one thread and a pool. The embedded provider advertises
`ExhaustiveQuantized`, so every top-k it runs sets the fork's
`SearchOptions::exhaustive` (ai-pipestream/turbovec#8): such an image is
scanned chunk by chunk with every row exactly rescored, bit for bit the
whole-index scan on any thread count. Mapped images and the streaming
collector were exhaustive already. Using the staged search for an
approximate (`ConfiguredAnn`) route is tracked in #68.

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
