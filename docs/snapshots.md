# Snapshot repositories

Landed 2026-09-04. A node can publish its shard as a directory of hashed
artifacts and pull such a directory back from a NAS path, an HTTP(S)
server, or another node — without an operator holding the files.
`InstallSnapshot` (the client stream, `README.md` "Bulk load") is
unchanged; this is the second way an image reaches a shard, and it ends
in the same install.

## The repository

`NodeService.ExportSnapshot(directory)` flushes the shard and copies its
current generation into `directory`, which must be empty (a non-empty
one is refused naming its entry count). What lands there depends on the
shard's layout (`docs/immutable-segments.md`):

```
single-image                        segments
  vector.index                        catalog/segments.json
  vectors.f32          (when kept)    catalog/segments/<id>/segment.json
  documents.bm25       (when kept)    catalog/segments/<id>/vector.index
  live-docs.bin        (when kept)    catalog/segments/<id>/...
  snapshot-manifest.json              vectors.f32, live-docs.bin, vector.index (when kept)
                                      snapshot-manifest.json
```

A single-image repository is exactly a staging generation directory:
the names are the ones `InstallSnapshot` writes. A segment repository
is the catalog tree under `catalog/` plus the shard-level sidecars the
node keeps beside the catalog (the serving live bitmap and exact-vector
copy; a plain vector image when the shard had one before its first
document).

`snapshot-manifest.json` (`snapshot_repository::RepositoryManifest`,
format 1) records:

- `layout`, the provider descriptor (`backend_kind`,
  `scoring_fingerprint`, `dim`), `slot_offset`, `collection`;
- `vector_rows`, `document_rows`, `live_rows`, and the per-field
  `analysis_fingerprints`;
- the WAL cutoff the artifacts contain — `wal_generation`,
  `wal_high_watermark`, `wal_clocked` — every record at or before that
  clock is in the image, none after it;
- `artifacts`: each file's repository-relative path, byte size, and
  lower-case hex SHA-256.

The file is written last, with fsync, and the export response reports
its own SHA-256 (`manifest_sha256`) — the value a later install can pin.
The manifest encodes deterministically (fixed field order, one trailing
newline), so a manifest that reaches a node as a protobuf frame hashes
to the same digest as the file it was written from.

**The copy holds the seal mutex and shard state read lock.** Queries proceed;
mutation and catalog publication wait. After acquiring both locks, export
checks an internal flag proving the current state was flushed and, when a WAL
exists, that it is clean. A write between flush and copy or a fresh unsealed
tail triggers another flush, up to eight attempts, then `ABORTED`. The same
check protects shards without WAL; their cutoff is zero with
`wal_clocked=false`. `copy_millis` includes lock acquisition and any additional flush retries.
Budget one read plus one write of the generation at the disk's copy rate.

A bound, zero-row generation can export and install before any provider image
exists. Its identity travels in an empty BM25 image or a format 2 segment
catalog. See [empty generation bindings](empty-generation-binding.md) for
validation, recovery and downgrade restrictions. Segment installs also check
the whole-shard FP32 shape against provider images before replacing live data.

A NAS path is the intended repository: export to it from the primary,
install from it on every replica, keep it as the base image a
`replication::sync_once` catch-up resumes from. Nothing in the format
is specific to a filesystem, which is what the HTTP source is for.

## Installing from a repository

`NodeService.InstallSnapshotFrom(source, expected_manifest_sha256?,
bearer_token?)` takes one of three sources:

| source | what the node does |
|---|---|
| `directory` | reads the manifest and copies every artifact into its staging generation directory (`<index>.snap-tmp/`) |
| `url` | GETs `<url>/snapshot-manifest.json`, then each artifact by `GET <url>/<file>`; an artifact whose transfer breaks resumes with `Range: bytes=<staged>-` (206 with a matching `Content-Range`; a 200 restarts it) up to eight attempts; `bearer_token` goes out as `Authorization: Bearer` on every request |
| `peer_addr` | opens the peer's `StreamSnapshot` and writes the frames to staging: the manifest first, then each artifact's bytes in manifest order, counted against the manifest |

Then, before the live shard is touched: every staged artifact is hashed
and compared to the manifest's size and SHA-256 (the first mismatch is
refused naming the artifact and both values); the manifest's digest is
compared to `expected_manifest_sha256` when one was given; the
manifest's `slot_offset` and `collection` must be this shard's (a
repository image describes one shard; the client stream stays the way
to place a prebuilt image at any offset); and the layout must be one
the shard can adopt. What passes goes through the same install as the
client stream: the single-image path is `apply_snapshot` itself, with
its provider-identity checks (a foreign backend kind or another scoring
fingerprint refuses `FAILED_PRECONDITION` naming both, calibration
included) and its atomic generation rename. The response carries the
manifest, so the caller knows the WAL cutoff the image contains.

The segment layout installs through its own swap: the staged catalog is
opened and validated (the catalog verifies its own per-segment hashes
and fingerprints), the provider identity is checked the same way, then
under the write lock the live catalog is renamed to
`<index>.segments.snap-old`, the staged one into `<index>.segments/`,
the shard-level sidecars are moved after it, and the shard reopens over
the new files exactly as it does at start. A crash between the two
catalog renames is recovered at the next open (`recover_segments_swap`:
old-without-live renames back, both-present deletes the old); a crash
between the catalog rename and the sidecar moves leaves sidecars that
disagree with the catalog, which the next open refuses by name rather
than serving.

Layouts do not mix: a segment repository refuses a `--layout=single-image`
shard or one serving an installed generation, and a single-image
repository refuses a segment shard that holds rows (an empty one adopts
the single-image layout, the same way an `InstallSnapshot` stream does).
After any install the WAL rotates to a fresh generation with a snapshot
marker, as it always has, so the installed image is recorded as
preexisting state the log does not contain (`docs/resharding.md`).

### `StreamSnapshot`

`NodeService.StreamSnapshot` is the peer side of `peer_addr`: the node
exports into a private staging directory (`<index>.snap-export-<pid>-<n>/`)
under its read lock exactly as `ExportSnapshot` does, releases the lock,
streams the manifest frame and then the artifacts in 1 MiB chunks, and
deletes the staging directory when the stream ends (stray ones are
removed at the next open). The lock is held for the local copy, never
for the network transfer; the price is one transient extra copy of the
generation on the exporting node's disk.

### Catch-up from the cutoff

The manifest's cutoff is exactly where replication resumes: a replica
installed from the repository and then driven by
`replication::sync_once` with a cursor of `{wal_generation,
clock: wal_high_watermark}` receives the primary's tail after the image
and nothing before it (a replay of the image's own records would refuse
as a vector gap or a partial batch, by the existing positional-tip
rule). `docs/cluster-control.md` "Replica bootstrap" is that sequence
run by a node worker.

## HTTP(S) client

The `url` source is hyper's HTTP/1.1 client over hyper-util's connector,
the client tonic already links; `http-body-util` streams the body to
disk frame by frame. HTTPS goes through `hyper-rustls` over rustls 0.23
with `ring`, the provider tonic's `tls` feature already puts in the
tree — there is no second TLS stack. The trust store is the public web
roots (`webpki-roots`) plus the cluster CA installed with `--tls-ca`
(`docs/security.md`), parsed with `rustls-pki-types`, so a repository
behind an internal certificate is reachable without pointing the node
at a public CA. The client presents no certificate; the bearer is the
credential. Every one of these dependencies sits behind `net` (hyper,
http-body-util) or `tls` (hyper-rustls, webpki-roots, rustls,
rustls-pki-types); the embedded crate links none of them
(`tests/security.rs` gates the tree). A build without `tls` refuses an
https URL by name; a build without `net` refuses `url` and `peer_addr`
and still installs from a directory.

Refusals name their cause: HTTP 401/403 as `UNAUTHENTICATED` ("bearer_token
missing or wrong?"), 404 as `NOT_FOUND`, any other status as
`UNAVAILABLE` with the status, a scheme other than http/https as
`INVALID_ARGUMENT`, a TLS failure with the TLS error text and no
status, an artifact that never completes with the attempt count and the
last error. Nothing is installed on any refusal, and the staging
directory is removed.

## Costs

- Disk: an export is one extra copy of the generation at the
  repository; a `StreamSnapshot` is one transient copy on the exporting
  node; an install stages one copy before the rename (the client stream
  always did). None of it grows postings or resident memory.
- Locks: the copy holds the read lock (writes wait, queries do not);
  the segment swap and the generation rename hold the write lock for
  the renames and the reopen only.
- Hashing: every artifact is hashed once on export (during the copy)
  and once on install (before the swap), with the crate's own SHA-256.

## Tests

`tests/snapshot_repository.rs`: the export's manifest matches the files
and the response's digest, and a non-empty directory or a shard without
a path refuses; an install from a directory equals the source bitwise
(vector Search, BM25 with facets and a filter, health counts), survives
a reopen from disk, and the refusal table — a flipped byte names the
artifact and its digest, a truncated one its size, the wrong manifest
digest, a missing repository, an empty request, another shard's slot
offset — leaves the target empty with no staging behind; a peer install
equals and leaves no export staging on the peer; a URL install over a
hand-rolled HTTP/1.1 file server equals, resumes with a `Range` request
after the server drops the first artifact at 64 KiB, and refuses a
missing bearer (401), a missing artifact (404), and an `ftp` URL; the
segment layout exports, installs, reopens, installs from a peer, and
refuses both layout mixes; a shard seeded with another calibration
refuses the image from every source naming both fingerprints; and
`sync_once` from the manifest's cutoff catches a replica up to a primary
that kept ingesting, idempotently. `tests/snapshot_https.rs` serves the
same file server under TLS from the test certificates: the install
completes and resumes under the cluster CA, a host the certificate does
not name fails the handshake without an HTTP status, and the wrong
bearer is the same 401. `src/snapshot.rs` unit-tests the request
builder and the HTTPS connector; `src/snapshot_repository.rs` the
manifest encoding, path validation, and verification.
