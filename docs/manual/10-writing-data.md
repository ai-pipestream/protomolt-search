# Writing data

## Ingesting documents

`NodeService.AddDocuments` is a client stream, one document per message. A
document has:

- `text`: non-empty, at most 1 MiB.
- `analysis`: the spec. Absent means sidecar defaults; the native analyzer
  requires an explicit spec.
- `lineage`: optional `parent_id`, `group_id`, and the chunk's span in the
  parent's text.
- `fields`: additional indexed fields, each with its own analysis.
- `facets`, `numerics`, `integers`, `timestamps`, `geo_points`, `map_facets`,
  `map_numerics`, the typed column values.
- `quality`, `geography`, `materialize`: the derive-at-ingest specs.
- `cased_field`: the field receiving the body's cased identity.

Every column value is checked before anything mutates. An unknown name is
rejected naming the column and the flag that declares it; a field valued twice
in one document is rejected; empty facet values, empty map keys, non-finite
numbers, `INT64_MIN`, and out-of-range coordinates are rejected.

A few fields are filled by the node from its own configuration before the
document is logged: the phrase postings, the position and bigram field lists,
the sentence field list, and the collection name. On replay, a record naming
fields the node is not configured with is rejected by name.

The response reports how many documents were added, the shard total, the global
id of the first, and the write-ahead log generation.

## Ingesting vectors

`NodeService.AddVectors` takes flat row-major batches. Ids are assigned by the
server and positional. Coordinates must be finite. Batches apply in stream order
under the shard's write lock, so no search observes a partly applied batch.

## Protobuf-native ingest

`NodeService.IngestMapped` takes your serialized protobuf messages as they
already are. The first message is a `MappedBind` with the descriptor set,
the message type, the plan fingerprint you reviewed, which text field is the
body, and the analysis spec. The node derives the plan again and rejects a
fingerprint mismatch naming both values. It also checks up front that the
shard declares every column the plan writes to, and names every gap in one
message.

Later messages are serialized documents. Each brings its vector, and the
document and vector append in lockstep under one lock. A document that fails
extraction fails the stream by position ("document 17: ..."), and no part of it
is applied.

The first bind pins the shard to the plan fingerprint, the body path, and the
materialize spec. A later bind must match. Changing the mapping is a
rebuild, not a rebind.

A chunked plan produces one row per chunk. Chunk-scope values land under their
plan names, and parent values denormalize onto every chunk row, so a filter reads
both with no query-time join. `IngestMappedResponse.added` counts rows and
`parents` counts source documents.

`SearchService.RoutedIngestMapped` is the product-level form: the coordinator
hashes each document's stable key into one topology generation and forwards one
ordinary stream per owning shard. The generation must be named explicitly; zero
is rejected, so an automatic retry cannot cross a reshard cutover.

- Under a placement tree (`docs/placement.md`) the coordinator first evaluates
  the tree over the document's own columns, first match per level and the
  level's default when no predicate is true, then hashes the key inside that
  leaf's shards. The shard fills the placement column from the leaf it is
  pinned to (`--placement-leaf`). A direct `AddDocuments` on a node with the
  column declared must carry the value or arrive at a pinned node; anything
  else is rejected naming the column and the flag.

## Deletes and replacements

No value mutates in place. `NodeService.DeleteDocuments` records idempotent
tombstones for global ids in one live-row overlay that every read path consults.
Term statistics subtract deleted postings and lengths, so distributed BM25 uses
statistics for the live corpus.

An update is append-then-retire: append the new document and its vector, then
call `NodeService.CommitReplacements` with the old and new ids. The node checks
both rows exist and the new one is live, then tombstones the old one under the
write lock. The appended row is queryable before the commit, so a client that
cannot show both must keep the new id out of its visible selection until the
retirement commits.

Both requests take an optional `expected_wal_generation`. Positional ids are
scoped to a generation, and a compaction or snapshot install renumbers rows, so
a claim naming another generation is rejected with FAILED_PRECONDITION instead
of applied to whatever rows now include those ids. Presence matters, not the
value: generation 0 is a generation.

## Compaction

`NodeService.CompactShard` removes tombstones while writes continue. It sets a
cutoff clock, builds a dense all-live image from the log, tails the live log into
a shadow shard through the same functions ingest uses, and cuts over holding the
write lock only for the last few records. It also writes a rewritten
full-history log generation, so the shard remains splittable and compactable
afterwards.

- `work_dir`: empty selects `<index>.compact`. It must be empty or absent and
  on the same filesystem as the index, because the finished generation is
  renamed and not copied. A leftover directory from a failed run rejects the
  retry by name.
- `tail_bound`: records the final locked apply may take; 0 selects 256.
- `dry_run`: the preflight and counts only, writing no files.
- `partition_column`: an integer column. The rebuilt shard is ordered by that
  column's value, cut into segments of at most `tail_bound` rows with ascending,
  disjoint value ranges, and rows without the column go to a final unkeyed
  segment. Each segment's range is recorded in its summary, so a filter on that
  column skips entire segments (chapter 3). Empty keeps the bucket layout. A
  double or facet column, or a column no document has, is rejected by name.
  Rows that arrive after the cutoff land in the new tail unordered; the next
  partitioned compaction takes them in.

It is unary and long-running, because it re-analyzes every live document. Set a
deadline in minutes.

It is rejected by name on an in-memory shard, a shard with no log, a log with
legacy unclocked records, a generation that began from a snapshot install (its
log does not hold that history), a compaction or bulk build already running, no
analysis backend, a non-empty work directory, or a work directory on another
filesystem.

Global ids change across a compaction. A cursor issued before the cutover is
rejected by name on resumption.

## The write-ahead log

Each persisted shard keeps `<index>.wal/gen-NNNNNN/`: a manifest, a set of
bucket files, and a marker file. Records route to buckets by
`fnv1a64(id) >> (64 - bucket_bits)`, the same partition function a split uses,
so a split into at most `--wal-buckets` children hands each child a contiguous
set of bucket files with no re-hashing. `--wal-buckets` defaults to 64, must be
a power of two, and is fixed when the log is created.

A record is applied first and logged immediately after, under one lock. `Flush`
is the durability point and fsyncs the log before writing the index images. A
crash can leave a torn tail frame; replay ignores it and the file continues.

Two rules follow:

- A shard without a log can serve, but can only be rebuilt, not split or
  merged. The log is the resharding input.
- The log stores raw float32 vectors inline, roughly the size of the raw
  embeddings. For a from-scratch bulk load, build the shard images offline and
  install them instead.

## Snapshots and bulk load

`NodeService.InstallSnapshot` streams a finished shard image to a node: build
once anywhere with the cluster's provider configuration, push it to every shard
owner, and each node stages it in a generation directory, validates it, and
swaps it in with one atomic rename. A crash mid-swap is recovered at startup.
An image with a provider identity other than the shard's is rejected, so scores
stay comparable cluster-wide.

`NodeService.ExportSnapshot` publishes a shard's generation into an empty
directory (a NAS path is the intended use) with a `snapshot-manifest.json`
naming the provider descriptor, slot offset, collection, row counts, analysis
fingerprints, the log cutoff the image contains, and every artifact's size and
SHA-256. The copy runs under the shard's read lock: queries proceed and writes
wait.

`NodeService.InstallSnapshotFrom` pulls such a repository from a `directory`, an
HTTP(S) `url` (with `Range` resume and an optional bearer token), or a
`peer_addr` (that peer's `StreamSnapshot`). Every artifact is verified against
the manifest's size and digest before the live shard is touched. A size or
digest mismatch, a manifest digest other than `expected_manifest_sha256`,
another shard's slot offset or collection, or a layout this shard cannot adopt
is rejected by name, and no artifact is installed. The manifest's log cutoff is
where replica catch-up resumes.

After any install the log rotates to a fresh generation with a snapshot marker,
recording the image as pre-existing state. Such a shard serves normally but
cannot be resharded from its log.

## Segment layout

A new persisted shard uses the segment layout by default (`--layout=segments`;
`--layout=single-image` keeps the one-file shape). A segment is four row-aligned
artifacts: the provider vector image, the exact FP32 rows, the BM25 image, and a
live-document bitmap, with a `segment.json` recording generation, row range,
counts, fingerprints, and the SHA-256 of every artifact. Opening a set verifies
every hash and count before it can serve.

Queries read every segment and the tail as one shard, with global statistics and
chained block-max cursors. `--seal-tail-docs` (default 500,000) seals a full
tail into a new immutable segment between documents; 0 seals on flush only.
Sealed vector images are memory-mapped by default (`--vector-mmap=false` loads
them into memory instead).

A shard has the layout its files have. No file converts on open, and a path
holding both a single image and a catalog is rejected by name.

## Collections

`[[collections]]` in the config file gives one cluster several datasets with no
shared state: each has its own shard set, topology, statistics, provider
configuration, analysis backend, and control plane. A node serves one collection
(`--collection=NAME`), checked at ingest, in the log manifest, and in health.

Names are printable ASCII, at most 128 bytes, with no whitespace, quotes,
colons, or slashes.

Resolution: with one unnamed dataset, an unnamed request is served and a named
one is rejected. With named collections and no `default_collection`, an unnamed
request is rejected naming the collections that exist. With a default, an
unnamed request gets the default. An unknown name is always rejected. An unnamed
request is not routed to a dataset it did not name.

Row counts are not summed across collections: `ClusterHealth` with no name on
a named set returns one entry per collection.

Reference: `docs/mutations.md`, `docs/snapshots.md`, `docs/collections.md`.
