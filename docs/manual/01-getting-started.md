# Getting started

## The pieces

A **node** owns one or more shards. A shard is a slice of the corpus: a vector
index, a BM25 postings index, a document store, and typed columns.

A **coordinator** takes client requests, fans them out to the nodes, merges what
comes back, and replies. Clients talk only to the coordinator.

```text
                 your client
                     |  gRPC (SearchService)
                     v
        +------------------------+
        |  coordinator           |  one HTTP/2 channel per node,
        |  (--role=coordinator)  |  merges shard answers into one
        +------------------------+  exact global answer
           |          |          |
           v          v          v
        +------+  +------+  +------+
        | node |  | node |  | node |   (--role=node, one listener
        |shard0|  |shard1|  |shardN|    per shard)
        +------+  +------+  +------+
```

*One binary, two jobs: the coordinator is the only address clients need; each
node serves the shards that hold a slice of the corpus.*

One binary serves both. `--role=node`, `--role=coordinator`, or `--role=both`.

A **collection** is a dataset. One cluster can serve several: each collection has
its own shard set, topology, statistics, vector configuration, and analysis
backend, and a node serves one collection (`--collection=NAME`). Every public
request has a `collection` field. An empty name selects the unnamed dataset, or
the configured default. An unknown name gives an error.

## Words used everywhere

These terms appear in every chapter:

- **floor** — a proven lower bound on the global k-th score. Shards publish
  their k-th best; the coordinator broadcasts the highest. Candidates below
  the floor cannot make the top-k and are skipped.
- **calibration** — the shared vector encoding parameters, fitted once and
  configured on every shard, which is what makes scores comparable across
  shards.
- **tombstone** — the delete marker for a row; deletes set a bit instead of
  rewriting files, and compaction reclaims the space.
- **seal** — to freeze the writable tail of a shard into a new immutable
  on-disk segment.
- **segment** — one immutable, checksummed piece of a persisted shard.
- **manifest** — the small file naming a generation's pieces and geometry
  (WAL `manifest.toml`, `snapshot-manifest.json`, a segment's `segment.json`).
- **lineage** — a document's relationships: `parent_id`, `group_id`, and its
  span inside its parent document.

## Run a node and a coordinator

```bash
cargo build --release

# one shard node
./target/release/pipestream-search --role=node \
    --index=/data/shard-0.tv --node-listen=0.0.0.0:50051 --slot-offset=0 \
    --analysis-addr=native --bm25-fields=body --facet-fields=court

# a coordinator over three nodes
./target/release/pipestream-search --role=coordinator \
    --coord-listen=0.0.0.0:50050 --nodes=node0:50051,node1:50051,node2:50051
```

Real deployments use a TOML file instead: `--config cluster.toml`, or
`PIPESTREAM_SEARCH_CONFIG`. Settings resolve in the order CLI flag, environment
variable, config file, built-in default. Each shard gets a `[[shards]]` block
with its `listen`, `index`, and `slot_offset`.

Text analysis is required. `--analysis-addr=native` uses the in-process
analyzer; an address points at the OpenNLP analysis sidecar, which adds
embeddings and model-backed analysis such as sentence detection and
named-entity recognition. With no analysis backend, ingest returns
UNAVAILABLE.

## Declare your columns first

Columns are declared per node at startup and cannot appear later by surprise:
`--bm25-fields`, `--facet-fields`, `--numeric-fields`, `--integer-fields`,
`--geo-fields`, `--map-facet-fields`, `--map-numeric-fields`. Ingesting a value
for a field no node declared is INVALID_ARGUMENT naming the field.

## Put documents in

Text goes in through `NodeService.AddDocuments`, one document per message on a
client stream. Each document has its text, its analysis options, optional
lineage, and its typed column values.

Vectors go in through `NodeService.AddVectors` in flat batches. Ids are assigned
by the server and positional: the i-th vector of a shard is `slot_offset + i`.
Document ids share that space.

For vectors the deployment order is fit, configure, ingest, search. Fit the
provider state on a sample, push it to every shard with
`SearchService.BroadcastVectorBackend`, then ingest. Shards that score under
different provider state produce scores that cannot be merged, so the engine
checks the fingerprints and rejects a mixed fleet before any shard is contacted.

`NodeService.Flush` writes the shard to disk. A restart with the same config
comes back with everything.

## First query

`SearchService.Query` is the public route. A minimal lexical request:

```bash
grpcurl -plaintext -d '{"k": 10, "selection": {"search": {"id": "lex",
  "lexical": {"text": "qualified immunity",
  "analysis": {"tokenizer": 1, "stemmer": 2, "termVectorSource": 3}}}}}' \
  localhost:50050 ai.protomolt.search.v1.SearchService/Query
```

The analysis options must match the options the documents were ingested with.
Term identity is the contract: a query analyzed one way against an index built
another way scores terms that do not exist and returns a confident empty list.
The engine cannot detect every case of this, so treat the analysis spec as part
of the index definition.

`SearchService.ClusterHealth` reports every shard's shape and reachability
without failing when a node is down.

Reference: `docs/query-api.md`.
