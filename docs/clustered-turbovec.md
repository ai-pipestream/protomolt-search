# Clustered TurboVec backend

## Boundary

Pipestream Search owns document ids, BM25, CEL, columns, hybrid behavior, and
the public result order. `turbovec-grpc` owns one distributed TurboVec
collection: calibration agreement, vector shard topology, the exact global
heap, live floors, completion, and encoded row movement.

The product calls that collection once. It never expands the vector node table
into its own fan-out or merges a second set of local vector heaps.

```text
Search API
   |
   +-- BM25, filters, documents --> Pipestream Search shard nodes
   |
   `-- vector query --> clustered TurboVec adapter
                           |
                           +-- in-process CoordinatorService (default)
                           |       `-- gRPC --> TurboVec shard nodes
                           |
                           `-- tonic client --> standalone coordinator
```

The in-process transport invokes the generated coordinator trait on the
library service directly. It performs no localhost connection and no protobuf
serialization. The external transport sends the same request to the same
service implementation over tonic.

## Identity and exactness

Every vector row must carry a persisted `turbovec-grpc` label equal to its
Pipestream Search document id. Labels survive vector Split and Join even when
slots and vector shard ownership change.

Every product request sets stable-label ordering. The public total order is:

1. provider score descending;
2. stable vector id ascending.

Filters remain product-owned. Product shard nodes resolve CEL and geo filters
against their columns into packed stable-id bitmap ranges. `None` means no
provider filter; an explicitly present empty bitmap set means match nothing.
Small candidate-scoped rescoring sets use explicit labels instead. The
collection maps either representation to each vector shard's positional mask
before its scan. Product and vector shard boundaries do not need to match.
Unknown filter columns still follow the product's existing fail-loud typo rule.

The collection returns a result only after every vector shard reports
`completed=true`. It refuses an unlabelled shard whenever stable ordering or
filtering is requested. An optional initial floor is inclusive. A tie-complete
request retains every row at the final k-th score.

`tests/clustered_turbovec.rs` builds one calibrated corpus as:

- a monolithic embedded `VectorIndex`;
- a three-shard in-process `turbovec-grpc` collection;
- the same collection behind a standalone tonic server.

It requires identical ids, score bits, and order for batches, explicit-label
and packed-bitmap filters, empty filters, and all-score-tied input. Product and
vector shard cuts deliberately differ. It also drives the public Pipestream
Search `Search` handler through both transports and checks health.

## Configuration

Exactly one transport may be configured.

For an in-process coordinator:

```toml
[clustered_turbovec]
nodes = [
  "vector-a:50051 stable-shard-a 14",
  "vector-b:50051 stable-shard-b 11",
]
state = "/var/lib/pipestream-search/turbovec-topology.json"
```

Node-table entries use `turbovec-grpc` syntax:

```text
primary|replica1|replica2  stable-index-id  durable-generation
```

Durable state is required. Tests and disposable demos may instead set
`allow_ephemeral = true`.

For a standalone coordinator:

```toml
[clustered_turbovec]
coordinator = "http://vector-coordinator:50050"
```

Equivalent CLI options are `--turbovec-cluster-nodes`,
`--turbovec-cluster-state`, `--allow-ephemeral-turbovec-cluster`, and
`--turbovec-coordinator`.

## Current capability boundary

Exact vector `Search`, filtered vector `Search`, public dense selections, and
candidate-scoped dense boost rescoring use the clustered backend. Parent
collapse and hybrid fusion need a provider-to-product candidate stream so the
product can own parent and cross-leg bounds. They return `UNIMPLEMENTED`
instead of silently falling back to vectors stored on product shard nodes.

Cluster construction, append, persistence, Split, and Join stay on the
`turbovec-grpc` admin surface. This increment does not dual-write vector rows
from Pipestream Search ingest. A generation is deployable only when its stable
labels match the product document ids.
