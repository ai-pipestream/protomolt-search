# Pipestream Search: the user manual

Pipestream Search is a distributed search engine for text and vectors. You hand
it documents, their embeddings, and their typed values, spread across as many
machines as you like, and it answers keyword queries, vector queries, hybrid
queries, filtered browses, and exact aggregations over the entire set. Clients
speak gRPC to a coordinator; the coordinator fans each request out to the shard
nodes and merges what comes back.

It is built for corpora that do not fit on one machine and for teams who have to
defend a ranking. If you have had to explain why a result moved, or why a
document you expected did not appear, this engine is shaped around answering
that.

## What is different about it

**The distributed answer is the single-machine answer.** Splitting a corpus
across 8 shards does not change which documents come back or in what order. That
is the property everything else is built on.

**The vector scan looks at every row.** Vectors are stored at 4 bits per
dimension and scanned in full, so a dense query returns the true top-k instead
of an approximation with a recall figure attached. When you want the original
float precision on top, an exact rerank over the candidate pool is one flag, and
the depth that meets a recall target comes from a profile built by measurement,
and not from a rule of thumb.

**Shards share score cutoffs.** As soon as any shard has k candidates, its
k-th best is a lower bound on the global k-th best. The coordinator collects
those bounds, takes the highest, and pushes it back to every shard. A lexical
shard then skips index blocks whose best possible score is under the cutoff,
and a vector shard drops candidates under it before they cost heap work or a
trip to the coordinator. The vector scan still visits its rows in full, which
is what lets a shard certify that its part of the answer is complete. No
correct hit is discarded: this is pruning, not sampling.

**BM25 scores over global statistics.** Before scoring, the coordinator collects
per-term document frequencies and corpus totals from every shard and sends the
sums back down, so each shard scores with identical idf and average length.
Shard-local statistics would make scores incomparable and the merged ranking
silently wrong.

**Filters are masks over the scan, not a pass afterwards.** A filter is written
in CEL, compiled once at the coordinator, and resolved per shard into slot
allowlists and column predicates that gate scoring. Because a filter only
removes documents, no pruning arithmetic changes, and a shard's cutoff becomes
the filtered cutoff, which is higher, so the fleet prunes sooner.

**Aggregations are exact.** Sums, means, variances, distinct counts, histograms,
and percentiles aggregate over the entire match set, merged in shard order, so
the same request answers the same bits every run. Percentiles are real order statistics
found by a bounded binary search, not a sketch.

**An explain tree that adds up.** Turn on `explain` and every hit includes the
arithmetic that produced its score, term by term and stage by stage, with the
top node's value equal to the score that was served. No score is recomputed to
build it; the numbers are the ones the engine already computed.

**Everything rejects by name.** A misspelled column, an unsupported query shape,
a query analyzed differently from the index, a fleet scoring in two different
spaces, a phrase on a field with no positions, a percentile no measurement
covered: each of these gives you an error that states what was wrong, and often
what would fix it. The engine does not substitute something that runs. An empty
result means no document in the corpus matches, and you can rely on that.

## The chapters

1. [Getting started](01-getting-started.md): run a node and a coordinator,
   declare columns, ingest documents and vectors, issue your first query, and
   what a collection is.
2. [The query request](02-the-query-request.md): the selection shapes (lexical
   leaf, dense leaf, hybrid composites and their fusion modes, the recursive
   boolean tree, the browse), `k` and `selection_k`, cursor paging, and the
   streaming query.
3. [Filters](03-filters.md): the CEL surface, geo predicates, how a filter
   applies to the vector branch, three-valued absence, and the full list of what is
   rejected.
4. [Sorting and collapse](04-sorting-and-collapse.md): multi-key ordering over
   columns and lineage keys, and one representative per group with inner hits.
5. [Facets, range facets, and
   aggregations](05-facets-range-facets-and-aggregations.md): counts over a
   match set, the exact aggregates, group-by, histograms including calendar
   ones, percentiles, cardinality, and aggregating a query's own pool.
6. [Explain and profile](06-explain-and-profile.md): the score tree per hit and
   the per-phase timings, both of which leave results unchanged.
7. [Text features](07-text-features.md): analysis and the sidecar, dual-cased
   terms, phrases and proximity, prefix terms and string ranges, highlighting,
   autocomplete, synonyms and did-you-mean, and searching several fields at once.
8. [Relevance tuning](08-relevance-tuning.md): score functions, boosts, the
   composite scorer and its named signals, A/B variants and interleaving, and
   the dense execution policy and quality profile.
9. [Columns and mappings](09-columns-and-mappings.md): the facet, numeric,
   integer, map, geo, and quality columns, plans derived from a protobuf
   descriptor, and projections and materialized columns.
10. [Writing data](10-writing-data.md): ingest, mapped ingest, deletes and
    replacements, compaction, the write-ahead log, snapshots and bulk load, the
    segment layout, and collections.
11. [Operating a cluster](11-operating-a-cluster.md): nodes and the
    coordinator, replicas and hedging, the shard map and topology generations,
    splits and resharding, cluster control, metrics, and the work queue.
12. [Security](12-security.md): TLS, mutual TLS, bearer principals and their
    quotas, signed datagrams, and what is out of scope.
13. [Embedded and mobile use](13-embedded-and-mobile.md): the same engine
    linked into one process, with no socket bound or dialed, on desktop,
    Android, and iOS.

## Where the contract is stored

The wire contract is `proto/ai/pipestream/search/v1/search.proto`, and its field
comments are the authority when this manual and the code differ. Generate
client stubs from it; the service is
`ai.pipestream.search.v1.SearchService`.
