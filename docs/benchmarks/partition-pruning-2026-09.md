# Partitioned layout and segment pruning: one local shard

Machine: this workstation (32 cores, 121 GB), one segment-layout node and an in-process coordinator on loopback, native analysis. Rows: 2000000. Vector dimension: 128 at 4 bits. Seal bound: 125000 rows. Queries per case: 20 (p50 and p90 of the coordinator wall, after two warm-ups). Command: `./target/release/examples/partition_bench`.

Ingest: 60.8 s. Compaction to the partitioned layout: 271.1 s (16 sealed segments before, 20 after: 20 keyed and ascending by `year`, 0 unkeyed).

### Bucket layout (rows in arrival order)

| case | segments skipped / total | p50 on (ms) | p90 on (ms) | p50 off (ms) | p90 off (ms) |
|---|---:|---:|---:|---:|---:|
| dense k=10, no filter | 0 / 16 | 410.6 | 418.5 | 430.3 | 454.1 |
| dense k=10, year >= 2018 (5%) | 0 / 16 | 430.2 | 437.8 | 449.2 | 467.9 |
| dense k=10, year >= 2010 (25%) | 0 / 16 | 442.2 | 452.1 | 448.0 | 458.8 |
| dense k=10, year >= 2000 (50%) | 0 / 16 | 445.5 | 452.7 | 461.9 | 468.6 |
| BM25 common term, year >= 2018 (5%) | 0 / 16 | 0.4 | 0.4 | 0.4 | 0.4 |
| BM25 common term, year >= 2010 (25%) | 0 / 16 | 0.2 | 0.2 | 0.1 | 0.1 |
| BM25 common term, year >= 2000 (50%) | 0 / 16 | 0.1 | 0.1 | 0.1 | 0.1 |
| boolean AND(rare term, dense) | 0 / 16 | 143.3 | 144.5 | 145.5 | 146.6 |
| boolean AND(common term, dense) | 0 / 16 | 2103.0 | 2189.5 | 2136.2 | 2292.5 |
| browse year >= 2018, sorted by year | 0 / 16 | 25.2 | 27.8 | 23.9 | 24.2 |
| aggregation count, year >= 2018 | 0 / 16 | 23.1 | 23.3 | 21.9 | 22.1 |

### Partitioned layout (rows ordered by `year`)

| case | segments skipped / total | p50 on (ms) | p90 on (ms) | p50 off (ms) | p90 off (ms) |
|---|---:|---:|---:|---:|---:|
| dense k=10, no filter | 0 / 20 | 400.9 | 404.1 | 415.1 | 428.6 |
| dense k=10, year >= 2018 (5%) | 19 / 20 | 22.8 | 23.0 | 45.1 | 48.7 |
| dense k=10, year >= 2010 (25%) | 15 / 20 | 108.5 | 109.8 | 130.0 | 137.3 |
| dense k=10, year >= 2000 (50%) | 10 / 20 | 212.7 | 214.3 | 234.6 | 248.6 |
| BM25 common term, year >= 2018 (5%) | 19 / 20 | 0.1 | 0.1 | 31.2 | 32.4 |
| BM25 common term, year >= 2010 (25%) | 15 / 20 | 0.1 | 0.1 | 23.8 | 24.1 |
| BM25 common term, year >= 2000 (50%) | 10 / 20 | 0.1 | 0.1 | 15.9 | 16.8 |
| boolean AND(rare term, dense) | 0 / 20 | 150.6 | 160.2 | 150.6 | 157.0 |
| boolean AND(common term, dense) | 0 / 20 | 2083.8 | 2281.5 | 2075.6 | 2089.9 |
| browse year >= 2018, sorted by year | 19 / 20 | 4.1 | 5.3 | 22.6 | 22.9 |
| aggregation count, year >= 2018 | 19 / 20 | 3.2 | 3.4 | 41.0 | 41.3 |

Equality: with pruning on and off, the hits, score bits, order, and counts are identical on both layouts. Across the compaction, which renumbers positional ids, the sorted score bits and the counts are identical on every query, and the hit identities are the same set above the tie boundary; 80 of 220 query answers had a tie at the k-th score whose members the id order picks differently.

What the numbers show: on the bucket layout every segment holds every year, so a `year` predicate rules no segment out and pruning changes no time. On the partitioned layout the same predicate rules out the segments whose range sits below it, and the filtered dense, lexical, browse, and aggregation cases skip them without opening them; the unfiltered dense scan and the keyword-gated boolean cases read what they always read, since the vector kernel still visits every row of a segment it opens and the boolean planner scores per surviving id.
## Reading the numbers

- **The filtered dense scan is where the layout pays.** At 5% selectivity the
  same query takes 430 ms on the bucket layout and 23 ms on the partitioned
  one with pruning on: 19 of 20 segments are ruled out from their summaries
  and never opened. With pruning off the partitioned layout still answers in
  45 ms, because the allowlist over an ordered shard leaves whole scan chunks
  empty and the scan already skips those; pruning removes the remaining cost
  of building that allowlist row by row and of opening the images.
- **Locality without pruning can hurt a postings walk.** The filtered BM25
  cases on the partitioned layout with pruning off (16 to 31 ms) are slower
  than on the bucket layout (under 0.5 ms): the survivors of `year >= 2018`
  sit in the last segments, so the block-max cursor walks most of the common
  term's postings through masked-out documents before its heap fills. With
  pruning on those segments are never walked and the case is back under a
  millisecond. Order the rows and prune from the summaries together.
- **Browse and aggregation under the same filter** drop from about 23 ms to
  3 to 4 ms; these are slot loops over the admitted segments only.
- **The unfiltered dense scan is unchanged** (about 400 ms for 2,000,000 rows
  on one node), as the layout promises: a scan that opens a segment reads
  every row of it.
- **The boolean keyword-gated cases do not move,** by design: the boolean
  planner resolves the lexical membership as a bitmap and scores the dense
  clause per surviving id, without a scan, so there is no segment to skip.
  The common-term case (about 30% of the rows) costs about 2.1 s on either
  layout, an order of magnitude above the masked scan the composite `AND`
  runs for a filter of the same size; that per-id scoring path is the next
  thing to measure on its own.
- **Compaction cost.** 271 s for 2,000,000 rows on one node: the ordered
  build makes two passes over the write-ahead log plus one over the spill
  logs, then seals 20 segments. Rows with equal keys move to the next
  partition as a unit, which is why 40 years at 50,000 rows each cut into 20
  segments of two years rather than 16 of the bound.

## 2026-09-05, evening: the boolean cases after the candidate scorer fix

Same machine, same corpus and command (2,000,000 rows, dimension 128 at 4
bits, seal bound 125,000, 20 queries per case), main with the linear BM25
candidate scorer, the `signal_batch` knob, and the dense clause's
membership as the universe (`docs/query-api.md`, "Recursive boolean
execution"). Ingest 60.8 s, compaction 288.7 s. The other rows did not move
and are not repeated.

| case | layout | before p50 (ms) | after p50 (ms) | after p90 (ms) |
|---|---|---:|---:|---:|
| boolean AND(rare term, dense) | bucket | 143.3 | 18.9 | 19.6 |
| boolean AND(rare term, dense) | partitioned | 150.6 | 18.8 | 19.1 |
| boolean AND(common term, dense) | bucket | 2103.0 | 1334.2 | 1361.9 |
| boolean AND(common term, dense) | partitioned | 2083.8 | 1270.4 | 1290.4 |

Survivors sent in pieces of 10,000 ids per rescore call against one call
per shard, partitioned layout:

| case | pieces of 10,000 p50 (ms) | one call p50 (ms) |
|---|---:|---:|
| boolean AND(rare term, dense) | 18.6 | 18.8 |
| boolean AND(common term, dense) | 1199.1 | 1270.4 |

What moved, measured with phase traces on a 400,000-row run of the same
shape before the numbers above:

- **The earlier finding named the wrong clause.** The dense clause was
  already one masked scan of the shard per rescore call (13.7 ms for
  120,000 survivors of 400,000 rows). The 2 s went to the lexical clause:
  the BM25 candidate scorer searched its growing result list on every
  match, quadratic in the candidates of one call (25 ms per piece of
  10,000, 60 pieces for a 600,000-row membership). Each match now lands
  in its candidate's slot.
- **The rare-term case paid for a list of every id.** A dense clause's
  membership was fetched as the corpus's id set (2,000,000 ids, 16 MB on
  the wire and a tree of them at the coordinator) before the intersection
  with a 2,000-row term. The clause is now the universe and the term's
  bitmap names the rows: 143 ms to 19 ms.
- **What remains is the coordinator's set arithmetic** over 600,000 ids:
  the membership tree built from the bitmaps, one hit record per member
  with its signals, and the sort for the page. Pieces of 10,000 pipeline
  the wire with the shard's work and edge out one call. A sorted-vector
  membership and slot-indexed hits are the next step on this shape.
- **Segment skipping still does not apply** to these two cases, as the
  earlier note says: a keyword clause without a range predicate rules no
  segment out, and the rescore of survivors reads only the parts and
  blocks the survivors sit in.


## 2026-09-06: the boolean tree evaluated on the shard

The same bench after the boolean planner moved to the shards
(`EvaluateBoolean`, `docs/query-api.md` "Recursive boolean execution"):
the coordinator sends the planned tree once, the shard resolves the
clauses over its bitmaps and scores the members in one pass per clause,
and only the best `depth` candidates come back. Same machine, same
corpus rule, fresh run (ingest 59.3 s, compaction 335.8 s). The two
filtered shapes are new to the bench: a MUST filter under a common term
and under a dense clause, the shapes that took the fleet's coordinator
down at 66 million rows (`fleet-placement-2026-09.md`).

| case | layout | segments skipped / total | p50 before (ms) | p50 after (ms) | p90 after (ms) | p50 pruning off (ms) |
|---|---|---:|---:|---:|---:|---:|
| boolean AND(rare term, dense) | bucket | 0 / 16 | 143.3 | 7.2 | 7.6 | 7.2 |
| boolean AND(common term, dense) | bucket | 0 / 16 | 2103.0 | 54.0 | 57.6 | 55.8 |
| boolean MUST(common term, year >= 2010) | bucket | 0 / 32 | not in the bench | 36.3 | 36.6 | 36.4 |
| boolean MUST(dense, year >= 2010) | bucket | 0 / 16 | not in the bench | 41.5 | 41.8 | 41.8 |
| boolean AND(rare term, dense) | partitioned | 0 / 20 | 150.6 | 6.9 | 7.3 | 7.0 |
| boolean AND(common term, dense) | partitioned | 0 / 20 | 2083.8 | 54.9 | 55.2 | 55.0 |
| boolean MUST(common term, year >= 2010) | partitioned | 15 / 40 | not in the bench | 11.2 | 11.3 | 27.0 |
| boolean MUST(dense, year >= 2010) | partitioned | 15 / 20 | not in the bench | 9.6 | 9.7 | 25.5 |

The other cases are within noise of the tables above. Equality held as
before: pruning on and off, and the two layouts, answer the same hits,
score bits, order, and counts; `signal_batch` at 10,000 and at `max_k`
answer the same bits (the knob now sizes only an FP32 clause's pieces).

What the numbers show: the dense clause of a boolean group is one
streaming pass of the shard under the members as the allowlist (the
same pass a filtered `Search` runs), so AND(common term, dense) over
600,000 members costs about what a filtered dense search costs, not
sixty masked rescore calls and a coordinator id set; AND(rare term,
dense) is the rare term's postings and a sparse pass. A MUST filter now
prunes segments on the shard (the filter leaf and the lexical leaf are
counted apart, hence 15 of 40), and the filtered dense shape at 25%
selectivity is under 10 ms on the partitioned layout.
