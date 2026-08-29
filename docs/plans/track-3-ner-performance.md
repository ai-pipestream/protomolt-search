# Track 3: the NER throughput ceiling

Written 2026-08-02. This is a research brief for a fresh-eyes
investigation of OpenNLP NER throughput, written so the investigator
does not re-walk paths we have already ruled out. The work happens in
the OpenNLP fork, not in this repo; this repo contributes the
measurement harness and the numbers below.

## 1. The shape of the problem

After removing the sidecar's stale annotator lock, person-only NER
went from 748 to 5,039 docs/s at concurrency 32 on 1.3 KB chunks, and
concurrent results were verified span-identical to serial on 500
documents. That was the easy 6.7x. The remaining problem is different
in kind:

1. Throughput plateaus near 8 cores' worth of CPU on a 32-core box,
   regardless of offered concurrency.
2. Four separate JVM processes together consumed 18 cores to deliver
   slightly less total throughput than one process consuming 8.
   Per-core efficiency fell 58%.

Point 2 is the load-bearing clue. Separate processes share no locks, no
GC, no ThreadLocals, no JIT, and no allocator. When adding processes
makes aggregate throughput worse, the contended resource is below the
process: memory bandwidth, last-level cache, or something else the
socket shares. Every in-process explanation has to be reconciled with
this observation before it is believed.

Something in the hot path is churning memory hard enough to pin the
bus. The goal of this track is to name it with a profile, then decide
whether a refactor is warranted or the current shape is kept.

## 2. Already ruled out

Each of these was tested, not assumed. Do not spend tokens re-deriving
them; do spend tokens overturning one if the profile points there,
because a profile beats a prior test.

1. The annotator lock. Removed entirely; wrappers deleted.
2. GC. Pauses around 2 ms; not a 4x ceiling.
3. Processor visibility. The JVM sees all 32 cores.
4. Pipeline construction. Cached; not rebuilt per request.
5. The stream worker pool. 32 workers, matches the unary path, and
   unary and streaming plateau at the same place.
6. The client. Four independent client processes aggregate to the same
   number, so the bottleneck is not request generation.
7. Process count as a workaround. Tried; made it worse (section 1).

Also known: `NameFinderME` in the fork's 3.x line is thread safe via
the owner-fast-path plus ThreadLocal design, and the contribution
cache is a lock-free `AtomicReferenceArray` with `compareAndSet`.
Thread safety is settled; this investigation is about cost, not
correctness.

## 3. Where to look

Suspects consistent with the memory-bus signature, in rough order of
suspicion. These are hypotheses to test, not conclusions:

1. **Allocation churn in feature generation.** Maxent NER generates
   String features per token per context; at thousands of documents
   per second that is a very large transient object rate. Allocation
   itself is cheap; the traffic it induces (cache-line fills, card
   marking, promotion) is not. An allocation profile will settle this
   fast.
2. **The beam search.** Per-token candidate copying inside
   `BeamSearch` multiplies the above.
3. **The model's probability arrays.** If evaluation strides large
   float arrays per outcome, several threads may simply saturate LLC.
   This would show as high LLC miss rate scaling with thread count and
   would be the hardest to refactor away.
4. **False sharing on the contribution cache.** `AtomicReferenceArray`
   entries share cache lines; heavy CAS traffic from many threads on
   adjacent slots would degrade exactly this way and would also explain
   why more processes (each with its own cache, all hitting the same
   DRAM) do not help as much as they should. `perf c2c` answers this
   directly.
5. **ThreadLocal adaptive-data copies.** If each thread's path clones
   or rebuilds per-document state that could be shared read-only.

## 4. Method

Round 1 is measurement only. No refactors until the profile names the
cost.

1. `async-profiler` on the live sidecar under the standard load: one
   run each of `-e cpu`, `-e alloc`, and `-e lock`, at 1, 8, and 32
   offered concurrency. The diff between the 8 and 32 flamegraphs is
   the ceiling's portrait.
2. `perf stat` (IPC, LLC-load-misses, stalled-cycles-backend) on the
   JVM at the same three levels. If IPC falls as threads rise, the bus
   story is confirmed; if IPC holds and utilization stalls, look for
   scheduling or a hidden wait instead.
3. `perf c2c` for false sharing if step 2 shows heavy HITM traffic.
4. Reconcile whatever is found against the multi-process observation
   before believing it. A theory that does not explain why four
   processes were worse is incomplete.

Harness: `examples/annotation_throughput` in pipestream-search sweeps
concurrency over unary and streaming and reports docs/s and MB/s;
`examples/annotation_cost` prices layers and re-measures its own
baseline; `examples/annotator_race_check` proves span-exactness of any
change against a serial reference and warns when a layer is vacuously
empty. Load documents are real corpus chunks, not synthetic text.

The code under study is the fork's `kristian-3.x-features` branch (the
uber branch the deployed jar is built from). Read
`PIPESTREAM-PROVENANCE.txt` in the fork before trusting any branch
name; the fork's `main` mirrors Apache and is not what runs.

## 5. The refactor decision

Round 2 happens only if round 1 names a cause. The instruction is to
prototype the smallest refactor known to remove the named cost, on a
branch, and let the harness judge it. The bar for adoption:

1. Span-identical output on the race check across all loaded models.
   No exactness trades, in keeping with how this whole system argues.
2. A real multiple at high concurrency, not single digits of percent.
   The plateau is 4x deep; a refactor that recovers less than 2x at 32
   cores probably is not worth carrying against upstream drift.
3. The multi-process experiment repeated after the change. If one
   process now scales past 8 cores, the diagnosis was right.

If the named cause is the model's memory traffic itself (suspect 3),
the honest outcome may be "kept, and documented": the fix would be a
model-format or quantization change, which is a different project. In
that case the ceiling gets written down as a property, the fleet plan
(more small boxes rather than bigger ones) becomes the scaling story,
and this track closes with a measurement instead of a merge.

Fork policy applies as always: changes ride the fork's feature branch,
upstream is offered nothing cold, and anything adopted gets the same
provenance note treatment as the thread-safety work.

## 6. Current operating point, for reference

Person plus location at concurrency 32: 2,235 docs/s, which prices the
86.6M-chunk capture pass at roughly 11 hours. That pass does not wait
for this track. This track exists because 5x more silicon is sitting
idle during it, and because every future capture pass (organization,
all seven models at 749 docs/s) inherits whatever ceiling this one has.
