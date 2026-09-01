# Experimental TurboVec IVF evaluation

This standalone crate evaluates Ryan Codrai's unmerged residual-IVF branch
through ProtoMolt Search's `VectorProvider` seam. It deliberately has its own
workspace and lockfile. Building it does not change the production TurboVec
dependency or the root `Cargo.lock`.

The whole path is Rust. The benchmark links Ryan's `turbovec` crate directly;
it does not build or invoke `turbovec-python` or PyO3. `run-matrix.sh` records
the resolved Cargo tree and fails if either Python binding appears.

The adapter advertises `configured_ann`, not exhaustive completion. The
prototype has no persistence, live-floor stream, stable caller-supplied IDs,
removal, calibration, or dense-mask operation. Those operations fail by name
instead of falling back to a different search.

## One run

```bash
cargo run --release --locked \
  --manifest-path benchmarks/ivf-eval/Cargo.toml -- \
  --source=court \
  --input=/work/court-corpus/embeddings-full.bin \
  --vectors=1000000 \
  --queries=16 \
  --k=10,100,10000 \
  --nprobe=8,16,32,64,128,all \
  --warmup=2 \
  --iterations=5 \
  --out=/work/court-corpus/bench/ivf-eval/1m-court.json
```

Court queries come from rows immediately after the indexed prefix. They are
real corpus-distributed embeddings but are not held-out user-query judgments.
Synthetic mode uses independent topic-shaped queries:

```bash
cargo run --release --locked \
  --manifest-path benchmarks/ivf-eval/Cargo.toml -- \
  --source=synthetic --vectors=100000 --dimensions=64 --topics=16 \
  --queries=16 --k=10,100,10000 --nprobe=8,16,32,64,128,all \
  --out=/work/court-corpus/bench/ivf-eval/100k-synthetic.json
```

Each invocation builds and measures both production `embedded-turbovec` and
the experimental adapter in one process. Reported RSS is therefore diagnostic;
`rss_after_build_bytes - rss_before_build_bytes` is the comparable retained
increment and `peak_rss_bytes` is the process high-water mark.

The JSON records build time, retained memory, batch QPS, single-query
p50/p95/p99, mean and worst FP32 recall for every `k`, result completeness,
and filtered behavior. The flat provider executes a deterministic 10% dense
mask. IVF records the current hard refusal because post-filtering an ANN top-k
cannot certify the top-k of the allowed population.

`run-matrix.sh` also samples CPU use while each cell is running. It reads actual
busy-time deltas from `/proc/stat` and subtracts the benchmark process's CPU
time. With `CPUSET`, it counts only those logical CPUs; without a set, it counts
the whole host. A cell that exceeds
`MAX_EXTERNAL_CPU_PERCENT` (100% by default) keeps its deterministic recall
evidence but is marked invalid for latency and build comparisons. This prevents
an unrelated build or test wave from silently becoming benchmark evidence.
`CPUSET` pins the benchmark itself; it does not make those CPUs exclusive from
other host work. Include both logical siblings of each selected physical core
when simultaneous multithreading is enabled.

## Decision gate

Production lifecycle work may begin only when current artifacts show all of
the following on both the one-million and larger CourtListener cells:

1. an IVF operating point meets or exceeds the flat provider's FP32 recall at
   each requested `k` while improving both batch QPS and p95 latency;
2. the all-cell ceiling is at least the flat provider's recall and returns a
   complete result row;
3. retained memory and build time are no more than twice the flat provider;
4. every artifact records the exact Ryan revision and passes its SHA-256
   manifest check.

Passing this gate authorizes implementation work, not production enablement.
Filters, persistence, mutation, calibration, snapshot/WAL/resharding, mobile
compile checks, and truthful public query modes remain mandatory before the
backend can ship.
