# Residual-IVF provider screening, 2026-09-01

> **AI-GENERATED EXPERIMENT REPORT. VERIFY BEFORE RELYING ON IT.** This is a
> bounded engineering screen of an unmerged upstream branch, not a publication
> result, production recommendation, or claim about IVF generally.

## Outcome

Ryan Codrai's residual-IVF branch did **not** pass the gate for production
lifecycle work in ProtoMolt Search. Keep it isolated from the production
TurboVec pin. The current useful work is the provider adapter, reproducible
matrix, and truthful `EXACT`/`ANN`/`AUTO` contract, not persistence or mobile
support for this IVF implementation.

The experiment is pure Rust. `protomolt-ivf-eval` links the upstream Rust
`turbovec` crate at `1452b6e8f1eee9d071c22bd8f850cd9ada2acf7a`; it does not
build or invoke `turbovec-python` or PyO3. The matrix runner now records the
resolved Cargo dependency tree and rejects either Python binding by name.

## Reproducibility boundary

- Product checkout: `28ac31e0b1a0f5e1012ae4f309718a30e28ee183`, with a clean
  working tree recorded in each artifact's `git-status.txt`.
- Input: `/work/court-corpus/embeddings-full.bin`.
- Shape: 256 dimensions, 16 corpus-distributed query rows held after each
  indexed prefix, k = 10, 100, and 10,000.
- Matrix: 100K, 500K, 1M, and 2M rows; IVF `nlist=floor(sqrt(rows))`;
  `nprobe=8,16,32,64,128,256,all`; two warmups and five measured iterations.
- Runtime: four Rayon threads pinned to both logical siblings of physical cores
  4-7 (`CPUSET=4-7,20-23`).
- Host validity: the runner measured busy-time deltas on the selected CPUs,
  subtracted the benchmark process, and used a declared 110% cumulative
  external-CPU ceiling. Observed peaks were 83%, 54%, 88%, and 103% at 100K,
  500K, 1M, and 2M. All four latency/build cells are valid under that rule.
- Artifacts:
  `/work/court-corpus/bench/ivf-eval/2026-09-01-28ac31e-court-small` and
  `/work/court-corpus/bench/ivf-eval/2026-09-01-28ac31e-court-large-r3`.
  Both `SHA256SUMS` manifests verify.

Earlier runs that overlapped unrelated build waves remain on disk and are
marked invalid by the same guard. They were not selected for the tables below.
A deliberately competing-process smoke test also proved that the guard trips.

These queries are real embedding rows, but they are not held-out user search
judgments. Results characterize this corpus slice and build only.

## One-million-row result

The host-valid 1M cell failed every gate:

| k | Flat mean / worst FP32 recall | IVF all-list mean / worst recall | Result |
|---:|---:|---:|---|
| 10 | 0.88125 / 0.80 | 0.86250 / 0.70 | IVF ceiling below flat |
| 100 | 0.884375 / 0.83 | 0.868125 / 0.79 | IVF ceiling below flat |
| 10,000 | 0.9344625 / 0.9206 | 0.93305 / 0.9198 | Close, still below flat |

Lower `nprobe` values sometimes reduced latency substantially, but none met
both the flat provider's mean and worst-query recall while also improving QPS
and p95. At k=10,000, for example, `nprobe=256` reached mean/worst recall
0.93123125/0.9159 at 27.45 batch QPS and 36.49 ms p95, versus the flat
provider's 0.9344625/0.9206 at 9.22 QPS and 202.82 ms p95. The speedup is real
for this cell, but it does not satisfy the requested quality floor.

The construction and retained-memory costs independently fail the gate:

| Provider | Build time | Retained RSS increment |
|---|---:|---:|
| Embedded TurboVec | 479 ms | 140.7 MB |
| Experimental residual IVF | 42.12 s | 647.0 MB |
| IVF / flat | 87.84x | 4.60x |

The failure grew rather than disappeared with corpus size:

| Rows | IVF / flat build | IVF / flat retained RSS | Host peak / ceiling |
|---:|---:|---:|---:|
| 100K | 16.04x | 1.38x | 83% / 110% |
| 500K | 55.14x | 6.70x | 54% / 110% |
| 1M | 87.84x | 4.60x | 88% / 110% |
| 2M | 139.79x | 10.18x | 103% / 110% |

At 2M, all-list IVF was also below flat recall at every required k. At
k=10,000 it produced 0.9318375 mean and 0.9118 worst-query recall versus
0.9346625 and 0.922 for flat. No scale point authorized lifecycle investment.

## Filter behavior

The product's flat provider executed a deterministic 10% dense mask before
scoring. The experimental IVF adapter refused the same query because the
upstream branch does not expose dense-mask traversal. Post-filtering an ANN
top-k would not certify the top-k of the allowed population, so the adapter
does not fake support.

## Decision

Do not add this IVF branch to production pins, snapshots, WAL, resharding, or
mobile builds. Retain the standalone adapter and negative result. Reopen the
gate only after an upstream change addresses all three observed blockers:

1. construction and memory overhead;
2. a quality ceiling at least equal to the flat provider at every required k;
3. filter-aware traversal or another honest way to search an allowlist.

The public query API can still land now. On the current provider,
`UNSPECIFIED` and `EXACT` require exhaustive completion, `AUTO` resolves to
`EXACT`, and `ANN` fails closed. A future ANN backend must disclose its quality
contract and qualify an adaptive policy before `AUTO` may select it.
