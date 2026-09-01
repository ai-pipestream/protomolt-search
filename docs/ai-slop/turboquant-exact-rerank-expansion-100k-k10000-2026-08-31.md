# TurboQuant candidate expansion for exact reranking

> **AI-GENERATED EXPERIMENT REPORT. VERIFY BEFORE RELYING ON IT.** This is a
> deterministic synthetic same-host measurement. It is not a production-corpus
> quality or latency claim.

## Result

For 16 vector queries over 100,000 unit-normalized 64-dimensional vectors,
TurboQuant must return **35,777 candidates** before exact FP32 reranking to
recover every exact top-10,000 neighbor. That is a **3.5777x expansion** over
the requested result depth.

Lower quality targets require materially less expansion:

| Recall target | Depth for mean recall | Expansion | Depth guaranteed for every query | Expansion |
|---|---:|---:|---:|---:|
| 95% | 12,560 | 1.2560x | 13,967 | 1.3967x |
| 99% | 17,704 | 1.7704x | 19,486 | 1.9486x |
| 99.9% | 24,107 | 2.4107x | 26,194 | 2.6194x |
| 100% | 35,777 | 3.5777x | 35,777 | 3.5777x |

The measured expansion ladder was:

| TurboQuant candidates | Expansion | Mean recall after FP32 rerank | Worst-query recall | Mean linear-gain NDCG |
|---:|---:|---:|---:|---:|
| 10,000 | 1.00x | 0.89163125 | 0.8548 | 0.97612889 |
| 11,000 | 1.10x | 0.92168750 | 0.8894 | 0.98295827 |
| 12,500 | 1.25x | 0.94905000 | 0.9272 | 0.98971640 |
| 15,000 | 1.50x | 0.97608125 | 0.9624 | 0.99553523 |
| 20,000 | 2.00x | 0.99554375 | 0.9917 | 0.99928114 |
| 30,000 | 3.00x | 0.99996250 | 0.9998 | 0.99999590 |
| 50,000 | 5.00x | 1.00000000 | 1.0000 | 1.00000000 |
| 100,000 | 10.00x | 1.00000000 | 1.0000 | 1.00000000 |

The exact 35,777 result is not inferred from the sampled ladder. The analyzer
retrieved the complete quantized ranking and recorded the quantized rank of
every exact FP32 top-10,000 neighbor. It then exact-reranked each requested
candidate prefix from the retained FP32 corpus vectors. As a final consistency
check, reranking all 100,000 candidates reproduced the generated judgments in
exact order for every query. The run fails if that check does not hold.

## Product implication

Candidate expansion plus exact reranking closes the measured quantization gap.
A useful initial quality policy would expose expansion as an explicit knob:

- about 2x for at least 99% recall on every query in this fixture;
- about 2.62x for at least 99.9% recall on every query;
- 3.5777x for exact top-10,000 recovery on this fixture.

Two constraints remain before this becomes a production search path. The
current persisted `.tv` index does not itself retain the FP32 vectors used by
this experiment, so production exact reranking needs a retained FP32 or
equivalent residual store. This run also fetched one complete ranking per query
to determine exact thresholds, so it does not measure end-to-end latency at
each expansion depth. The next implementation benchmark should retain exact
rerank data, request only the configured candidate depth, and report scan,
rerank, first-hit, and terminal latency separately.

## Reproduction

```bash
./deploy/opensearch-challenge/run.sh \
  --documents=100000 --dimensions=64 --topics=16 --k=10000 \
  --iterations=1 --warmup=0 --concurrency=1 \
  --rerank-depths=10000,11000,12500,15000,20000,30000,50000,100000 \
  --cpuset=0-7 \
  --out=/work/court-corpus/bench/opensearch-challenge/2026-08-31-3d26ba0-100k-k10k-rerank
```

- Protomolt Search revision: `3d26ba0631c600607679f37dcb373041bc6457f5`
- TurboVec revision: `65699eff623cefa0aeddbf7c67847372c106a3e2`
- Seed: `3235795367`
- CPU allocation: CPUs 0 through 7 on an AMD Ryzen 9 9950X3D
- Rerank report format: `protomolt-exact-rerank-sweep-v1`
- Raw result directory:
  `/work/court-corpus/bench/opensearch-challenge/2026-08-31-3d26ba0-100k-k10k-rerank`

Artifact digests:

| Artifact | SHA-256 |
|---|---|
| `protomolt-rerank.json` | `5564607a122ec529b47403d13dc60e5a0e65ab858ec9bc6a012925411294b61a` |
| `report.json` | `888ff217698496bd39caec616b44a324853566db8d4afc77e9f5a12cc8b47154` |
| `resources.json` | `b333857102038cca3000fcc4d66e7d277a0d7133f2d77eda396b0e79b3e7fe5c` |
| `manifest.json` | `a8df785947bdba77131ebaa7f8739a7e5c91c8f29c83680dac4e6ea25e1abfa4` |

The result directory's `SHA256SUMS` verified successfully after the run.
