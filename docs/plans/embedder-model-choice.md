# On-device embedder: the model choice, and why it blocks the code less than it looks

Status: spike (`crates/protomolt-embedder`, branch `embedder-spike`), 2026-09-01.

The embedded mobile runtime (`docs/embedded-mobile.md`) accepts caller-supplied
vectors and deliberately ships no embedder. That boundary is documented policy:
`docs/native-analysis.md` keeps "static embeddings or another sidecar embedding
configuration" on the sidecar side of the OpenNLP boundary, and
`embed_text` on the `native` backend fails by naming the missing embedding
provider. This document records what an on-device provider costs, what it must
reproduce, and the one decision that gates shipping it: **which table**.

## Why the table choice is structural, not cosmetic

Embedding output is persisted vector identity — the vector-space analogue of
the term identity that pins ICU4X to `=2.0.0` in `protomolt-analyzer`. Three
consequences:

- **dim is forever.** Per-vector index bytes and per-query scan cost on the
  device scale linearly with dim. Choosing 512 over 256 doubles both, for the
  life of every index built on a phone.
- **The table is the app's largest asset.** The engine staticlib strips to
  ~10 MB; every candidate table is larger (see matrix).
- **Local/remote score comparability requires table identity.** Hybrid
  local+remote with comparable scores needs the phone and the server to embed
  in the same space: same table, same tokenizer, same precision. Quantizing
  the table on device only silently forks the score space.

## The tables in play (measured 2026-09-01)

| | court corpus | e2e default | future |
|---|---|---|---|
| Model | `minilm-l6-v2-static` | `minishlab/potion-retrieval-32M` | bge-m3 distillation |
| Vocab | ~30,522 (unverified¹) | **63,091** (safetensors header) | — |
| Dim | 256 | **512** | — |
| Tokenizer | WordPiece (BERT vocab) | WordPiece (`baai/bge-base-en-v1.5` vocab) | — |
| Table f32 | ~31 MB | **123 MB** (measured) | — |
| Table int8 | ~8 MB | ~31 MB | — |
| License | model2vec output, MIT family | MIT | — |

¹ The court model lives only at `/work/court-corpus/models/minilm-l6-v2-static`
on the build host; vocab count inferred from the all-MiniLM-L6-v2 lineage and
not yet read from the file. Verify before deciding.

Discrepancies to resolve while deciding:

- `README.md:491` describes the sidecar default as "distilled
  all-MiniLM-L6-v2 … 256-dim", but `deploy/court-e2e/model/download_model.sh`
  fetches potion-retrieval-32M, whose config says `baai/bge-base-en-v1.5`
  tokenizer and 512 dim. One of the two is the actual default; the README
  should say which.
- `download_model.sh` names the intended future: "a bge-m3 distillation once
  the Java distiller produces it." If that is near, deciding now for either
  current table buys little; the spike is model-agnostic (it loads any
  Model2Vec WordPiece layout), so the code does not have to wait.

## The pooling contract (learned by differential test, not from docs)

Established against the `model2vec` 0.9 reference and pinned by
`crates/protomolt-embedder/tests/model2vec_conformance.rs`:

- the saved tokenizer's post-processor injects `[CLS]`/`[SEP]`; pooling
  excludes them;
- `[UNK]` ids are dropped before pooling — and the `[UNK]` row is NOT zero
  (norm ≈ 21 in potion-retrieval-32M), so the exclusion changes every vector
  for out-of-vocabulary text;
- mean pool, then L2 normalize (turbovec scores true dot products; unit
  length is a requirement);
- text that pools nothing (empty / all-`[UNK]`) has no vector, matching the
  engine's refusal of zero vectors.

None of this is written down upstream. It is version behavior of model2vec
0.9, which is why the reference fixture and its regeneration script pin the
package version and treat regeneration as an oracle change.

## Spike results

- Tokenization matches the reference exactly on all fixture cases (accents,
  soft hyphen/zero-width removal, NBSP, CJK isolation, `İstanbul`,
  emoji-in-word collapse). Vectors match to ≤ 3e-8 max-abs (f32 epsilon;
  the reference pools in numpy f32, the crate in f64).
- ~143k texts/s single-threaded on an M-series host for a one-sentence
  query; the table is mmapped and pages stay clean/evictable, so the
  100 MB-class asset does not become 100 MB of resident memory.
- `aarch64-apple-ios` and `aarch64-linux-android` compile clean.
- Zero new external packages: the crate reuses the exact-pinned ICU4X 2.0
  crates, memmap2, and serde_json already in the audited tree. (Two feature
  flags — `icu_locale_core/alloc`, `writeable/alloc` — are enabled
  explicitly because the casemap-free graph exposes upstream feature-wiring
  gaps that `protomolt-analyzer`'s graph masks.)

## Recommendation

For a phone-first, offline-first product: a **256-dim table**, bundled, f32
unless asset size forces int8. If local/remote comparability is a goal, the
phone must ship byte-identically the table the server queries with — that
decision belongs to whoever settles the sidecar conversion and the bge-m3
distillation, not to this crate.

## Before this graduates from spike

- sentence/chunk pooling for long documents: port the court chunking scheme
  (pack ~256 tokens, hard cap 1024, token-weighted pooling — exact for a
  mean-pooled table; contiguity invariant) and extend the conformance test
  to multi-block texts against the sidecar;
- a model fingerprint (content hash of `tokenizer.json` + table) recorded
  next to vectors, the analogue of `analysis_fingerprint`;
- the sidecar (or its conversion successor) as a second differential oracle,
  three-way with the Python reference;
- table asset shipping (bundle vs first-launch download) and, if int8 ever
  ships, a stated position on score-space forking.
