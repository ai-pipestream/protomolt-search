# Source reference bundle

> **AI-COLLECTED RESEARCH MATERIAL. VERIFY BEFORE RELYING ON IT.** These files
> were downloaded by an AI coding agent on 2026-08-28 for the tentative
> TurboVec/RaBitQ backend report. Inclusion is not endorsement, and paper or
> repository claims have not been independently reproduced.

This directory preserves the sources used by
[`../turbovec-rabitq-vector-backend-research.md`](../turbovec-rabitq-vector-backend-research.md).
`SHA256SUMS` records the exact locally retained paper, OpenReview, and
OpenSearch documentation bytes.

## Papers

| Local file | Source | Notes |
|---|---|---|
| `papers/turboquant-arxiv-2504.19874.pdf` | <https://arxiv.org/pdf/2504.19874> | arXiv v1, dated 2025-04-28. Retained to compare the original public preprint with the later accepted version. |
| `papers/turboquant-iclr2026-openreview-86df3c70.pdf` | <https://openreview.net/pdf/86df3c70aa9b7035c407e886e8238951a5d6ec23.pdf> | Current 22-page accepted OpenReview PDF, produced 2026-05-13 and supplied by the user. |
| `papers/rabitq-1bit-arxiv-2405.12497.pdf` | <https://arxiv.org/pdf/2405.12497> | Original 1-bit RaBitQ paper. |
| `papers/rabitq-multibit-arxiv-2409.09913.pdf` | <https://arxiv.org/pdf/2409.09913> | Multi-bit RaBitQ extension. |
| `papers/rabitq-turboquant-comparison-arxiv-2604.19528v2.pdf` | <https://arxiv.org/pdf/2604.19528v2> | Adversarial comparison and reproduction note authored by the RaBitQ team. |
| `papers/eden-turboquant-note-arxiv-2604.18555v1.pdf` | <https://arxiv.org/pdf/2604.18555v1> | Related technical criticism from authors of earlier EDEN work. Not neutral arbitration. |
| `papers/turbovec-case-study-arxiv-2607.16973.pdf` | <https://arxiv.org/pdf/2607.16973> | Third-party TurboVec case study. Not a RaBitQ system comparison. |

## OpenReview metadata

| Local file | Provenance | Notes |
|---|---|---|
| `openreview/turboquant-submission-note.json` | OpenReview note response supplied by the user on 2026-08-28 | Confirms submission `16479`, forum `tO3ASKZlok`, `ICLR 2026 Poster`, CC BY 4.0, and camera-ready PDF path `/pdf/86df3c70aa9b7035c407e886e8238951a5d6ec23.pdf`. It is the submission note only, not the forum replies. |
| `openreview/turboquant-forum-notes.json` | <https://api2.openreview.net/notes?forum=tO3ASKZlok&limit=1000>, supplied by the user on 2026-08-28 | 28 unique current public notes: submission, four reviews, rebuttals, follow-ups, area-chair summary, decision, and public comments through 2026-04-22. |

## Repository checkouts

Repository source is kept as normal Git checkouts under
`/work/main/reference-code`, not copied into this documentation tree.

| Checkout | Source | Pinned revision |
|---|---|---|
| `/work/main/reference-code/RaBitQ-Library` | <https://github.com/VectorDB-NTU/RaBitQ-Library> | `94a9b277571eecbed7e1338dce23d76c1420d874` |
| `/work/main/reference-code/rabitq-turboquant-comparison` | <https://github.com/VectorDB-NTU/rabitq-turboquant-comparison> | `59994ecf2371a78dd5dc191afbdc4c91686803e7` |

At collection time both checkouts were clean on `main`. The comparison
repository's submodules were initialized at the revisions pinned by its
superproject. These external checkouts are convenience copies and are not part
of this repository.

## OpenSearch 3.8 documentation

These official pages were retained on 2026-08-31 for the reproducible
challenge suite in `deploy/opensearch-challenge`. They are HTML snapshots,
not claims that the challenge results generalize beyond the pinned 3.8.0
container.

| Local file | Source | Used for |
|---|---|---|
| `opensearch/version-history-3.8.html` | <https://docs.opensearch.org/3.8/version-history/> | Version provenance. |
| `opensearch/docker-install-3.8.html` | <https://docs.opensearch.org/3.8/install-and-configure/install-opensearch/docker/> | Official container setup. |
| `opensearch/rrf-3.8.html` | <https://docs.opensearch.org/3.8/vector-search/ai-search/hybrid-search/rrf/> | Rank-constant 60 RRF pipeline and fusion-depth controls. |
| `opensearch/knn-query-3.8.html` | <https://docs.opensearch.org/3.8/query-dsl/specialized/k-nn/index/> | k-NN request and inline-filter contract. |
| `opensearch/knn-filtering-3.8.html` | <https://docs.opensearch.org/3.8/vector-search/filter-search-knn/index/> | Lucene HNSW efficient-filter support. |

## OpenReview collection status

Requested official source:

- TurboQuant forum: <https://openreview.net/forum?id=tO3ASKZlok>
- TurboQuant PDF: <https://openreview.net/pdf?id=tO3ASKZlok>

The forum, PDF, attachment endpoint, immutable camera-ready PDF path, and both
public note APIs returned an OpenReview browser-verification challenge or HTTP
403 to the agent on 2026-08-28. The user downloaded and supplied the accepted
PDF and complete public-note response through a browser. Both validate locally
and are covered by `SHA256SUMS`.

The note export contains current public notes, not their full revision
histories. It also does not contain private email records, correspondence sent
to conference chairs, or any private integrity process mentioned by
commenters. The report distinguishes facts visible in this bundle from those
unverified claims.
