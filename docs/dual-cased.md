# Dual-cased term identity in one analysis pass

Implemented on branch 2026-09-02 (roadmap item 18). A shard can keep the
body's folded identity and its cased identity as two BM25 fields — the A/B
pair the corpus rebuild wanted — from **one** analysis of the text. The
ingest names the cased field on the request; the analyzer returns both
identities from the same tokenization; each field gets its own fingerprint;
and nothing analyzes the text twice, at ingest or at WAL replay.

## The request

`AddDocumentsRequest.cased_field` names a declared BM25 field (not
`"body"`, not a phrase-glossary or bigram column, and not also in `fields`)
that receives the body's cased term identity. The body's `analysis` must be
an explicit spec with a step-chain source (`SOURCE_TOKENS` or
`SOURCE_NORMALIZED_STEMS`); `SOURCE_STEMS` ignores the step chain, so it has
no folded form to contrast with, and is refused by name. A body without a
`cased_field` is unchanged.

The cased identity is the body's chain minus case folding: the same
tokenizer, stemmer, mode, and source, and the same normalizer steps without
`FULL_CASE_FOLD` / `CASE_FOLD`. So `"COURT court Court"` under the folded
body spec is one term `court` × 3, and under the cased twin three terms
`COURT`, `court`, `Court`. Accent folding, invisible stripping, and the
stemmer stay on both sides; the cased column is not the old
`cased_body_spec` (`SOURCE_STEMS`, no steps), which remains a separate,
explicitly requested analyzer for anyone who wants that column.

## One pass, both identities

- **Sidecar.** The session sets `TermVectorOptions.dual_cased = true`; the
  response carries `cased_term_vectors` beside `term_vectors`, both from the
  same tokenization, so occurrence spans and token ordinals coincide vector
  for vector. Opening the session preflights
  `GetCapabilities.dual_term_identity_available` and refuses by name when
  the running jar does not serve it — an open port is not the jar. A jar
  that ignores the flag (no cased vectors for a document with terms) is
  refused at the first document, for the same reason. `cargo run --example
  sidecar_capabilities -- --addr=...` prints what a running sidecar answers
  and exits non-zero without the dual identity.
- **Native analyzer.** `protomolt_analyzer::analyze_dual` runs both
  identities through one tokenization with two accumulators; there is no
  second native pass either.
- **Node.** The analyzer hands back the cased identity on the analyzed
  document; the node places it at the named field, with the same token
  positions (a positional cased field works) and the same sentence spans (a
  sentence cased field works), and fingerprints the field as the twin spec's
  fingerprint. A query leg on the cased field must analyze its terms under
  that twin (`QueryField.analysis`), or the fingerprint check refuses the
  leg by name.
- **Replay.** WAL replay (reshard split/merge, replica catch-up) carries
  `cased_field` on the logged request and analyzes the body once with
  `dual_cased`. The batch analyzer takes the session layers each text
  needs (the cased identity, and sentence spans for a sentence field —
  the sidecar replay of sentence fields requested none before this) and
  groups by `(spec, layers)`, so a replayed document costs one analysis,
  not two.

Quality and geography layers ride the same pass as before and stay out of
the term fingerprint: the fingerprint hashes the analysis spec (tokenizer,
stemmer, mode, source, steps) and nothing else.

## Tests

`tests/dual_cased.rs`: the mock sidecar's call meter reads one analysis per
document with a cased field (ingest and WAL replay alike); the folded field
matches every case variant and the cased field only the exact form; the two
fields' occurrence spans and token positions coincide; a query leg on the
cased field under the folded spec is refused naming the fingerprints; a
sidecar without the capability, `SOURCE_STEMS`, `"body"`, an undeclared
field, and a field supplied twice are refused by name; the flushed image
carries both fields; two shards equal one for the cased leg; the native
analyzer produces the same pair from one pass; quality and geography stay
out of the fingerprint.
