# Quality columns: noise and artifact measurements as ordinary columns

`AddDocumentsRequest.quality` (a `QualitySpec`) asks the node to
measure a document's text damage during ingest and store the
measurements as ordinary typed columns. It exists because of a corpus
fact: about 1.5% of the court corpus is hard garbage — OCR shift-cipher
text, base64-ish blobs, mojibake — and the right treatment is not to
guess at cleaning it but to MEASURE it, keep it addressable, and let
the query side decide (filter it out, decay it, or facet on it).

The whole feature is three scalars and a rule about where they live.

## 1. The measurements

The OpenNLP sidecar's tier-1 surface includes two model-free structural
scanners, requested per session via `AnalysisOptions.noise` and
`.artifacts`:

- a **noise** scanner that emits `NoiseSpan { span, severity, score }`
  per finding, score in (0, 1], higher is noisier (severities run
  "misspelled" → "damaged" → "gibberish" → "binaryish");
- an **artifact** scanner that flags structural damage —
  `TextArtifact { span, type }` for replacement characters, control
  characters, unpaired surrogates, zero-width characters, mojibake and
  the like. Detection never modifies the text.

The node folds a document's findings into three scalars
(`doc_quality`, `src/analyzer.rs`):

| column              | type | value                                                        |
|---------------------|------|--------------------------------------------------------------|
| `noise_column`      | f64  | the WORST finding's score; exactly `0.0` with no findings    |
| `noise_chars_column`| i64  | characters covered by the UNION of the findings' spans       |
| `artifact_column`   | i64  | count of flagged artifacts                                   |

Three deliberate choices in that folding:

- **Worst, not mean.** A mean invites normalization policy (weighted by
  what? span length in bytes or chars?). The worst finding is
  denominator-free and monotone: adding a finding never lowers it.
- **Union, not sum.** Overlapping findings count each damaged character
  once, computed by a sort-and-sweep, so the value does not depend on
  the order the sidecar emitted findings in.
- **No fractions.** `noise_chars` is a count, not a ratio. A caller
  that wants "fraction damaged" divides by a length it already knows;
  the engine does not pick the denominator (bytes? chars? tokens?) on
  its behalf.

A non-finite finding score is treated as no signal rather than
propagated: it would otherwise poison the f64 column's min/max
metadata, which score-chain bound math reads.

**Measured-clean is a value.** A document ingested under a spec always
gets values written — a clean one measures `noise == 0.0`. Absence in
the column (NaN / `i64::MIN` sentinels) means the document was ingested
WITHOUT a spec, and the usual Kleene rules apply to it in filters. The
two states are distinguishable on purpose.

## 2. Where the values live: materialize, then take the ordinary path

`materialize_quality` (`src/node.rs`) runs as the FIRST step of the
per-document apply: it takes the `QualitySpec` off the request, refuses
if the analysis session returned no quality layers (a contract break,
not a clean document), and appends the three scalars to the request's
own `numerics` / `integers` lists. Everything after that is the path
every explicit column value already takes:

- **Declaration.** Every named column must be declared on the node
  (`--numeric-fields` for `noise_column`, `--integer-fields` for the
  other two). An unknown name refuses by name, with the knob named.
- **Duplicates.** A spec column colliding with an explicit value in the
  same document is the ordinary "repeats in one document" refusal.
- **Durability.** The WAL logs the request AFTER materialization: the
  record carries the derived values and a cleared spec, so crash
  recovery and reshard replay reproduce the columns exactly without
  calling the sidecar again — replay of a quality-bearing log needs no
  sidecar at all, and a sidecar upgrade can never silently change
  replayed values.
- **Query surface.** The columns are ordinary f64 / i64 columns:
  CEL filters (`noise >= 0.4`, `artifacts == 0`), range facets, and
  score-function chains read them with no new machinery.

A spec with every column name blank asks for nothing: the sidecar is
not asked for the layers and nothing is written. A caller pays only for
the columns it names.

## 3. The session contract

The quality layers are requested in the analysis session's OPTIONS
message, so the spec must be constant per ingest stream the same way
`analysis` must be: a mid-stream change reopens the session
(`src/node.rs`, the `doc.analysis != spec || doc.quality != quality`
reopen condition). `AnalyzedDoc.quality` is `Some` exactly when the
session asked; `materialize_quality` refuses on the mismatch rather
than writing zeros, because "the sidecar's options and its responses
disagree" is impossible-state evidence, not a clean document.

## 4. What is deliberately NOT here

- **No new score op.** The quality decay is the existing
  `SCORE_OP_MULT_EXP_DECAY` with `origin = 0` over the noise column:
  `score *= exp(-noise / scale)`. The multiplier lies in (0, 1] and
  absence is identity, so the stage is monotone and its bound lift is
  identity — the pruning math is untouched
  (`docs/score-functions.md`).
- **Nothing on `AnalysisSpec`.** The analysis fingerprint
  (`b"turbovec.analysis.v1"`) pins TERM IDENTITY. Noise scoring does
  not change a single term, so folding the quality flags into the spec
  would have invalidated every shard ever built (fingerprint domain
  bump, fleet-wide rebuild) for a change that cannot affect what
  matches. `QualitySpec` lives on `AddDocumentsRequest` instead, next
  to the other per-document column values.
- **No cleaning, no dropping.** The engine measures; policy stays with
  the caller. The noise/artifact GATE for the court pipeline (skip or
  reparse garbage before it ingests) lives upstream in the OpenNLP
  branch, not here.

## 5. Tests

- `src/analyzer.rs` unit tests: the union-not-sum sweep over
  overlapping/duplicate/empty spans in any emission order, non-finite
  score handling, and clean-measures-zero.
- `tests/quality_columns.rs` end to end, against the mock sidecar's
  deterministic rules (a token made entirely of `#` is a noise finding
  scored `len/10` capped at 1.0; every U+FFFD is a "replacement"
  artifact): predicted column values selected exactly via public CEL,
  the decay stage reproducing `base * exp(-noise)` bitwise, the blank
  spec writing nothing, and both ordinary refusals (undeclared column,
  duplicate collision) firing on materialized values.
