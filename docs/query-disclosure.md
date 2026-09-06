# Query execution disclosure

The shared Query/QueryStream executor applies document execution disclosure
after checking the full physical read set and applying field disclosure.
Selection and candidate scoring must already have enforced the document view;
response redaction cannot repair an unauthorized candidate set.

`QueryResponse.execution_details_redacted = 13` declares that physical execution
details were withheld. It is true for a document-scoped execution even when no
profile was requested or no hits matched. Clients must show these values as
unavailable, not measured zero:

- `QueryProfile`: rerank rows, logical bytes, pages and tasks; segment and shard
  totals and skipped counts.
- `DenseQualityOutcome`: physical corpus rows and generation, profile display
  name and embedding-model label.
- `DenseExecutionOutcome`: free-form planner explanation, policy display name,
  live filter selectivity, and both selectivity bounds in its policy point.

The response retains authorized hits, ranks, scores, groups, aggregates,
projections and other field-authorized content. It also retains request and
cursor continuity, served topology generation, requested/resolved traversal,
provider and scoring identity, completion semantics, candidate depth, measured
recall, and opaque measurement fingerprints. Those fingerprints identify the
evidence for an authorized operator without publishing its display labels or
corpus geometry. Timing remains observable and configuration versions remain
correlatable. This contract does not claim timing or traffic noninterference.

Field disclosure remains independent: `field_details_redacted` describes
withheld identity, stored-value dimensions or dictionary details. Neither flag
grants an operation, changes ranking, or authorizes raw document retrieval.
Exhaustive Rust patterns require each new response/profile/quality field to
receive an explicit disposition before the service compiles.

## Evidence scope

`DenseQualityOutcome.evidence_scope = 10` and
`DenseExecutionOutcome.evidence_scope = 13` use `DenseEvidenceScope`:

- `UNSPECIFIED`: no explicit scope claim, including replies from older servers.
- `NOT_APPLICABLE`: traversal resolution did not consult a measured AUTO policy.
- `CORPUS_BENCHMARK`: the FP32 depth profile's benchmark over its whole corpus.
- `SELECTIVITY_BAND_BENCHMARK`: the AUTO traversal policy's measured query cohort
  within a selectivity band.

A corpus benchmark does not establish recall inside every filtered match set.
A selectivity-band benchmark does not establish recall for every predicate or
document grant with the same cardinality. The scope survives redaction, as do
the chosen depth and the evidence fingerprint. FP32 rescoring of ANN candidates
still reports approximate traversal; neither a profile nor disclosure changes
its completion contract.

## Admission and validation boundary

Public document-restricted Query remains gated. This change covers successful
terminal response metadata in the shared executor; it does not certify error
messages, provisional stream surfaces, or every remaining document-query shape.
Network delegation and stable identity on every public hit also remain open.

The executor regression covers native and FP32 dense selection, lexical,
Boolean, and all four served hybrid strategies. It compares authorized hits
before and after disclosure, proves real FP32 row work was withheld, and checks
unrestricted execution retains its metadata. Unit tests cover every withheld
field, protobuf round-tripping, empty responses and repeated application.
The existing measured dense-profile and ANN-policy suites assert evidence scope
at real resolution boundaries. These are three additive fields and one enum;
no existing field is renumbered, no route is added, and no stored format changes.
