# Score functions: first-class chained stages over numeric columns

Landed 2026-08-03 (track 1, `plans/track-1-features.md` section 3).
Recency decay, level boosts, citation weights — as a chain of named
score transforms that keeps every exactness property the engine already
has.

## The two-language split

Column features divide into selection and scoring, and the two demand
different machinery:

- **CEL selects** (future increment): filters compile per shard to
  dictionary-resolved ordinal predicates. A filter only REMOVES
  documents, so every block-max bound remains a valid upper bound with
  no new math.
- **Function chains score** (this increment): transforms change scores,
  so bounds must be carried explicitly — each stage ships its own bound
  rule, and that is the whole trick.

CEL is the right syntax for the first and the wrong engine for the
second: pruning needs a derivable upper bound on every score
contribution, and an arbitrary expression has none. A fixed stage
vocabulary where every op proves one small theorem is strictly stronger
than a general language that proves nothing.

## The stage contract

A stage is `(eval, bound)`, not just `eval`:

- `eval(score, x) -> score` runs per candidate on the node, where `x`
  is the document's value in the stage's numeric column.
- `bound(score_ub) -> score_ub` lifts an upper bound through the stage,
  using the column's min/max metadata (stored in the v7 column table,
  computed at write time).

Admission requires: **monotone non-decreasing in the incoming score**,
and a bound valid over the column's whole domain INCLUDING absence.
Under that condition the chain's bound is the composition of stage
bounds — "upper bound in, upper bound out" survives chaining — so the
block-max scorer simply lifts every bound sum through the chain before
its inert tests, and inserts candidates by their FINAL (chained) score.
MaxScore partitioning, the seeded floor, `kth_best` emission, and the
termination test all operate on the final-score scale unchanged.

Two consequences worth naming:

- A document without a value passes through the stage unchanged
  (identity: factor 1, addend 0). This is exact semantics, not
  degradation — and it is why a shard that lacks the column entirely is
  still correct: all its documents are absent. The coordinator refuses
  a column NO shard knows (the typo rule), via known flags on the shard
  responses, exactly as facets and scoring fields do.
- Because absence means identity, a multiplicative stage's bound always
  includes factor 1 (a block might contain valueless docs). A decay
  therefore lifts bounds by 1.0 — sound, never tightening. Tightening
  needs per-block column ranges plus presence counts; that is a future
  optimization, not a correctness gap.

## The vocabulary (first cut)

| op | eval | bound lift | refused when |
|---|---|---|---|
| `MULT_EXP_DECAY` | `score * exp(-abs(x - origin) / scale)` | `ub * 1` | `scale <= 0`, non-finite params |
| `MULT_LOG` | `score * (1 + weight * ln(1 + max(x, 0)))` | `ub * (1 + weight * ln(1 + max(col_max, 0)))`, at least 1 | `weight < 0` (factor could go negative — non-monotone), non-finite |
| `ADD_LINEAR` | `score + weight * x` | `ub + max(0, weight*col_min, weight*col_max)` | non-finite weight |

Absent `x`: identity in every op. `MULT_LINEAR` (`score * (a + b*x)`)
is deliberately NOT in the vocabulary yet: it is admissible only when
the factor is provably non-negative over the column's [min, max], which
the metadata can verify — an example of the admission rule doing real
work — but nothing needs it yet.

`ADD_LINEAR` with negative contributions can push final scores to or
below zero. Correctness is unaffected (heap, floors, and merge are
sign-agnostic), but the wire convention "min_score 0 = unseeded" and
`floor_seed`'s clamp mean seeding degrades to unseeded for non-positive
kth-bests. Chains that keep scores positive keep re-query seeding.

## Determinism and distribution

Chain order is request-list order, and that order is the pinned
evaluation order on every shard — the same rule as fused field-leg
order, for the same reason (IEEE addition and multiplication are not
associative across reorderings). The coordinator forwards the identical
stage list to every shard; every shard resolves the column by name
against its own table and evaluates the identical float ops. Result:
distributed == monolith bitwise, chained pruned == chained exhaustive
bitwise (the test gates), and `SearchVariant` A/Bs a chain against its
absence with rankdiff for free, since variants carry whole
`Bm25SearchRequest`s.

Scope: the flat BM25 route first. The fused route needs nothing new in
principle (the chain applies to the fused sum; bounds lift the same
way) and lands when something needs it. Hybrid composition waits for
the same filter groundwork facets wait for.

## Numeric columns

The shared prerequisite ("one mechanism, three features": facets, CEL
filters, chains). The v7 column table — revised in place 2026-08-03,
before any v7 file existed outside test artifacts — now carries a
`kind` byte per entry:

```text
u32 n_columns
per column: u16 name_len | name bytes | u8 kind
  kind 0 (string dict): u32 n_values | u64 dict_off | u64 ords_off
  kind 1 (f64):         u64 min_bits | u64 max_bits | u64 vals_off
per kind-0 column: dict | ords          (unchanged from the facet cut)
per kind-1 column: vals (n_slots x f64, NaN = absent)
```

An unknown kind refuses at open by number — the extensible-table lesson
from the facet increment, built in this time. Values are declared with
`--numeric-fields`, arrive as `AddDocumentsRequest.numerics`
(field/value pairs; non-finite values refused — NaN is the absence
sentinel and infinities break bounds), ride the WAL verbatim like
facets and fields, and reshard replay re-derives the child's table from
the records. min/max are computed at write time over present values and
validated against a full scan at open.
