# Phrase and proximity queries

Landed 2026-09-02 (roadmap item 5). A `PhraseMatch` on a lexical query
requires its analyzed terms to occur in the field **in order**, inside a
window that may contain at most `slop` token positions beyond the phrase's
own length. `slop = 0` is an exact phrase. The constraint only removes
documents; the score is the ordinary BM25 of what matched.

Two ingest-time payloads serve it, both opt-in per field, both priced
below. Nothing is ever approximated from character offsets: a dropped
token and a run of whitespace look identical between two spans, and a
phrase index built on that guess would match `new york` against
`new — york` and call it exact. The engine refuses by name instead.

## Positions are token ordinals

Both analysis providers number their tokens. The ordinal of a token is its
index in the tokenizer's complete output, counting every token produced,
including one whose term identity normalized to nothing and was therefore
never emitted as a term. Two occurrences are adjacent exactly when their
ordinals differ by one.

- The native analyzer numbers tokens in the same pass that emits terms.
- The OpenNLP sidecar's `tokens` layer rides the **same** `Analyze`
  response as the term vectors; the node matches each occurrence span to
  its token by one merge over both lists. There is no second sidecar call,
  and a response without a token layer makes a positional field refuse
  the document rather than guess.

`tests/phrase_proximity.rs` pins the case that motivates all of this: a
soft hyphen between `new` and `york` is a token under the whitespace
tokenizer and normalizes to nothing under `STRIP_INVISIBLE`, so the two
terms' spans are separated by exactly the whitespace a real `new york`
has, while their ordinals are two apart. The document does not match the
exact phrase, and matches it at `slop = 1`.

## Payload 1: the bigram column

`--bigram-fields=body` derives a BM25 field named `body.bigrams` whose
terms are the source field's adjacent-ordinal pairs, `first SPACE second`
(neither tokenizer can put a space inside a token; the derivation checks
rather than assumes). A bigram is a term: one posting per distinct pair per
document, ordinary global df and average length, the ordinary scorers and
block-max pruning, no new query machinery. The column must be declared in
`--bm25-fields`, and clients never supply it — the node derives it from
the source's positions at ingest and the derived column's analyzer
fingerprint is a function of the source's, so a column built from a
differently analyzed source never shares a name with it.

A two-term exact phrase whose bigram column every shard indexes becomes
one term of that column. The hit's occurrences then name the column
(`body.bigrams`, span from the first token's start to the second's end),
and `Bm25SearchResponse.phrase_routing` says so.

A bigram column answers only two-term exact phrases. Three adjacent
bigrams `a b`, `b c` present in one document do not certify `a b c` (the
document may hold `a b … b c`), so longer phrases and any slop need
positions.

## Payload 2: token positions

`--position-fields=body` keeps one `u32` ordinal per occurrence of the
field, stored in the `.bm25` file as a **kind-7 entry** of the v7 column
table (`positions:<field>`, section `column:positions:<field>:vals`):

```text
u32 n_terms | (n_terms + 1) x u32 base | total x u32 ordinal
```

`base[i]` is the cumulative occurrence count before directory entry `i`,
so a posting's ordinals sit at `base[i] + occ_start .. base[i] + occ_end`,
parallel to its occurrence-run slice. A lookup is the binary search the
occurrence read already does, plus one slice. The heap builder and the
spill builder write the section byte-identically; the mmap reader serves
it without loading it; open-time validation checks the entry names a field
of the table, the term count matches that field's directory, and the base
table is monotone from zero to the declared total. The ordinals are
payload (CRC-covered, checked by the deep pass) — reading them all at open
would fault in the section and defeat paging.

A file without the entry opens, serves every old query, and refuses
phrase and slop on that field by name. A binary predating kind 7 refuses
the file by number, as the kinded table intends. Nothing converts on open;
ingest into a positional field refuses when the active storage never
declared positions for it (`rebuild or reshard the generation`), and a
document whose analysis carried no positions refuses into a positional
field rather than leaving a hole.

The query-side predicate runs at the one heap gate every scorer and
facet count already share (`DocFilter::passes`, after the cheaper column
predicates): for each start occurrence of the first term, the earliest
later occurrence of each next term gives the tightest window from that
start, so the minimum over starts is exact. A repeated term in the phrase
needs distinct tokens. Because the gate only removes, every block-max
bound over the survivors stays a bound, distributed results equal the
monolithic ones, and facet counts count the phrase-matched set.

## Routing and refusals

The coordinator decides the route from what the fleet answers in the
stats round (`FieldStats.positions`, and a probe of the bigram column):

| phrase | bigram column on every shard | positions on every shard | route |
|---|---|---|---|
| 2 terms, slop 0 | yes | any | bigram column |
| 2 terms, slop 0 | no | yes | positions |
| 3+ terms, or slop > 0 | any | yes | positions |
| otherwise | | | `INVALID_ARGUMENT` naming the field and what it lacks |

A one-term phrase is the ordinary term query. A mixed fleet — positions on
some shards only — refuses; the phrase is never matched on half the
corpus. A phrase is served on `Bm25Search` (flat body, or per
`QueryField`) and on the single lexical leaf of `Query`; the boolean
planner, composite strategies, and boosts refuse it by name until their
paths carry a gate. Score stages, stats, cardinality, and projections
refuse alongside a phrase until certified with it.

## Cost

Measured 2026-09-02 on the first 20,000 CourtListener chunks
(`/work/court-corpus/canary-chunks.ndjson`, `examples/bigram_cost.rs`,
native analysis under `body_spec`; 1,256 B of text, 204 occurrences, and
122 distinct terms per chunk):

| section | bytes/doc | relative to the body index |
|---|---:|---:|
| stored text | 1,260 | |
| body index (lengths + postings + directory) | 3,807 | 100% |
| **token positions** (`column:positions:body:vals`) | **847** | **+22%** |
| **bigram column** (lengths + postings + directory) | **9,146** | **+240%** |

The bigram column costs what it does because a chunk's pairs are nearly
all distinct — 183 bigram postings for 204 occurrences — and every one
of them is a dictionary entry: 6,504 B/doc of postings plus 2,638 B/doc
of directory, against 847 B/doc for one ordinal per occurrence. That
inverts the expectation the roadmap recorded. On this corpus the cheap
payload is positions, which also serve every phrase length and every
slop; the bigram column buys a single-term lookup for two-term exact
phrases at 2.4 times the whole body index, which is a trade only a
phrase-heavy workload over a corpus that fits page cache with room to
spare should make. Both stay opt-in per field, and the recommendation
is now: positions on a corpus body first; a bigram column only where
two-term exact phrases dominate the query mix and the page-cache budget
has been checked against the file sizes above.

`tests/phrase_proximity.rs::positions_and_bigrams_are_priced_exactly`
holds the encodings to their formulas byte for byte on a synthetic
corpus: the positions section is exactly `4 + 4 (n_terms + 1) + 4 total`,
and the bigram column's sections are exactly an ordinary field's over the
same postings. A hack that changed either encoding fails the gate.

## Durability

`AddDocumentsRequest.position_fields` and `.bigram_fields` are the
document's proximity record. Fresh ingest fills them from the node's
configuration before the WAL append; a replayed record must agree with the
configuration exactly, or the node refuses by name. Replay and resharding
re-analyze under the fingerprinted spec — the same tokenizer numbers the
same tokens — and rebuild the bigram column from the record, so children
carry the same positions and the same derived column without the source
configuration. `tests/phrase_proximity.rs` pins reopen-from-mmap and
split-replay against the live index.
