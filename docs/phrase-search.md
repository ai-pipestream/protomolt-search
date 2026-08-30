# Phrase and entity search

Protomolt Search can add glossary-backed phrase evidence without replacing
ordinary lexical recall. A document containing `New York City` is indexed as:

- ordinary body terms under the body analyzer;
- `$phrase:...` canonical concept postings in one dedicated BM25 field;
- optional `glossary:nyc = matched` data in a map-facet entity column.

The posting term is an internal encoding of the concept id. Clients use the
concept id in the glossary and the entity-map key, not that internal term.
No arbitrary n-grams are generated. `New York` is indexed alongside
`New York City` only when both are explicit glossary entries. A meaningless
`York City` shingle never appears unless the vocabulary registers it.

## Vocabulary

The vocabulary is UTF-8 TSV, one alias per line:

```text
# concept-id<TAB>surface-form
nyc	New York City
nyc	NYC
new-york	New York
hot-dog	Hot Dog
```

Repeated concept ids are aliases. Empty values, malformed rows, exact
duplicate pairs, and punctuation-only surfaces are refused. Matching uses
Aho-Corasick over Unicode full case folding by default, requires Unicode word
boundaries, and retains original-text spans. The portable matcher can report
UTF-16 code units or UTF-8 bytes; Protomolt Search persists UTF-16. Indexing
retains every explicit nested match. Annotation consumers can instead use the
portable matcher's deterministic leftmost-longest, non-overlapping view.

The vocabulary fingerprint is independent of TSV row order and includes the
field mapping, entity mapping, and NER setting. Nodes and the coordinator must
load equivalent configuration. A mismatch is a hard error.

## Configuration

The storage fields are explicit because both are persisted schema:

```bash
protomolt-search \
  --analysis-addr=native \
  --bm25-fields=body,phrases \
  --map-facet-fields=entities \
  --phrase-glossary=/data/search/concepts.tsv \
  --phrase-field=phrases \
  --entity-map-field=entities
```

Equivalent TOML:

```toml
analysis_addr = "native"
bm25_fields = ["body", "phrases"]
map_facet_fields = ["entities"]
phrase_glossary = "/data/search/concepts.tsv"
phrase_field = "phrases"
entity_map_field = "entities"
phrase_ignore_case = true
phrase_ner = false
```

`phrase_ner = true` also asks the OpenNLP sidecar for its NER layer and
materializes normalized `ner:<type-and-surface>` keys into the same entity
map. It requires `entity_map_field` and a sidecar whose capabilities report an
NER model. Native analysis deliberately refuses model NER, while native
glossary phrases and glossary entity keys remain fully supported.

Entity keys use a readable, collision-free escaped form. Typical glossary
ids produce keys such as `glossary:nyc`; reserved bytes are percent-encoded.
They are ordinary map-facet entries, so existing CEL and facet APIs apply:

```text
entities["glossary:nyc"] == "matched"
```

## Scoring

`SearchService.PhraseSearch` wraps a `Bm25SearchRequest`. It analyzes the
ordinary body or base fields normally, derives canonical phrase terms from the
same raw query, obtains global document frequency and average-length data for
every field, then calls the exact phrase scorer on every shard.

For document `d`:

```text
score(d) = sum(base field BM25 scores)
         + max_i(phrase_weight_i * BM25(phrase_i, d))

phrase_weight_i = min(max_weight, token_count_i * weight_per_token)
```

Defaults are `weight_per_token = 1` and `max_weight = 3`, so a registered
bigram receives weight 2 and a trigram receives weight 3. BM25 idf, term
frequency saturation, and phrase-field length normalization still apply. The
max group is the key rule: a document matching both `New York City` and its
registered `New York` parent receives the stronger evidence, not both summed.
All matching spans remain in the hit for highlighting.

The phrase path is currently exhaustive on each shard. The ordinary fused
path still uses block-max pruning where supported. This is intentional: a
safe max-group upper-bound implementation has not landed, so phrase search
does not claim pruning it cannot prove. Global results remain exact because
every shard scores with the same global statistics and returns its local
top-k for the coordinator's exact merge.

Filters, plain facets, map facets, range facets, and seeded score floors are
supported. Score-function stages, stats, cardinality, and projections are
refused on the phrase route until their interaction with max-group scoring is
certified. An ordinary `Bm25Search` remains available for those combinations.

## Ingest, WAL, and resharding

Clients leave `AddDocumentsRequest.phrases`, `phrase_fingerprint`, and
`phrase_field` empty. Before applying a fresh document, the node:

1. derives canonical concept postings and original UTF-16 spans;
2. derives glossary and optional NER entity-map entries;
3. installs the dedicated analyzed field;
4. writes the populated request to the WAL.

Replay recognizes the non-zero fingerprint and uses the durable values. It
does not rerun a possibly changed vocabulary or NER model. Phrase postings
also carry their field identity, so generic WAL split and merge rebuilds can
restore the phrase field without the source glossary. Resharding restores all
analysis fingerprints as part of the same path.

## Reindex rule

Enabling phrase search adds a required BM25 field and usually an entity map
column. Changing the glossary changes its persisted vocabulary fingerprint.
Either event requires a new index generation and normal verified cutover. Do
not point a phrase-configured binary at an older generation and treat empty
phrase data as degraded behavior. Missing fields and mismatched fingerprints
fail loudly.

## Mobile use

Glossary compilation, matching, canonical posting identity, Unicode case
folding, UAX #29 tokenization, and selectable UTF-16 or UTF-8 span accounting
live in the portable `protomolt-analyzer` Rust crate. They require no
filesystem, Tokio, gRPC, JVM, or model runtime. A mobile application can embed
the vocabulary bytes through its own bridge and use the same semantics in
process. OpenNLP remains the optional server-side boundary for model NER and
other model-backed layers.
