# Geography columns: geocoded mentions as ordinary columns

`AddDocumentsRequest.geography` (a `GeographySpec`) asks the node to
geocode a document's location mentions during ingest and store the
result as ordinary typed columns. It connects two things that were both
already built and tested but never touched: the sidecar's geography
layer (`GeoLocation` mentions geocoded against the bundled Natural
Earth gazetteer, plus document-level `RegionVote`s) and the engine's
kind-5 geo-point columns with their bbox/radius filters and
distance-decay stages (`docs/geo-columns.md`).

The result is spatial search over a corpus whose source data carries no
coordinates anywhere.

## 1. The reduction

The sidecar emits a layer per document; a column holds one scalar. The
node reduces (`doc_geography`, `src/analyzer.rs`):

| column              | family | value                                                     |
|---------------------|--------|-----------------------------------------------------------|
| `point_column`      | geo    | best resolved location: highest confidence, first on ties |
| `country_column`    | facet  | top `RegionVote`'s ISO country code                       |
| `confidence_column` | f64    | the chosen location's resolution confidence in [0, 1]     |

Two deliberate choices:

- **The point is one mention; the country is the aggregate.** The point
  takes the single highest-confidence mention (ties keep the first in
  text order, so the reduction cannot depend on iteration order). The
  country deliberately does NOT come from that winning mention: it is
  the sidecar's document-level vote over ALL location evidence. A
  document mentioning Berlin twice and Paris once places at Berlin and
  countries as DE — and if the mentions tie differently, the two
  columns may legitimately disagree; each answers its own question.
- **A non-finite confidence is no signal.** It cannot be chosen and
  cannot poison the f64 column's min/max metadata.

**Absence is a legitimate measurement** — the deliberate asymmetry with
`docs/quality-columns.md`, where a clean document measures `noise = 0`.
There is no neutral coordinate: (0,0) is a real place in the Gulf of
Guinea. A document that mentions no resolvable place writes NO point,
NO confidence, and (with no votes) NO country. Geo filters then skip it
and CEL over its columns reads UNKNOWN, per the documented Kleene
absence rules — it is nowhere, not "confidently at the origin".

## 2. Materialize, then take the ordinary path

`materialize_geography` (`src/node.rs`) runs at the top of the
per-document apply, right after `materialize_quality` and in the same
shape: take the spec off the request, refuse if the session returned no
geocoding layer (a contract break, not a place-less document), and
append the values to the request's own `geo_points` / `facets` /
`numerics`. Everything downstream is the path explicit column values
already take: declaration checks (`--geo-fields` / `--facet-fields` /
`--numeric-fields`, unknown names refused by name), duplicate refusal,
the apply, and the WAL record — which carries the derived values with
the spec cleared, so crash recovery and reshard replay reproduce the
columns exactly without a sidecar and cannot drift under a gazetteer
update.

A spec with every column blank asks for nothing. The layers ride the
analysis session's options message, so the spec must be constant per
stream like `analysis` and `quality`; a mid-stream change reopens the
session. Nothing enters the analysis fingerprint: geocoding changes no
term.

## 3. The NER preflight: refusing the silent place-less corpus

The geocoding layer consumes the sidecar's entity layer. The sidecar's
contract when no NER model is configured is deliberately non-fatal on
its side: the request succeeds, `locations` and `regions` stay empty,
and a free-form warning string is returned. For ingest that state is
indistinguishable per-response from "this document mentions no places"
— accepting it would silently ingest an entire corpus as place-less,
the exact failure mode this project refuses to ship.

So a session that asks for geography preflights
`GetCapabilities.ner_available` at open (`AnalyzeStream::open_with_vocab`,
`src/analyzer.rs`) and refuses with FAILED_PRECONDITION naming the
capability and the fix. The refusal is keyed on the sidecar's own
structured capability flag — impossible-state evidence, not a parse of
warning strings.

## 4. Query surface

Nothing new. The materialized columns are ordinary columns:

- bbox and radius `geo_filters` over `point_column`, exact at the
  boundary, on every route including the vector leg
  (`docs/vector-filters.md`);
- CEL over `country_column` and `confidence_column`
  (`country == "FR" && geo_confidence >= 0.7`), with absence UNKNOWN;
- facet counts over `country_column`;
- `SCORE_OP_MULT_GEO_DECAY` distance decay from any origin over
  `point_column` (`docs/score-functions.md`).

## 5. Tests

- `src/analyzer.rs` unit tests: best-confidence selection with
  first-on-ties, the aggregate-vote country diverging from the winning
  mention's, non-finite confidence handling, and
  no-locations-reduce-to-absence.
- `tests/geography_wiring.rs` end to end against the mock's
  deterministic gazetteer (paris → FR 0.9, berlin → DE 0.9,
  springfield → US 0.4; region votes are evidence shares): bbox
  selection over materialized points, the tie rule visible on the wire,
  country-vote versus point divergence, confidence thresholds, the
  whole-world bbox proving the place-less document is nowhere, the
  no-NER FAILED_PRECONDITION refusal by name, and the ordinary
  undeclared-column refusal on materialized values.
