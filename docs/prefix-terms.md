# Prefix terms and string ranges

Implemented on branch 2026-09-02 (roadmap item 6). One structural fact underlies both
features: every dictionary in a `.bm25` file is in **byte order**. The term
directory always was (its lookup is a binary search); facet, map-key, and
map-value dictionaries are now written in that order at flush too. Byte
order over UTF-8 is code-point order, which is the order stock CEL
compares strings in, so a dictionary range and a CEL string comparison
never disagree.

## Prefix terms

`TermPrefix { prefix, max_expansions }` on `Bm25SearchRequest` (the body),
on a `QueryField` (that field), or on the single lexical leaf of `Query`
adds every term of the field's dictionary that starts with the prefix to
the query's terms. Each expansion is scored as the ordinary term it is:
BM25 sums their contributions with the global statistics of each, and the
hit's occurrences name each matched term. `Bm25SearchResponse.prefix_expansions`
reports what each prefix expanded to. A request with a prefix and no
text is the prefix-only query (`cour*`): the expansions are its whole
term list, and the analyzer is not consulted for text it does not have.

The coordinator normalizes the prefix under the field's analysis spec's
char filters — case folding, accent folding, invisible stripping — and
never its stemmer: the dictionary holds stems, and a prefix of a stem is
what the caller wrote. `SOURCE_STEMS` ignores char filters at ingest, so
the prefix is compared as written there. An absent spec is refused by
name: the sidecar's default chain is not known to the coordinator, and a
prefix normalized under the wrong chain expands to the wrong terms.

Every shard expands the prefix in its own directory (`NodeService.ExpandTermPrefix`):
a binary search to the first term at or above the prefix, then a scan
while the prefix holds. The heap builder walks its ordered map the same
way. The fleet-wide union of the shards' expansions is the term list —
exactly the list a monolithic index would expand — so distributed
scoring equals monolithic scoring bitwise.

The cap is the contract: `max_expansions` (default 128, at most 1024). A
prefix that expands past it on any shard, or in the union, is
`INVALID_ARGUMENT` naming the count. The engine never truncates a prefix
to a quieter match set, and it never linear-scans a dictionary to serve
one. Wildcard matching remains deliberately absent; fuzzy matching is
served as did-you-mean over the same bounded scan (`docs/synonyms.md`),
never as a query term.

Prefixes on a composite strategy, a boolean clause, a boost, or a
`PhraseSearch` field refuse by name, as does a top-level prefix list on a
fused request (put it on the `QueryField` it expands in).

The same lower bound and bounded scan serve autocomplete:
`SearchService.Suggest` (`docs/suggest.md`) returns the terms under a
prefix ranked by df summed over the shards, normalized under the same
char-filter chain and never the stemmer, with `max_scan` playing the
cap's role — past it the request refuses naming the count.

## String ranges and prefixes in CEL

`docs/cel-filters.md` recorded string ordering as blocked on the sorted
dictionary. It now compiles:

| CEL | Compiles to | Resolution |
|---|---|---|
| `court < "b"`, `<=`, `>`, `>=` (either side) | `StringRangePredicate` | one ordinal range of the facet dictionary |
| `court >= "a" && court < "b"` | two ranges under AND | two ordinal ranges |
| `court.startsWith("ca")` | `StringPrefixPredicate` | the ordinal range of the entries sharing the prefix |
| `tags["color"] < "m"`, `tags["color"].startsWith("re")` | the same on a map-facet value | the value dictionary's ordinal range under the key |

Resolution is per shard, once per request, and evaluation is one ordinal
comparison per candidate at the heap gate — no per-document string walk.
Kleene absence is unchanged: a document without the value is UNKNOWN
and does not pass; `!(court < "b")` admits nothing a court-less document
could sneak through. The typo rule is the facet rule: a column no shard
knows refuses by name; a value the corpus never held simply bounds an
empty range.

`endsWith`, `contains`, `matches()` (regex), and unbounded fuzzy or
wildcard matching stay refused by name: a byte-sorted dictionary resolves
prefixes and ranges, not suffixes or substrings, and a regex engine is a
dependency this engine does not link.

## Old files

A file's dictionaries are checked for byte order at open. Every file this
version writes passes; a file written with first-seen ordinals (any
generation before 2026-09-02) opens, serves every query it served before,
and refuses string ordering and prefixes on that column by name
(`FAILED_PRECONDITION`, naming the column and the rebuild). Nothing is
re-sorted on open — that would be a silent format conversion. The heap
builder, whose first-seen dictionary is in memory anyway, answers a
string range as plain ordinal membership computed once per request, so a
shard mid-ingest is never refused; after its first flush the file is in
byte order like every other.

Sorting dictionaries at flush changed the on-disk ordinals of every facet
and map column and nothing else: the reader maps values through the
dictionary, both writers still produce byte-identical files, and a reload
of such a file yields a heap store whose first-seen order is the byte
order. `tests/prefix_terms.rs` and `tests/cel_filters.rs` pin the prefix
expansion against a brute-force dictionary scan, the cap refusal with its
count, distributed equality, the mmap binary-search path, string ranges
against the sorted values, the old-file refusal, and the differential
oracle against `cel-interpreter` on every string ordering it defines.
