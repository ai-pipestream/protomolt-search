# Sorting and collapse

## Sorting by columns

`QueryRequest.sort` orders the result by stored values instead of by relevance.
Keys are listed most significant first; ties break on the next key, then on doc
id. Each key names an i64 column, an f64 column, a facet column (compared by the
term's bytes), or one of the lineage keys `parent_id` and `group_id`.
`descending` reverses one key.

A document with no value for any of the keys is dropped. Absence has no
defensible position in a value order, and this matches the stance filters take.

Two selection shapes serve a column order:

- **A browse** (filters only). Each shard traverses its admitted set with a
  k-bounded heap over the keys, and the coordinator merges shard by shard, key
  by key.
- **A single lexical leaf.** The documents holding at least one of the leaf's
  analyzed terms are traversed the same way, without scoring. Hits come back
  with `score = 0`, the leaf id in `matched`, and `executed = browse_shard:lexical`.

No pruning happens in either case, so there is no pruning certificate to
invalidate.

A dense or composite selection is rejected with a name: every document is a
candidate there, so a column order over it would be a relevance cut in disguise.
On a sorted lexical leaf, anything that only affects relevance (a phrase
constraint, prefixes, score stages, a boost, the scorer, highlighting) is
rejected and not silently ignored. A column that no shard declares is
rejected by name; a shard that lacks the column contributes no rows.

Each hit reports `sort_values`, one typed value per key, and keeps `sort_key` as
the first key's numeric view.

## Collapse

`QueryRequest.collapse` returns one representative per key value. `k` then means
`k` groups. The key is an i64 column, a facet column, or `parent_id` /
`group_id`. A document with no value for the key forms no group and is dropped.

`inner_hits` states how many hits each group lists, the representative first, with
ranks counting inside the group; 0 lists the representative on its own. Every
group reports `pool_hits` (how many of its hits were in the candidate pool) and
`complete`.

`complete` is true when the group has at least `inner_hits` hits in the pool
(anything outside the pool scores at or below the pool's last hit, so the listed
ones are the best), or when the pool came back short of the requested depth
(no hits follow it). A full pool with fewer listed hits than requested
cannot tell the end of the corpus from a cut, and reports false.

Depth behaves as it does for paging. A single leaf has a depth-independent
order, so the coordinator starts at `selection_k` and doubles the depth, up to
`max_k`, until the page has its groups. A fixed pool (a composite strategy, a
scorer, a boost, an FP32 rerank, a policy-chosen depth) is not deepened, because
its order moves with the pool; a full pool short of the groups the page needs
gives FAILED_PRECONDITION naming `selection_k`.

Paging counts groups: the cursor is the last representative.

A browse rejects collapse, since it has no order to pick a representative by.
Collapse and sort do not combine: collapse picks representatives by relevance,
and a sorted query computes none.

`executed` takes a `+collapse` suffix, and the profile reports `collapse_ms`.

Reference: `docs/query-api.md`.
