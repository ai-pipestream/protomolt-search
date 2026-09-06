# Relay folds: state (2026-09-06)

Branch `feat/relay-folds-2026-09-r2` from the reconciled contracts at
1565d07 (main 5fdedf3, the dense-membership fix 7c44e28, the scoped
read contracts). The first branch `feat/relay-folds-2026-09` (eb45b61,
the groundwork on main 5fdedf3) is kept as it was.

## Done

- `src/coordinator.rs`: `AggMerge` / `PctMerge` `pub(crate)` with
  `partial()`, rendering the merged state as one shard's partial.
- `src/relay.rs`: `GetDocuments`, `ResolveParents`, `FetchValues`
  (routed by child slot range, caller-order reassembly, every child
  asked where the receipt or the known flags need it), `BrowseShard`
  (pages merged in sort order, cut to `k`), `AggregateShard`,
  `QuantileCounts` (the Boolean plan's claim translated per child), and
  the aggregate inside `EvaluateBoolean` (folded in child order with a
  receipt of its own). Shared pieces: `fan_out`, `in_caller_order`,
  `merge_column_types`, `merge_aggregate_shares`, `merge_browse_pages`,
  `open_children`. Every route translates the claim per child through
  `child_claims`, forwards `DocumentVisibility`, and answers through
  `read_receipt` (fingerprint echo validated, versions checked against
  the claim, one relay token allocated), as the reconciled routes do.
- `HybridShard` still refuses, and the text says why (two-level fusion
  is partition dependent by design).
- `src/visibility.rs`: `FetchValuesResponse` reads like the other
  scoped responses.
- Docs: module doc, `docs/relay-coordinators.md` (the fetches, browse,
  the folds, exactness, "What is not composed yet" shrunk), README.

## Tests

- `tests/relay.rs`: the lexical fixture carries `year` (int) and
  `pages` (double, exact in binary). New: fetches by id through one and
  two levels equal the children (repeated id, foreign id, caller order,
  receipt token round trip, foreign incarnation refused, an unknown
  stage column reported false); browse pages sorted and unsorted with a
  cursor through relay roots equal the flat root; aggregates (count,
  exact sums, extrema, cardinality, group-by, histogram, percentiles)
  through one level, permuted, and two levels equal the flat root, the
  moments through a relay over all and through a chain; the Boolean
  aggregate through relays equals the direct root. `HybridShard` is the
  refusal in `unsupported_routes_refuse_by_name`.
- `src/relay.rs` unit test: `read_receipt` refuses a child off the
  scope, off the claim, or missing.

## Left out

- `stats_fields` / `cardinality_fields` on the keyword routes: refusal
  stands; the fold-state approach would serve them (a later gate).
- A cursor the root mints binds relay tokens under a relay root, so a
  cursor pages with the root that minted it (the test pages each root
  with its own).
