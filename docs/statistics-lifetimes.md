# Statistics lifetimes and fenced retries

A process-local mutation count cannot identify a shard. The regression in
`tests/stats_cache.rs` constructs two node lifetimes with equal mutation counts
and different document frequencies. Before this change the replacement accepted
the first node's cached statistics and returned a score under the wrong corpus.

`TermStatsResponse` and lexical `MembershipBitmapResponse` now carry a 32-byte
opaque `stats_incarnation`, alongside `stats_epoch`. Each in-memory shard
lifetime generates this identity once using OS randomness. It is retained for
reads and mutations in that lifetime, not persisted into an index or reused
on reopen. Compaction swaps the in-memory shard state and therefore also
changes this identity. Entropy failures refuse statistics publication; clocks
and counters are not fallback identities. Mutation-counter exhaustion rotates
the incarnation and restarts at epoch one instead of wrapping a version.

`Bm25Query`, its streaming form, `Bm25Rescore`, `ShardLegs`, and `HybridShard`
echo both fields, with `expected_stats_incarnation` naming the identity. The
node checks the complete claim under the same guard that reads postings.
The phrase route embeds a BM25 request and uses the same guarded fused scorer.
Nonzero claims without exactly 32 identity bytes refuse. Zero and an empty
identity retain the explicit no-claim protocol for direct callers supplying
statistics themselves; the coordinator's statistics consumers never use that
escape on retry. A valid identity paired with epoch zero also refuses.

The cache requires complete versions even for empty shares. Its per-node entries
retain both fields, and a version change evicts every document visibility scope.
Lexical membership planning carries the same pair into candidate rescoring.
A stale response triggers one fresh fetch and a retry with the newly fetched
version. Another change refuses; continuous writes cannot cause the coordinator
to turn off fencing. Missing identities from older statistics servers refuse
before use or caching. Upgrade a coordinator and all its nodes and relays as
one protocol cohort. No index, WAL, or protobuf source format changes are needed.

Relays issue their own lifetime identity and retain complete child versions
behind the numeric token. A child restart with the same mutation count changes
the tuple. An old parent claim is translated to the old child lifetime and
refused; a refetch obtains a new token. Relay token counter exhaustion refuses
rather than reusing a prior token. The existing numeric clock prefix is not
used as proof of lifetime identity.

These are statistics fencing identities. They do not provide a cross-shard
snapshot, durable document identity, an authorization credential, or cursor
stability across compaction. Document/field grants and the remaining durable
write contract are separate unfinished parts of the foundations work.

Evidence is in `tests/stats_incarnation.rs`, `tests/stats_cache.rs`, the relay
and visibility integration tests, and unit tests in `stats_identity`,
`stats_cache`, `node` and `relay`. The same-address replacement fixture retains
one TCP listener and pooled connection while replacing the complete node
handler. It compares a warm coordinator with a fresh one directly and through
one and two relay levels, and injects replacements at both scoring attempts to
prove that retries remain bounded and fenced.
