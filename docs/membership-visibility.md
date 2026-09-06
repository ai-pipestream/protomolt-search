# Document views in membership planning

The Boolean planner must never start with unrestricted membership and rely on
later result filtering to enforce a document grant. A negative-only group is
particularly sensitive: subtracting its negative clauses from the wrong live
universe can introduce every hidden document into the plan.

`ResolveFilterBitmap`, `ResolveLexicalBitmap` and `ResolveVectorBitmap` now carry
the planner's `DocumentVisibility` separately from user predicates or terms.
Nodes apply that view before returning bitmap bits. They return its canonical
fingerprint, column-known flags, mutation epoch and lifetime incarnation under
the same shard read lock. Present but empty or malformed views refuse, including
on empty shards. The view is internal planner context, not a credential.

## Node membership

Filter membership intersects the authority predicate with the user's CEL/geo
predicate. User-column and authority-column handshakes stay separate. A user
OR, NOT or empty filter cannot widen the authority's universe. Missing values
retain the existing typed filter semantics; a column absent everywhere is
rejected by the coordinator's authority handshake with a generic policy error.

Lexical membership intersects matching body postings with the live authority
view. It retains the same analyzed-term and mapped-analysis identity checks.
Vector membership retains live provider rows when unrestricted. Under a document
view, a vector row must also have document metadata satisfying the predicate;
a vector-only row cannot become authorized through a test for a missing value.
Deletes remain excluded on all three routes.

Existing unrestricted scans retain their paths. Restricted lexical and vector
reads resolve a local membership mask under the read guard; that costs memory
and predicate evaluation proportional to the shard's row domain. This is not a
new sparse-filter optimization. The response bitmap still uses the shard's
physical label range and is internal metadata, not a public result or stable
document identity.

## Coordinator checks

Membership helpers carry their authority-bound document view on every request.
Before merging any bitmap into a plan, they verify the view echo and complete
version claim. If the public query has captured a read set, the bitmap must
match its admitted version for that shard. Changed data fails the whole query;
a newly fetched bitmap cannot silently replace the earlier query version.
The product filter bitmaps supplied to an external vector provider pass the
same metadata checks, without changing that provider's own image protocol.

`MembershipSet.epochs` now follows node order on all three routes. A filter
shard pruned without a read has an empty claim, as does a lexical request that
analyzes to no terms with no mandatory view. With a mandatory view, every shard
supplies the column handshake even when the user's filter would prune it.
Empty lexical terms still perform that handshake when a view is present.

User filter dependencies require field `USE` before membership fan-out. Lexical
membership requires `USE` on `body`; disclosure is not needed merely to select
IDs. The authority predicate may use a column the caller cannot name in their
own filter. Vector-field grants still lack an explicit indexed field name in
the current dense contract, so a field-restricted vector membership call refuses
instead of inventing an implicit grant for the unnamed vector channel.

## Integration boundary

Restricted public `Query` and `QueryStream` remain unavailable. Their remaining
selection paths, candidate scoring, source/lineage reads, aggregation, disclosure
and provisional frames must all consume the same authority before those routes
can be enabled. This change completes the shared membership boundary, not that
full integration. The local coordinator test exercises the internal negative-only
planner; it does not enable a new public authorized route.

Relays still refuse the three bitmap RPCs. Direct node authentication and
network delegation remain separate work. Complete response versions are now
required on filter and vector membership as well as lexical membership; use
matching coordinator and node builds. This adds nine protobuf fields and changes
no stored index, original source or WAL format.

## Evidence

`tests/membership_visibility.rs` checks independent public/private views,
conflicting user predicates and attempts to widen them, vector-only rows,
malformed and unknown views, deletes, single-image and segmented storage,
compaction and reopen. All membership responses are compared with one another
and with their read versions; fresh lifetimes differ after reopening.

Coordinator tests use an authority predicate on a field the caller cannot query,
check a negative-only Boolean plan over two private shards, and reject stale
membership on all three routes. Metadata tests reject missing/wrong view echoes,
incomplete claims and malformed column handshakes before their flags can affect
the merged known-column state.

Validation: 460 library tests, 625 integration tests across 109 targets and
12 embedded tests passed (1,097 total); one existing live-sidecar conformance
test remains ignored. The coordinator's stale-membership test also passed after
its fixture received wider shard spacing to preserve valid ranges after append.
All five Android/iOS Rust target checks, tests/examples compilation, formatting
and vendored-proto checks passed. Descriptor comparison against `e9873c1`
confirms exactly nine additive fields with existing declarations unchanged.
No fleet benchmark, deployment or device-runtime test ran. Stored index, WAL
and original-source formats are unchanged.
