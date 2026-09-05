# Dense result identity

The product-owned node paths return `ScoredHit.identity` from the metadata
snapshot used for scoring. Dense `Query` copies it into hits and terminal
`QueryStream` responses. Imported source keys, versions and optional chunk
ordinals survive positional row changes; legacy rows retain explicit absence.

Classic `SearchShard` captures an immutable identity view under its scan guard,
in solo and coalesced scans. Its final top-k carries identity, which the
coordinator preserves when merging. The classic collapsed path also binds each
representative's identity to its scored row.

This does not complete identity on every route. The remote vector provider,
streaming parent-collapse, hybrid, Boolean and browse results need their own
provenance integration. Provisional revisions remain compact without identity.
Legacy ingest imports metadata but is not the collection-wide version authority.
Logical publication and grants remain tracked in
[search-foundations.md](search-foundations.md).

## Streaming exchange

A source key may be 16 KiB, while a packed candidate is 12 bytes. Instead of
attaching keys to every transient candidate, `StartStreamSearch.identity_limits`
opts into a bounded selection after scanning:

1. `Batch` messages retain their existing packed layout and floor semantics.
2. `IdentityReady` carries the completed scan's certificate. No later batches
   are allowed. This is nonterminal: the node retains matching identity metadata
   and live/filter eligibility, while releasing the shard lock.
3. Once all participants are ready and the global winners are known, the
   coordinator sends `ResolveStreamIdentities` once on every open stream.
   Only winner IDs are requested; an empty selection releases an unused shard.
4. `Identities` returns one entry per requested ID in caller order, including
   explicit absence. IDs outside the captured range, deleted/filtered IDs,
   duplicates and excessive response sizes are refused.
5. `Summary` terminates the stream. The coordinator requires identities first
   and a terminal certificate identical to `IdentityReady`. A missing/changed
   certificate, early close, unexpected message or failed child aborts the query.

The node supports this exchange for ordinary and parent-tagged scans. The
plain dense coordinator opts in; collapsed/decomposed collectors continue to
use the legacy protocol pending their own identity integration.

Without `identity_limits`, nodes preserve `Batch* -> Summary`. A stopped scan
returns `completed=false` without readiness. Stop during the identity wait also
returns an incomplete terminal summary. Closing the request stream during an
opted-in exchange cancels it: retain the sender until resolution finishes.

An upgraded plain dense coordinator requires the handshake. An older node or
relay that ignores the field and sends a terminal summary is refused, not
mistaken for a corpus whose identities are absent. Upgrade nodes/relays before
using that coordinator path. Stored-index formats are unchanged.

## Bounds and consistency

Request limits must be positive. Server maxima are 1,000,000 selected rows,
64 MiB for the encoded identity response, and 60 seconds after scanning.
The plain coordinator requests k rows, 32 MiB and 60 seconds per child.
Oversized results error as a whole, never silently truncate identities. These
bounds do not replace transport message limits or the overall query deadline.

The post-scan timer covers readiness delivery, waiting, response construction
and terminal enqueueing. Construction runs on the blocking pool and checks
deadline/cancellation while resolving rows. Disconnect, cancellation and timeout
release retained metadata. Identity views retain no original payload bytes,
index files or mapped vector images.

Coordinator cancellation gives all request lanes one 250 ms grace period to
enqueue `Stop`, then aborts the owned response readers. A full request lane
cannot make timeout cleanup hang indefinitely. Successful completion also
releases readers if a peer leaves its response stream open after `Summary`.

Eligibility comes from the same guarded scan that resolved filters and the
live bitmap. Fetching today's row after scoring is not a substitute: compaction
or installation may reuse it for another source. The view belongs to this
stream, not a reusable global row token. It does not grant access, pin a catalog
head, or implement missing document/field policy. Those policies must enter
mandatory selection and disclosure before exposing a device/public bridge.

## Relay handoff

Relays supporting the extension must retain child stream ownership through the
exchange, preserve original IDs and route selected IDs to the originating
child's captured view. Wait for every child readiness certificate. Resolve the
parent's winners and release other children with empty selections. Return rows
in the parent's order and issue a terminal certificate only after every child
finishes its own exchange. Bound aggregate memory and propagate remaining time;
never turn timeout into identity absence or fetch current rows from a replica.

Relays without support must refuse opt-in requests; they may still serve the
legacy candidate protocol. This adds no k or relay heap requirement and does
not change the restricted scope in
[the scale-out review](scale-out-coordination-review-2026-09-05.md).

## Validation coverage

`tests/stream_identity.rs` exercises real gRPC: same-row replacement during the
identity wait, lock release, binary keys, legacy absence, deleted and invalid
IDs, duplicates, response limits, timeout, Stop and request closure.
`tests/query_api.rs` compares dense public identities and score bits through
classic/coalesced and streaming scans and terminal query streams.
`tests/compaction.rs` compares both dense paths after compaction, reopen and
replay on single-image and segmented layouts. Existing streaming tests retain
the monolithic bitwise ranking and floor-sharing gates.
Coordinator protocol tests reject wrong IDs, missing or changed certificates,
duplicate replies and premature closure. A full request-lane regression failed
with unbounded cancellation and now verifies timeout cleanup completes.
