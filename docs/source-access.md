# Access-controlled local source catalogs

`AccessControlledCatalog` binds a durable local document history to a workspace
and collection. Its constructor and each operation use the existing
`AccessPermit` and `Authorizer`; the store contains resource identity, never
user grants or a cached permission decision.

| Operation | Required action |
|---|---|
| Explicit create or open | Admin |
| Accept a conditional write or retry | Ingest |
| Begin retirement or seal history | Admin |
| Read retirement or seal metadata | Admin |

Admin does not imply Ingest or Search. The wrapper exposes no inner
`DocumentCatalog`, source retrieval, backup, index publication or search handle.
Those adapters still need their own document and field disclosure rules. A
workspace binding does not grant exclusive distributed ownership or authorize
replacement of another source owner.

## Permission through commit

`AccessPermit::pin` requires the provider to hold the exact admitted decision
valid until its guard is dropped. `PolicyAuthority` implements this with the
policy read lock; policy replacement takes the write lock and cannot complete
until admitted synchronous operations release their guards. A stale permit
refuses even when a newer policy would allow the same action; callers reacquire
permission. A retry also needs current Ingest permission before its stored
receipt can be returned.

An authorizer that supports snapshot decisions but has no pin implementation
returns `Unimplemented`. There is no check-then-write fallback. Remote providers
must supply an equivalent enforcement protocol; a watch revision alone cannot
serialize a remote revocation with a local commit.

Acquire the pin before node/catalog locks and the source database writer, and
retain it through the synchronous commit or failure. Never hold it across an
async suspension or call authorization/policy replacement recursively while it
is held: a queued policy writer can make recursive acquisition deadlock.
Long synchronous operations can delay policy replacement; use bounded work and
report enforcement when the provider has actually completed replacement.

## Persistent resource scope

The storage protobuf `SourceResourceBinding` has version 1 and an exact
workspace/collection pair. Catalog header field 8 retains it. Controlled
catalogs use format 7 in every lifecycle state; admission closure and terminal
seal remain represented by their existing markers. Readers predating format 7
refuse it. Current readers validate resource and lifecycle metadata together.

Factories validate the caller's action and resource before opening any file.
Open never creates a missing history. It requires the persisted binding to
match; changing a policy's collection-to-workspace mapping cannot redirect a
newly granted writer into the previous workspace's store. The write transaction
also checks the persisted binding against the open handle's binding after
acquiring the database writer. No API relabels an existing history.

Ordinary `DocumentCatalog::open` refuses a controlled catalog. The controlled
constructor refuses to adopt an unbound catalog. Existing unbound formats and
explicit standalone operation retain their previous behavior. This API protects
application access; it is not encryption or a defense against a process that
can arbitrarily edit the database file.

Retirement and sealing retain format 7 and the resource binding across reopen.
A file copy retains these fences, but making a copy is not permission to
activate a second writer. The [source activation design](source-authority-activation.md)
still requires committed ownership, incarnation and generation checks, complete
history recovery and explicit retirement before replacement. Routing public
source writes and connecting this local guard to that authority remain work
under the foundations goal.

## Verification scope

The focused tests cover action separation, stale decisions, workspace remapping,
refusal by providers without pinning, binding persistence, unsupported ordinary
open, and lifecycle markers. Policy-lock exclusion is checked directly without
thread scheduling assumptions. An observing provider reads the accepted sequence
when its guard drops, proving the source commit precedes permission release.

The focused gate passed 71 catalog unit tests, one direct policy-lock test,
five authorization integration tests, 21 existing catalog integration tests and
eight resource-access integration tests: 106 passed, zero failures. The two new
catalog unit tests also passed after a source-fixture correction, under an
8 GiB hard memory limit with swap disabled (2.51 GiB peak, zero OOM events).
Full regression, mobile and wire compatibility validation is pending. These
local tests do not claim distributed owner or server-route conformance.
