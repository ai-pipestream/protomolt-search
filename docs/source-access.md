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

New Unix source files request mode `0600`, for both standalone and controlled
catalogs, matching the backup writers. A permissive host umask cannot make the
source bytes group- or world-readable. Open preserves existing file permissions;
this is a creation rule, not a permissions migration for existing data.

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

## Retried acceptance after retirement

Admission closure and a terminal seal prevent new source writes. They do not
erase committed retry decisions. An exact accepted request still returns its
original receipt with `replayed=true`, including after reopening or recovering
an abrupt process exit. Changed input under an existing operation ID still
returns `ALREADY_EXISTS`; a new operation remains fenced. A replay neither
advances the accepted sequence nor changes the retired watermark.

The controlled API checks and pins current Ingest permission before looking up
the receipt. Both the read-only retry path and the writer recheck validate the
persisted resource binding and requested history. After a read miss, acceptance
rechecks the operation while holding the database writer before applying the
new-write fence, so a concurrent acceptance followed by retirement cannot hide
the stored decision. Existing retries do not acquire the database writer.

This correction does not scope operation IDs by actor: that separate storage
and authorization change remains required. Existing actorless retry records
contain no evidence from which to infer the authenticated principal.

The focused regression run reproduced five retirement/seal failures before the
fix. After the change and correction of an older test that expected retry
refusal, 73 catalog unit tests and 32 catalog/access integration tests pass,
including the isolated file-mode child check. A held-writer test verifies the
receipt lookup does not wait for an unrelated database writer. Validation used
unchanged runtime/test hashes, an 8 GiB swap-disabled scope, 3.34 GiB peak memory,
and zero OOM events. The complete gate is recorded after it finishes.

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
The complete gate at `7be8a3a` passed 667 library tests, 864 integration tests
across 146 targets, 13 embedded tests and two IVF tests: 1,546 passed and zero
failures. The existing `native_matches_opennlp_contract` test remains ignored
because it requires a live OpenNLP service. All five Android/iOS compilation
checks, test/example compilation, formatting, vendored protos and diff checks
passed. A descriptor comparison against `92b11fb` preserved every existing
search/storage declaration; the resource binding is additive.

The driver verified identical HEAD and hashes for all 636 files before and
after the gate. It ran under an 8 GiB hard limit with swap disabled, reached
that limit, and recorded zero OOM events or kills. Memory-limit events and
socket throttling occurred; this is correctness evidence, not a performance
measurement. These local tests do not claim distributed owner or server-route
conformance.

The subsequent file-creation regression reproduced mode `0666` under an isolated
child's `umask 000`. Requesting `0600` fixed both ordinary and controlled source
creation. The 31 catalog/access integration tests and 71 catalog unit tests
passed afterward, including the child process check. This follow-up changes no
protobuf or stored identity bytes; it was validated with the focused catalog
suite rather than repeating the preceding complete gate.
