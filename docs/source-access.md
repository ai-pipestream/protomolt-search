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
| Attribute an actorless legacy retry decision | Admin |
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
valid until its guard is dropped. `PolicyAuthority` implements this with an
immutable policy epoch and a counted admission guard. Admission checks the full
decision and increments that epoch's count under the short publication lock.
Replacement publishes a new epoch and watched revision, releases the publication
lock, then waits for the previous epoch's admitted operations to finish. A stale permit
refuses even when a newer policy would allow the same action; callers reacquire
permission. A retry also needs current Ingest permission before its stored
receipt can be returned.

New checks and admissions use the new policy while the old epoch drains. An
operation already admitted under the previous epoch may finish its synchronous
commit. Seeing the new watched revision proves publication; only successful
return from `PolicyAuthority::replace` proves the old admissions have drained.
Concurrent replacements remain serialized through that completion boundary,
so a later replacement cannot skip an earlier unfinished drain. A new-epoch
pin does not delay the previous epoch's drain, but does delay the following
replacement's completion.

Admission count exhaustion refuses with `ResourceExhausted` and leaves the
count unchanged. Guard destruction releases admission on both ordinary return
and unwinding. The notification mutex protects wakeup ordering, not mutable
policy state; its poison can be recovered to finish cleanup. Poison of the
replacement serializer instead refuses later replacements by name, because an
earlier replacement may have published without finishing its drain. That error
does not roll back the published revision or report completed revocation.

An authorizer that supports snapshot decisions but has no pin implementation
returns `Unimplemented`. There is no check-then-write fallback. Remote providers
must supply an equivalent enforcement protocol; a watch revision alone cannot
serialize a remote revocation with a local commit.

Acquire the pin before node/catalog locks and the source database writer, and
retain it through the synchronous commit or failure. Never hold it across an
async suspension or call authorization/policy replacement recursively while it
is held: a provider may retain locks, and replacement waits for admitted work.
Long synchronous operations can delay policy replacement; use bounded work and
report enforcement when the provider has actually completed replacement.

## Policy admission validation

Three new public regressions reproduced the old implementation's inability to
publish a policy while an admitted operation retained its read lock. Each
test exited 101 with the intended publication assertion. The first also
reported a worker `SendError` during teardown after the observation timeout;
the unwind test deliberately injected an admitted-operation panic. These
failed invocations remain under `/tmp/psearch-policy-admission-red-*` on the
validation host. Their input hashes were unchanged, peak memory was
1,478,643,712 bytes, and swap/OOM remained zero.

With counted epochs, the focused gate passed four pin unit tests and 47
integration cases across authorization, existing pin contracts, diagnostics,
controlled source access and actor-scoped retries/migration. The source-access
target additionally runs a child-process regression. Actual grant revocation
denies fresh reader requests while allowing a still-granted writer; the old
admitted operation retains its original decision until release. Concurrent
replacement coverage checks both public completion and retention of the
internal replacement serializer during the old epoch's drain. The source
commit-before-guard-release checks remain unchanged and pass.

The focused green scope reached its 8 GiB hard limit with 893 memory-limit
events and zero swap/OOM. A fifth unit test then verified recovery from poison
of the notification-only mutex without skipping a nonzero admission count;
all five pin unit tests passed in a scope peaking at 4,042,563,584 bytes, with
zero swap/OOM. Both gates retained identical before/after hashes for all 648
input files. Evidence uses `/tmp/psearch-policy-admission-green-*` and
`/tmp/psearch-policy-admission-final-pin-*`. These tests establish the local
admission/drain behavior; remote write authorization remains a separate
[network integration requirement](network-ingest-authorization.md).

The complete local gate at `d0e623fc`, incorporating main `02150a7`, passed
688 library tests and all 149 integration targets (897 reported passes and
one existing ignored live-OpenNLP test), 13 embedded tests and two IVF tests:
1,600 reported Rust passes, including the source-access child invocation.
The nine reference-comparator tests, all five Android/iOS compilation checks,
tests/examples compilation, formatting, vendored-proto checks and diff checks
also passed. All 21 product/vendored protobuf files and the original protobuf
fixtures remain byte-identical to `48a4f50`.

The full driver verified clean HEAD and all 648 identical input hashes before
and after the run. Its scope reached the 8 GiB hard limit and recorded 42,041
memory-limit events, with zero swap, OOM or socket-throttling events. Raw
evidence uses `/tmp/psearch-policy-admission-full-*` on the validation host.
This is local correctness validation; it does not establish a fleet rollout,
hosted CI or the unfinished network authorization/owner activation protocol.

## Persistent resource scope

The storage protobuf `SourceResourceBinding` has version 1 and an exact
workspace/collection pair. Catalog header field 8 retains it. Controlled
catalogs use format 8 in every lifecycle state; admission closure and terminal
seal remain represented by their existing markers. Format 7 is the legacy
controlled format with actorless retry records. Older readers refuse format 8.
Current readers validate resource, lifecycle and actor migration metadata together.

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

Retirement and sealing retain the controlled format and resource binding across reopen.
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

The retirement correction originally retained actorless retry keys. The actor
namespace extension below now separates controlled receipts by principal;
legacy records still contain no evidence from which to infer their owner.

The focused regression run reproduced five retirement/seal failures before the
fix. After the change and correction of an older test that expected retry
refusal, 73 catalog unit tests and 32 catalog/access integration tests pass,
including the isolated file-mode child check. A held-writer test verifies the
receipt lookup does not wait for an unrelated database writer. Validation used
unchanged runtime/test hashes, an 8 GiB swap-disabled scope, 3.34 GiB peak memory,
and zero OOM events.

The complete run at `610203c` passed 669 library tests, all 146 integration
targets, the embedded package, all five mobile targets and test/example
compilation. It stopped at formatting drift in the fleet benchmark example
merged from main. Commit `18b62e7` changes only that example's formatting.
The final checks recompiled the example and passed formatting, vendored proto
checks, diff checks and both IVF adapter tests. The preserved full-run manifest
and final-run manifest prove that every other file is unchanged. Combined
reported test results are 1,558 passed and one existing ignored live-OpenNLP
test; the original formatting failure remains recorded as a failed invocation.
Product protobuf files are byte-identical to `c4f5eb4`.

The full run reached its 8 GiB cap and recorded memory-limit and socket-throttling
events, with zero swap and zero OOM events. The final checks peaked at 3.60 GiB
with zero swap and OOM events. This is correctness evidence, not a performance
measurement or a fleet deployment. Actor-scoped retry ownership and network
ingest commit authorization remain unfinished.

## Verification scope

The initial focused tests covered action separation, stale decisions, workspace remapping,
refusal by providers without pinning, binding persistence, unsupported ordinary
open, and lifecycle markers. The original guard's policy-lock exclusion was
checked directly without thread scheduling assumptions. An observing provider reads the accepted sequence
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

## Actor-scoped retry ownership

Controlled acceptance keys the retry decision by the pinned authenticated
principal and exact operation ID, within the persisted workspace/collection
history. The caller cannot supply or override this principal. Principal strings
must be stable subject identifiers, never reassigned to another actor; display
names are unsuitable. The storage bound is 1..16384 UTF-8 bytes for the principal
and 1..1024 bytes for the operation ID. Policy revision is not part of the key:
a permission change does not create another actor or erase its decisions.
Current Ingest permission is still required before any lookup or replay.

Format 8 stores canonical format-1 `ActorOperationKey` protobuf bytes in a
separate `actor_operations` table. The original `operations` table holds only
unattributed legacy decisions. Separate tables prevent a caller's arbitrary
raw operation ID from colliding with an encoded actor key. Request digests
continue to hash the original request, and receipt values retain their exact
serialized bytes. Trusted unbound local catalogs keep their existing raw-key
semantics; ordinary open and acceptance cannot bypass the controlled wrapper.

An empty format-7 catalog upgrades atomically during an Admin-authorized open.
A populated format-7 catalog opens for administrative migration but refuses
all controlled acceptance, including retries, until every old decision has an
explicit owner. No principal is guessed, and no old history is deleted.

`assign_legacy_actor` takes a version-1 `SourceActorAssignment` naming the exact
history, operation ID, original request SHA-256 and stable principal. It requires
current pinned Admin permission. Each transaction bounds the stored operation
record to 64 KiB, moves one exact decision into the actor table, and increments
the persisted assignment count. The first
successful assignment captures the old accepted sequence as the migration
watermark and upgrades the format. A failed assignment rolls the whole transaction
back. Repeating the same attribution succeeds; changed digests or another actor
cannot rewrite the assignment. A newly accepted operation beyond the migration
watermark cannot be mistaken for an earlier attribution.

Partial attribution survives restart and checkpoint copying. New acceptance and
receipt disclosure remain fenced until assignment completes. Assignment itself
grants no Ingest permission and does not change accepted versions, source bytes,
receipts, history identity or lifecycle markers. A sealed legacy source can be
attributed, but stays sealed and cannot admit new writes. Earlier backups remain
actorless and require their own explicit attribution before controlled acceptance.

Open and acceptance check operation-table counts against the migration metadata.
An incomplete migration cannot contain acceptances beyond its captured watermark.
The complete source-history audit validates canonical actor keys, checks migrated
receipt counts against their watermark, and uses one sequence-uniqueness proof
across both tables. Checkpoints copy both tables without re-encoding their records.
This local migration does not implement network source routing or distributed
owner activation.

Focused validation passed 77 catalog unit tests, 21 catalog integration tests,
11 source-access tests, two actor-isolation regressions and seven migration
tests. The isolation regressions failed on the preceding implementation before
the fix. The expanded catalog run initially exposed an old assertion expecting
a missing receipt to reach the full history audit; capture now refuses that
inconsistency earlier. The corrected assertion and all other corruption cases
passed in the final run. Both final drivers verified unchanged runtime and test
file hashes. Peak memory was 3.06 GiB and 1.25 GiB respectively, under an 8 GiB
limit with swap disabled and zero OOM events. The additive storage descriptor
comparison passed. Three literal persisted-key fixtures then pinned binary
operation IDs, UTF-8 principal lengths and protobuf key framing.

The complete gate at `ceaa86d`, including main's fleet documentation at
`0c5c7ef`, passed 676 library tests and all 148 integration targets. Across the
library, integration, embedded and IVF logs there were 1,574 reported passes,
zero failures and one existing ignored test requiring a live OpenNLP service.
All five mobile target checks, tests/examples compilation, formatting, vendored
protos and diff checks passed. The descriptor comparison preserved every prior
product declaration; only the actor messages and header field 9 were added.

The driver verified identical HEAD, clean status and hashes for all 644 files
before and after validation. The scope reached its 8 GiB hard limit and recorded
memory-limit events, with zero swap and zero OOM events or kills. Another agent
started a build during this gate; these results establish local correctness,
not performance under an idle host. The following validation record changes
documentation only. This checkpoint does not activate network source routing,
distributed ownership, or a fleet rollout.

The proposed [network ingest authorization boundary](network-ingest-authorization.md)
records why a coordinator guard held across an RPC is insufficient, how durable
source acceptance could establish the authorization decision, and the cancellation,
revocation and partial-progress cases the network integration must prove.
