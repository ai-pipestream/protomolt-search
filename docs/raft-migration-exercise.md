# The migration and disaster-recovery exercise

Status: run on disposable state 2026-09-11, `tests/control_raft_migration.rs`,
one test in the release gate. This is step 5 of the
[control-authority design](raft-control-design.md)'s implementation
sequence, "exercise migration and disaster recovery with disposable
state", done against the fleet's own root map rather than a fixture
made up for the purpose. The separately authorized deployment that the
same step names is not part of this; it is the operator's call, after this
exercise has been repeated on the fleet's own state.

## What the fleet has, and what the exercise starts from

The fleet's roots route on a shard map file (`root-map-v10.toml`: six
archive shards on krick-1 and the recent group behind the relay on pi5v1,
a two-leaf placement tree with codes 0 and 1 << 54). No root runs the
durable `ClusterControl` state and no member of any Raft group exists, so
there is no legacy authority to retire and no store to copy. The
migration path the design specifies, retire the legacy JSON and import its
checkpoint, is therefore exercised from a legacy control plane built from
the fleet's map: one route per shard in file order with its placement
code, and stable-key hash ranges tiled per leaf, because the legacy plane
requires ranges and the fleet's map has slot offsets and codes
instead. The fixture is a copy of the fleet's map,
`tests/fixtures/fleet/root-map-v10.toml`.

Three things in the exercise stand in for what the fleet would supply:

- The workspace and collection are the harness's (`test`/`books`); the
  routes, codes and tree are the fleet's.
- The provider geometry in the import supplement is the harness's
  placeholder; the fleet's is a deployment input the supplement contract
  requires and the exercise does not invent.
- The access policy is the harness's (alice Admin and Ingest, bob Admin).

## The run

Every step prints one timed line, so a run with `--nocapture` is its own
evidence; the run of 2026-09-11 is in
`/tmp/psearch-import-evidence/step4-exercise-4.log` and three repeats
next to it. In order:

1. **Legacy plane from the fleet map.** `DurableControlPlane::open` on a
   fresh path, bound to the collection, `bootstrap_topology(10, routes)`
   with the seven routes.
2. **Three voters bootstrapped** over loopback mTLS (the kit's group).
3. **Retirement.** `retire_legacy_control` under alice's Admin on the
   leader's store replaces the legacy JSON with the checksummed record;
   reopening the legacy path as a writer is rejected.
4. **Import.** The retirement record plus a supplement with the
   fleet's placement tree and one code per route, in bounded chunks
   through `propose_import` (begin, chunks, commit), every step a
   committed entry.
5. **The committed map.** On every voter, `AuthorityMapSource` (what the
   relay routes on) reads the fleet's seven routes in order with their
   addresses, codes and ranges, and the two-leaf tree, at generation 10.
6. **An owner and two writes.** A managed source prepared, bound,
   confirmed and activated on the group; one write through the leader and
   one through a member that does not lead, under a forwarded lease.
7. **Leader lost with a write in flight.** A write paused inside its
   transaction on the leader; the leader cut from both peers; the
   survivors elect; the write commits inside its lease; the links healed;
   the exact retry replays through the old leader under a lease forwarded
   from the successor.
8. **A member lost and replaced while the image is corrupt.** A voter
   removed and its process stopped; the successor's published image given
   changed bytes; the member prepared on a fresh directory and added: the
   seed is rejected by name (`DataLoss`, digest), the leader verifies and
   rebuilds its image, the second seed installs, the member is promoted,
   three voters again, and it publishes the fleet map.
9. **Cold restart.** Every member shut down, every member started on its
   directory, a leader elected, the map unchanged on every member, the
   managed source recovered through the binary's own recovery function on
   a member that does not lead, and a write admitted there.
10. **Retirement evidence.** With every holder dropped,
    `recover_legacy_retirement` under the same actor and request returns
    the record with the same checkpoint digest.

The full run takes under five seconds.

## The rollback rule

The design allows no automatic rollback from a retired record to the
legacy writer, and the exercise does not build one. For the fleet the rule
is simpler still: the roots' shard map file stays where it is and stays
authoritative until the operator cuts a root over to `--raft-map-*`. Before
that cutover, stopping the group costs the roots no state they use. After it, the
retirement record and the import decision are the recovery evidence the
design names, and a return to the file map is an operator decision made
with them in hand, not a rollback the software performs.

## What the exercise does not prove

- Durability across power loss on the fleet's filesystems: the fault tests
  exercise process interruption, not simulated power loss.
- The fleet's own provider geometry, policy and collection names: the
  three stand-ins above.
- A deployment. Bootstrapping the group on krick-1, pi5v1 and pi5v3 and
  pointing a root at the committed map is a roll, staged next to the
  running generation like every roll, and the cutover is the user's call.
