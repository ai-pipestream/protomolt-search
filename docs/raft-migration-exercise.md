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
there is no legacy authority to retire and no store to copy. The exercise
therefore installs the fleet's map directly as a collection's control state
([control state](control-state.md)): one route per shard in file order with
its placement code, and stable-key hash ranges tiled per leaf, because
control state requires ranges and the fleet's map has slot offsets and codes
instead. The fixture is a copy of the fleet's map,
`tests/fixtures/fleet/root-map-v10.toml`. (Until 2026-11-14 the exercise
built a legacy control plane from the map, retired it and imported its
checkpoint; that path had no clients and was deleted in #133.)

Three things in the exercise stand in for what the fleet would supply:

- The workspace and collection are the harness's (`test`/`books`); the
  routes, codes and tree are the fleet's.
- The provider geometry in the install is the harness's placeholder; the
  fleet's is a deployment input the install requires and the exercise does
  not invent.
- The access policy is the harness's (alice Admin and Ingest, bob Admin).

## The run

Every step prints one timed line, so a run with `--nocapture` is its own
evidence; the run of 2026-09-11, on the earlier import path, is in
`/tmp/psearch-import-evidence/step4-exercise-4.log` and three repeats
next to it. In order:

1. **Three voters bootstrapped** over loopback mTLS (the kit's group).
2. **Install.** The fleet's routes, its placement tree and one code per
   route, at generation 10, in one `ControlInstallCommand` through
   `propose_install`, a committed entry.
3. **The committed map.** On every voter, `AuthorityMapSource` (what the
   relay routes on) reads the fleet's seven routes in order with their
   addresses, codes and ranges, and the two-leaf tree, at generation 10.
4. **An owner and two writes.** A managed source prepared, bound,
   confirmed and activated on the group; one write through the leader and
   one through a member that does not lead, under a forwarded lease.
5. **Leader lost with a write in flight.** A write paused inside its
   transaction on the leader; the leader cut from both peers; the
   survivors elect; the write commits inside its lease; the links healed;
   the exact retry replays through the old leader under a lease forwarded
   from the successor.
6. **A member lost and replaced while the image is corrupt.** A voter
   removed and its process stopped; the successor's published image given
   changed bytes; the member prepared on a fresh directory and added: the
   seed is rejected by name (`DataLoss`, digest), the leader verifies and
   rebuilds its image, the second seed installs, the member is promoted,
   three voters again, and it publishes the fleet map.
7. **Cold restart.** Every member shut down, every member started on its
   directory, a leader elected, the map unchanged on every member, the
   managed source recovered through the binary's own recovery function on
   a member that does not lead, and a write admitted there.
The full run takes under five seconds.

## The rollback rule

The exercise builds no automatic rollback. The roots' shard map file stays
where it is and stays authoritative until the operator cuts a root over to
`--raft-map-*`. Before that cutover, stopping the group costs the roots no
state they use. After it, the install decision is the recovery evidence, and
a return to the file map is an operator decision made with it in hand, not a
rollback the software performs.

## What the exercise does not prove

- Durability across power loss on the fleet's filesystems: the fault tests
  exercise process interruption, not simulated power loss.
- The fleet's own provider geometry, policy and collection names: the
  three stand-ins above.
- A deployment. Bootstrapping the group on krick-1, pi5v1 and pi5v3 and
  pointing a root at the committed map is a roll, staged next to the
  running generation like every roll, and the cutover is the user's call.
