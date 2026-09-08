# The committed map feed

Status: implemented 2026-09-08 in `src/source_authority/map_feed.rs`, tests
in `src/source_authority/map_feed_tests.rs`. This is the single-authority
form of the [published map interface](raft-control-design.md#published-map-interface-for-fable):
the producer is the source authority store, the value is produced from
committed rows only, and the consumer rules are the ones a Raft learner's
applied feed will have to satisfy unchanged.

## The value

`PublishedMap` (storage proto `published_map.proto`) carries the authority
identity, the resource, the applied `control_revision`, the
`topology_generation`, the current topology's routes in committed order with
their placement codes and the committed tree (absent for `no_placement`),
the history generations, the committed replicas and nodes (with residency
and failure domain), the resource's logical owners with their committed
phase and — for ACTIVE owners only — the write epoch, and a SHA-256 digest
over the canonical value with the digest field empty. It carries no lease
credential and grants nothing; the write epoch is the fence data-plane
requests must present, checked by the owner's admission, not by the map.

`control_revision` advances on every accepted control change, including
capacity-only and ownership ones. `topology_generation` advances only when
a different map is published; one generation never names two different maps
(the `TOPOLOGIES` rows are immutable per generation).

## Producer

`SourceAuthorityStore::published_map(principal, key)` reads the headers,
policy, control rows and owner rows in one read transaction and returns the
map at the current applied revision; current Admin on the resource is
required to read it. `subscribe_applied()` is a watch over the applied
control revision, published after every commit that moved it — a recorded
rejection commits a decision, not a revision, and wakes nobody. A consumer
waits on the watch, then reads the map; what it reads is at least that
revision and never an uncommitted proposal. Reopen republishes the
committed revision and reproduces the map byte for byte.

## Consumer

`MapConsumer::offer(map)` validates format, identity, digest, nonzero
revisions and code coverage, then applies the rules from the design:

| Frame | Result |
|---|---|
| older revision than held | ignored (`Ok(false)`) |
| same revision, same digest | ignored |
| same revision, different content | error, named by revision |
| newer revision, generation behind the held one | error |
| newer revision, same generation, different routes/codes/tree | error: a generation names one map |
| another authority or resource | error |
| newer revision otherwise | swapped atomically (`Ok(true)`) |

A consumer keeps a query's original map until its readers finish; that is
the caller's discipline, the consumer only swaps.

## What this does not decide

- The transport (subscription frames, reset-after-compaction markers) and
  the relay/server wiring to this feed; locally every frame is a complete
  map, so there is no compaction to reset from.
- Process incarnations of nodes in the map; they are observation state
  (capacity reporters), not committed topology.
- Owner-level route identities for phone owners beyond the logical owner
  row; the map names owners, not their per-device shards.
