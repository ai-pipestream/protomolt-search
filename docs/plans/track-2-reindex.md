# Track 2: the v7 rebuild

Written 2026-08-02. This is the operational plan for rebuilding the
86.6M-chunk corpus index. It exists because the last rebuild taught us
that the build itself is the easy part and the traps are all in the
sequencing. Whoever runs this track should read `work-queue.md` section
1.1 first; this document turns that item into a runbook.

## 1. Why this rebuild happens

Three reasons, any one of which would be sufficient:

1. **The engine overtook the index.** Upstream `fd851e5` extended the
   v7 layout (per-block slot bases) without a version bump, so a binary
   built from current `main` rejects every shard in
   `/work/court-corpus/shards-v7` at load. The live cluster runs a
   binary built from `e858f1e` for exactly this reason. The rebuild is
   what lets the cluster return to `main`.
2. **The analyzer changed.** `body_spec()` now carries ACCENT_FOLD and
   the corpus fingerprint moved to `0x55eb_d3a6_febd_2ac3`. The live
   shards were built without it, so accented litigant names are split
   terms today (`Rodriguez` 1,114 documents versus `Rodríguez` 21, and
   neither query reaches the other's documents).
3. **The case-fold gap.** The live shards were built from a sidecar
   that did not fold case into stems, so `court`, `Court`, and `COURT`
   are three terms. The fix (SOURCE_NORMALIZED_STEMS) is in the running
   sidecar; only a rebuild applies it to the corpus.

Reindexing is accepted policy, not an emergency: upstream owns the
format, there are no external clients, and 280 GB rebuilds in roughly
10 hours. We rebuild rather than migrate.

## 2. The cutover is atomic

This is the one rule that, if violated, takes the cluster down again.

The new shards can only be written by a binary from current `main`, and
that same binary cannot serve the old shards. Therefore build and swap
are one motion: write new shards to a new directory, keep the old
cluster serving from the old shards on the `e858f1e` binary throughout,
and only when the new shards verify, stop the old processes and start
the new binary against the new directory. There is no intermediate
state in which one binary serves both generations.

Until cutover, do not rebuild the cluster binary from `main` and
restart it. The `prebump` worktree that produces the `e858f1e` binary
stays alive until the cutover completes.

## 3. Preconditions

Check every one before the first byte is written. Each traces to a
failure we have already had once.

1. **The sidecar is the one you think it is.** The build must run
   against sidecar `main` at `be4db91` or later, and the check is made
   against the running jar, not the source tree: probe the live
   analysis service and confirm it serves normalized stems (term vector
   source 3) and that `analyze_probe --analyzer=ingest` reports
   fingerprint `0x55eb_d3a6_febd_2ac3`. A stale jar under a live JVM
   port was one of the five traps last time; an open port is not
   readiness, and a jar swap under a running JVM does not take effect.
2. **Disk.** New shards need roughly 280 GB alongside the old 280 GB
   until cutover, plus build scratch. Measure free space on the shard
   volume and on the NAS before starting, not during.
3. **Ports are pinned.** Node ports 59300-59307 and coordinator 59291
   are recorded in `/work/court-corpus/cluster-logs/start-cluster.sh`.
   The new cluster reuses them at cutover; the build cluster, if any,
   uses a disjoint range so nothing ephemeral collides.
4. **Source data is where the script says.** `opinions.ndjson` and the
   embeddings directory paths are read from the launch script, not from
   memory. The embeddings-directory mixup was another of the five.
5. **Backup is current.** `opinions.ndjson` was backed up and verified
   byte-identical on the NAS 2026-08-02. If `chunks-full.ndjson` is an
   input to this rebuild, back it up first over the LAN path:
   `rsync -aW` to `192.168.1.211`, never the Tailscale hostname, which
   is 4x slower for bulk transfer.

## 4. Build

The rebuild is scripted, not hand-run; the previous event's script and
its measured disk model are the starting point. Parameters that must
match the launch script: 8 shards, slot offset `i * 21659648`,
`--chunk-blocks=8192 --dim=256 --bit-width=4 --bm25-fields=body,case_name`,
analysis at the live sidecar. Cuts stay block-aligned to the 8192-row
block size, which is what makes the slot arithmetic exact.

What is new in this build relative to the last one:

1. Shards are written by the current `main` engine, so they carry
   per-block slot bases and per-block calibration.
2. Both BM25 fields are stamped with the real analysis fingerprint
   instead of 0. From then on, an analyzer drift between query time and
   ingest time is refused loudly rather than silently scored.

The NLP capture pass (person and location NER, roughly 11 hours) is
deliberately not folded in. That decision is argued in `work-queue.md`
section 1.2 and stands unless the repo service is wired before this
track starts.

Decided 2026-08-02: the rebuild writes a second body column under the
cased analyzer as a standing A/B arm. The A/B machinery is ready for
it, multi-field columns make it affordable, and it cannot be added
later without another full rebuild. The benefit is a permanent
measurement surface for case-sensitivity questions: the SOURCE_STEMS
case-fold trap is exactly the kind of question this arm answers with a
rankdiff instead of an argument. The cost is one more body column's
disk and build time; if free space on the shard volume fails the
two-generations check in section 3, this column is the first thing
dropped. Track 1's facet columns are not expected in time and the
rebuild does not wait for them; a later rebuild carries them, which is
accepted policy.

## 5. Verification, then cutover

Verification happens against the new shards before any old process
stops:

1. Vector count equals document count equals 86,633,399, and every
   shard's section table parses (the previous event verified 14 of 14
   checks; reuse that checklist).
2. Fingerprints on both fields read back as `0x55eb_d3a6_febd_2ac3`.
3. A canary query set runs against a temporary cluster on the disjoint
   port range: the accent pairs (`Rodríguez`/`Rodriguez`) now co-match,
   case variants collapse, and a rankdiff against the live cluster
   shows movement only where the analyzer change predicts it.
4. Latency on the temporary cluster is within family of the live
   baseline: vector p50 near 250 ms, bm25 near 55 ms, hybrid near
   390 ms.

Cutover: stop old nodes, start `main`-built binaries on the pinned
ports against the new directory, run the canary set again, watch the
first minutes for cold-page latency (minutes-long first queries are
cold cache, not regression). Keep the old shards and the `prebump`
worktree until the new cluster has served real traffic for a day, then
reclaim both, and back up the new shards to the NAS over the LAN path.

## 6. After the rebuild

Downstream items this unblocks, in queue order: fingerprint enforcement
becomes real rather than vacuous, the accent and case recall fixes
reach users, the cluster returns to a binary anyone can rebuild from
`main`, and the analyzer A/B arms (cased column if adopted, DEHYPHENATE
and NFKC arms if a PDF corpus ever arrives) have their baseline. The
NER capture pass (track 2's sibling in section 1.2) can run any time
after, independent of cutover.
