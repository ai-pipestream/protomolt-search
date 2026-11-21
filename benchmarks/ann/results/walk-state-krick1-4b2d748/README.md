# Walk state on krick-1 at 4b2d748

The measurements behind the visited bitset and the bounded integer-key heaps
of the `hnsw` walk (`docs/hnsw.md`, "Walk state"). `summary.txt` has every
table; this file says how they were taken.

Host: krick-1, AMD Ryzen 9 9950X (16 cores, 32 threads), AVX-512F kernel
(`lscpu.txt`). Base revision: `revision`. Every run held the host gate lock,
so no other build ran beside it.

## Binaries

`benchmarks/ann` built with `cargo +nightly build --release --locked` once per
variant, each from the same checkout directory and target directory, with only
`src/hnsw` differing. "before" is `main` at the base revision; "after" is this
change. The other variants were built to choose between designs and are not
in the tree:

- **epoch**: one bit per row in 16-byte words that carry the epoch of the
  walk that last wrote them; a reset bumps the epoch.
- **sorted arrays**: the results as one array sorted best first, the
  candidates as a second sorted array with the ones a full beam had passed
  parked behind it; insertion by binary search and a shift.
- **bounded heap**: the shipped result heap (replace the worst with one sift)
  over `Scored` entries instead of integer keys.

The first touched-word and epoch variants branched on "is this the word's
first row"; both lost up to 11 percent to the stamps on one core. The tables
are the branch-free rewrites.

## Runs

`scripts/ab.sh <out> <rounds> one|all <variant...>` runs the variants in
interleaved rounds over the challenge fixture (`fixture --source=challenge
--queries=500`), or over `synthetic:1000000:64` with 300 queries for the 1M
tables: `--m=16 --ef-construction=200 --ef=16,64,128,256,1000
--recall-stop=2 --min-time-ms=1000`, unfiltered at k = 10, 100 and 1,000, and
`uncorrelated-50` and `uncorrelated-10` at k = 10 and 100 (filtered truth is
100 deep). `one` pins to one CPU with `--threads=1`: a one-thread build is
the same graph for every binary, and `scripts/analyse.py` checks that hops,
scored rows and recall are equal, which they are in every one-core table.
`all` is unpinned with `--threads=32`.

Recall is the same before and after at every beam because the walks are the
same walks, so a comparison at one `ef` is a comparison at matched recall.
The `uncorrelated-10` cells from `ef = 128` up are the enumerate regime (9
hops or none): they do not walk and are there as a control.

`json/` holds the raw results of the before and after tables.

## Profiles

`perf` is refused on this host (`perf_event_paranoid = 4`), so the profiles
use the ITIMER_PROF sampler of `../gap-split-krick1-63268d6/sampler/sampler.c`
at 1 kHz, started after the build, and `scripts/symbolize.py`, that
directory's symbolizer with the buckets extended to the new files. Self time
only. `profiles/` holds both cells, before and after.

`a`: unfiltered, k = 10, ef = 96. `b`: `uncorrelated-50`, k = 100, ef = 128.
Share of samples, and microseconds per query (share over the one-core QPS
under the sampler: a 20,399 before and 23,648 after; b 7,216 and 7,628):

| a: none, k = 10, ef = 96 | before | after | before µs | after µs |
| --- | --- | --- | --- | --- |
| distance kernel | 33.0% | 36.2% | 16.2 | 15.3 |
| result and candidate heaps | 27.0% | 15.7% | 13.2 | 6.7 |
| visited set and neighbor filter | 20.8% | 21.2% | 10.2 | 9.0 |
| layer search body | 6.6% | 13.4% | 3.2 | 5.7 |
| rescore and report | 6.2% | 7.3% | 3.0 | 3.1 |
| link reads (the hop) | 4.4% | 3.9% | 2.2 | 1.6 |
| whole query | | | 49.0 | 42.3 |

| b: uncorrelated-50, k = 100, ef = 128 | before | after | before µs | after µs |
| --- | --- | --- | --- | --- |
| distance kernel | 25.9% | 27.6% | 35.9 | 36.1 |
| visited set and neighbor filter | 19.4% | 18.7% | 26.8 | 24.5 |
| result and candidate heaps | 16.2% | 10.6% | 22.5 | 13.8 |
| mask build and tests | 19.9% | 21.4% | 27.6 | 28.0 |
| layer search body | 11.0% | 14.5% | 15.3 | 18.9 |
| whole query | | | 138.6 | 131.1 |

The layer search body grows because the key conversions and the offer logic
that the compiler used to inline into heap code are charged to it now; heaps
and body together go from 16.5 to 12.3 µs in `a` and from 37.8 to 32.8 µs in
`b`.

## Binary layout

One filtered cell (`uncorrelated-50`, k = 10, ef = 16) read 0.95x in a build
of this change made from a second checkout directory and 1.00x in the build
from the directory "before" was built in, with identical source (table 4).
Every before and after table compares binaries built in one directory.
