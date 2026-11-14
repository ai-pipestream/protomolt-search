# #107 gap split on krick-1 at 63268d6

Host: krick-1, AMD Ryzen 9 9950X, kernel AVX-512F. Every run was pinned with `taskset -c 12-15 nice -n 10`
(profiles: `taskset -c 15`). Fixture: `fixture --source=challenge --queries=500` (format v2, unfiltered
truth 10,000 deep, filtered truth 100 deep). The sweeps used M=16,32, efc=200, k=10,100,
ef=16..512, filters none/uncorrelated-50/near-10, recall-stop 0.995, --threads=4.

- `rust.json`, `rust.log`: hnsw and usearch. `faiss.json`, `faiss.log`: FAISS 1.16 IndexHNSWFlat.
- `summary.txt`: the best one-core point per engine, filter, k and recall, plus the profile breakdowns.
- `profile-*.txt|json|log`: the three profiles. `sampler/`: the profiler and summary scripts
  (perf is blocked on this host by perf_event_paranoid=4).
- `lscpu.txt`, `revision`, `build.log`, `fixture.log`.
