import sys, subprocess, collections, re
prof, binpath = sys.argv[1], sys.argv[2]
maps, rips = [], []
for line in open(prof):
    if line.startswith("# "):
        parts = line[2:].split()
        if len(parts) >= 6:
            a, b = (int(x, 16) for x in parts[0].split("-"))
            maps.append((a, b, int(parts[2], 16), parts[5]))
    else:
        rips.append(int(line, 16))
# PT_LOAD segments of the binary: (offset, vaddr, filesz)
loads = []
for l in subprocess.run(["readelf", "-lW", binpath], capture_output=True, text=True).stdout.splitlines():
    f = l.split()
    if f and f[0] == "LOAD":
        loads.append((int(f[1], 16), int(f[2], 16), int(f[4], 16)))
def to_vaddr(off):
    for o, v, sz in loads:
        if o <= off < o + sz:
            return off - o + v
    return None
counts = collections.Counter(); other = collections.Counter()
for r in rips:
    for a, b, off, path in maps:
        if a <= r < b:
            if path == binpath:
                va = to_vaddr(r - a + off)
                if va is not None:
                    counts[va] += 1
                    break
            other[path.rsplit("/", 1)[-1]] += 1
            break
    else:
        other["[unmapped]"] += 1
addrs = list(counts)
out = subprocess.run(["addr2line", "-f", "-i", "-C", "-a", "-e", binpath] + [hex(a) for a in addrs],
                     capture_output=True, text=True).stdout.splitlines()
# parse: 0xADDR line, then (func, file:line) pairs
frames = {}; cur = None
i = 0
while i < len(out):
    if out[i].startswith("0x"):
        cur = int(out[i], 16); frames[cur] = []; i += 1
    else:
        frames[cur].append((out[i], out[i + 1] if i + 1 < len(out) else "")); i += 2
def short(fn):
    fn = re.sub(r"<[^<>]*>", "", fn); fn = re.sub(r"<[^<>]*>", "", fn)
    fn = re.sub(r"::h[0-9a-f]{16}$", "", fn)
    return fn
total = len(rips)
inner = collections.Counter(); outer = collections.Counter(); line_c = collections.Counter()
for a, c in counts.items():
    fr = frames.get(a) or [("??", "")]
    inner[short(fr[0][0])] += c
    outer[short(fr[-1][0])] += c
    line_c[(short(fr[0][0]), fr[0][1].rsplit("/", 1)[-1].split(" ")[0])] += c

RULES = [
    ("distance kernel (score_nodes/dot_*)", lambda f, p: "hnsw::distance" in f or "core_arch" in f),
    ("heap ops (BinaryHeap push/pop/peek)", lambda f, p: "binary_heap" in p or "BinaryHeap" in f or "sift_" in f),
    ("visited set (visit/seen/begin, retain)", lambda f, p: re.search(r"Scratch>::(visit|seen|begin)|::visit$|::seen$|retain", f) is not None),
    ("links (copy_links/list_start/cells/cap)", lambda f, p: re.search(r"copy_links|list_start|View>::cells|View>::cap|LinkCells|AtomicU32", f) is not None),
    ("mask tests / iteration (Mask::test, try_for_each)", lambda f, p: re.search(r"Mask|try_for_each|bitmap|::test$", f) is not None),
    ("enumerate body (consider/bar/survives)", lambda f, p: "enumerate" in f),
    ("rescore / report / results", lambda f, p: re.search(r"rescore|report|drain_results|exact_|sort|into_sorted|dedup|truncate", f) is not None),
    ("far probe", lambda f, p: "View>::far" in f),
    ("search_layer_aware body (filter-aware walk)", lambda f, p: "search_layer_aware" in f),
    ("search_layer body", lambda f, p: "search_layer" in f),
    ("greedy descent", lambda f, p: "greedy" in f),
    ("walk / search entry", lambda f, p: re.search(r"View>::walk|HnswIndex|::search", f) is not None),
    ("bench harness", lambda f, p: "ann_bench" in f or "protomolt_ann_bench" in f),
]
buckets = collections.Counter(); bucket_detail = collections.defaultdict(collections.Counter)
for a, c in counts.items():
    fr = frames.get(a) or [("??", "")]
    name = None
    for fn, pos in fr:
        for bname, rule in RULES:
            if rule(short(fn), pos):
                name = bname; break
        if name: break
    if not name:
        name = "other: " + short(fr[-1][0])[:60]
    buckets[name] += c
    proj = next((short(fn) for fn, _ in fr if "pipestream_search" in fn), short(fr[-1][0]))
    bucket_detail[name][short(fr[0][0])[:50] + "  <-  " + proj[:70]] += c
print("-- self time by bucket (first match walking the inline chain outward) --")
for b, c in buckets.most_common():
    print(f"{100*c/total:6.2f}%  {b}")
    for d, dc in bucket_detail[b].most_common(4):
        print(f"           {100*dc/total:5.2f}%  {d}")
print()
print(f"samples: {total}  (in binary {sum(counts.values())}, other DSOs {sum(other.values())})")
print("\n-- self time, innermost (inlined) function --")
for f, c in inner.most_common(40):
    print(f"{100*c/total:6.2f}%  {f}")
print("\n-- self time, outermost real symbol --")
for f, c in outer.most_common(15):
    print(f"{100*c/total:6.2f}%  {f}")
print("\n-- other DSOs --")
for f, c in other.most_common(10):
    print(f"{100*c/total:6.2f}%  {f}")
print("\n-- hottest source lines (innermost) --")
for (f, l), c in line_c.most_common(40):
    print(f"{100*c/total:6.2f}%  {l:40s} {f}")
