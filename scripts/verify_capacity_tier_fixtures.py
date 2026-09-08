#!/usr/bin/env python3
"""Independent fixture verifier for docs/capacity-tiers.md section 10.

Recomputes every pinned literal in the worked examples from the canonical
encodings the document specifies, then requires the document to contain the
recomputed values verbatim. Stdlib only; deterministic; no network, no
workspace state. Exit status 0 means every fixture in the document agrees
with the rules the document states.

Usage: python3 scripts/verify_capacity_tier_fixtures.py [path-to-doc]
"""

import hashlib
import struct
import sys

DOC = sys.argv[1] if len(sys.argv) > 1 else "docs/capacity-tiers.md"

FAILURES = []


def check(name, computed, expected):
    if computed == expected:
        print("PASS %s" % name)
    else:
        FAILURES.append(name)
        print("FAIL %s\n  computed: %s\n  expected: %s" % (name, computed, expected))


def check_in_doc(name, computed, doc):
    ok = computed in doc
    if ok:
        print("PASS %s (pinned in doc)" % name)
    else:
        FAILURES.append(name)
        print("FAIL %s\n  recomputed %s is not pinned verbatim in %s" % (name, computed, DOC))


# Canonical encoding primitives (section 5): u32le/u64le, length-prefixed
# UTF-8 strings, raw 32-byte digests, lists as count plus elements in
# semantic order.

def u32(n):
    assert 0 <= n < 2**32
    return struct.pack("<I", n)


def u64(n):
    assert 0 <= n < 2**64
    return struct.pack("<Q", n)


def s(x):
    b = x.encode("utf-8")
    return u32(len(b)) + b


def sha(b):
    return hashlib.sha256(b).digest()


def hx(b):
    return b.hex()


# ---------------------------------------------------------------- fixtures

T = 1788868800000            # 2026-09-08T12:00:00Z, the frozen planning instant
W0, W1 = T - 600000, T       # the current cohort window [W0, W1)
COHORT_MS = 600000

check("cohort: planning instant is a cohort boundary", T % COHORT_MS, 0)
check("cohort: window start is a cohort boundary", W0 % COHORT_MS, 0)

DECL_TEXT = "key_bucket = hash.fnv64(stable_key()) % 64u"
DECL_FP = sha(DECL_TEXT.encode())

# Policy P (section 10 common fixture), canonical bytes per section 5.
def tier(name, residency, min_replicas, lo, hi, warmth):
    return s(name) + bytes([residency]) + u32(min_replicas) + u64(lo) + u64(hi) + u64(warmth)


TIERS = [
    tier("hot", 1, 3, 100, 1_000_000_000_000, 0),
    tier("warm", 1, 2, 1, 100, 86_400),
    tier("archive", 1, 2, 0, 1, 0),
]
POLICY = b"protomolt.capacity-tier-policy.v1\x00" + u32(1) + u32(3) + b"".join(TIERS)
POLICY_FP = sha(POLICY)
POLICY_SWAPPED = (b"protomolt.capacity-tier-policy.v1\x00" + u32(1) + u32(3)
                  + TIERS[1] + TIERS[0] + TIERS[2])

check("policy: canonical byte length", len(POLICY), 155)

# Incarnations, 16 bytes each.
INC_A = bytes.fromhex("00" * 15 + "0a")   # krick-1 process
INC_B = bytes.fromhex("00" * 15 + "0b")   # pi5v1 process
INC_C = bytes.fromhex("00" * 15 + "0c")   # pi5v3 process
STOR_A1 = bytes.fromhex("00" * 15 + "a1") # krick-1's s6 install
STOR_A2 = bytes.fromhex("00" * 15 + "a2") # krick-1's s7 install
STOR_B1 = bytes.fromhex("00" * 15 + "b1") # pi5v1's s6 install
STOR_B2 = bytes.fromhex("00" * 15 + "b2") # pi5v1's s7 install
STOR_C1 = bytes.fromhex("00" * 15 + "c1") # pi5v3's s6 install

# Observation canonical bytes, fields in the section 3 specification order.
# The stored key is the tuple through storage_incarnation; the set is sorted
# by that key tuple, not by encoded bytes.
def obs(key, rows, rb, scans, sb, p50, p99, last, w0, w1, samples):
    ws, coll, fp, col, bucket, tgen, leaf, shard, sgen, oepoch, node, pinc, sinc = key
    enc = (s(ws) + s(coll) + fp + s(col) + u64(bucket) + u64(tgen) + s(leaf)
           + s(shard) + u64(sgen) + u64(oepoch) + s(node) + pinc + sinc
           + u64(rows) + u64(rb) + u64(scans) + u64(sb) + u64(p50) + u64(p99)
           + u64(last) + u64(w0) + u64(w1) + u32(samples))
    return (key, enc)


def obs_set(entries):
    ordered = sorted(entries, key=lambda e: e[0])
    return (b"protomolt.capacity-observations.v1\x00" + u32(1) + u32(len(ordered))
            + b"".join(e[1] for e in ordered))


def key7(shard, sgen, oepoch, leaf, node, pinc, sinc):
    return ("ws-court", "cases", DECL_FP, "key_bucket", 7, 9, leaf,
            shard, sgen, oepoch, node, pinc, sinc)


def key_b(bucket, shard, sgen, oepoch, leaf, node, pinc, sinc):
    return ("ws-court", "cases", DECL_FP, "key_bucket", bucket, 9, leaf,
            shard, sgen, oepoch, node, pinc, sinc)


# Fixture A: bucket 7 spans leaves L4 (shard s6) and L7 (shard s7).
# Fragment (7, L4): owner krick-1 plus verified replicas on pi5v1, pi5v3.
# Fragment (7, L7): owner pi5v1 plus verified replica on krick-1.
A = [
    obs(key7("s6", 3, 5, "L4", "krick-1", INC_A, STOR_A1),
        1_000_000, 268_435_456, 3_000_000, 786_432_000_000, 1200, 9400,
        T - 5000, W0, W1, 3000),
    obs(key7("s6", 3, 5, "L4", "pi5v1", INC_B, STOR_B1),
        1_000_000, 268_435_456, 0, 0, 0, 0, 0, W0, W1, 0),
    obs(key7("s6", 3, 5, "L4", "pi5v3", INC_C, STOR_C1),
        1_000_000, 268_435_456, 0, 0, 0, 0, 0, W0, W1, 0),
    obs(key7("s7", 3, 2, "L7", "pi5v1", INC_B, STOR_B2),
        500_000, 134_217_728, 0, 0, 0, 0, T - 3_600_000, W0, W1, 0),
    obs(key7("s7", 3, 2, "L7", "krick-1", INC_A, STOR_A2),
        500_000, 134_217_728, 0, 0, 0, 0, 0, W0, W1, 0),
]
OBS_A = sha(obs_set(A))

# Fragment rates under section 5: owner-only bytes, scans over all reporters.
S_L4, B_L4 = 3_000_000, 268_435_456
RATE_L4 = (S_L4 * 10**12) // (600000 * B_L4)
check("fixture A: fragment (7,L4) rate", RATE_L4, 18626)
check("fixture A: fragment (7,L4) classifies hot", 100 <= RATE_L4 < 10**12, True)
RATE_L7 = (0 * 10**12) // (600000 * 134_217_728)
check("fixture A: fragment (7,L7) rate (measured idle)", RATE_L7, 0)
check("fixture A: fragment (7,L7) classifies archive", 0 <= RATE_L7 < 1, True)

# Warm literal: 12,000 scans on the L4 bytes.
RATE_WARM = (12_000 * 10**12) // (600000 * B_L4)
check("warm literal: 12,000 scans rate", RATE_WARM, 74)
check("warm literal: 74 nanos classifies warm", 1 <= RATE_WARM < 100, True)

# Aggregation bounds (section 5): <= 2^20 reports per fragment, u64 fields,
# window <= 2^40 ms, so W*B < 2^124 and S*10^12 < 2^124 fit u128.
check("u128 bound: W*B", (2**40) * (2**84) < 2**128, True)
check("u128 bound: S*10^12", (2**84) * 10**12 < 2**128, True)

# Coverage evidence digests (example-local manifest construction).
COV_L4 = sha(b"example manifest: fragment (key_bucket=7, gen 9, L4), "
             b"source generation 3, rows 1000000, bytes 268435456")
COV_L7 = sha(b"example manifest: fragment (key_bucket=7, gen 9, L7), "
             b"source generation 3, rows 500000, bytes 134217728")

# Fixture M: buckets 5 and 6 in leaf L4, one complete copy each on pi5v3.
M = [
    obs(key_b(5, "s6", 3, 5, "L4", "pi5v3", INC_C, STOR_C1),
        80_000, 21_474_836_480, 0, 0, 0, 0, T - 7_200_000, W0, W1, 0),
    obs(key_b(6, "s6", 3, 5, "L4", "pi5v3", INC_C, STOR_C1),
        72_000, 19_327_352_832, 0, 0, 0, 0, T - 10_800_000, W0, W1, 0),
]
OBS_M = sha(obs_set(M))

# Frozen context shared by both fixtures.
TREE_D = sha(b"example placement tree: gen 9, leaf L4 = {krick-1, pi5v1, pi5v3}, "
             b"leaf L7 = {krick-1, pi5v1}")
PROV_D = sha(b"example provider geometry: turbovec 64-shard mapped image; "
             b"committed free bytes: krick-1=26843545600, pi5v1=16106127360, pi5v3=0")
AUTH_INC = bytes.fromhex("00" * 15 + "01")


def plan_input(instant, max_moves, max_age, skew, epoch, obs_digest):
    return sha(b"protomolt.capacity-plan-input.v1\x00" + u32(1)
               + u64(instant) + u64(max_moves) + u64(max_age) + u64(skew)
               + s("control-0") + AUTH_INC + u64(41) + u64(7) + POLICY_FP
               + u64(epoch) + obs_digest + u64(9) + TREE_D
               + s("ws-court") + s("cases") + DECL_FP + PROV_D)


PLAN_A = plan_input(T, 16, 600_000, 5_000, 118, OBS_A)
PLAN_M = plan_input(T, 16, 600_000, 5_000, 119, OBS_M)

# Move-plan arithmetic for fixture M (section 4's repair loop).
FREE = {"krick-1": 26_843_545_600, "pi5v1": 16_106_127_360}
B5, B6 = 21_474_836_480, 19_327_352_832
check("fixture M: bucket 5 fits only krick-1",
      (B5 <= FREE["krick-1"], B5 <= FREE["pi5v1"]), (True, False))
proj_krick = FREE["krick-1"] - B5
check("fixture M: krick-1 projected free after bucket 5", proj_krick, 5_368_709_120)
check("fixture M: bucket 6 has no eligible destination",
      (B6 <= proj_krick, B6 <= FREE["pi5v1"]), (False, False))

# Staleness boundary (E4).
check("staleness: age == bound is fresh", (T - (T - 600000)) > 600000, False)
check("staleness: age == bound+1 is stale", (T - (T - 600001)) > 600000, True)

# Unequal windows (E10): the misaligned pair is not cohort-aligned.
check("E10: misaligned window start rejected", (T - 660000) % COHORT_MS != 0, True)

# ------------------------------------------------------------ doc pinning

doc = open(DOC, encoding="utf-8").read()
check_in_doc("declaration fingerprint", hx(DECL_FP), doc)
check_in_doc("policy fingerprint", hx(POLICY_FP), doc)
check_in_doc("policy fingerprint (swapped precedence)", hx(sha(POLICY_SWAPPED)), doc)
check_in_doc("fixture A observation-set digest", hx(OBS_A), doc)
check_in_doc("fixture A plan digest", hx(PLAN_A), doc)
check_in_doc("fixture M observation-set digest", hx(OBS_M), doc)
check_in_doc("fixture M plan digest", hx(PLAN_M), doc)
check_in_doc("coverage digest L4", hx(COV_L4), doc)
check_in_doc("coverage digest L7", hx(COV_L7), doc)

print()
if FAILURES:
    print("%d fixture(s) FAILED" % len(FAILURES))
    sys.exit(1)
print("all fixtures agree with the stated rules and the pinned doc values")
