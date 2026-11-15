//! Single-row and multi-row FP32 inner-product kernels with runtime dispatch.
//!
//! The walk and the enumerate pass score rows from the contiguous row store
//! against one query, either four (eight on AVX-512F) at a time so `q` stays
//! in vector registers while independent row streams overlap in the load
//! queue, or one at a time on a tail. The product compiles for the x86-64-v2
//! baseline; the AVX2+FMA and AVX-512F kernels are enabled per function and
//! chosen once per index from `is_x86_feature_detected!`. aarch64 uses NEON,
//! which is part of its baseline. Everything else runs the scalar kernel.
//!
//! Every batch kernel preserves its single-row kernel's exact product and
//! accumulation order per row: `dot_batch4` and `dot_batch8` produce scores
//! bit for bit identical to calling `dot` on each row in turn. Two builds or
//! walks on the same machine therefore visit the exact same nodes whether
//! scored one row or a batch at a time; kernels still differ from one another
//! in the last bits of a sum (different accumulation widths), which is why a
//! fixed-order build is reproducible per kernel, not across machines. The
//! kernels steer the traversal only: every score the provider reports is
//! recomputed by the scalar row-order sum (`crate::exact_vectors::dot`).
//!
//! Error against that scalar sum: every kernel accumulates the same `d`
//! products in some order with FMA or separate rounding, so both it and the
//! scalar sum are within the classic bound `γ(d) · Σ|qᵢxᵢ|` of the real
//! inner product, where `γ(d) = d·u / (1 − d·u)` and `u = 2⁻²⁴` (Higham,
//! Accuracy and Stability of Numerical Algorithms, §3.1). The difference
//! between a kernel and the scalar sum is therefore at most
//! `2·γ(d)·Σ|qᵢxᵢ|`; the tests check each single-row and batch kernel
//! against that bound and check batch bit-identity against `dot`.

/// The dot-product kernel an index runs, fixed when the index is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kernel {
    Scalar,
    #[cfg(target_arch = "x86_64")]
    Avx2Fma,
    #[cfg(target_arch = "x86_64")]
    Avx512f,
    #[cfg(target_arch = "aarch64")]
    Neon,
}

impl Kernel {
    /// The widest kernel this machine runs.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                return Self::Avx512f;
            }
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return Self::Avx2Fma;
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Self::Neon;
        }
        #[allow(unreachable_code)]
        Self::Scalar
    }

    /// Every kernel this machine can run, the scalar one first.
    pub fn available() -> Vec<Self> {
        let mut kernels = vec![Self::Scalar];
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                kernels.push(Self::Avx2Fma);
            }
            if is_x86_feature_detected!("avx512f") {
                kernels.push(Self::Avx512f);
            }
        }
        #[cfg(target_arch = "aarch64")]
        kernels.push(Self::Neon);
        kernels
    }

    /// The kernel's name, for diagnostics.
    pub fn name(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            #[cfg(target_arch = "x86_64")]
            Self::Avx2Fma => "avx2+fma",
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => "avx512f",
            #[cfg(target_arch = "aarch64")]
            Self::Neon => "neon",
        }
    }

    /// Rows scored together by [`Self::score_nodes`].
    #[inline]
    pub const fn batch_width(self) -> usize {
        match self {
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => 8,
            Self::Scalar => 4,
            #[cfg(target_arch = "x86_64")]
            Self::Avx2Fma => 4,
            #[cfg(target_arch = "aarch64")]
            Self::Neon => 1,
        }
    }

    /// The inner product of two rows of equal length.
    #[inline]
    pub fn dot(self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        match self {
            Self::Scalar => dot_scalar(a, b),
            // SAFETY: the variant is only constructed by `detect` after the
            // feature test passed on this machine, and `a`/`b` are valid
            // slices whose lengths the kernels never exceed.
            #[cfg(target_arch = "x86_64")]
            Self::Avx2Fma => unsafe { x86::dot_avx2_fma(a, b) },
            // SAFETY: as above, for AVX-512F.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => unsafe { x86::dot_avx512f(a, b) },
            // SAFETY: NEON is part of the aarch64 baseline this crate
            // targets; `a`/`b` are valid slices.
            #[cfg(target_arch = "aarch64")]
            Self::Neon => unsafe { arm::dot_neon(a, b) },
        }
    }

    /// The inner products of `q` with four rows of equal length, each
    /// accumulated in the exact association order of [`Self::dot`].
    #[inline]
    pub fn dot_batch4(self, q: &[f32], rows: [&[f32]; 4]) -> [f32; 4] {
        debug_assert!(rows.iter().all(|r| r.len() == q.len()));
        match self {
            Self::Scalar => dot_batch4_scalar(q, rows),
            // SAFETY: `Avx2Fma` is only constructed after `avx2` and `fma`
            // were detected on this machine, and every slice length is
            // bounded before pointer arithmetic.
            #[cfg(target_arch = "x86_64")]
            Self::Avx2Fma => unsafe { x86::dot_batch4_avx2_fma(q, rows) },
            // SAFETY: as above, for `avx512f`.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => unsafe { x86::dot_batch4_avx512f(q, rows) },
            // SAFETY: NEON is baseline on `aarch64`; slice lengths are
            // bounded before pointer arithmetic.
            #[cfg(target_arch = "aarch64")]
            Self::Neon => unsafe { arm::dot_batch4_neon(q, rows) },
        }
    }

    /// The inner products of `q` with eight rows of equal length, each
    /// accumulated in the exact association order of [`Self::dot`].
    #[inline]
    pub fn dot_batch8(self, q: &[f32], rows: [&[f32]; 8]) -> [f32; 8] {
        debug_assert!(rows.iter().all(|r| r.len() == q.len()));
        match self {
            // SAFETY: `Avx512f` is only constructed after `avx512f` was
            // detected on this machine, and every slice length is bounded
            // before pointer arithmetic.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => unsafe { x86::dot_batch8_avx512f(q, rows) },
            other => {
                let lo = other.dot_batch4(q, [rows[0], rows[1], rows[2], rows[3]]);
                let hi = other.dot_batch4(q, [rows[4], rows[5], rows[6], rows[7]]);
                [lo[0], lo[1], lo[2], lo[3], hi[0], hi[1], hi[2], hi[3]]
            }
        }
    }

    /// Cache lines of one row to prefetch ahead of scoring (`16` floats per
    /// 64-byte line): capped at `8` on AVX-512F and `4` on AVX2/scalar so one
    /// prefetched batch stays inside L1/L2 MSHR capacity across cores, and `0`
    /// on NEON where `dot_batch4_neon`'s four interleaved demand-load streams
    /// overlap in the load queue without saturating the shared DSU prefetch
    /// queue across cores.
    #[inline]
    pub const fn prefetch_lines(self, dim: usize) -> usize {
        let cap = match self {
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => 8,
            Self::Scalar => 4,
            #[cfg(target_arch = "x86_64")]
            Self::Avx2Fma => 4,
            #[cfg(target_arch = "aarch64")]
            Self::Neon => 0,
        };
        let total = dim.div_ceil(16);
        if total < cap {
            total
        } else {
            cap
        }
    }

    /// Score every node id in `nodes` against `q` from a contiguous
    /// `n_rows × dim` `rows` table, writing `nodes.len()` scores into
    /// `out[..nodes.len()]` in order, each bit-identical to [`Self::dot`].
    ///
    /// Running the full list inside one target-feature boundary keeps the
    /// batch SIMD loops inline without per-batch ABI or slice-packing
    /// overhead. Prefetching is pipelined one batch ahead (`lines = 0`
    /// disables software prefetch when `nodes` are already cache-resident).
    #[inline]
    pub fn score_nodes(
        self,
        q: &[f32],
        rows: &[f32],
        dim: usize,
        nodes: &[u32],
        lines: usize,
        out: &mut [f32],
    ) {
        assert_eq!(q.len(), dim);
        assert!(out.len() >= nodes.len());
        if nodes.is_empty() {
            return;
        }
        if dim == 0 {
            out[..nodes.len()].fill(0.0);
            return;
        }
        let lines = lines.min(dim.div_ceil(16));
        match self {
            Self::Scalar => score_nodes_scalar(q, rows, dim, nodes, lines, out),
            // SAFETY: `Avx2Fma` is only constructed after `avx2` and `fma`
            // were detected on this machine; `score_nodes_avx2_fma` checks
            // every node id against `rows.len() / dim` before indexing.
            #[cfg(target_arch = "x86_64")]
            Self::Avx2Fma => unsafe {
                x86::score_nodes_avx2_fma(q, rows, dim, nodes, lines, out);
            },
            // SAFETY: as above, for `avx512f`.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => unsafe {
                x86::score_nodes_avx512f(q, rows, dim, nodes, lines, out);
            },
            // SAFETY: NEON is baseline on `aarch64`; node ids are checked
            // against `rows.len() / dim` before indexing.
            #[cfg(target_arch = "aarch64")]
            Self::Neon => unsafe {
                arm::score_nodes_neon(q, rows, dim, nodes, lines, out);
            },
        }
    }
}

#[inline(always)]
fn checked_row_ptr(rows_ptr: *const f32, dim: usize, row_cap: usize, node: u32) -> *const f32 {
    let idx = node as usize;
    assert!(idx < row_cap, "node {idx} out of bounds for {row_cap} rows");
    // SAFETY: `idx < rows.len() / dim`, so `idx * dim + dim <= rows.len()`.
    unsafe { rows_ptr.add(idx * dim) }
}

#[inline(always)]
fn prefetch_range(row_ptr: *const f32, start_line: usize, end_line: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        let bytes = row_ptr as *const i8;
        for line in start_line..end_line {
            // SAFETY: `_mm_prefetch` is an architectural cache hint with no
            // effect on memory safety; callers bound `end_line` to
            // `dim.div_ceil(16)` as well.
            unsafe { _mm_prefetch::<_MM_HINT_T0>(bytes.add(line * 64)) };
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = (row_ptr, start_line, end_line);
    }
}

fn score_nodes_scalar(
    q: &[f32],
    rows: &[f32],
    dim: usize,
    nodes: &[u32],
    lines: usize,
    out: &mut [f32],
) {
    let row_cap = rows.len() / dim;
    let rows_ptr = rows.as_ptr();
    if lines > 0 {
        for &v in &nodes[..nodes.len().min(4)] {
            prefetch_range(checked_row_ptr(rows_ptr, dim, row_cap, v), 0, lines);
        }
    }
    let row_slice = |node: u32| -> &[f32] {
        let idx = node as usize;
        assert!(idx < row_cap);
        let start = idx * dim;
        &rows[start..start + dim]
    };
    let mut at = 0usize;
    while at + 4 <= nodes.len() {
        if lines > 0 {
            for &ahead in &nodes[nodes.len().min(at + 4)..nodes.len().min(at + 8)] {
                prefetch_range(checked_row_ptr(rows_ptr, dim, row_cap, ahead), 0, lines);
            }
        }
        let s = dot_batch4_scalar(
            q,
            [
                row_slice(nodes[at]),
                row_slice(nodes[at + 1]),
                row_slice(nodes[at + 2]),
                row_slice(nodes[at + 3]),
            ],
        );
        out[at..at + 4].copy_from_slice(&s);
        at += 4;
    }
    while at < nodes.len() {
        out[at] = dot_scalar(q, row_slice(nodes[at]));
        at += 1;
    }
}

/// Four independent accumulators so the scalar loop is not one long
/// dependency chain; the remainder is summed in order.
pub fn dot_scalar(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    let (a, b) = (&a[..n], &b[..n]);
    let (ca, ra) = a.as_chunks::<4>();
    let (cb, rb) = b.as_chunks::<4>();
    let mut acc = [0.0f32; 4];
    for (x, y) in ca.iter().zip(cb) {
        for lane in 0..4 {
            acc[lane] += x[lane] * y[lane];
        }
    }
    let mut sum = (acc[0] + acc[1]) + (acc[2] + acc[3]);
    for (x, y) in ra.iter().zip(rb) {
        sum += x * y;
    }
    sum
}

/// Four rows scored together in the exact per-row accumulation order of
/// [`dot_scalar`], reusing `q` across all four rows.
pub fn dot_batch4_scalar(q: &[f32], rows: [&[f32]; 4]) -> [f32; 4] {
    let n = rows.iter().fold(q.len(), |m, r| m.min(r.len()));
    let (cq, rq) = q[..n].as_chunks::<4>();
    let (c0, r0) = rows[0][..n].as_chunks::<4>();
    let (c1, r1) = rows[1][..n].as_chunks::<4>();
    let (c2, r2) = rows[2][..n].as_chunks::<4>();
    let (c3, r3) = rows[3][..n].as_chunks::<4>();
    let mut acc = [[0.0f32; 4]; 4];
    for (step, xq) in cq.iter().enumerate() {
        let y0 = &c0[step];
        let y1 = &c1[step];
        let y2 = &c2[step];
        let y3 = &c3[step];
        for lane in 0..4 {
            let v = xq[lane];
            acc[0][lane] += v * y0[lane];
            acc[1][lane] += v * y1[lane];
            acc[2][lane] += v * y2[lane];
            acc[3][lane] += v * y3[lane];
        }
    }
    let mut sum = [
        (acc[0][0] + acc[0][1]) + (acc[0][2] + acc[0][3]),
        (acc[1][0] + acc[1][1]) + (acc[1][2] + acc[1][3]),
        (acc[2][0] + acc[2][1]) + (acc[2][2] + acc[2][3]),
        (acc[3][0] + acc[3][1]) + (acc[3][2] + acc[3][3]),
    ];
    for (k, &v) in rq.iter().enumerate() {
        sum[0] += v * r0[k];
        sum[1] += v * r1[k];
        sum[2] += v * r2[k];
        sum[3] += v * r3[k];
    }
    sum
}

/// Hint the cache that `row` is about to be read: the first `lines` cache
/// lines of it. A no-op on targets without a prefetch instruction.
#[inline]
pub fn prefetch(row: &[f32], lines: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        let bytes = row.as_ptr() as *const i8;
        let available = row.len() * 4;
        let mut at = 0usize;
        for _ in 0..lines {
            if at >= available {
                break;
            }
            // SAFETY: prefetch is a hint and has no effect on memory
            // safety, whatever the address; the pointer stays within the
            // row's bytes here anyway.
            unsafe { _mm_prefetch::<_MM_HINT_T0>(bytes.add(at)) };
            at += 64;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let bytes = row.as_ptr() as *const u8;
        let available = row.len() * 4;
        let mut at = 0usize;
        for _ in 0..lines {
            if at >= available {
                break;
            }
            // SAFETY: `prfm pldl2keep` is a read-only architectural cache
            // hint into per-core L2 that never faults and has no effect on
            // memory safety; `p` stays within `row`'s bytes here as well.
            unsafe {
                let p = bytes.add(at);
                core::arch::asm!(
                    "prfm pldl2keep, [{p}]",
                    p = in(reg) p,
                    options(readonly, nostack, preserves_flags),
                );
            }
            at += 64;
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = (row, lines);
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::{checked_row_ptr, prefetch_range};

    #[target_feature(enable = "avx2")]
    #[inline]
    unsafe fn hsum256(acc: __m256) -> f32 {
        let lo = _mm256_castps256_ps128(acc);
        let hi = _mm256_extractf128_ps::<1>(acc);
        let s = _mm_add_ps(lo, hi);
        let s = _mm_hadd_ps(s, s);
        let s = _mm_hadd_ps(s, s);
        _mm_cvtss_f32(s)
    }

    #[target_feature(enable = "avx2,fma")]
    #[inline]
    unsafe fn dot_avx2_fma_ptr(pa: *const f32, pb: *const f32, n: usize) -> f32 {
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut acc2 = _mm256_setzero_ps();
        let mut acc3 = _mm256_setzero_ps();
        let mut i = 0usize;
        while i + 32 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            acc1 = _mm256_fmadd_ps(
                _mm256_loadu_ps(pa.add(i + 8)),
                _mm256_loadu_ps(pb.add(i + 8)),
                acc1,
            );
            acc2 = _mm256_fmadd_ps(
                _mm256_loadu_ps(pa.add(i + 16)),
                _mm256_loadu_ps(pb.add(i + 16)),
                acc2,
            );
            acc3 = _mm256_fmadd_ps(
                _mm256_loadu_ps(pa.add(i + 24)),
                _mm256_loadu_ps(pb.add(i + 24)),
                acc3,
            );
            i += 32;
        }
        while i + 8 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            i += 8;
        }
        let mut sum = hsum256(_mm256_add_ps(
            _mm256_add_ps(acc0, acc1),
            _mm256_add_ps(acc2, acc3),
        ));
        while i < n {
            sum += *pa.add(i) * *pb.add(i);
            i += 1;
        }
        sum
    }

    /// Eight lanes, four accumulators over 32-value blocks, then one
    /// accumulator over 8-value blocks, then a scalar tail.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn dot_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
        dot_avx2_fma_ptr(a.as_ptr(), b.as_ptr(), a.len().min(b.len()))
    }

    /// Two rows scored in lockstep over the 16 YMM register file: eight
    /// accumulators (`2 × 4`) plus the shared `q` vector stay in registers
    /// without spilling, while each row keeps [`dot_avx2_fma`]'s exact
    /// reduction tree.
    #[target_feature(enable = "avx2,fma")]
    #[inline]
    unsafe fn dot_pair_avx2_fma_ptr(
        pq: *const f32,
        p0: *const f32,
        p1: *const f32,
        n: usize,
    ) -> (f32, f32) {
        let mut a0 = _mm256_setzero_ps();
        let mut a1 = _mm256_setzero_ps();
        let mut a2 = _mm256_setzero_ps();
        let mut a3 = _mm256_setzero_ps();
        let mut c0 = _mm256_setzero_ps();
        let mut c1 = _mm256_setzero_ps();
        let mut c2 = _mm256_setzero_ps();
        let mut c3 = _mm256_setzero_ps();
        let mut i = 0usize;
        while i + 32 <= n {
            let q0 = _mm256_loadu_ps(pq.add(i));
            a0 = _mm256_fmadd_ps(q0, _mm256_loadu_ps(p0.add(i)), a0);
            c0 = _mm256_fmadd_ps(q0, _mm256_loadu_ps(p1.add(i)), c0);
            let q1 = _mm256_loadu_ps(pq.add(i + 8));
            a1 = _mm256_fmadd_ps(q1, _mm256_loadu_ps(p0.add(i + 8)), a1);
            c1 = _mm256_fmadd_ps(q1, _mm256_loadu_ps(p1.add(i + 8)), c1);
            let q2 = _mm256_loadu_ps(pq.add(i + 16));
            a2 = _mm256_fmadd_ps(q2, _mm256_loadu_ps(p0.add(i + 16)), a2);
            c2 = _mm256_fmadd_ps(q2, _mm256_loadu_ps(p1.add(i + 16)), c2);
            let q3 = _mm256_loadu_ps(pq.add(i + 24));
            a3 = _mm256_fmadd_ps(q3, _mm256_loadu_ps(p0.add(i + 24)), a3);
            c3 = _mm256_fmadd_ps(q3, _mm256_loadu_ps(p1.add(i + 24)), c3);
            i += 32;
        }
        while i + 8 <= n {
            let qv = _mm256_loadu_ps(pq.add(i));
            a0 = _mm256_fmadd_ps(qv, _mm256_loadu_ps(p0.add(i)), a0);
            c0 = _mm256_fmadd_ps(qv, _mm256_loadu_ps(p1.add(i)), c0);
            i += 8;
        }
        let mut s0 = hsum256(_mm256_add_ps(_mm256_add_ps(a0, a1), _mm256_add_ps(a2, a3)));
        let mut s1 = hsum256(_mm256_add_ps(_mm256_add_ps(c0, c1), _mm256_add_ps(c2, c3)));
        while i < n {
            let qv = *pq.add(i);
            s0 += qv * *p0.add(i);
            s1 += qv * *p1.add(i);
            i += 1;
        }
        (s0, s1)
    }

    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn dot_batch4_avx2_fma(q: &[f32], rows: [&[f32]; 4]) -> [f32; 4] {
        let pq = q.as_ptr();
        let n01 = q.len().min(rows[0].len()).min(rows[1].len());
        let n23 = q.len().min(rows[2].len()).min(rows[3].len());
        let (s0, s1) = dot_pair_avx2_fma_ptr(pq, rows[0].as_ptr(), rows[1].as_ptr(), n01);
        let (s2, s3) = dot_pair_avx2_fma_ptr(pq, rows[2].as_ptr(), rows[3].as_ptr(), n23);
        [s0, s1, s2, s3]
    }

    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn score_nodes_avx2_fma(
        q: &[f32],
        rows: &[f32],
        dim: usize,
        nodes: &[u32],
        lines: usize,
        out: &mut [f32],
    ) {
        let row_cap = rows.len() / dim;
        let pq = q.as_ptr();
        let rows_ptr = rows.as_ptr();
        if lines > 0 {
            for &v in &nodes[..nodes.len().min(4)] {
                prefetch_range(checked_row_ptr(rows_ptr, dim, row_cap, v), 0, lines);
            }
        }
        let mut at = 0usize;
        while at + 4 <= nodes.len() {
            if lines > 0 {
                for &ahead in &nodes[nodes.len().min(at + 4)..nodes.len().min(at + 8)] {
                    prefetch_range(checked_row_ptr(rows_ptr, dim, row_cap, ahead), 0, lines);
                }
            }
            let p0 = checked_row_ptr(rows_ptr, dim, row_cap, nodes[at]);
            let p1 = checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 1]);
            let p2 = checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 2]);
            let p3 = checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 3]);
            let (s0, s1) = dot_pair_avx2_fma_ptr(pq, p0, p1, dim);
            let (s2, s3) = dot_pair_avx2_fma_ptr(pq, p2, p3, dim);
            out[at] = s0;
            out[at + 1] = s1;
            out[at + 2] = s2;
            out[at + 3] = s3;
            at += 4;
        }
        if at + 2 <= nodes.len() {
            let p0 = checked_row_ptr(rows_ptr, dim, row_cap, nodes[at]);
            let p1 = checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 1]);
            let (s0, s1) = dot_pair_avx2_fma_ptr(pq, p0, p1, dim);
            out[at] = s0;
            out[at + 1] = s1;
            at += 2;
        }
        while at < nodes.len() {
            let pb = checked_row_ptr(rows_ptr, dim, row_cap, nodes[at]);
            out[at] = dot_avx2_fma_ptr(pq, pb, dim);
            at += 1;
        }
    }

    #[target_feature(enable = "avx512f")]
    #[inline]
    unsafe fn dot_avx512f_ptr(pa: *const f32, pb: *const f32, n: usize) -> f32 {
        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();
        let mut i = 0usize;
        while i + 32 <= n {
            acc0 = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)), acc0);
            acc1 = _mm512_fmadd_ps(
                _mm512_loadu_ps(pa.add(i + 16)),
                _mm512_loadu_ps(pb.add(i + 16)),
                acc1,
            );
            i += 32;
        }
        while i + 16 <= n {
            acc0 = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)), acc0);
            i += 16;
        }
        if i < n {
            // The remaining lanes: a mask with one bit per value left.
            let mask: __mmask16 = ((1u32 << (n - i)) - 1) as __mmask16;
            let x = _mm512_maskz_loadu_ps(mask, pa.add(i));
            let y = _mm512_maskz_loadu_ps(mask, pb.add(i));
            acc1 = _mm512_fmadd_ps(x, y, acc1);
        }
        _mm512_reduce_add_ps(_mm512_add_ps(acc0, acc1))
    }

    /// Sixteen lanes, two accumulators over 32-value blocks, one over
    /// 16-value blocks, a masked block for the tail.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn dot_avx512f(a: &[f32], b: &[f32]) -> f32 {
        dot_avx512f_ptr(a.as_ptr(), b.as_ptr(), a.len().min(b.len()))
    }

    #[target_feature(enable = "avx512f")]
    #[inline]
    unsafe fn dot_batch4_avx512f_ptr(
        pq: *const f32,
        p0: *const f32,
        p1: *const f32,
        p2: *const f32,
        p3: *const f32,
        n: usize,
    ) -> [f32; 4] {
        let mut a0 = _mm512_setzero_ps();
        let mut b0 = _mm512_setzero_ps();
        let mut a1 = _mm512_setzero_ps();
        let mut b1 = _mm512_setzero_ps();
        let mut a2 = _mm512_setzero_ps();
        let mut b2 = _mm512_setzero_ps();
        let mut a3 = _mm512_setzero_ps();
        let mut b3 = _mm512_setzero_ps();
        let mut i = 0usize;
        while i + 32 <= n {
            let q0 = _mm512_loadu_ps(pq.add(i));
            a0 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p0.add(i)), a0);
            a1 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p1.add(i)), a1);
            a2 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p2.add(i)), a2);
            a3 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p3.add(i)), a3);
            let q1 = _mm512_loadu_ps(pq.add(i + 16));
            b0 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p0.add(i + 16)), b0);
            b1 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p1.add(i + 16)), b1);
            b2 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p2.add(i + 16)), b2);
            b3 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p3.add(i + 16)), b3);
            i += 32;
        }
        while i + 16 <= n {
            let qv = _mm512_loadu_ps(pq.add(i));
            a0 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p0.add(i)), a0);
            a1 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p1.add(i)), a1);
            a2 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p2.add(i)), a2);
            a3 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p3.add(i)), a3);
            i += 16;
        }
        if i < n {
            let mask: __mmask16 = ((1u32 << (n - i)) - 1) as __mmask16;
            let qv = _mm512_maskz_loadu_ps(mask, pq.add(i));
            b0 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p0.add(i)), b0);
            b1 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p1.add(i)), b1);
            b2 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p2.add(i)), b2);
            b3 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p3.add(i)), b3);
        }
        [
            _mm512_reduce_add_ps(_mm512_add_ps(a0, b0)),
            _mm512_reduce_add_ps(_mm512_add_ps(a1, b1)),
            _mm512_reduce_add_ps(_mm512_add_ps(a2, b2)),
            _mm512_reduce_add_ps(_mm512_add_ps(a3, b3)),
        ]
    }

    /// Four rows in one pass over `q` (8 ZMM accumulators out of 32), each
    /// matching [`dot_avx512f`] bit for bit.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn dot_batch4_avx512f(q: &[f32], rows: [&[f32]; 4]) -> [f32; 4] {
        let n = rows.iter().fold(q.len(), |m, r| m.min(r.len()));
        dot_batch4_avx512f_ptr(
            q.as_ptr(),
            rows[0].as_ptr(),
            rows[1].as_ptr(),
            rows[2].as_ptr(),
            rows[3].as_ptr(),
            n,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[target_feature(enable = "avx512f")]
    #[inline]
    unsafe fn dot_batch8_avx512f_ptr(
        pq: *const f32,
        p0: *const f32,
        p1: *const f32,
        p2: *const f32,
        p3: *const f32,
        p4: *const f32,
        p5: *const f32,
        p6: *const f32,
        p7: *const f32,
        n: usize,
    ) -> [f32; 8] {
        let mut a0 = _mm512_setzero_ps();
        let mut b0 = _mm512_setzero_ps();
        let mut a1 = _mm512_setzero_ps();
        let mut b1 = _mm512_setzero_ps();
        let mut a2 = _mm512_setzero_ps();
        let mut b2 = _mm512_setzero_ps();
        let mut a3 = _mm512_setzero_ps();
        let mut b3 = _mm512_setzero_ps();
        let mut a4 = _mm512_setzero_ps();
        let mut b4 = _mm512_setzero_ps();
        let mut a5 = _mm512_setzero_ps();
        let mut b5 = _mm512_setzero_ps();
        let mut a6 = _mm512_setzero_ps();
        let mut b6 = _mm512_setzero_ps();
        let mut a7 = _mm512_setzero_ps();
        let mut b7 = _mm512_setzero_ps();
        let mut i = 0usize;
        while i + 32 <= n {
            let q0 = _mm512_loadu_ps(pq.add(i));
            a0 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p0.add(i)), a0);
            a1 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p1.add(i)), a1);
            a2 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p2.add(i)), a2);
            a3 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p3.add(i)), a3);
            a4 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p4.add(i)), a4);
            a5 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p5.add(i)), a5);
            a6 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p6.add(i)), a6);
            a7 = _mm512_fmadd_ps(q0, _mm512_loadu_ps(p7.add(i)), a7);
            let q1 = _mm512_loadu_ps(pq.add(i + 16));
            b0 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p0.add(i + 16)), b0);
            b1 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p1.add(i + 16)), b1);
            b2 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p2.add(i + 16)), b2);
            b3 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p3.add(i + 16)), b3);
            b4 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p4.add(i + 16)), b4);
            b5 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p5.add(i + 16)), b5);
            b6 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p6.add(i + 16)), b6);
            b7 = _mm512_fmadd_ps(q1, _mm512_loadu_ps(p7.add(i + 16)), b7);
            i += 32;
        }
        while i + 16 <= n {
            let qv = _mm512_loadu_ps(pq.add(i));
            a0 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p0.add(i)), a0);
            a1 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p1.add(i)), a1);
            a2 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p2.add(i)), a2);
            a3 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p3.add(i)), a3);
            a4 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p4.add(i)), a4);
            a5 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p5.add(i)), a5);
            a6 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p6.add(i)), a6);
            a7 = _mm512_fmadd_ps(qv, _mm512_loadu_ps(p7.add(i)), a7);
            i += 16;
        }
        if i < n {
            let mask: __mmask16 = ((1u32 << (n - i)) - 1) as __mmask16;
            let qv = _mm512_maskz_loadu_ps(mask, pq.add(i));
            b0 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p0.add(i)), b0);
            b1 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p1.add(i)), b1);
            b2 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p2.add(i)), b2);
            b3 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p3.add(i)), b3);
            b4 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p4.add(i)), b4);
            b5 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p5.add(i)), b5);
            b6 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p6.add(i)), b6);
            b7 = _mm512_fmadd_ps(qv, _mm512_maskz_loadu_ps(mask, p7.add(i)), b7);
        }
        [
            _mm512_reduce_add_ps(_mm512_add_ps(a0, b0)),
            _mm512_reduce_add_ps(_mm512_add_ps(a1, b1)),
            _mm512_reduce_add_ps(_mm512_add_ps(a2, b2)),
            _mm512_reduce_add_ps(_mm512_add_ps(a3, b3)),
            _mm512_reduce_add_ps(_mm512_add_ps(a4, b4)),
            _mm512_reduce_add_ps(_mm512_add_ps(a5, b5)),
            _mm512_reduce_add_ps(_mm512_add_ps(a6, b6)),
            _mm512_reduce_add_ps(_mm512_add_ps(a7, b7)),
        ]
    }

    /// Eight rows in one pass over `q` (16 ZMM accumulators out of 32),
    /// each matching [`dot_avx512f`] bit for bit.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn dot_batch8_avx512f(q: &[f32], rows: [&[f32]; 8]) -> [f32; 8] {
        let n = rows.iter().fold(q.len(), |m, r| m.min(r.len()));
        dot_batch8_avx512f_ptr(
            q.as_ptr(),
            rows[0].as_ptr(),
            rows[1].as_ptr(),
            rows[2].as_ptr(),
            rows[3].as_ptr(),
            rows[4].as_ptr(),
            rows[5].as_ptr(),
            rows[6].as_ptr(),
            rows[7].as_ptr(),
            n,
        )
    }

    #[target_feature(enable = "avx512f")]
    pub unsafe fn score_nodes_avx512f(
        q: &[f32],
        rows: &[f32],
        dim: usize,
        nodes: &[u32],
        lines: usize,
        out: &mut [f32],
    ) {
        let row_cap = rows.len() / dim;
        let pq = q.as_ptr();
        let rows_ptr = rows.as_ptr();
        if lines > 0 {
            for &v in &nodes[..nodes.len().min(8)] {
                prefetch_range(checked_row_ptr(rows_ptr, dim, row_cap, v), 0, lines);
            }
        }
        let mut at = 0usize;
        while at + 8 <= nodes.len() {
            if lines > 0 {
                for &ahead in &nodes[nodes.len().min(at + 8)..nodes.len().min(at + 16)] {
                    prefetch_range(checked_row_ptr(rows_ptr, dim, row_cap, ahead), 0, lines);
                }
            }
            let s = dot_batch8_avx512f_ptr(
                pq,
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 1]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 2]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 3]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 4]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 5]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 6]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 7]),
                dim,
            );
            out[at..at + 8].copy_from_slice(&s);
            at += 8;
        }
        if at + 4 <= nodes.len() {
            if lines > 0 {
                for &ahead in &nodes[nodes.len().min(at + 4)..nodes.len().min(at + 8)] {
                    prefetch_range(checked_row_ptr(rows_ptr, dim, row_cap, ahead), 0, lines);
                }
            }
            let s = dot_batch4_avx512f_ptr(
                pq,
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 1]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 2]),
                checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 3]),
                dim,
            );
            out[at..at + 4].copy_from_slice(&s);
            at += 4;
        }
        while at < nodes.len() {
            out[at] = dot_avx512f_ptr(pq, checked_row_ptr(rows_ptr, dim, row_cap, nodes[at]), dim);
            at += 1;
        }
    }
}

#[cfg(target_arch = "aarch64")]
mod arm {
    use std::arch::aarch64::*;

    use super::checked_row_ptr;

    #[inline]
    unsafe fn dot_neon_ptr(pa: *const f32, pb: *const f32, n: usize) -> f32 {
        let mut acc0 = vdupq_n_f32(0.0);
        let mut acc1 = vdupq_n_f32(0.0);
        let mut acc2 = vdupq_n_f32(0.0);
        let mut acc3 = vdupq_n_f32(0.0);
        let mut i = 0usize;
        while i + 16 <= n {
            acc0 = vfmaq_f32(acc0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
            acc1 = vfmaq_f32(acc1, vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
            acc2 = vfmaq_f32(acc2, vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
            acc3 = vfmaq_f32(acc3, vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
            i += 16;
        }
        while i + 4 <= n {
            acc0 = vfmaq_f32(acc0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
            i += 4;
        }
        let acc = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
        let mut sum = vaddvq_f32(acc);
        while i < n {
            sum += *pa.add(i) * *pb.add(i);
            i += 1;
        }
        sum
    }

    /// Four lanes, four accumulators over 16-value blocks, then one, then
    /// a scalar tail.
    pub unsafe fn dot_neon(a: &[f32], b: &[f32]) -> f32 {
        dot_neon_ptr(a.as_ptr(), b.as_ptr(), a.len().min(b.len()))
    }

    #[inline]
    unsafe fn dot_batch4_neon_ptr(
        pq: *const f32,
        p0: *const f32,
        p1: *const f32,
        p2: *const f32,
        p3: *const f32,
        n: usize,
    ) -> [f32; 4] {
        let mut a0 = vdupq_n_f32(0.0);
        let mut a1 = vdupq_n_f32(0.0);
        let mut a2 = vdupq_n_f32(0.0);
        let mut a3 = vdupq_n_f32(0.0);
        let mut b0 = vdupq_n_f32(0.0);
        let mut b1 = vdupq_n_f32(0.0);
        let mut b2 = vdupq_n_f32(0.0);
        let mut b3 = vdupq_n_f32(0.0);
        let mut c0 = vdupq_n_f32(0.0);
        let mut c1 = vdupq_n_f32(0.0);
        let mut c2 = vdupq_n_f32(0.0);
        let mut c3 = vdupq_n_f32(0.0);
        let mut d0 = vdupq_n_f32(0.0);
        let mut d1 = vdupq_n_f32(0.0);
        let mut d2 = vdupq_n_f32(0.0);
        let mut d3 = vdupq_n_f32(0.0);
        let mut i = 0usize;
        while i + 16 <= n {
            let q0 = vld1q_f32(pq.add(i));
            a0 = vfmaq_f32(a0, q0, vld1q_f32(p0.add(i)));
            b0 = vfmaq_f32(b0, q0, vld1q_f32(p1.add(i)));
            c0 = vfmaq_f32(c0, q0, vld1q_f32(p2.add(i)));
            d0 = vfmaq_f32(d0, q0, vld1q_f32(p3.add(i)));
            let q1 = vld1q_f32(pq.add(i + 4));
            a1 = vfmaq_f32(a1, q1, vld1q_f32(p0.add(i + 4)));
            b1 = vfmaq_f32(b1, q1, vld1q_f32(p1.add(i + 4)));
            c1 = vfmaq_f32(c1, q1, vld1q_f32(p2.add(i + 4)));
            d1 = vfmaq_f32(d1, q1, vld1q_f32(p3.add(i + 4)));
            let q2 = vld1q_f32(pq.add(i + 8));
            a2 = vfmaq_f32(a2, q2, vld1q_f32(p0.add(i + 8)));
            b2 = vfmaq_f32(b2, q2, vld1q_f32(p1.add(i + 8)));
            c2 = vfmaq_f32(c2, q2, vld1q_f32(p2.add(i + 8)));
            d2 = vfmaq_f32(d2, q2, vld1q_f32(p3.add(i + 8)));
            let q3 = vld1q_f32(pq.add(i + 12));
            a3 = vfmaq_f32(a3, q3, vld1q_f32(p0.add(i + 12)));
            b3 = vfmaq_f32(b3, q3, vld1q_f32(p1.add(i + 12)));
            c3 = vfmaq_f32(c3, q3, vld1q_f32(p2.add(i + 12)));
            d3 = vfmaq_f32(d3, q3, vld1q_f32(p3.add(i + 12)));
            i += 16;
        }
        while i + 4 <= n {
            let qv = vld1q_f32(pq.add(i));
            a0 = vfmaq_f32(a0, qv, vld1q_f32(p0.add(i)));
            b0 = vfmaq_f32(b0, qv, vld1q_f32(p1.add(i)));
            c0 = vfmaq_f32(c0, qv, vld1q_f32(p2.add(i)));
            d0 = vfmaq_f32(d0, qv, vld1q_f32(p3.add(i)));
            i += 4;
        }
        let mut s0 = vaddvq_f32(vaddq_f32(vaddq_f32(a0, a1), vaddq_f32(a2, a3)));
        let mut s1 = vaddvq_f32(vaddq_f32(vaddq_f32(b0, b1), vaddq_f32(b2, b3)));
        let mut s2 = vaddvq_f32(vaddq_f32(vaddq_f32(c0, c1), vaddq_f32(c2, c3)));
        let mut s3 = vaddvq_f32(vaddq_f32(vaddq_f32(d0, d1), vaddq_f32(d2, d3)));
        while i < n {
            let qv = *pq.add(i);
            s0 += qv * *p0.add(i);
            s1 += qv * *p1.add(i);
            s2 += qv * *p2.add(i);
            s3 += qv * *p3.add(i);
            i += 1;
        }
        [s0, s1, s2, s3]
    }

    /// Four rows in one pass over `q` (16 128-bit NEON accumulators out of
    /// 32), each matching [`dot_neon`] bit for bit.
    pub unsafe fn dot_batch4_neon(q: &[f32], rows: [&[f32]; 4]) -> [f32; 4] {
        let n = rows.iter().fold(q.len(), |m, r| m.min(r.len()));
        dot_batch4_neon_ptr(
            q.as_ptr(),
            rows[0].as_ptr(),
            rows[1].as_ptr(),
            rows[2].as_ptr(),
            rows[3].as_ptr(),
            n,
        )
    }

    /// Score each row sequentially inside one function call: on 128-bit NEON
    /// over shared single-channel LPDDR4X, completing all contiguous cache
    /// lines of one row before starting the next preserves DRAM row-buffer
    /// hits and hardware stride prefetching across cores, whereas interleaving
    /// multiple rows every 64 bytes thrashes DRAM banks under multi-core load.
    pub unsafe fn score_nodes_neon(
        q: &[f32],
        rows: &[f32],
        dim: usize,
        nodes: &[u32],
        _lines: usize,
        out: &mut [f32],
    ) {
        let row_cap = rows.len() / dim;
        let pq = q.as_ptr();
        let rows_ptr = rows.as_ptr();
        for (&node, slot) in nodes.iter().zip(out.iter_mut()) {
            *slot = dot_neon_ptr(pq, checked_row_ptr(rows_ptr, dim, row_cap, node), dim);
        }
    }
}

/// Additive `f64` slack `(dim.max(1) as f64) * f64::from(f32::MIN_POSITIVE)`
/// bounding IEEE-754 FP32 gradual-underflow and FMA-vs-scalar lane rounding
/// across `dim` coordinates near `0.0`.
///
/// For normal-magnitude scores, `~1.18e-38 * dim` falls below the 53-bit `f64`
/// mantissa horizon of the proportional Higham bound, leaving normal slack
/// bit-for-bit unchanged.
#[inline]
pub(crate) const fn dot_underflow_slack(dim: usize) -> f64 {
    let lanes = if dim == 0 { 1 } else { dim };
    (lanes as f64) * (f32::MIN_POSITIVE as f64)
}

/// Proportional Higham error factor `4 * gamma(dim) * ||query||_2` bounding
/// kernel-vs-scalar inner-product drift per unit row norm (`||row||_2`).
#[inline]
pub(crate) fn kernel_dot_slack_per_norm(dim: usize, query: &[f32]) -> f64 {
    let du = dim as f64 / (1u64 << 24) as f64;
    let gamma = du / (1.0 - du);
    let query_norm = query
        .iter()
        .map(|&v| f64::from(v) * f64::from(v))
        .sum::<f64>()
        .sqrt();
    4.0 * gamma * query_norm
}

/// Total conservative kernel-to-scalar error bound for a row with finite
/// positive reciprocal norm `inv_norm`: proportional Higham slack
/// `slack_per_norm / inv_norm` plus [`dot_underflow_slack`] outside the norm
/// division so subnormal products (including split-exponent pairs where
/// `inv_norm` is large) are never falsely pruned before scalar verification.
#[inline]
pub(crate) fn kernel_row_slack(dim: usize, slack_per_norm: f64, inv_norm: f32) -> f64 {
    slack_per_norm / f64::from(inv_norm) + dot_underflow_slack(dim)
}

/// Conservative `f32` pre-filter floor keeping every exact score `>= exact_floor`.
#[cfg(any(feature = "net", test))]
#[inline]
pub(crate) fn simd_score_floor(dim: usize, exact_floor: f32) -> f32 {
    if !exact_floor.is_finite() {
        return exact_floor;
    }
    let f = f64::from(exact_floor);
    let du = dim as f64 / (1u64 << 24) as f64;
    let prop = if du < 1.0 {
        2.0 * (du / (1.0 - du)) * f.abs()
    } else {
        f.abs()
    };
    let safe = f - prop - dot_underflow_slack(dim);
    (safe as f32).next_down()
}

#[cfg(test)]
#[path = "distance_tests.rs"]
mod tests;
