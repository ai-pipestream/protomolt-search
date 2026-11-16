//! AVX2+FMA kernels: single-row, paired-row, and node-list scoring.

use std::arch::x86_64::*;

use super::scalar::{checked_row_ptr, prefetch_range};

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
