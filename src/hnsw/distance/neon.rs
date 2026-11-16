//! NEON kernels: single-row, four-row, and node-list scoring.

use std::arch::aarch64::*;

use super::scalar::checked_row_ptr;

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
