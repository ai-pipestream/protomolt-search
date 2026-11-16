//! AVX-512F kernels: single-row, four-row, eight-row, and node-list scoring.

use std::arch::x86_64::*;

use super::scalar::{checked_row_ptr, prefetch_range};

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

#[target_feature(enable = "avx512f")]
#[inline]
unsafe fn dot_batch8_avx512f_ptr(pq: *const f32, rows: [*const f32; 8], n: usize) -> [f32; 8] {
    let [p0, p1, p2, p3, p4, p5, p6, p7] = rows;
    let zero = _mm512_setzero_ps();
    let (mut a0, mut b0) = (zero, zero);
    let (mut a1, mut b1) = (zero, zero);
    let (mut a2, mut b2) = (zero, zero);
    let (mut a3, mut b3) = (zero, zero);
    let (mut a4, mut b4) = (zero, zero);
    let (mut a5, mut b5) = (zero, zero);
    let (mut a6, mut b6) = (zero, zero);
    let (mut a7, mut b7) = (zero, zero);
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
    dot_batch8_avx512f_ptr(q.as_ptr(), rows.map(<[f32]>::as_ptr), n)
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
        let batch = [
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at]),
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 1]),
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 2]),
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 3]),
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 4]),
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 5]),
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 6]),
            checked_row_ptr(rows_ptr, dim, row_cap, nodes[at + 7]),
        ];
        let s = dot_batch8_avx512f_ptr(pq, batch, dim);
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
