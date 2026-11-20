//! Scalar kernels, bounds-checked row pointers, and cache prefetch hints.

#[inline(always)]
pub(super) fn checked_row_ptr(
    rows_ptr: *const f32,
    dim: usize,
    row_cap: usize,
    node: u32,
) -> *const f32 {
    let idx = node as usize;
    assert!(idx < row_cap, "node {idx} out of bounds for {row_cap} rows");
    // SAFETY: `idx < rows.len() / dim`, so `idx * dim + dim <= rows.len()`.
    unsafe { rows_ptr.add(idx * dim) }
}

#[inline(always)]
pub(super) fn prefetch_range(row_ptr: *const f32, start_line: usize, end_line: usize) {
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

pub(super) fn score_nodes_scalar(
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
    prefetch_bytes(row.as_ptr() as *const u8, row.len() * 4, lines);
}

/// Hint the cache that the first `lines` cache lines of the `available`
/// bytes at `bytes` are about to be read.
#[inline]
pub(crate) fn prefetch_bytes(bytes: *const u8, available: usize, lines: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        let bytes = bytes as *const i8;
        let mut at = 0usize;
        for _ in 0..lines {
            if at >= available {
                break;
            }
            // SAFETY: prefetch is a hint and has no effect on memory
            // safety; `wrapping_add` forms the address without the in-bounds
            // requirement of `add`, so no caller contract is needed.
            unsafe { _mm_prefetch::<_MM_HINT_T0>(bytes.wrapping_add(at)) };
            at += 64;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let mut at = 0usize;
        for _ in 0..lines {
            if at >= available {
                break;
            }
            // SAFETY: `prfm pldl2keep` is a read-only architectural cache
            // hint into per-core L2 that never faults and has no effect on
            // memory safety; `wrapping_add` needs no in-bounds contract.
            unsafe {
                let p = bytes.wrapping_add(at);
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
        let _ = (bytes, available, lines);
    }
}
