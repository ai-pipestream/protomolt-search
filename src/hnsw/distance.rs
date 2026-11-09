//! Single-row FP32 inner-product kernels with runtime dispatch.
//!
//! The walk scores one row at a time against one query, straight from the
//! contiguous row store, so the kernel is a plain dot product over two
//! slices of equal length. The product compiles for the x86-64-v2 baseline;
//! the AVX2+FMA and AVX-512F kernels are enabled per function and chosen
//! once per index from `is_x86_feature_detected!`. aarch64 uses NEON, which
//! is part of its baseline. Everything else runs the scalar kernel.
//!
//! Each kernel is deterministic for a given dimension, so two builds on the
//! same machine score identically; kernels differ from one another in the
//! last bits of a sum (different accumulation order), which is why a
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
//! `2·γ(d)·Σ|qᵢxᵢ|`; the tests check each kernel against that bound.

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

/// Hint the cache that `row` is about to be read: the first `lines` cache
/// lines of it. A no-op on targets without a prefetch intrinsic.
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
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = (row, lines);
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    /// Eight lanes, four accumulators over 32-value blocks, then one
    /// accumulator over 8-value blocks, then a scalar tail.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn dot_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let pa = a.as_ptr();
        let pb = b.as_ptr();
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
        let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
        let lo = _mm256_castps256_ps128(acc);
        let hi = _mm256_extractf128_ps::<1>(acc);
        let s = _mm_add_ps(lo, hi);
        let s = _mm_hadd_ps(s, s);
        let s = _mm_hadd_ps(s, s);
        let mut sum = _mm_cvtss_f32(s);
        while i < n {
            sum += *pa.add(i) * *pb.add(i);
            i += 1;
        }
        sum
    }

    /// Sixteen lanes, two accumulators over 32-value blocks, one over
    /// 16-value blocks, a masked block for the tail.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn dot_avx512f(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let pa = a.as_ptr();
        let pb = b.as_ptr();
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
}

#[cfg(target_arch = "aarch64")]
mod arm {
    use std::arch::aarch64::*;

    /// Four lanes, four accumulators over 16-value blocks, then one, then
    /// a scalar tail.
    pub unsafe fn dot_neon(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let pa = a.as_ptr();
        let pb = b.as_ptr();
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(seed: u64, n: usize) -> (Vec<f32>, Vec<f32>) {
        let mut state = seed;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0
        };
        let a: Vec<f32> = (0..n).map(|_| next()).collect();
        let b: Vec<f32> = (0..n).map(|_| next()).collect();
        (a, b)
    }

    /// Every kernel this machine has agrees with the scalar kernel to
    /// within accumulation rounding, at every length from 0 to 70 (so the
    /// 32-, 16-, 8- and 4-wide blocks and every tail length are covered).
    #[test]
    fn kernels_agree_with_scalar_at_every_length() {
        let kernels = Kernel::available();
        assert!(kernels.contains(&Kernel::detect()));
        for n in 0..=70 {
            let (a, b) = rows(0x5EED_0000 + n as u64, n);
            let exact: f64 = a
                .iter()
                .zip(&b)
                .map(|(x, y)| f64::from(*x) * f64::from(*y))
                .sum();
            for kernel in &kernels {
                let got = kernel.dot(&a, &b);
                assert!(
                    (f64::from(got) - exact).abs() < 1.0e-4 * (1.0 + n as f64),
                    "{} at n={n}: {got} vs {exact}",
                    kernel.name()
                );
            }
        }
    }

    /// `2·γ(d)·Σ|qᵢxᵢ|`, the documented bound on a kernel's distance from
    /// the scalar row-order sum.
    fn bound(q: &[f32], x: &[f32]) -> f64 {
        let d = q.len() as f64;
        let u = 2f64.powi(-24);
        let gamma = d * u / (1.0 - d * u);
        let mass: f64 = q
            .iter()
            .zip(x)
            .map(|(a, b)| (f64::from(*a) * f64::from(*b)).abs())
            .sum();
        2.0 * gamma * mass
    }

    /// Every kernel stays within the documented bound of the scalar sum on
    /// inputs built to expose accumulation order: negative products,
    /// cancellation between large terms of opposite sign, lengths that are
    /// odd or straddle every lane width, and near ties. The scalar sum here
    /// is `exact_vectors::dot`, the scorer every reported score comes from.
    #[test]
    fn kernels_stay_within_the_bound_of_the_scalar_scorer_on_adversarial_rows() {
        let kernels = Kernel::available();
        let dims = [
            1usize, 2, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 1023,
        ];
        for &d in &dims {
            let (a, b) = rows(0xADD_0000 + d as u64, d);
            // Plain random signs: negative scores and mixed products.
            let negative: Vec<f32> = a.iter().map(|v| -v.abs()).collect();
            // Cancellation: large terms of opposite sign around the
            // small rest of the row.
            let mut cancel_q = a.clone();
            let mut cancel_x = b.clone();
            cancel_q[0] = 1.0e6;
            cancel_x[0] = 1.0;
            if d > 1 {
                cancel_q[d - 1] = -1.0e6;
                cancel_x[d - 1] = 1.0;
            }
            if d > 2 {
                cancel_q[d / 2] = 3.0e5;
                cancel_x[d / 2] = -3.3333333;
            }
            // Near ties: two rows differing in one last-place bit.
            let mut tie = b.clone();
            tie[d / 2] = f32::from_bits(tie[d / 2].to_bits() ^ 1);
            let cases: [(&str, &[f32], &[f32]); 5] = [
                ("random", &a, &b),
                ("negative", &negative, &b),
                ("negative-both", &negative, &a),
                ("cancellation", &cancel_q, &cancel_x),
                ("near-tie", &a, &tie),
            ];
            for (name, q, x) in cases {
                let scalar = crate::exact_vectors::dot(x, q);
                let limit = bound(q, x);
                for kernel in &kernels {
                    let got = kernel.dot(q, x);
                    let delta = (f64::from(got) - f64::from(scalar)).abs();
                    assert!(
                        delta <= limit,
                        "{} d={d} {name}: |{got} - {scalar}| = {delta} exceeds {limit}",
                        kernel.name()
                    );
                }
            }
        }
    }

    #[test]
    fn prefetch_accepts_short_rows() {
        prefetch(&[], 4);
        prefetch(&[1.0, 2.0], 4);
        prefetch(&[0.0; 100], 8);
    }
}
