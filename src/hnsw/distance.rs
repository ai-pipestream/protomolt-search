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
            Self::Avx2Fma => unsafe { avx2::dot_avx2_fma(a, b) },
            // SAFETY: as above, for AVX-512F.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => unsafe { avx512::dot_avx512f(a, b) },
            // SAFETY: NEON is part of the aarch64 baseline this crate
            // targets; `a`/`b` are valid slices.
            #[cfg(target_arch = "aarch64")]
            Self::Neon => unsafe { neon::dot_neon(a, b) },
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
            Self::Avx2Fma => unsafe { avx2::dot_batch4_avx2_fma(q, rows) },
            // SAFETY: as above, for `avx512f`.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => unsafe { avx512::dot_batch4_avx512f(q, rows) },
            // SAFETY: NEON is baseline on `aarch64`; slice lengths are
            // bounded before pointer arithmetic.
            #[cfg(target_arch = "aarch64")]
            Self::Neon => unsafe { neon::dot_batch4_neon(q, rows) },
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
            Self::Avx512f => unsafe { avx512::dot_batch8_avx512f(q, rows) },
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
                avx2::score_nodes_avx2_fma(q, rows, dim, nodes, lines, out);
            },
            // SAFETY: as above, for `avx512f`.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512f => unsafe {
                avx512::score_nodes_avx512f(q, rows, dim, nodes, lines, out);
            },
            // SAFETY: NEON is baseline on `aarch64`; node ids are checked
            // against `rows.len() / dim` before indexing.
            #[cfg(target_arch = "aarch64")]
            Self::Neon => unsafe {
                neon::score_nodes_neon(q, rows, dim, nodes, lines, out);
            },
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod avx2;
#[cfg(target_arch = "x86_64")]
mod avx512;
#[cfg(target_arch = "aarch64")]
mod neon;
mod scalar;
mod slack;

use scalar::score_nodes_scalar;
pub use scalar::{dot_batch4_scalar, dot_scalar, prefetch, prefetch_bytes};
#[cfg(any(feature = "net", test))]
pub(crate) use slack::simd_score_floor;
pub(crate) use slack::{dot_underflow_slack, kernel_dot_slack_per_norm, kernel_row_slack};

#[cfg(test)]
#[path = "distance_tests.rs"]
mod tests;
