//! Conservative kernel-versus-scalar error bounds for floor pre-filtering.

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
