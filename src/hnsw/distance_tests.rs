//! Unit tests for SIMD/scalar inner-product kernels and subnormal error bounds.

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

/// Every kernel this machine has agrees with the scalar kernel to within
/// accumulation rounding, and its 4-row and 8-row batch calls match single-row
/// `dot` bit for bit, at every length from 0 to 70.
#[test]
fn kernels_agree_with_scalar_and_batches_are_bit_identical_at_every_length() {
    let kernels = Kernel::available();
    assert!(kernels.contains(&Kernel::detect()));
    for n in 0..=70 {
        let (q, b0) = rows(0x5EED_0000 + n as u64, n);
        let (b1, b2) = rows(0x5EED_1000 + n as u64, n);
        let (b3, b4) = rows(0x5EED_2000 + n as u64, n);
        let (b5, b6) = rows(0x5EED_3000 + n as u64, n);
        let (_, b7) = rows(0x5EED_4000 + n as u64, n);
        let all = [&b0[..], &b1, &b2, &b3, &b4, &b5, &b6, &b7];
        let exact: f64 = q
            .iter()
            .zip(&b0)
            .map(|(x, y)| f64::from(*x) * f64::from(*y))
            .sum();
        for kernel in &kernels {
            let got = kernel.dot(&q, &b0);
            assert!(
                (f64::from(got) - exact).abs() < 1.0e-4 * (1.0 + n as f64),
                "{} at n={n}: {got} vs {exact}",
                kernel.name()
            );
            let batch4 = kernel.dot_batch4(&q, [all[0], all[1], all[2], all[3]]);
            let batch8 = kernel.dot_batch8(&q, all);
            for (lane, &row) in all.iter().enumerate() {
                let single = kernel.dot(&q, row);
                assert_eq!(
                    batch8[lane].to_bits(),
                    single.to_bits(),
                    "{} batch8[{lane}] at n={n}: {} vs {single}",
                    kernel.name(),
                    batch8[lane]
                );
                if lane < 4 {
                    assert_eq!(
                        batch4[lane].to_bits(),
                        single.to_bits(),
                        "{} batch4[{lane}] at n={n}: {} vs {single}",
                        kernel.name(),
                        batch4[lane]
                    );
                }
            }
        }
    }
}

/// `2·γ(d)·Σ|qᵢxᵢ|`, the documented bound on a kernel's distance from the
/// scalar row-order sum on normal-magnitude operands.
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

fn assert_pack_within_bound(kernel: Kernel, name: &str, q: &[f32], pack8: [&[f32]; 8]) {
    let d = q.len();
    let x = pack8[0];
    let scalar = crate::exact_vectors::dot(x, q);
    let limit = bound(q, x);
    let got = kernel.dot(q, x);
    let delta = (f64::from(got) - f64::from(scalar)).abs();
    assert!(
        delta <= limit,
        "{} d={d} {name}: |{got} - {scalar}| = {delta} exceeds {limit}",
        kernel.name()
    );
    let got4 = kernel.dot_batch4(q, [pack8[0], pack8[1], pack8[2], pack8[3]]);
    let got8 = kernel.dot_batch8(q, pack8);
    for (lane, &rx) in pack8.iter().enumerate() {
        let single = kernel.dot(q, rx);
        let row_scalar = crate::exact_vectors::dot(rx, q);
        let row_limit = bound(q, rx);
        assert_eq!(
            got8[lane].to_bits(),
            single.to_bits(),
            "{} d={d} {name} batch8[{lane}] drifted from dot",
            kernel.name()
        );
        assert!(
            (f64::from(got8[lane]) - f64::from(row_scalar)).abs() <= row_limit,
            "{} d={d} {name} batch8[{lane}] exceeded bound",
            kernel.name()
        );
        if lane < 4 {
            assert_eq!(
                got4[lane].to_bits(),
                single.to_bits(),
                "{} d={d} {name} batch4[{lane}] drifted from dot",
                kernel.name()
            );
        }
    }
}

#[test]
fn kernels_stay_within_the_bound_of_the_scalar_scorer_on_adversarial_rows() {
    let kernels = Kernel::available();
    let dims = [
        1usize, 2, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 1023,
    ];
    for &d in &dims {
        let (a, b) = rows(0xADD_0000 + d as u64, d);
        let (extra0, extra1) = rows(0xADD_8000 + d as u64, d);
        let negative: Vec<f32> = a.iter().map(|v| -v.abs()).collect();
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
            let pack8 = [x, &b, &a, &negative, &cancel_x, &tie, &extra0, &extra1];
            for &kernel in &kernels {
                assert_pack_within_bound(kernel, name, q, pack8);
            }
        }
    }
}

#[test]
fn score_nodes_matches_single_row_dot_across_lengths_and_list_sizes() {
    let kernels = Kernel::available();
    let list_sizes = [0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 35];
    let max_nodes = 36usize;
    for d in 0..=70 {
        let (q, _) = rows(0xBEEF_0000 + d as u64, d);
        let mut table = Vec::with_capacity(max_nodes * d);
        for idx in 0..max_nodes {
            let (row, _) = rows(0xBEEF_8000 + (d * 64 + idx) as u64, d);
            table.extend_from_slice(&row);
        }
        for &count in &list_sizes {
            let nodes: Vec<u32> = (0..count)
                .map(|i| ((i * 5 + 3) % max_nodes) as u32)
                .collect();
            for kernel in &kernels {
                for lines in [0usize, kernel.prefetch_lines(d)] {
                    let mut got = vec![f32::NAN; count];
                    kernel.score_nodes(&q, &table, d, &nodes, lines, &mut got);
                    for (pos, &node) in nodes.iter().enumerate() {
                        let start = node as usize * d;
                        let want = kernel.dot(&q, &table[start..start + d]);
                        assert_eq!(
                            got[pos].to_bits(),
                            want.to_bits(),
                            "{} score_nodes(lines={lines}) mismatch at d={d} count={count} pos={pos}",
                            kernel.name()
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn subnormal_products_stay_within_kernel_row_slack_and_simd_score_floor() {
    let d = 64usize;
    let u = f32::from_bits(1);
    let mut q = vec![0.0f32; d];
    q[0] = u;
    q[32] = u;
    let mut row = vec![0.0f32; d];
    row[0] = 1.0;
    row[32] = -0.5;

    let exact = crate::exact_vectors::dot(&row, &q);
    assert_eq!(exact.to_bits(), u.to_bits());

    let slack_per_norm = kernel_dot_slack_per_norm(d, &q);
    for kernel in Kernel::available() {
        let inv_norm = 1.0 / kernel.dot(&row, &row).sqrt();
        let simd = kernel.dot(&q, &row);
        let slack = kernel_row_slack(d, slack_per_norm, inv_norm);
        assert!(
            f64::from(simd) + slack >= f64::from(exact),
            "{} simd={simd} + slack={slack} < exact={exact}",
            kernel.name()
        );
        assert!(
            simd >= simd_score_floor(d, exact),
            "{} simd={simd} < simd_score_floor={}",
            kernel.name(),
            simd_score_floor(d, exact)
        );
    }

    // Normal-magnitude slack remains bit-for-bit identical in f64 addition.
    let (norm_q, _) = rows(0x1234_5678, 128);
    let spn = kernel_dot_slack_per_norm(128, &norm_q);
    let inv = 1.0f32;
    assert_eq!(
        kernel_row_slack(128, spn, inv).to_bits(),
        (spn / f64::from(inv)).to_bits(),
        "subnormal additive slack must stay below f64 ULP of normal-magnitude bounds"
    );
}

#[test]
fn prefetch_accepts_short_rows() {
    prefetch(&[], 4);
    prefetch(&[1.0, 2.0], 4);
    prefetch(&[0.0; 100], 8);
}
