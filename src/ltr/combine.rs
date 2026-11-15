//! The combination step: one hit's normalized, weighted entries folded
//! under the scorer's operation, each report's contribution filled in.

use crate::pb::{CompositeScoreOperation, DimensionScore};

use super::Scorer;

/// One dimension's `(normalized value, weight, active)` before the fold.
type Entry = (f64, f64, bool);

impl Scorer {
    /// Combine one hit's per-dimension entries, filling each report's
    /// contribution (and the positive-value skips of the geometric and
    /// harmonic operations). A hit with no active dimension scores 0.
    pub(super) fn combine(&self, entries: &mut [Entry], reports: &mut [DimensionScore]) -> f64 {
        use CompositeScoreOperation as Op;
        match self.operation {
            Op::Unspecified => unreachable!("validated"),
            Op::WeightedSum => weighted_sum(entries, reports),
            Op::WeightedMean => weighted_mean(entries, reports),
            Op::Maximum => maximum(entries, reports),
            Op::Product => product(entries, reports),
            Op::GeometricMean => positive_mean(entries, reports, true),
            Op::HarmonicMean => positive_mean(entries, reports, false),
        }
    }
}

/// Each active entry's `w * n` as its contribution, in dimension order.
fn weighted<'a>(
    entries: &'a [Entry],
    reports: &'a mut [DimensionScore],
) -> impl Iterator<Item = f64> + 'a {
    entries
        .iter()
        .zip(reports.iter_mut())
        .filter(|((_, _, active), _)| *active)
        .map(|((n, w, _), report)| {
            report.contribution = w * n;
            report.contribution
        })
}

fn weighted_sum(entries: &[Entry], reports: &mut [DimensionScore]) -> f64 {
    let mut sum = 0.0;
    for contribution in weighted(entries, reports) {
        sum += contribution;
    }
    sum
}

fn active_weight(entries: &[Entry]) -> f64 {
    entries.iter().filter(|e| e.2).map(|e| e.1).sum()
}

fn weighted_mean(entries: &[Entry], reports: &mut [DimensionScore]) -> f64 {
    let total = active_weight(entries);
    if total == 0.0 {
        return 0.0;
    }
    let mut sum = 0.0;
    for ((n, w, active), report) in entries.iter().zip(reports.iter_mut()) {
        if *active {
            report.contribution = w * n / total;
            sum += report.contribution;
        }
    }
    sum
}

fn maximum(entries: &[Entry], reports: &mut [DimensionScore]) -> f64 {
    let mut best: Option<f64> = None;
    for contribution in weighted(entries, reports) {
        best = Some(best.map_or(contribution, |b: f64| b.max(contribution)));
    }
    best.unwrap_or(0.0)
}

fn product(entries: &[Entry], reports: &mut [DimensionScore]) -> f64 {
    let mut product = 1.0;
    let mut any = false;
    for contribution in weighted(entries, reports) {
        product *= contribution;
        any = true;
    }
    if any {
        product
    } else {
        0.0
    }
}

/// The geometric (or harmonic) mean under the blend's positive-value
/// rule: only dimensions with a POSITIVE normalized value participate,
/// weights renormalized over those; none positive scores 0.
fn positive_mean(entries: &mut [Entry], reports: &mut [DimensionScore], geometric: bool) -> f64 {
    for ((n, _, active), report) in entries.iter_mut().zip(reports.iter_mut()) {
        if *active && *n <= 0.0 {
            *active = false;
            report.skipped = true;
        }
    }
    let total = active_weight(entries);
    if total == 0.0 {
        return 0.0;
    }
    let active = entries.iter().zip(reports.iter_mut()).filter(|(e, _)| e.2);
    if geometric {
        let mut product = 1.0;
        for ((n, w, _), report) in active {
            report.contribution = n.powf(w / total);
            product *= report.contribution;
        }
        product
    } else {
        let mut denom = 0.0;
        for ((n, w, _), report) in active {
            report.contribution = w / n;
            denom += report.contribution;
        }
        total / denom
    }
}
