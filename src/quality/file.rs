//! Admission of a profile document: the format version, the identity
//! strings, the ladder, the points each rung must justify, and the default
//! target. Everything a file claims beyond what was measured is refused.

use super::{
    DenseQualityProfile, ProfileFile, ProfileIdentity, FORMAT_VERSION, LEGACY_FORMAT_VERSION, PPM,
};

impl DenseQualityProfile {
    pub(super) fn from_file(mut file: ProfileFile, fingerprint: String) -> Result<Self, String> {
        check_header(&file)?;
        file.measurements.sort_by_key(|m| (m.k, m.candidates));
        check_measurements(&file)?;
        file.points
            .sort_by_key(|point| (point.k, point.target_recall_ppm));
        check_points(&file)?;
        check_default_target(&file)?;
        Ok(Self {
            identity: ProfileIdentity {
                profile_id: file.profile_id,
                embedding_model: file.embedding_model,
                corpus_generation: file.corpus_generation,
                corpus_rows: file.corpus_rows,
                dimensions: file.dimensions,
                provider_backend: file.provider_backend,
                scoring_fingerprint: file.scoring_fingerprint,
            },
            measured_queries: file.measured_queries,
            default_target_recall_ppm: file.default_target_recall_ppm,
            measurements: file.measurements,
            points: file.points,
            fingerprint,
        })
    }
}

/// The version, the plain-text identity strings, and the positive sizes.
fn check_header(file: &ProfileFile) -> Result<(), String> {
    if file.format_version != FORMAT_VERSION && file.format_version != LEGACY_FORMAT_VERSION {
        return Err(format!(
                "format_version {} is unsupported; expected {LEGACY_FORMAT_VERSION} or {FORMAT_VERSION}",
                file.format_version
            ));
    }
    if file.format_version == LEGACY_FORMAT_VERSION
        && (file.default_target_recall_ppm.is_some() || !file.measurements.is_empty())
    {
        return Err(format!(
            "format_version {LEGACY_FORMAT_VERSION} carries neither default_target_recall_ppm \
                 nor measurements; write format_version {FORMAT_VERSION}"
        ));
    }
    for (name, value) in [
        ("profile_id", file.profile_id.as_str()),
        ("embedding_model", file.embedding_model.as_str()),
        ("provider_backend", file.provider_backend.as_str()),
        ("scoring_fingerprint", file.scoring_fingerprint.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("{name} must not be empty"));
        }
        if let Some(bad) = value
            .chars()
            .find(|c| c.is_control() || matches!(c, '"' | '\\'))
        {
            return Err(format!(
                "{name} contains {bad:?}; profile strings are plain text without quotes, \
                     backslashes, or control characters"
            ));
        }
    }
    if file.corpus_rows == 0 || file.dimensions == 0 || file.measured_queries == 0 {
        return Err("corpus_rows, dimensions, and measured_queries must all be positive".into());
    }
    if file.points.is_empty() {
        return Err("points must not be empty".into());
    }
    Ok(())
}

/// Every rung: a positive k, a depth between k and the corpus, queries
/// measured, recall in range with the minimum at most the mean, finite
/// non-negative latencies, and no duplicate `(k, candidates)`.
fn check_measurements(file: &ProfileFile) -> Result<(), String> {
    let mut previous = None;
    for m in &file.measurements {
        if m.k == 0 {
            return Err("measurement k must be positive".into());
        }
        if m.candidates < m.k || u64::from(m.candidates) > file.corpus_rows {
            return Err(format!(
                "measurement k={} has invalid candidate depth {} for {} rows",
                m.k, m.candidates, file.corpus_rows
            ));
        }
        if m.queries == 0 {
            return Err(format!(
                "measurement k={} candidates={} measured no queries",
                m.k, m.candidates
            ));
        }
        if m.mean_recall_ppm > PPM || m.min_recall_ppm > PPM || m.min_recall_ppm > m.mean_recall_ppm
        {
            return Err(format!(
                "measurement k={} candidates={} has recall mean={} min={} outside 0..=1000000 \
                     or with the minimum above the mean",
                m.k, m.candidates, m.mean_recall_ppm, m.min_recall_ppm
            ));
        }
        for (name, ms) in [
            ("p50_total_ms", m.p50_total_ms),
            ("p50_selection_ms", m.p50_selection_ms),
            ("p50_rerank_ms", m.p50_rerank_ms),
        ] {
            if !ms.is_finite() || ms < 0.0 {
                return Err(format!(
                    "measurement k={} candidates={} has {name} = {ms:?}; latencies are finite \
                         and non-negative",
                    m.k, m.candidates
                ));
            }
        }
        let key = (m.k, m.candidates);
        if previous == Some(key) {
            return Err(format!(
                "duplicate measurement k={} candidates={}",
                m.k, m.candidates
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

/// Every point: a positive k, a target in range, a depth between k and
/// the corpus, no duplicate `(k, target)`, and with a ladder present a
/// rung that justifies it.
fn check_points(file: &ProfileFile) -> Result<(), String> {
    let mut previous = None;
    for point in &file.points {
        if point.k == 0 {
            return Err("profile point k must be positive".into());
        }
        if !(1..=PPM).contains(&point.target_recall_ppm) {
            return Err(format!(
                "profile point target_recall_ppm {} is outside 1..=1000000",
                point.target_recall_ppm
            ));
        }
        if point.candidates < point.k || u64::from(point.candidates) > file.corpus_rows {
            return Err(format!(
                "profile point k={} target={} has invalid candidate depth {} for {} rows",
                point.k, point.target_recall_ppm, point.candidates, file.corpus_rows
            ));
        }
        let key = (point.k, point.target_recall_ppm);
        if previous == Some(key) {
            return Err(format!(
                "duplicate profile point k={} target_recall_ppm={}",
                point.k, point.target_recall_ppm
            ));
        }
        previous = Some(key);
        if !file.measurements.is_empty() {
            check_justified(file, point)?;
        }
    }
    Ok(())
}

/// With a ladder present every point must be one of its rungs and
/// promise no more than that rung's worst query delivered.
fn check_justified(file: &ProfileFile, point: &super::ProfilePoint) -> Result<(), String> {
    let rung = file
        .measurements
        .iter()
        .find(|m| m.k == point.k && m.candidates == point.candidates)
        .ok_or_else(|| {
            format!(
                "profile point k={} target_recall_ppm={} candidates={} is not \
                             justified: no measurement at that k and depth",
                point.k, point.target_recall_ppm, point.candidates
            )
        })?;
    if rung.min_recall_ppm < point.target_recall_ppm {
        return Err(format!(
            "profile point k={} target_recall_ppm={} candidates={} claims more than \
                         was measured: min_recall_ppm={} over {} queries at that depth",
            point.k, point.target_recall_ppm, point.candidates, rung.min_recall_ppm, rung.queries
        ));
    }
    Ok(())
}

/// A default target must be in range and name a target some point carries.
fn check_default_target(file: &ProfileFile) -> Result<(), String> {
    if let Some(target) = file.default_target_recall_ppm {
        if !(1..=PPM).contains(&target) {
            return Err(format!(
                "default_target_recall_ppm {target} is outside 1..=1000000"
            ));
        }
        if !file.points.iter().any(|p| p.target_recall_ppm == target) {
            let mut targets: Vec<u32> = file.points.iter().map(|p| p.target_recall_ppm).collect();
            targets.sort_unstable();
            targets.dedup();
            return Err(format!(
                    "default_target_recall_ppm {target} names no point; the points carry targets {targets:?}"
                ));
        }
    }
    Ok(())
}
