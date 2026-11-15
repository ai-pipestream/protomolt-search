//! The strict TOML document form of a dense execution policy, and the
//! validation that turns a parsed document into a
//! [`DenseExecutionPolicy`].

use std::ops::RangeInclusive;

use serde::Deserialize;

use super::{
    DenseExecutionPolicy, PolicyPoint, FORMAT_VERSION, MAX_FINISH_SETTLE_US, UNFILTERED_PPM,
};

/// The band every parts-per-million field of a point must fall in.
const PPM: RangeInclusive<u32> = 1..=UNFILTERED_PPM;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PolicyFile {
    format_version: u32,
    policy_id: String,
    embedding_model: String,
    corpus_generation: u64,
    corpus_rows: u64,
    dimensions: u32,
    provider_backend: String,
    scoring_fingerprint: String,
    measured_queries: u32,
    points: Vec<PointFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PointFile {
    k: u32,
    filter_selectivity_ppm_min: u32,
    filter_selectivity_ppm_max: u32,
    candidates: u32,
    measured_recall_ppm: u32,
    enumerate_max_rows: Option<u32>,
    patience_saturation_ppm: Option<u32>,
    patience_hops: Option<u32>,
    finish_settle_us: Option<u32>,
    floor_greediness_ppm: Option<u32>,
    floor_clamp_min: Option<u32>,
}

impl PolicyFile {
    /// The document-level checks: the format version, the plain-text
    /// string fields, and the positive counts.
    fn validate_header(&self) -> Result<(), String> {
        if self.format_version != FORMAT_VERSION {
            return Err(format!(
                "format_version {} is unsupported; expected {FORMAT_VERSION}",
                self.format_version
            ));
        }
        for (name, value) in [
            ("policy_id", self.policy_id.as_str()),
            ("embedding_model", self.embedding_model.as_str()),
            ("provider_backend", self.provider_backend.as_str()),
            ("scoring_fingerprint", self.scoring_fingerprint.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{name} must not be empty"));
            }
            if let Some(bad) = value
                .chars()
                .find(|c| c.is_control() || matches!(c, '"' | '\\'))
            {
                return Err(format!(
                    "{name} contains {bad:?}; policy strings are plain text without quotes, \
                     backslashes, or control characters"
                ));
            }
        }
        if self.corpus_rows == 0 || self.dimensions == 0 || self.measured_queries == 0 {
            return Err(
                "corpus_rows, dimensions, and measured_queries must all be positive".into(),
            );
        }
        if self.points.is_empty() {
            return Err("points must not be empty".into());
        }
        Ok(())
    }
}

impl PointFile {
    /// The key of the point: `k`, the selectivity band, the measured
    /// recall, and the candidate depth.
    fn validate_key(&self, corpus_rows: u64) -> Result<(), String> {
        if self.k == 0 {
            return Err("policy point k must be positive".into());
        }
        if !PPM.contains(&self.filter_selectivity_ppm_min)
            || !PPM.contains(&self.filter_selectivity_ppm_max)
            || self.filter_selectivity_ppm_min > self.filter_selectivity_ppm_max
        {
            return Err(format!(
                "policy point k={} has selectivity band {}..={} outside 1..=1000000 or \
                 inverted",
                self.k, self.filter_selectivity_ppm_min, self.filter_selectivity_ppm_max
            ));
        }
        if !PPM.contains(&self.measured_recall_ppm) {
            return Err(format!(
                "policy point k={} has measured_recall_ppm {} outside 1..=1000000",
                self.k, self.measured_recall_ppm
            ));
        }
        if self.candidates < self.k || u64::from(self.candidates) > corpus_rows {
            return Err(format!(
                "policy point k={} has invalid candidate depth {} for {} rows",
                self.k, self.candidates, corpus_rows
            ));
        }
        Ok(())
    }

    fn validate_enumerate(&self, corpus_rows: u64) -> Result<(), String> {
        let Some(rows) = self.enumerate_max_rows else {
            return Ok(());
        };
        if self.filter_selectivity_ppm_min == UNFILTERED_PPM {
            return Err(format!(
                "policy point k={} is unfiltered and names enumerate_max_rows {rows}; \
                 an unfiltered search has no mask to enumerate",
                self.k
            ));
        }
        if u64::from(rows) > corpus_rows {
            return Err(format!(
                "policy point k={} has enumerate_max_rows {rows} above the corpus's {} \
                 rows",
                self.k, corpus_rows
            ));
        }
        Ok(())
    }

    fn validate_patience(&self) -> Result<(), String> {
        match (self.patience_saturation_ppm, self.patience_hops) {
            (Some(sat), Some(hops)) => {
                if !PPM.contains(&sat) {
                    return Err(format!(
                        "policy point k={} has patience_saturation_ppm {sat} outside \
                         1..=1000000",
                        self.k
                    ));
                }
                if hops == 0 {
                    return Err(format!(
                        "policy point k={} has patience_hops 0; patience_hops must be positive",
                        self.k
                    ));
                }
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err(format!(
                "policy point k={} must set patience_saturation_ppm and patience_hops \
                 together",
                self.k
            )),
        }
    }

    fn validate_finish_settle(&self) -> Result<(), String> {
        match self.finish_settle_us {
            Some(settle_us) if !(1..=MAX_FINISH_SETTLE_US).contains(&settle_us) => Err(format!(
                "policy point k={} has finish_settle_us {settle_us} outside \
                 1..={MAX_FINISH_SETTLE_US}",
                self.k
            )),
            _ => Ok(()),
        }
    }

    fn validate_floor_prune(&self, corpus_rows: u64) -> Result<(), String> {
        match (self.floor_greediness_ppm, self.floor_clamp_min) {
            (Some(greediness), Some(clamp_min)) => {
                if greediness > UNFILTERED_PPM {
                    return Err(format!(
                        "policy point k={} has floor_greediness_ppm {greediness} outside \
                         0..=1000000",
                        self.k
                    ));
                }
                if u64::from(clamp_min) > corpus_rows {
                    return Err(format!(
                        "policy point k={} has floor_clamp_min {clamp_min} above the \
                         corpus's {} rows",
                        self.k, corpus_rows
                    ));
                }
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err(format!(
                "policy point k={} must set floor_greediness_ppm and floor_clamp_min \
                 together",
                self.k
            )),
        }
    }

    /// The validated point, checked in the order the fields appear in
    /// the document so the first bad field is the one named.
    fn into_point(self, corpus_rows: u64) -> Result<PolicyPoint, String> {
        self.validate_key(corpus_rows)?;
        self.validate_enumerate(corpus_rows)?;
        self.validate_patience()?;
        self.validate_finish_settle()?;
        self.validate_floor_prune(corpus_rows)?;
        Ok(PolicyPoint {
            k: self.k,
            selectivity_min_ppm: self.filter_selectivity_ppm_min,
            selectivity_max_ppm: self.filter_selectivity_ppm_max,
            candidates: self.candidates,
            measured_recall_ppm: self.measured_recall_ppm,
            enumerate_max_rows: self.enumerate_max_rows,
            patience_saturation_ppm: self.patience_saturation_ppm,
            patience_hops: self.patience_hops,
            finish_settle_us: self.finish_settle_us,
            floor_greediness_ppm: self.floor_greediness_ppm,
            floor_clamp_min: self.floor_clamp_min,
        })
    }
}

/// Two points at one `(k, candidates)` must not share any selectivity.
/// `prev` is the point sorted immediately before `point`.
fn refuse_overlap(prev: Option<&PolicyPoint>, point: &PolicyPoint) -> Result<(), String> {
    let Some(prev) = prev.filter(|q| q.k == point.k && q.candidates == point.candidates) else {
        return Ok(());
    };
    if point.selectivity_min_ppm <= prev.selectivity_max_ppm {
        return Err(format!(
            "policy points for k={} candidates={} overlap on selectivity: {}..={} \
             and {}..={}",
            point.k,
            point.candidates,
            prev.selectivity_min_ppm,
            prev.selectivity_max_ppm,
            point.selectivity_min_ppm,
            point.selectivity_max_ppm
        ));
    }
    Ok(())
}

impl DenseExecutionPolicy {
    pub(super) fn from_file(mut file: PolicyFile, fingerprint: String) -> Result<Self, String> {
        file.validate_header()?;
        file.points.sort_by_key(|p| {
            (
                p.k,
                p.candidates,
                p.filter_selectivity_ppm_min,
                p.filter_selectivity_ppm_max,
            )
        });
        let mut points: Vec<PolicyPoint> = Vec::with_capacity(file.points.len());
        for p in file.points {
            let point = p.into_point(file.corpus_rows)?;
            refuse_overlap(points.last(), &point)?;
            points.push(point);
        }
        Ok(DenseExecutionPolicy {
            policy_id: file.policy_id,
            embedding_model: file.embedding_model,
            corpus_generation: file.corpus_generation,
            corpus_rows: file.corpus_rows,
            dimensions: file.dimensions,
            provider_backend: file.provider_backend,
            scoring_fingerprint: file.scoring_fingerprint,
            measured_queries: file.measured_queries,
            points,
            fingerprint,
        })
    }
}
