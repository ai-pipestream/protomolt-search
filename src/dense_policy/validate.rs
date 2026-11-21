//! Strict validation of a version-2 policy document.

use crate::pb::{
    DenseExecutionMode as Mode, DenseExecutionPolicyDocument, DenseExecutionShape as Shape,
    DenseFilterStrategy as Strategy, DensePolicyPoint, DenseScorePrecision as Precision,
    DenseTechnique as Technique,
};

use super::{FORMAT_VERSION, MAX_FINISH_SETTLE_US, UNFILTERED_PPM};

/// Refuse anything but a complete, unambiguous version-2 document.
pub(super) fn document(doc: &DenseExecutionPolicyDocument) -> Result<(), String> {
    if doc.format_version != FORMAT_VERSION {
        return Err(format!(
            "format_version {} is not {FORMAT_VERSION}; the version-1 TOML schema is not read",
            doc.format_version
        ));
    }
    for (name, value) in [
        ("policy_id", &doc.policy_id),
        ("embedding_model", &doc.embedding_model),
        ("provider_backend", &doc.provider_backend),
        ("scoring_fingerprint", &doc.scoring_fingerprint),
        ("corpus_fingerprint", &doc.corpus_fingerprint),
        ("hardware_fingerprint", &doc.hardware_fingerprint),
    ] {
        if value.trim().is_empty() {
            return Err(format!("{name} is empty"));
        }
    }
    if doc.dimensions == 0 || doc.corpus_rows == 0 || doc.measured_queries == 0 {
        return Err("dimensions, corpus_rows and measured_queries must be positive".into());
    }
    if doc.deleted_docs > doc.corpus_rows {
        return Err(format!(
            "deleted_docs {} exceeds corpus_rows {}",
            doc.deleted_docs, doc.corpus_rows
        ));
    }
    if !(1..=UNFILTERED_PPM).contains(&doc.target_recall_ppm) {
        return Err(format!(
            "target_recall_ppm {} is outside 1..=1000000",
            doc.target_recall_ppm
        ));
    }
    if doc.points.is_empty() {
        return Err("a policy measures at least one point".into());
    }
    if doc.node_count == 0 {
        return Err("hardware_fingerprint and node_count name the measured fleet".into());
    }
    if doc.hosts.is_empty() {
        return Err("hosts must list the measured shard DenseHostProfiles".into());
    }
    if doc.hosts.len() != doc.node_count as usize {
        return Err(format!(
            "hosts length {} does not match node_count {}",
            doc.hosts.len(),
            doc.node_count
        ));
    }
    if doc.hosts.iter().any(|h| !super::is_profiled_host(h)) {
        return Err("hosts contains an unprofiled DenseHostProfile".into());
    }
    let expected_hw = super::fleet_fingerprint(&doc.hosts);
    if doc.hardware_fingerprint != expected_hw {
        return Err(format!(
            "hardware_fingerprint {:?} does not match hosts fingerprint {expected_hw:?}",
            doc.hardware_fingerprint
        ));
    }
    for (index, point) in doc.points.iter().enumerate() {
        self::point(point, doc.corpus_rows).map_err(|why| format!("point {index}: {why}"))?;
    }
    bands_and_duplicates(&doc.points)
}

fn enums(point: &DensePolicyPoint) -> Result<(), String> {
    match Mode::try_from(point.mode) {
        Ok(Mode::Balanced | Mode::Fast) => {}
        _ => return Err(format!("mode {} is not BALANCED or FAST", point.mode)),
    }
    let technique = match Technique::try_from(point.technique) {
        Ok(t @ (Technique::HnswWalk | Technique::BitplaneStaged)) => t,
        _ => {
            return Err(format!(
                "technique {} is not an approximate technique",
                point.technique
            ))
        }
    };
    for (name, value) in [
        ("selection_precision", point.selection_precision),
        ("output_precision", point.output_precision),
    ] {
        match Precision::try_from(value) {
            Ok(Precision::Fp32 | Precision::Quantized) => {}
            _ => return Err(format!("{name} {value} is not FP32 or QUANTIZED")),
        }
    }
    match (technique, Strategy::try_from(point.filter_strategy)) {
        (Technique::HnswWalk, Ok(Strategy::Speed | Strategy::EnumerateSmall)) => {}
        (Technique::HnswWalk, _) => {
            return Err(format!(
                "HNSW_WALK filter_strategy {} must be SPEED or ENUMERATE_SMALL",
                point.filter_strategy
            ))
        }
        (Technique::BitplaneStaged, Ok(Strategy::Default)) => {}
        (Technique::BitplaneStaged, _) => {
            return Err(format!(
                "BITPLANE_STAGED filter_strategy {} must be DEFAULT",
                point.filter_strategy
            ))
        }
        _ => unreachable!("technique was validated above"),
    }
    let shape = match Shape::try_from(point.shape) {
        Ok(s @ (Shape::Unary | Shape::Streaming)) => s,
        _ => return Err(format!("shape {} is not UNARY or STREAMING", point.shape)),
    };
    if technique == Technique::BitplaneStaged && shape == Shape::Streaming {
        return Err("BITPLANE_STAGED points are UNARY-only and cannot declare STREAMING".into());
    }
    Ok(())
}

fn point(point: &DensePolicyPoint, corpus_rows: u64) -> Result<(), String> {
    enums(point)?;
    if point.k == 0 || point.candidates < point.k || u64::from(point.candidates) > corpus_rows {
        return Err(format!(
            "k {} and candidates {} must satisfy 1 <= k <= candidates <= corpus_rows {}",
            point.k, point.candidates, corpus_rows
        ));
    }
    let band = point.filter_selectivity_ppm_min..=point.filter_selectivity_ppm_max;
    if band.is_empty() || *band.start() == 0 || *band.end() > UNFILTERED_PPM {
        return Err(format!(
            "selectivity band {}..={} ppm is not inside 1..=1000000",
            band.start(),
            band.end()
        ));
    }
    if !(1..=UNFILTERED_PPM).contains(&point.measured_recall_ppm) {
        return Err(format!(
            "measured_recall_ppm {} is outside 1..=1000000",
            point.measured_recall_ppm
        ));
    }
    latency(point)?;
    controls(point, corpus_rows)
}

/// Both techniques' latencies and QPS are measured: a positive p50 at most
/// the p99 and a finite positive QPS.
fn latency(point: &DensePolicyPoint) -> Result<(), String> {
    for (name, p50, p99, qps) in [
        (
            "approximate",
            point.approximate_p50_us,
            point.approximate_p99_us,
            point.approximate_qps,
        ),
        (
            "exhaustive",
            point.exhaustive_p50_us,
            point.exhaustive_p99_us,
            point.exhaustive_qps,
        ),
    ] {
        if p50 == 0 || p99 < p50 {
            return Err(format!(
                "{name} latency p50 {p50} us / p99 {p99} us is not a measurement"
            ));
        }
        if !qps.is_finite() || qps <= 0.0 {
            return Err(format!("{name} qps {qps} must be finite and positive"));
        }
    }
    Ok(())
}

fn controls(point: &DensePolicyPoint, corpus_rows: u64) -> Result<(), String> {
    if point.technique == Technique::BitplaneStaged as i32
        && (point.enumerate_max_rows.is_some()
            || point.patience_saturation_ppm.is_some()
            || point.patience_hops.is_some()
            || point.finish_settle_us.is_some()
            || point.floor_greediness_ppm.is_some()
            || point.floor_clamp_min.is_some())
    {
        return Err(
            "BITPLANE_STAGED point must not set enumerate_max_rows, patience, finish_settle_us, \
             or floor controls"
                .into(),
        );
    }
    if point.shape == Shape::Unary as i32 && point.finish_settle_us.is_some() {
        return Err("UNARY point must not set finish_settle_us".into());
    }
    let unfiltered = point.filter_selectivity_ppm_min == UNFILTERED_PPM
        && point.filter_selectivity_ppm_max == UNFILTERED_PPM;
    match point.enumerate_max_rows {
        Some(_) if unfiltered => {
            return Err("unfiltered point must not set enumerate_max_rows".into())
        }
        Some(rows) if u64::from(rows) > corpus_rows => {
            return Err(format!(
                "enumerate_max_rows {rows} exceeds corpus_rows {corpus_rows}"
            ))
        }
        None if !unfiltered
            && point.technique == Technique::HnswWalk as i32
            && point.filter_strategy == Strategy::EnumerateSmall as i32 =>
        {
            return Err("filtered HNSW ENUMERATE_SMALL point must pin enumerate_max_rows".into())
        }
        _ => {}
    }
    match (point.patience_saturation_ppm, point.patience_hops) {
        (Some(ppm), Some(hops)) if (1..=UNFILTERED_PPM).contains(&ppm) && hops >= 1 => {}
        (None, None) => {}
        _ => return Err("patience_saturation_ppm and patience_hops are set together".into()),
    }
    if point
        .finish_settle_us
        .is_some_and(|us| !(1..=MAX_FINISH_SETTLE_US).contains(&us))
    {
        return Err(format!(
            "finish_settle_us is outside 1..={MAX_FINISH_SETTLE_US}"
        ));
    }
    match (point.floor_greediness_ppm, point.floor_clamp_min) {
        (Some(ppm), Some(clamp)) if ppm <= UNFILTERED_PPM && u64::from(clamp) <= corpus_rows => {
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err("floor_greediness_ppm and floor_clamp_min are set together and in range".into()),
    }
}

/// Reject exact duplicate measurement cells and overlapping non-identical
/// selectivity bands for the same `(k, output_precision, shape)`.
fn bands_and_duplicates(points: &[DensePolicyPoint]) -> Result<(), String> {
    let cell_key = |p: &DensePolicyPoint| {
        (
            p.k,
            p.candidates,
            p.filter_selectivity_ppm_min,
            p.filter_selectivity_ppm_max,
            p.mode,
            p.technique,
            p.selection_precision,
            p.output_precision,
            p.filter_strategy,
            p.shape,
        )
    };
    let mut cells: Vec<_> = points.iter().map(cell_key).collect();
    cells.sort_unstable();
    if let Some(pair) = cells.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(format!("duplicate point key {:?}", pair[0]));
    }
    let mut bands: Vec<_> = points
        .iter()
        .map(|p| {
            (
                (p.k, p.output_precision, p.shape),
                p.filter_selectivity_ppm_min,
                p.filter_selectivity_ppm_max,
            )
        })
        .collect();
    bands.sort_unstable();
    bands.dedup();
    for pair in bands.windows(2) {
        let (a_key, a_min, a_max) = pair[0];
        let (b_key, b_min, b_max) = pair[1];
        if a_key == b_key && a_min <= b_max && b_min <= a_max {
            return Err(format!(
                "overlapping selectivity bands {a_min}..={a_max} and {b_min}..={b_max} for {:?}",
                a_key
            ));
        }
    }
    Ok(())
}
