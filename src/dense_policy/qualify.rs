//! Exact-key qualification of a request against the measured points.

use super::{DenseExecutionPolicy, PolicyPoint, Qualified, RequestKey, UNFILTERED_PPM};

impl DenseExecutionPolicy {
    /// The point a request key matches exactly, or a refusal naming the
    /// key and what the policy measured for that `k`.
    pub fn qualify(&self, key: RequestKey) -> Result<Qualified, String> {
        if key.k == 0 {
            return Err("AUTO through a dense execution policy requires an explicit k".into());
        }
        if !(1..=UNFILTERED_PPM).contains(&key.filter_selectivity_ppm) {
            return Err(format!(
                "filter selectivity {} ppm is outside 1..=1000000",
                key.filter_selectivity_ppm
            ));
        }
        let for_k: Vec<&PolicyPoint> = self.points.iter().filter(|p| p.k == key.k).collect();
        if for_k.is_empty() {
            return Err(format!(
                "dense execution policy {:?} measured no point for k={}; measured k values: {:?}",
                self.policy_id,
                key.k,
                self.measured_ks()
            ));
        }
        let in_band: Vec<&PolicyPoint> = for_k
            .iter()
            .copied()
            .filter(|p| {
                (p.selectivity_min_ppm..=p.selectivity_max_ppm)
                    .contains(&key.filter_selectivity_ppm)
            })
            .collect();
        if in_band.is_empty() {
            let bands: Vec<String> = for_k
                .iter()
                .map(|p| {
                    format!(
                        "{}..={} ppm at depth {}",
                        p.selectivity_min_ppm, p.selectivity_max_ppm, p.candidates
                    )
                })
                .collect();
            return Err(format!(
                "dense execution policy {:?} measured no point for k={} at filter selectivity \
                 {} ppm; measured bands for that k: [{}]",
                self.policy_id,
                key.k,
                key.filter_selectivity_ppm,
                bands.join(", ")
            ));
        }
        let point = if key.candidate_depth == 0 {
            match in_band.as_slice() {
                [only] => **only,
                many => {
                    let depths: Vec<u32> = many.iter().map(|p| p.candidates).collect();
                    return Err(format!(
                        "dense execution policy {:?} measured k={} at selectivity {} ppm at \
                         several candidate depths {depths:?}; set selection_k to one of them",
                        self.policy_id, key.k, key.filter_selectivity_ppm
                    ));
                }
            }
        } else {
            match in_band.iter().find(|p| p.candidates == key.candidate_depth) {
                Some(point) => **point,
                None => {
                    let depths: Vec<u32> = in_band.iter().map(|p| p.candidates).collect();
                    return Err(format!(
                        "dense execution policy {:?} measured k={} at selectivity {} ppm at \
                         candidate depths {depths:?}, not at the requested {}; the depth is not \
                         interpolated",
                        self.policy_id, key.k, key.filter_selectivity_ppm, key.candidate_depth
                    ));
                }
            }
        };
        Ok(Qualified {
            policy_id: self.policy_id.clone(),
            policy_fingerprint: self.fingerprint.clone(),
            embedding_model: self.embedding_model.clone(),
            point,
        })
    }

    fn measured_ks(&self) -> Vec<u32> {
        let mut ks: Vec<u32> = self.points.iter().map(|p| p.k).collect();
        ks.dedup();
        ks
    }
}
