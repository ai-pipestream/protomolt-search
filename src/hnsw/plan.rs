//! The plan of one search: the filter regime, the far probe's routing, and
//! the patience and floor-prune gates.

use std::ops::ControlFlow;

use crate::boolean_bits::Bits;
use crate::vector::{
    FilterRegime, FilterRouting, FloorPruneSpec, Mask, PatienceSpec, PatienceWindow, VectorError,
    VectorProvider, VectorSearchOptions,
};

use super::graph::{Filter, FloorPrune, Patience, Route};
use super::{
    HnswIndex, ENUMERATE_ROWS_PER_PLAIN_ROW, ENUMERATE_ROWS_PER_WALK_ROW, HNSW, PLAIN_LOCAL_SHARE,
    WALK_WORK_ENUMERATE_ROWS,
};

/// The mask of one search with what the regimes need of it.
#[derive(Clone, Copy)]
pub(super) struct Plan<'a> {
    pub(super) regime: Option<FilterRegime>,
    pub(super) allow: Option<Mask<'a>>,
    /// Rows the mask allows.
    pub(super) allowed: usize,
    /// The provider chose the regime, so the far probe routes a filter-aware
    /// plan to plain or enumerate; a pinned regime runs as pinned (`None`).
    pub(super) route: Option<Route>,
    /// Work-unit cap before a filter-aware walk hands the remaining allowed
    /// rows to enumerate (`None` when pinned).
    pub(super) budget: Option<u64>,
    pub(super) patience: PatienceSpec,
    /// Clamped local allowed-row gate when floor pruning is enabled.
    pub(super) floor_prune_gate: Option<usize>,
}

impl Plan<'_> {
    const UNFILTERED: Plan<'static> = Plan {
        regime: None,
        allow: None,
        allowed: 0,
        route: None,
        budget: None,
        patience: PatienceSpec::OFF,
        floor_prune_gate: None,
    };

    /// An unfiltered walk over `eligible` rows.
    fn unfiltered(
        patience: PatienceSpec,
        floor_prune: Option<FloorPruneSpec>,
        eligible: usize,
    ) -> Plan<'static> {
        let (patience, floor_prune_gate) = floor_gates(patience, floor_prune, eligible);
        Plan {
            patience,
            floor_prune_gate,
            ..Plan::UNFILTERED
        }
    }

    pub(super) fn filter(&self) -> Filter<'_> {
        match (self.regime, self.allow) {
            (Some(FilterRegime::FilterAware), Some(allow)) => Filter::Aware {
                allow,
                allowed: self.allowed,
                route: self.route,
                budget: self.budget,
            },
            (Some(FilterRegime::Plain), Some(allow)) => Filter::Plain(allow),
            _ => Filter::None,
        }
    }

    pub(super) fn patience(&self, k: usize, floor: f32) -> Patience {
        if self.patience.patience == 0 {
            return Patience::OFF;
        }
        let top_k = match self.patience.window {
            PatienceWindow::Beam => 0,
            PatienceWindow::TopK => k.max(1),
        };
        Patience {
            saturation: self.patience.saturation,
            saturation_ppm: self.patience.saturation_ppm,
            hops: self.patience.patience,
            top_k,
            ascent_gate: self
                .patience
                .ascent_gate
                .max(self.floor_prune_gate.unwrap_or(0)),
            floor_active: floor.is_finite(),
        }
    }

    pub(super) fn floor_prune(&self, slack_per_norm: f64) -> FloorPrune {
        match self.floor_prune_gate {
            Some(gate) => FloorPrune {
                enabled: true,
                gate,
                slack_per_norm,
            },
            None => FloorPrune::OFF,
        }
    }
}

/// Raise the patience ascent gate to the floor-prune ascent gate over
/// `eligible` rows, and resolve the gate an enabled floor prune opens at.
fn floor_gates(
    mut patience: PatienceSpec,
    floor_prune: Option<FloorPruneSpec>,
    eligible: usize,
) -> (PatienceSpec, Option<usize>) {
    if let Some(fp) = floor_prune {
        patience.ascent_gate = patience.ascent_gate.max(fp.ascent_gate(eligible));
    }
    let gate = floor_prune
        .filter(|fp| fp.enabled)
        .map(|fp| fp.effective_gate(eligible));
    (patience, gate)
}

impl HnswIndex {
    /// When this index carries a seal-time `row_to_node` permutation and the
    /// query specifies a non-trivial `allow` mask in external row coordinates,
    /// translate `allow` once into internal node coordinates (`Bits`).
    pub(super) fn node_allow_bits(
        &self,
        allow: Option<Mask<'_>>,
    ) -> Result<Option<Bits>, VectorError> {
        self.check_allow(allow)?;
        let (Some(ext_allow), Some(row_to_node)) = (allow, self.row_to_node_slice()) else {
            return Ok(None);
        };
        if matches!(ext_allow, Mask::All(_)) {
            return Ok(None);
        }
        let mut bits = Bits::empty(self.len());
        let _ = ext_allow.try_for_each(|ext_row| {
            bits.set(row_to_node[ext_row] as usize);
            ControlFlow::<()>::Continue(())
        });
        Ok(Some(bits))
    }

    pub(super) fn check_allow(&self, allow: Option<Mask<'_>>) -> Result<(), VectorError> {
        if let Some(allow) = allow {
            if allow.len() != self.len() {
                return Err(VectorError::new(format!(
                    "allow mask covers {} rows, the index holds {}",
                    allow.len(),
                    self.len()
                )));
            }
        }
        Ok(())
    }

    pub(super) fn m0(&self) -> usize {
        2 * self.config.m as usize
    }

    fn check_patience(patience: Option<PatienceSpec>) -> Result<PatienceSpec, VectorError> {
        let Some(spec) = patience else {
            return Ok(PatienceSpec::OFF);
        };
        if spec.patience == 0 {
            return Ok(PatienceSpec::OFF);
        }
        if let Some(ppm) = spec.saturation_ppm {
            if !(1..=1_000_000).contains(&ppm) {
                return Err(VectorError::new(format!(
                    "{HNSW} patience saturation_ppm {ppm} must be in 1..=1000000"
                )));
            }
        }
        if !spec.saturation.is_finite() || spec.saturation <= 0.0 || spec.saturation > 1.0 {
            return Err(VectorError::new(format!(
                "{HNSW} patience saturation {} must be finite and in (0.0, 1.0]",
                spec.saturation
            )));
        }
        Ok(spec)
    }

    fn check_floor_prune(
        floor_prune: Option<FloorPruneSpec>,
    ) -> Result<Option<FloorPruneSpec>, VectorError> {
        let Some(spec) = floor_prune else {
            return Ok(None);
        };
        if spec.share_ppm > 1_000_000 {
            return Err(VectorError::new(format!(
                "{HNSW} floor_prune share_ppm {} must be in 0..=1000000",
                spec.share_ppm
            )));
        }
        if spec.greediness_ppm > 1_000_000 {
            return Err(VectorError::new(format!(
                "{HNSW} floor_prune greediness_ppm {} must be in 0..=1000000",
                spec.greediness_ppm
            )));
        }
        Ok(Some(spec))
    }

    /// Choose how a search at beam `ef` answers under `options`: the regime
    /// the caller pinned, or the one the mask's cardinality calls for.
    ///
    /// - no mask, or a mask that allows every row: an unfiltered walk;
    /// - at most the enumerate threshold under `FilterRouting::Exact` or a
    ///   request's `enumerate_max_rows` (which a dense execution policy
    ///   point carries from its measurement), and otherwise at most
    ///   `ENUMERATE_ROWS_PER_PLAIN_ROW · ef · m0` allowed rows: enumerate.
    ///   The enumerate threshold is the request's `enumerate_max_rows` or,
    ///   without one, `ENUMERATE_ROWS_PER_WALK_ROW · ef · m0`;
    /// - otherwise a routed walk: the far probe samples the rows around
    ///   where the descent landed and walks plain when at least
    ///   `PLAIN_LOCAL_SHARE` of them are allowed, enumerates when the
    ///   neighborhood is all but empty of allowed rows or the mask allows at
    ///   most the enumerate threshold, and otherwise walks filter-aware,
    ///   which may still hand the query to enumerate (`graph::Walk`).
    pub(super) fn plan<'a>(
        &self,
        ef: usize,
        options: &VectorSearchOptions<'a>,
    ) -> Result<Plan<'a>, VectorError> {
        self.check_allow(options.allow)?;
        let patience = Self::check_patience(options.patience)?;
        let floor_prune = Self::check_floor_prune(options.floor_prune)?;
        let Some(allow) = options.allow else {
            if let Some(regime) = options.filter_regime {
                return Err(VectorError::new(format!(
                    "{HNSW} was asked for the {} filter regime on a search with no mask",
                    regime.name()
                )));
            }
            return Ok(Plan::unfiltered(patience, floor_prune, self.len()));
        };
        if let (Some(regime), Some(rows)) = (options.filter_regime, options.enumerate_max_rows) {
            return Err(VectorError::new(format!(
                "{HNSW} was asked for the {} filter regime and an enumerate threshold of \
                 {rows} rows; a pinned regime has no threshold to apply",
                regime.name()
            )));
        }
        let allowed = allow.count();
        let walk_units = ef * self.m0();
        let (regime, route, budget) = match options.filter_regime {
            Some(regime) => (regime, None, None),
            None if allowed == allow.len() => {
                return Ok(Plan::unfiltered(patience, floor_prune, self.len()));
            }
            None => Self::route_mask(walk_units, allowed, options),
        };
        let (patience, floor_prune_gate) = floor_gates(patience, floor_prune, allowed);
        Ok(Plan {
            regime: Some(regime),
            allow: Some(allow),
            allowed,
            route,
            budget,
            patience,
            floor_prune_gate,
        })
    }

    /// Route a mask of `allowed` rows no regime was pinned for, at a beam
    /// worth `walk_units` (`ef · m0`) rows: enumerate outright, or a
    /// filter-aware walk with the far probe's route and the work-unit budget
    /// after which it hands the query to enumerate.
    fn route_mask(
        walk_units: usize,
        allowed: usize,
        options: &VectorSearchOptions<'_>,
    ) -> (FilterRegime, Option<Route>, Option<u64>) {
        let default_threshold = ENUMERATE_ROWS_PER_WALK_ROW * walk_units;
        let threshold = options.enumerate_max_rows.unwrap_or(default_threshold);
        // A policy point's threshold was measured as the line below which it
        // enumerates, and is served as measured, as is the threshold under
        // exact routing; otherwise only a mask cheaper than a plain walk
        // enumerates before the probe looks.
        let outright = match (options.enumerate_max_rows, options.filter_routing) {
            (Some(rows), _) => rows,
            (None, FilterRouting::Exact) => threshold,
            (None, FilterRouting::Speed) => ENUMERATE_ROWS_PER_PLAIN_ROW * walk_units,
        };
        if allowed <= outright {
            return (FilterRegime::Enumerate, None, None);
        }
        let route = Route {
            plain_share: PLAIN_LOCAL_SHARE,
            enumerate_rows: threshold,
        };
        let mut budget = None;
        if threshold > 0 && default_threshold > 0 {
            // Scale the measured enumerate-rows-per-walk-unit ratio by the
            // policy's `enumerate_max_rows`, bounded below by 1.
            let cost_ratio =
                (WALK_WORK_ENUMERATE_ROWS * (threshold as f64 / default_threshold as f64)).max(1.0);
            budget = Some(((allowed as f64) / cost_ratio).ceil() as u64);
        }
        (FilterRegime::FilterAware, Some(route), budget)
    }
}
