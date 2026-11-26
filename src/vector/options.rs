//! Per-search options: patience early stopping, shared-floor pruning, and
//! the option set every provider receives.

use super::{FilterRegime, FilterRouting, Mask};
use crate::boolean_bits::Bits;

/// Result window over which hop-to-hop survival is measured for patience
/// early stopping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PatienceWindow {
    /// Measure hop-to-hop survival over the full `ef` result beam, avoiding
    /// the small-`k` quantization cliff (`1 / ef` granularity).
    #[default]
    Beam,
    /// Measure hop-to-hop survival over the top-`k` (`min(k, ef)`) result
    /// window (`PatienceKnnVectorQuery` top-`k` variant).
    TopK,
}

impl PatienceWindow {
    pub fn name(self) -> &'static str {
        match self {
            PatienceWindow::Beam => "beam",
            PatienceWindow::TopK => "top_k",
        }
    }
}

/// Hop-to-hop saturation early-stop parameters for a layer-0 graph walk.
/// `patience == 0` disables early stopping and reproduces the standard walk
/// bit for bit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PatienceSpec {
    /// Required fraction of the window from the start of an expansion that
    /// remains in the window at the end of the expansion (`(0.0, 1.0]`).
    pub saturation: f32,
    /// Optional exact parts-per-million saturation threshold (`1..=1_000_000`)
    /// carried from a dense execution policy point or protobuf request so
    /// survival counts use exact integer ceiling arithmetic.
    pub saturation_ppm: Option<u32>,
    /// Consecutive saturated expansions after the beam fills before ending
    /// the layer-0 walk early (`0` disables patience).
    pub patience: u32,
    /// Minimum local allowed rows scored at layer 0 before patience may count
    /// when a finite score floor is active (the ascent gate).
    pub ascent_gate: usize,
    /// Result window over which survival is measured.
    pub window: PatienceWindow,
}

impl PatienceSpec {
    pub const OFF: Self = Self {
        saturation: 1.0,
        saturation_ppm: None,
        patience: 0,
        ascent_gate: 0,
        window: PatienceWindow::Beam,
    };

    /// Construct a beam-window patience policy from exact parts-per-million
    /// saturation (`1..=1_000_000`) and consecutive hop count.
    pub fn from_ppm(saturation_ppm: u32, patience: u32) -> Self {
        Self {
            saturation: (saturation_ppm as f64 / 1_000_000.0) as f32,
            saturation_ppm: Some(saturation_ppm),
            patience,
            ascent_gate: 0,
            window: PatienceWindow::Beam,
        }
    }
}

/// Default `clampMin` in `max(clampMin, ceil((1 - g) * gateK))`.
pub const DEFAULT_FLOOR_CLAMP_MIN: u32 = 8;

/// Shared-floor candidate pruning specification for a layer-0 walk.
///
/// Given a global floor of rank `floor_k` and this walk's eligible-row share
/// `p = share_ppm / 1,000,000` across the planned shard set, the ascent gate is
/// `gateK = ceil(k * p + 16 * sqrt(k * p * (1 - p)))` and the clamped gate is
/// `min(local_eligible, max(clamp_min, ceil((1 - g) * gateK)))`, where
/// `g = greediness_ppm / 1,000,000`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorPruneSpec {
    pub enabled: bool,
    pub floor_k: usize,
    pub share_ppm: u32,
    pub greediness_ppm: u32,
    pub clamp_min: usize,
}

impl Default for FloorPruneSpec {
    fn default() -> Self {
        Self::OFF
    }
}

impl FloorPruneSpec {
    pub const OFF: Self = Self {
        enabled: false,
        floor_k: 0,
        share_ppm: 1_000_000,
        greediness_ppm: 0,
        clamp_min: 0,
    };

    /// Minimum local allowed rows scored at layer 0 before floor pruning may
    /// discard candidates (`0` when `local_eligible == 0` or `enabled == false`).
    pub fn effective_gate(self, local_eligible: usize) -> usize {
        if !self.enabled || local_eligible == 0 {
            return 0;
        }
        let p = (f64::from(self.share_ppm) / 1_000_000.0).clamp(0.0, 1.0);
        let kf = self.floor_k as f64;
        let gate_k = if kf <= 0.0 || p <= 0.0 {
            0.0
        } else {
            (kf * p + 16.0 * (kf * p * (1.0 - p)).max(0.0).sqrt()).ceil()
        };
        let g = (f64::from(self.greediness_ppm) / 1_000_000.0).clamp(0.0, 1.0);
        let scaled = ((1.0 - g) * gate_k).ceil().max(0.0) as usize;
        scaled.max(self.clamp_min).min(local_eligible)
    }

    /// Scale `share_ppm` proportionally to a segment's share of its shard's
    /// eligible rows (`p_seg = p_shard * seg_eligible / shard_eligible`).
    pub fn for_segment(self, seg_eligible: usize, shard_eligible: usize) -> Self {
        if shard_eligible == 0 {
            return Self {
                share_ppm: 0,
                ..self
            };
        }
        let num = u128::from(self.share_ppm) * (seg_eligible as u128);
        let scaled =
            ((num + (shard_eligible as u128) / 2) / (shard_eligible as u128)).min(1_000_000) as u32;
        Self {
            share_ppm: scaled,
            ..self
        }
    }

    /// Conservative ascent gate (`g = 0`) for guarding patience
    /// early stopping when a finite score floor is active, even when floor
    /// candidate pruning itself is disabled (`enabled == false`).
    pub fn ascent_gate(self, local_eligible: usize) -> usize {
        if self.floor_k == 0 || local_eligible == 0 {
            return 0;
        }
        Self {
            enabled: true,
            greediness_ppm: 0,
            clamp_min: self.clamp_min.max(DEFAULT_FLOOR_CLAMP_MIN as usize),
            ..self
        }
        .effective_gate(local_eligible)
    }
}

/// Options common to every provider search used by this product.
#[derive(Debug, Clone, Copy, Default)]
pub struct VectorSearchOptions<'a> {
    pub allow: Option<Mask<'a>>,
    pub minimum_score: Option<f32>,
    /// How many candidates the request asked the provider to consider: the
    /// beam a configured-ANN provider walks to. A streaming search on such
    /// a provider refuses without it; exhaustive providers ignore it.
    pub candidate_depth: Option<usize>,
    /// The regime a filtered search must run in. `None` lets the provider
    /// choose from the mask's cardinality; a measurement pins it so a
    /// measured point is served by the regime that measured it.
    pub filter_regime: Option<FilterRegime>,
    /// The enumerate threshold of a provider that chooses the regime: a
    /// mask allowing at most this many rows is scored row by row, exactly.
    /// `None` is the provider's own threshold. A dense execution policy
    /// point carries the one it was measured under.
    pub enumerate_max_rows: Option<usize>,
    /// What a provider that chooses the regime optimizes for; ignored
    /// under a pinned `filter_regime`.
    pub filter_routing: FilterRouting,
    /// Optional hop-to-hop saturation early-stop policy for layer-0 walks.
    pub patience: Option<PatienceSpec>,
    /// Optional shared-floor candidate pruning policy for layer-0 walks.
    pub floor_prune: Option<FloorPruneSpec>,
    /// Select by staged top-k on a provider that advertises
    /// `VectorCapability::StagedTopK`; ignored by every other provider.
    pub staged: bool,
}

impl<'a> VectorSearchOptions<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_allowlist(mut self, allow: &'a [bool]) -> Self {
        self.allow = Some(Mask::Bools(allow));
        self
    }

    /// The mask as a bitmap (`Mask::Bits`).
    pub fn with_bits(mut self, allow: &'a Bits) -> Self {
        self.allow = Some(Mask::Bits(allow));
        self
    }

    pub fn with_mask(self, allow: &'a [bool]) -> Self {
        self.with_allowlist(allow)
    }

    pub fn with_minimum_score(mut self, score: f32) -> Self {
        self.minimum_score = Some(score);
        self
    }

    pub fn with_initial_threshold(self, score: f32) -> Self {
        self.with_minimum_score(score)
    }

    pub fn with_candidate_depth(mut self, depth: usize) -> Self {
        self.candidate_depth = Some(depth);
        self
    }

    pub fn with_filter_routing(mut self, routing: FilterRouting) -> Self {
        self.filter_routing = routing;
        self
    }

    pub fn with_filter_regime(mut self, regime: FilterRegime) -> Self {
        self.filter_regime = Some(regime);
        self
    }

    pub fn with_enumerate_max_rows(mut self, rows: usize) -> Self {
        self.enumerate_max_rows = Some(rows);
        self
    }

    pub fn with_patience(mut self, patience: PatienceSpec) -> Self {
        self.patience = Some(patience);
        self
    }

    pub fn with_floor_prune(mut self, floor_prune: FloorPruneSpec) -> Self {
        self.floor_prune = Some(floor_prune);
        self
    }
}
