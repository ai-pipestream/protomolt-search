//! What steers a layer-0 walk and how it ended: the mask filter, the far
//! probe's routing, patience, and floor pruning.

use crate::vector::Mask;

/// How a mask steers the layer-0 walk.
#[derive(Clone, Copy)]
pub enum Filter<'a> {
    /// Every row may be returned.
    None,
    /// The mask gates results only: disallowed rows are scored and routed
    /// through like any other.
    Plain(Mask<'a>),
    /// The walk moves through the allowed rows: a disallowed neighbor is
    /// never scored, its own neighbors are looked through instead.
    /// `allowed` is the mask's count. With `route` the provider chose the
    /// regime, and the far probe routes the query: a neighborhood holding
    /// at least `Route::plain_share` allowed rows is walked plain, one all
    /// but empty of them, or too thin for bridging to beat enumerating
    /// `allowed <= Route::enumerate_rows` rows, is given up to the caller
    /// ([`Walk::Far`]); a filter-aware walk may still give the query up
    /// ([`Walk::Short`], [`Walk::Budget`]) where the graph cannot serve
    /// this mask or `budget` work units have been spent. Without it the
    /// walk runs regardless (a pinned regime).
    Aware {
        allow: Mask<'a>,
        allowed: usize,
        route: Option<Route>,
        budget: Option<u64>,
    },
}

/// How the far probe routes a query the provider planned (`Filter::Aware`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Route {
    /// Walk plain when the probe's sample holds at least this share of
    /// allowed rows: disallowed rows are then cheaper to score and route
    /// through than to bridge around.
    pub plain_share: f64,
    /// Below `plain_share`, enumerate when the mask allows at most this many
    /// rows: a bridging walk costs more than scoring them in mask order.
    pub enumerate_rows: usize,
}

/// Hop-to-hop saturation early-stop policy for a layer-0 walk.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Patience {
    /// Required fraction of the tracked window that survives an expansion.
    pub saturation: f32,
    /// Optional exact parts-per-million saturation threshold (`1..=1_000_000`).
    pub saturation_ppm: Option<u32>,
    /// Consecutive saturated expansions required before ending the walk
    /// (`0` disables patience).
    pub hops: u32,
    /// Optional `top_k` window size (`0` tracks the full `ef` beam).
    pub top_k: usize,
    /// Minimum allowed rows scored at layer 0 before patience may count when
    /// `floor_active` is true (the ascent gate).
    pub ascent_gate: usize,
    /// True when a finite score floor is active on this search.
    pub floor_active: bool,
}

impl Default for Patience {
    fn default() -> Self {
        Self::OFF
    }
}

impl Patience {
    pub const OFF: Self = Self {
        saturation: 1.0,
        saturation_ppm: None,
        hops: 0,
        top_k: 0,
        ascent_gate: 0,
        floor_active: false,
    };

    #[inline]
    pub fn enabled(self) -> bool {
        self.hops > 0
    }

    #[inline]
    pub(super) fn min_survived(self, window: usize) -> usize {
        if !self.enabled() || window == 0 {
            return usize::MAX;
        }
        if let Some(ppm) = self.saturation_ppm {
            let need = (u64::from(ppm) * window as u64).div_ceil(1_000_000);
            return usize::try_from(need).unwrap_or(window).clamp(1, window);
        }
        // Subtract one f32 ULP so binary-rounded thresholds such as `0.99f32`
        // (`0.9900000095...`) do not round `0.99 * 100` up to `100`.
        let raw = (f64::from(self.saturation) - f64::from(f32::EPSILON)) * (window as f64);
        raw.ceil().clamp(1.0, window as f64) as usize
    }
}

/// Layer-0 shared floor pruning gate and Higham kernel-vs-scalar error slack.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FloorPrune {
    pub enabled: bool,
    /// Minimum allowed rows scored at layer 0 before candidates below the
    /// live floor may be pruned.
    pub gate: usize,
    /// `4 * gamma(d) * |query|`: per-unit-norm kernel-vs-scalar dot-product
    /// error slack.
    pub slack_per_norm: f64,
}

impl Default for FloorPrune {
    fn default() -> Self {
        Self::OFF
    }
}

impl FloorPrune {
    pub const OFF: Self = Self {
        enabled: false,
        gate: 0,
        slack_per_norm: 0.0,
    };
}

/// How a walk ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Walk {
    Completed,
    /// The hook stopped it.
    Stopped,
    /// Ended early after `Patience::hops` consecutive saturated expansions
    /// once the beam (and ascent gate when a floor is active) filled.
    Patience,
    /// Pruned at least one candidate or expansion against an active score floor
    /// once the ascent gate was satisfied; never triggers a starvation
    /// fallback to `enumerate`.
    FloorPruned,
    /// Not started: the far probe found the query's neighborhood all but
    /// empty of allowed rows, or too thin for a filter-aware walk to beat
    /// enumerating the mask (`Route::enumerate_rows`). Nothing was scored at
    /// layer 0 or reported.
    Far,
    /// Ran dry with the beam unfilled although the mask allows more rows:
    /// the allowed rows it reached are an island. `Scratch::seen` tells
    /// which rows it scored.
    Short,
    /// Spent its layer-0 work-unit budget (`layer0_scored + bridges`) before
    /// the beam converged: enumerating the remaining allowed rows is cheaper
    /// than continuing the walk. `Scratch::seen` and `Scratch::scored_allowed`
    /// name the allowed rows already scored.
    Budget,
}

/// Everything that steers one layer-0 beam besides its width.
#[derive(Clone, Copy)]
pub struct Steering<'a> {
    pub filter: Filter<'a>,
    pub patience: Patience,
    pub floor_prune: FloorPrune,
}

impl<'a> Steering<'a> {
    /// `filter` with patience and floor pruning off.
    pub const fn filtered(filter: Filter<'a>) -> Self {
        Self {
            filter,
            patience: Patience::OFF,
            floor_prune: FloorPrune::OFF,
        }
    }
}
