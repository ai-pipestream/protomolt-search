//! Request validation and the provider matrix that resolves one dense plan.

use crate::pb::{
    DenseExecutionMode as Mode, DenseFilterStrategy as Strategy, DensePrecisionRequest, DenseQuery,
    DenseScorePrecision as Precision, DenseTechnique,
};

/// Transport-free domain error produced by dense plan validation or receipt
/// verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DenseExecutionError {
    InvalidRequest(String),
    FailedPrecondition(String),
    ReceiptContradiction(String),
}

impl std::fmt::Display for DenseExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequest(msg)
            | Self::FailedPrecondition(msg)
            | Self::ReceiptContradiction(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for DenseExecutionError {}

/// The validated dense execution contract of one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DenseRequest {
    pub(crate) mode: Mode,
    pub(crate) precision: DensePrecisionRequest,
    pub(crate) filter_strategy: Strategy,
}

/// Refuse a missing or unknown mode, an unknown precision or routing, and
/// routing named where the planner owns it.
pub(crate) fn parse_request(query: &DenseQuery) -> Result<DenseRequest, DenseExecutionError> {
    let refuse = |why: String| DenseExecutionError::InvalidRequest(format!("dense query: {why}"));
    let mode = match Mode::try_from(query.execution_mode) {
        Ok(Mode::Unspecified) => {
            return Err(refuse(
                "execution_mode is required: EXHAUSTIVE, BALANCED, FAST or AUTO".to_string(),
            ))
        }
        Ok(mode) => mode,
        Err(_) => {
            return Err(refuse(format!(
                "unknown execution_mode {}",
                query.execution_mode
            )))
        }
    };
    let precision = DensePrecisionRequest::try_from(query.precision)
        .map_err(|_| refuse(format!("unknown precision {}", query.precision)))?;
    let filter_strategy = Strategy::try_from(query.filter_strategy)
        .map_err(|_| refuse(format!("unknown filter_strategy {}", query.filter_strategy)))?;
    if filter_strategy != Strategy::Default && matches!(mode, Mode::Exhaustive | Mode::Auto) {
        return Err(refuse(format!(
            "filter_strategy {} applies to BALANCED and FAST; {} owns its routing",
            filter_strategy.as_str_name(),
            mode.as_str_name()
        )));
    }
    Ok(DenseRequest {
        mode,
        precision,
        filter_strategy,
    })
}

/// What the live provider fleet proved it can execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderCaps {
    pub(crate) provider: String,
    /// The precision the provider's own scores are in.
    pub(crate) native: Precision,
    /// The provider scans every admitted row and certifies completion.
    pub(crate) exhaustive_scan: bool,
    /// The provider runs a configured approximate traversal.
    pub(crate) walks: bool,
    /// The provider can pin enumeration of a mask.
    pub(crate) enumerates: bool,
    pub(crate) patience: bool,
    pub(crate) floor_pruning: bool,
    pub(crate) candidate_stream: bool,
    /// Physical rows across the fleet.
    pub(crate) rows: u64,
}

/// How the coordinator runs the resolved plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// The provider's own exhaustive scan at its native precision.
    NativeScan,
    /// Pinned enumeration of every admitted row on a graph provider.
    EnumerateAll,
    /// Every admitted row scored in FP32 from the shard-local exact sidecar.
    Fp32AllRows,
    /// A graph walk under a built-in preset.
    Walk,
    /// A quantized candidate pool rescored in FP32.
    Fp32Pool,
}

/// A versioned built-in approximate preset: controls, never a recall claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Preset {
    pub(crate) name: &'static str,
    /// `(saturation_ppm, hops)` patience early stop.
    pub(crate) patience: Option<(u32, u32)>,
    pub(crate) default_strategy: Strategy,
}

/// BALANCED on a graph: the request's depth as the beam, small masks enumerated.
pub(crate) const HNSW_BALANCED: Preset = Preset {
    name: "hnsw-balanced-v1",
    patience: None,
    default_strategy: Strategy::EnumerateSmall,
};

/// FAST on a graph: beam k, patience 0.99 over 15 hops, SPEED routing.
pub(crate) const HNSW_FAST: Preset = Preset {
    name: "hnsw-fast-v1",
    patience: Some((990_000, 15)),
    default_strategy: Strategy::Speed,
};

/// One validated dense plan, carried through every route that serves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedDensePlan {
    pub(crate) request: DenseRequest,
    pub(crate) resolved_mode: Mode,
    pub(crate) route: Route,
    pub(crate) selection_precision: Precision,
    pub(crate) output_precision: Precision,
    pub(crate) filter_strategy: Strategy,
    pub(crate) preset: Option<Preset>,
    /// The technique that selects the candidates of an FP32 pool.
    pub(crate) pool_selection: DenseTechnique,
    pub(crate) fallback_reason: String,
    pub(crate) planner_reason: String,
}

impl ResolvedDensePlan {
    pub(crate) fn approximate(&self) -> bool {
        matches!(self.route, Route::Walk | Route::Fp32Pool)
    }
}

fn output_precision(
    request: &DenseRequest,
    caps: &ProviderCaps,
) -> Result<Precision, DenseExecutionError> {
    match request.precision {
        DensePrecisionRequest::Auto => Ok(caps.native),
        DensePrecisionRequest::Fp32 => Ok(Precision::Fp32),
        DensePrecisionRequest::Quantized if caps.native == Precision::Quantized => {
            Ok(Precision::Quantized)
        }
        DensePrecisionRequest::Quantized => Err(DenseExecutionError::FailedPrecondition(format!(
            "QUANTIZED precision was required, but provider {} scores original FP32 rows \
             and has no quantized score space",
            caps.provider
        ))),
    }
}

/// The technique a mode resolved to, before it is bound to the request.
struct Choice {
    mode: Mode,
    route: Route,
    selection: Precision,
    preset: Option<Preset>,
    reason: &'static str,
    fallback: String,
}

impl Choice {
    fn new(mode: Mode, route: Route, selection: Precision, reason: &'static str) -> Self {
        Self {
            mode,
            route,
            selection,
            preset: None,
            reason,
            fallback: String::new(),
        }
    }
}

/// Resolve the request against the provider matrix. AUTO never selects
/// approximation here: that needs qualifying measured evidence.
pub(crate) fn resolve(
    request: DenseRequest,
    caps: &ProviderCaps,
) -> Result<ResolvedDensePlan, DenseExecutionError> {
    if request.filter_strategy != Strategy::Default && !caps.walks {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "filter_strategy {} requires an approximate graph provider; provider {} does not walk",
            request.filter_strategy.as_str_name(),
            caps.provider
        )));
    }
    if request.filter_strategy == Strategy::EnumerateSmall && !caps.enumerates {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "filter_strategy ENUMERATE_SMALL requires mask_enumeration; provider {} does not advertise it",
            caps.provider
        )));
    }
    let output = output_precision(&request, caps)?;
    let choice = match request.mode {
        Mode::Exhaustive => {
            let (route, selection) = exhaustive(caps, output)?;
            Choice::new(
                Mode::Exhaustive,
                route,
                selection,
                "caller required exhaustive coverage",
            )
        }
        Mode::Auto => {
            let (route, selection) = exhaustive(caps, output)?;
            Choice::new(
                Mode::Exhaustive,
                route,
                selection,
                "AUTO selected the exhaustive technique: approximation needs qualifying \
                 measured evidence",
            )
        }
        Mode::Balanced => balanced(caps, output)?,
        Mode::Fast => fast(caps, output)?,
        Mode::Unspecified => {
            return Err(DenseExecutionError::InvalidRequest(
                "dense query: execution_mode is required".to_string(),
            ))
        }
    };
    let filter_strategy = match (choice.preset, request.filter_strategy) {
        (Some(preset), Strategy::Default) => {
            if preset.default_strategy == Strategy::EnumerateSmall && !caps.enumerates {
                Strategy::Speed
            } else {
                preset.default_strategy
            }
        }
        (_, strategy) => strategy,
    };
    Ok(ResolvedDensePlan {
        request,
        resolved_mode: choice.mode,
        route: choice.route,
        selection_precision: choice.selection,
        output_precision: output,
        filter_strategy,
        preset: choice.preset,
        pool_selection: if caps.exhaustive_scan {
            DenseTechnique::QuantizedScan
        } else {
            DenseTechnique::HnswWalk
        },
        fallback_reason: choice.fallback,
        planner_reason: choice.reason.to_string(),
    })
}

/// BALANCED: the graph preset, a quantized pool rescored in FP32, or the
/// disclosed same-precision exhaustive technique.
fn balanced(caps: &ProviderCaps, output: Precision) -> Result<Choice, DenseExecutionError> {
    if caps.walks && output != caps.native {
        return Ok(Choice::new(
            Mode::Balanced,
            Route::Fp32Pool,
            caps.native,
            "BALANCED rescored the traversal's quantized candidate pool in FP32",
        ));
    }
    if caps.walks {
        let mut choice = Choice::new(
            Mode::Balanced,
            Route::Walk,
            output,
            "BALANCED walked the graph under its built-in preset",
        );
        choice.preset = Some(HNSW_BALANCED);
        return Ok(choice);
    }
    if caps.exhaustive_scan && output == Precision::Fp32 {
        let mut choice = Choice::new(
            Mode::Balanced,
            Route::Fp32Pool,
            Precision::Quantized,
            "BALANCED rescored a quantized candidate pool in FP32",
        );
        choice.fallback = "staged bit-plane selection is not enabled on this provider; an \
            exhaustive quantized scan selected the pool"
            .to_string();
        return Ok(choice);
    }
    let (route, selection) = exhaustive(caps, output)?;
    let mut choice = Choice::new(
        Mode::Exhaustive,
        route,
        selection,
        "BALANCED ran the same-precision exhaustive technique",
    );
    choice.fallback = format!(
        "provider {} has no balanced approximate technique; the exhaustive technique ran \
         at the same precision",
        caps.provider
    );
    Ok(choice)
}

/// FAST: the graph's speed preset, never a substitute.
fn fast(caps: &ProviderCaps, output: Precision) -> Result<Choice, DenseExecutionError> {
    if !(caps.walks && caps.patience) {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "FAST needs a speed-oriented technique with a work budget; provider {} has \
             none, and FAST is never substituted by another mode",
            caps.provider
        )));
    }
    if output != caps.native {
        return Err(DenseExecutionError::FailedPrecondition(format!(
            "FAST on provider {} scores at its native precision only",
            caps.provider
        )));
    }
    let mut choice = Choice::new(
        Mode::Fast,
        Route::Walk,
        output,
        "FAST walked the graph under its built-in preset",
    );
    choice.preset = Some(HNSW_FAST);
    Ok(choice)
}

/// The exhaustive technique at `output` precision, or a refusal.
fn exhaustive(
    caps: &ProviderCaps,
    output: Precision,
) -> Result<(Route, Precision), DenseExecutionError> {
    match (caps.native, output) {
        (Precision::Fp32, Precision::Fp32) if caps.walks && caps.enumerates => {
            Ok((Route::EnumerateAll, Precision::Fp32))
        }
        (Precision::Quantized, Precision::Quantized) if caps.exhaustive_scan => {
            Ok((Route::NativeScan, Precision::Quantized))
        }
        (Precision::Quantized, Precision::Fp32) if caps.exhaustive_scan => {
            Ok((Route::Fp32AllRows, Precision::Fp32))
        }
        _ => Err(DenseExecutionError::FailedPrecondition(format!(
            "provider {} cannot score every admitted row at {} precision",
            caps.provider,
            output.as_str_name()
        ))),
    }
}
