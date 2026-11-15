//! TurboEmbed bridge options.

use super::args::normalize_addr;
use super::file::FileConfig;
use super::option::{invalid, split_list, Options};

/// The TurboEmbed bridge options.
pub(super) struct TurboOptions {
    pub(super) turbo_nlp_columns: Option<crate::turbo_bridge::NlpColumnSpec>,
    pub(super) turbo_one_pass_query: bool,
    pub(super) turbo_rerank_endpoint: Option<String>,
    pub(super) turbo_rerank_model: Option<String>,
    pub(super) turbo_matryoshka_dims: Vec<usize>,
    pub(super) turbo_matryoshka_active_dim: Option<usize>,
    pub(super) turbo_vocab_endpoint: Option<String>,
    pub(super) turbo_model_sha256: Option<String>,
    pub(super) turbo_bearer_token: Option<String>,
}

/// The NLP column mapping: inline JSON, or a path to a JSON file.
fn nlp_columns(
    o: Options,
    file: &FileConfig,
) -> Result<Option<crate::turbo_bridge::NlpColumnSpec>, String> {
    let flag = "turbo-nlp-columns-json";
    let Some(raw) = o.text(flag, file.turbo_nlp_columns_json.as_deref()) else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    let json_text = if trimmed.starts_with('{') {
        trimmed.to_string()
    } else {
        std::fs::read_to_string(trimmed).map_err(|e| format!("read --{flag} {trimmed}: {e}"))?
    };
    let spec: crate::turbo_bridge::NlpColumnSpec =
        serde_json::from_str(&json_text).map_err(|e| format!("parse --{flag}: {e}"))?;
    spec.validate()
        .map_err(|s| format!("--{flag}: {}", s.message()))?;
    Ok(Some(spec))
}

/// The prefix dimensions, and the one of them the shard indexes.
fn matryoshka(o: Options, file: &FileConfig) -> Result<(Vec<usize>, Option<usize>), String> {
    let flag = "turbo-matryoshka-dims";
    let dims = match o.given(flag) {
        Some(raw) => split_list(&raw)
            .iter()
            .map(|s| s.parse::<usize>().map_err(|e| invalid(flag, s, e)))
            .collect::<Result<Vec<_>, _>>()?,
        None => file.turbo_matryoshka_dims.clone().unwrap_or_default(),
    };
    let active = o.value(
        "turbo-matryoshka-active-dim",
        file.turbo_matryoshka_active_dim,
    )?;
    if let Some(active) = active {
        if active == 0 || (!dims.is_empty() && !dims.contains(&active)) {
            return Err(format!(
                "--turbo-matryoshka-active-dim={active} must be > 0 and included in \
                 --turbo-matryoshka-dims"
            ));
        }
    }
    Ok((dims, active))
}

/// The outbound bearer token, inline or `@<path>` to a file holding it.
fn bearer_token(o: Options, file: &FileConfig) -> Result<Option<String>, String> {
    let Some(raw) = o.text("turbo-bearer-token", file.turbo_bearer_token.as_deref()) else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    let token = match trimmed.strip_prefix('@') {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("read --turbo-bearer-token {path}: {e}"))?
            .trim()
            .to_string(),
        None => trimmed.to_string(),
    };
    Ok((!token.is_empty()).then_some(token))
}

/// The TurboEmbed bridge: NLP columns, reranking, Matryoshka dimensions, and endpoints.
pub(super) fn parse_turbo(o: Options, file: &FileConfig) -> Result<TurboOptions, String> {
    let (turbo_matryoshka_dims, turbo_matryoshka_active_dim) = matryoshka(o, file)?;
    Ok(TurboOptions {
        turbo_nlp_columns: nlp_columns(o, file)?,
        turbo_one_pass_query: o.switch_or_bare(
            "turbo-one-pass-query",
            file.turbo_one_pass_query,
            false,
        ),
        turbo_rerank_endpoint: o
            .text(
                "turbo-rerank-endpoint",
                file.turbo_rerank_endpoint.as_deref(),
            )
            .map(normalize_addr),
        turbo_rerank_model: o.text("turbo-rerank-model", file.turbo_rerank_model.as_deref()),
        turbo_matryoshka_dims,
        turbo_matryoshka_active_dim,
        turbo_vocab_endpoint: o
            .text("turbo-vocab-endpoint", file.turbo_vocab_endpoint.as_deref())
            .map(normalize_addr),
        turbo_model_sha256: o.trimmed("turbo-model-sha256", file.turbo_model_sha256.as_deref()),
        turbo_bearer_token: bearer_token(o, file)?,
    })
}
