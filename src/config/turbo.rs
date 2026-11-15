//! TurboEmbed bridge options.

use super::args::normalize_addr;
use super::file::FileConfig;
use super::option::{split_list, Options};

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

/// The TurboEmbed bridge: NLP columns, reranking, Matryoshka dimensions, and endpoints.
pub(super) fn parse_turbo(o: Options<'_>, file: &FileConfig) -> Result<TurboOptions, String> {
    let turbo_matryoshka_dims = match o.given("turbo-matryoshka-dims") {
        Some(raw) => split_list(&raw)
            .iter()
            .map(|entry| {
                entry.parse::<usize>().map_err(|error| {
                    format!("invalid --turbo-matryoshka-dims entry {entry:?}: {error}")
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        None => file.turbo_matryoshka_dims.clone().unwrap_or_default(),
    };
    let turbo_matryoshka_active_dim = o.value::<usize>(
        "turbo-matryoshka-active-dim",
        file.turbo_matryoshka_active_dim,
    )?;
    if let Some(active) = turbo_matryoshka_active_dim {
        if active == 0
            || (!turbo_matryoshka_dims.is_empty() && !turbo_matryoshka_dims.contains(&active))
        {
            return Err(format!(
                "--turbo-matryoshka-active-dim={active} must be > 0 and included in --turbo-matryoshka-dims"
            ));
        }
    }
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
        turbo_model_sha256: o
            .text("turbo-model-sha256", file.turbo_model_sha256.as_deref())
            .map(|sha| sha.trim().to_string())
            .filter(|sha| !sha.is_empty()),
        turbo_bearer_token: bearer_token(o, file)?,
    })
}

/// The NLP column spec: inline JSON, or the path of a JSON file.
fn nlp_columns(
    o: Options<'_>,
    file: &FileConfig,
) -> Result<Option<crate::turbo_bridge::NlpColumnSpec>, String> {
    let Some(raw) = o.text(
        "turbo-nlp-columns-json",
        file.turbo_nlp_columns_json.as_deref(),
    ) else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    let json_text = if trimmed.starts_with('{') {
        trimmed.to_string()
    } else {
        std::fs::read_to_string(trimmed)
            .map_err(|e| format!("read --turbo-nlp-columns-json {trimmed}: {e}"))?
    };
    let spec: crate::turbo_bridge::NlpColumnSpec = serde_json::from_str(&json_text)
        .map_err(|e| format!("parse --turbo-nlp-columns-json: {e}"))?;
    spec.validate()
        .map_err(|s| format!("--turbo-nlp-columns-json: {}", s.message()))?;
    Ok(Some(spec))
}

/// The bearer token: the value itself, or `@path` to read it from a file.
fn bearer_token(o: Options<'_>, file: &FileConfig) -> Result<Option<String>, String> {
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
