//! TurboEmbed bridge options.

use super::args::{flag_present, normalize_addr, opt, parse_env_bool};
use super::file::FileConfig;

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
pub(super) fn parse_turbo(args: &[String], file: &FileConfig) -> Result<TurboOptions, String> {
    let turbo_nlp_columns = match opt(
        args,
        "turbo-nlp-columns-json",
        "PIPESTREAM_SEARCH_TURBO_NLP_COLUMNS_JSON",
        file.turbo_nlp_columns_json.as_deref(),
    ) {
        Some(raw) => {
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
            Some(spec)
        }
        None => None,
    };
    let turbo_one_pass_query = if flag_present(args, "turbo-one-pass-query") {
        true
    } else {
        match opt(
            args,
            "turbo-one-pass-query",
            "PIPESTREAM_SEARCH_TURBO_ONE_PASS_QUERY",
            None,
        )
        .as_deref()
        {
            Some(v) => parse_env_bool(v),
            None => file.turbo_one_pass_query.unwrap_or(false),
        }
    };
    let turbo_rerank_endpoint = opt(
        args,
        "turbo-rerank-endpoint",
        "PIPESTREAM_SEARCH_TURBO_RERANK_ENDPOINT",
        file.turbo_rerank_endpoint.as_deref(),
    )
    .map(normalize_addr);
    let turbo_rerank_model = opt(
        args,
        "turbo-rerank-model",
        "PIPESTREAM_SEARCH_TURBO_RERANK_MODEL",
        file.turbo_rerank_model.as_deref(),
    );
    let turbo_matryoshka_dims = match opt(
        args,
        "turbo-matryoshka-dims",
        "PIPESTREAM_SEARCH_TURBO_MATRYOSHKA_DIMS",
        None,
    ) {
        Some(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<usize>()
                    .map_err(|e| format!("invalid --turbo-matryoshka-dims entry {s:?}: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        None => file.turbo_matryoshka_dims.clone().unwrap_or_default(),
    };
    let turbo_matryoshka_active_dim = opt(
        args,
        "turbo-matryoshka-active-dim",
        "PIPESTREAM_SEARCH_TURBO_MATRYOSHKA_ACTIVE_DIM",
        file.turbo_matryoshka_active_dim
            .map(|v| v.to_string())
            .as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid --turbo-matryoshka-active-dim: {e}"))
    })
    .transpose()?;
    if let Some(active) = turbo_matryoshka_active_dim {
        if active == 0
            || (!turbo_matryoshka_dims.is_empty() && !turbo_matryoshka_dims.contains(&active))
        {
            return Err(format!(
                "--turbo-matryoshka-active-dim={active} must be > 0 and included in --turbo-matryoshka-dims"
            ));
        }
    }
    let turbo_vocab_endpoint = opt(
        args,
        "turbo-vocab-endpoint",
        "PIPESTREAM_SEARCH_TURBO_VOCAB_ENDPOINT",
        file.turbo_vocab_endpoint.as_deref(),
    )
    .map(normalize_addr);
    let turbo_model_sha256 = opt(
        args,
        "turbo-model-sha256",
        "PIPESTREAM_SEARCH_TURBO_MODEL_SHA256",
        file.turbo_model_sha256.as_deref(),
    )
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty());
    let turbo_bearer_token = match opt(
        args,
        "turbo-bearer-token",
        "PIPESTREAM_SEARCH_TURBO_BEARER_TOKEN",
        file.turbo_bearer_token.as_deref(),
    ) {
        Some(raw) => {
            let trimmed = raw.trim();
            let tok = if let Some(path) = trimmed.strip_prefix('@') {
                std::fs::read_to_string(path)
                    .map_err(|e| format!("read --turbo-bearer-token {path}: {e}"))?
                    .trim()
                    .to_string()
            } else {
                trimmed.to_string()
            };
            (!tok.is_empty()).then_some(tok)
        }
        None => None,
    };
    Ok(TurboOptions {
        turbo_nlp_columns,
        turbo_one_pass_query,
        turbo_rerank_endpoint,
        turbo_rerank_model,
        turbo_matryoshka_dims,
        turbo_matryoshka_active_dim,
        turbo_vocab_endpoint,
        turbo_model_sha256,
        turbo_bearer_token,
    })
}
