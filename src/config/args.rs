//! Flag, environment, and file lookup helpers shared by every section parser.

/// Normalize one node address the same way at startup and during hot reload.
pub fn normalize_addr(addr: String) -> String {
    normalize_addrs(vec![addr]).remove(0)
}

fn arg_value(args: &[String], key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    if let Some(v) = args
        .iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
    {
        return Some(v);
    }
    // Also accept the space-separated form (`--key value`).
    let flag = format!("--{key}");
    args.windows(2)
        .find(|w| w[0] == flag && !w[1].starts_with("--"))
        .map(|w| w[1].clone())
}

/// Both bounds or neither; an inverted range is refused.
pub(super) fn parse_hash_range(
    lo: Option<u64>,
    hi: Option<u64>,
    what: &str,
) -> Result<Option<(u64, u64)>, String> {
    match (lo, hi) {
        (Some(lo), Some(hi)) if lo <= hi => Ok(Some((lo, hi))),
        (Some(lo), Some(hi)) => Err(format!("{what}: range {lo}..={hi} is inverted")),
        (None, None) => Ok(None),
        _ => Err(format!("{what}: pass both bounds or neither")),
    }
}

pub(super) fn flag_present(args: &[String], key: &str) -> bool {
    args.iter().any(|a| a == &format!("--{key}"))
}

/// Every value of a repeatable `--key=value` option, in both the `=` and
/// the space-separated form. Hand-rolled exact matching, no patterns.
pub(super) fn arg_values(args: &[String], key: &str) -> Vec<String> {
    let prefix = format!("--{key}=");
    let mut out: Vec<String> = args
        .iter()
        .filter_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .collect();
    let flag = format!("--{key}");
    let mut i = 0;
    while i + 1 < args.len() {
        if args[i] == flag && !args[i + 1].starts_with("--") {
            out.push(args[i + 1].clone());
        }
        i += 1;
    }
    out
}

/// CLI > env > file, for string-valued options.
pub(super) fn opt(args: &[String], key: &str, env: &str, file: Option<&str>) -> Option<String> {
    let neutral_env = env
        .strip_prefix("TURBOVEC_")
        .map(|suffix| format!("PIPESTREAM_SEARCH_{suffix}"));
    arg_value(args, key)
        .or_else(|| {
            neutral_env
                .as_deref()
                .and_then(|name| std::env::var(name).ok())
        })
        .or_else(|| std::env::var(env).ok())
        .or_else(|| file.map(str::to_string))
}

pub(super) fn parse_env_bool(s: &str) -> bool {
    matches!(s, "1" | "true" | "on" | "yes")
}

pub(super) fn normalize_addrs(addrs: Vec<String>) -> Vec<String> {
    addrs
        .into_iter()
        .map(|s| {
            if s.starts_with("http://") || s.starts_with("https://") {
                s
            } else {
                format!("http://{s}")
            }
        })
        .collect()
}

pub(super) fn normalize_analysis_backend(value: String) -> String {
    if matches!(value.trim(), "native" | "native://") {
        crate::analyzer::NATIVE_ANALYSIS_BACKEND.to_string()
    } else {
        normalize_addrs(vec![value]).remove(0)
    }
}

/// One `u64` control knob: the flag, then the environment, then the file, then `default`.
pub(super) fn control_u64(
    args: &[String],
    key: &str,
    env: &str,
    file_value: Option<u64>,
    default: u64,
) -> Result<u64, String> {
    opt(
        args,
        key,
        env,
        file_value.map(|value| value.to_string()).as_deref(),
    )
    .map(|value| {
        value
            .parse::<u64>()
            .map_err(|error| format!("invalid --{key}: {error}"))
    })
    .transpose()
    .map(|value| value.unwrap_or(default))
}
