//! The console's flags: the bind, the cluster it fronts, the UI directory,
//! the operator explorer, and the credentials embedding calls carry.

use super::*;

/// What the facade needs to run, parsed from the tool flags.
pub struct ConsoleConfig {
    pub listen: String,
    pub coordinator: String,
    pub nodes: Vec<String>,
    pub analysis: Option<String>,
    pub security: ToolClient,
    pub allow_remote: bool,
}

fn flag(args: &[String], key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

/// `host:port` -> `http://host:port`; already-schemed addresses pass.
fn grpc_addr(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// The `--ui-dir` flag, as `--ui-dir=<path>` or `--ui-dir <path>`.
pub fn ui_dir_arg(args: &[String]) -> Option<PathBuf> {
    flag(args, "ui-dir").map(PathBuf::from).or_else(|| {
        let at = args.iter().position(|a| a == "--ui-dir")?;
        args.get(at + 1).map(PathBuf::from)
    })
}

/// The options every embedding call carries: the interactive lane, a
/// deadline, the tool's bearer token, and, when the tool dials TLS, its
/// client TLS material. `ToolClient` hands that material over through the
/// process-wide install, which this performs when nothing is installed
/// yet; an earlier install in the same process wins.
pub(super) fn embed_options(security: &ToolClient) -> RemoteCallOptions {
    if security.is_tls() && crate::security::process_client_tls().is_none() {
        security.install();
    }
    RemoteCallOptions {
        bearer_token: security.raw_bearer_token().map(str::to_string),
        interactive: true,
        timeout: Some(EMBED_DEADLINE),
        tls: if security.is_tls() {
            crate::security::process_client_tls().cloned()
        } else {
            None
        },
        ..Default::default()
    }
}

impl ConsoleConfig {
    /// Flags, all `--key=value`: `--listen` (default `127.0.0.1:8600`),
    /// `--coordinator` (default `http://127.0.0.1:50050`), `--nodes`
    /// (comma-separated, in shard order), `--analysis` (the sidecar,
    /// for embeddings), `--allow-remote`, and the tool security flags
    /// `ToolClient` reads.
    pub fn from_args(args: &[String]) -> Result<Self, String> {
        let security = ToolClient::from_args(args.iter().cloned())?;
        let coordinator = security.url(
            &flag(args, "coordinator").unwrap_or_else(|| "http://127.0.0.1:50050".to_string()),
        );
        let nodes = flag(args, "nodes")
            .map(|list| {
                list.split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| security.url(s))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            listen: flag(args, "listen").unwrap_or_else(|| "127.0.0.1:8600".to_string()),
            coordinator,
            nodes,
            // A bare address gets `http://`; with `--tls-ca` the embedding
            // call dials it as `https://` (see `embed_options`).
            analysis: flag(args, "analysis").map(|a| grpc_addr(&a)),
            security,
            allow_remote: args.iter().any(|a| a == "--allow-remote"),
        })
    }
}

/// The `--enable-raw-explorer` flag: the operator's `/api` routes, the
/// explorer, and the dashboard. Refused next to `--allow-remote`, because
/// those routes reach every diagnostics and search method with the
/// console's own credentials.
pub fn raw_explorer_arg(args: &[String]) -> Result<bool, String> {
    let raw = args.iter().any(|a| a == "--enable-raw-explorer");
    if raw && args.iter().any(|a| a == "--allow-remote") {
        return Err(RAW_WITH_REMOTE.to_string());
    }
    Ok(raw)
}

/// Why the raw explorer and a remote bind are refused together.
pub(super) const RAW_WITH_REMOTE: &str = "--enable-raw-explorer is refused with --allow-remote: \
    the raw explorer forwards any exposed cluster RPC, so it is served on loopback only";
