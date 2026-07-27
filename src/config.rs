//! Cluster configuration: TOML file + environment variables + CLI flags.
//!
//! Precedence (highest wins): CLI flag, then environment variable, then
//! config file, then built-in default. `--config <path>` (or
//! `TURBOVEC_CONFIG`) selects the file; every other flag is `--key=value`.
//!
//! Membership is STATIC: the coordinator's node list and each node's shard
//! set are fixed at startup. There is no discovery, re-sharding, or
//! failover — changing the topology means editing the configs and
//! restarting. That is deliberate for this phase.
//!
//! Example (`cluster.toml`):
//!
//! ```toml
//! role = "both"                        # node | coordinator | both
//! coord_listen = "0.0.0.0:50050"
//! nodes = ["host-a:50051", "krick-1:50051"]  # fan-out order = tie-break order
//! chunk_blocks = 64
//! floor_sharing = true
//! max_message_mib = 64
//!
//! [[shards]]                           # shards this process owns/serves
//! listen = "0.0.0.0:50051"
//! index = "/data/turbovec/shard-0.tv"
//! slot_offset = 0
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

use crate::chunked::DEFAULT_CHUNK_BLOCKS;
use crate::MAX_MESSAGE_BYTES;

/// Which role(s) this process serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Shard owner: serves `NodeService` only.
    Node,
    /// Query fan-out: serves `SearchService` only.
    Coordinator,
    /// One process serving both (single-machine demos/tests).
    Both,
}

/// Demo index shape for `--demo-vectors` (random unit vectors, calibration
/// fitted on a sample and seeded — the same flow real deployments use).
#[derive(Debug, Clone, Copy)]
pub struct DemoConfig {
    /// Number of random vectors to generate.
    pub vectors: usize,
    /// Vector dimensionality.
    pub dim: usize,
    /// Quantization bit width (2, 3, or 4).
    pub bit_width: usize,
}

/// One shard this process serves (one `NodeService` listener per shard).
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Listen address for this shard's `NodeService`.
    pub listen: SocketAddr,
    /// Path to a `.tv` index file. Mutually exclusive with `demo`.
    pub index_path: Option<PathBuf>,
    /// Build a random demo index instead of loading one.
    pub demo: Option<DemoConfig>,
    /// This shard's global id base (added to local slots).
    pub slot_offset: u64,
}

/// Full process configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Role(s) to serve.
    pub role: Role,
    /// Listen address for `SearchService` (roles coordinator/both).
    pub coord_listen: SocketAddr,
    /// Shard node addresses (`http://host:port`) for the coordinator, in
    /// fan-out order (= shard index for merge tie-breaks).
    pub node_addrs: Vec<String>,
    /// Shards this process owns and serves (roles node/both).
    pub shards: Vec<ShardConfig>,
    /// Scan chunk size in SIMD blocks.
    pub chunk_blocks: usize,
    /// Participate in floor sharing (publish + adopt floors).
    pub share_floors: bool,
    /// gRPC message size cap applied to clients and servers.
    pub max_message_bytes: usize,
    /// Issue one demo search against the coordinator at startup.
    pub demo_query: bool,
    /// Dimension of the demo-query vector.
    pub query_dim: usize,
    /// Bit width for from-scratch index construction via AddVectors.
    pub bit_width: usize,
    /// Flush shards to their index paths on graceful shutdown.
    pub save_on_shutdown: bool,
}

/// Raw TOML file shape; every field optional (file < env < CLI).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    role: Option<String>,
    node_listen: Option<String>,
    coord_listen: Option<String>,
    nodes: Option<Vec<String>>,
    index: Option<String>,
    slot_offset: Option<u64>,
    demo_vectors: Option<usize>,
    dim: Option<usize>,
    bit_width: Option<usize>,
    chunk_blocks: Option<usize>,
    floor_sharing: Option<bool>,
    max_message_mib: Option<usize>,
    demo_query: Option<bool>,
    query_dim: Option<usize>,
    save_on_shutdown: Option<bool>,
    shards: Vec<FileShard>,
}

/// One `[[shards]]` table in the TOML file.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileShard {
    listen: Option<String>,
    index: Option<String>,
    slot_offset: Option<u64>,
    demo_vectors: Option<usize>,
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

fn flag_present(args: &[String], key: &str) -> bool {
    args.iter().any(|a| a == &format!("--{key}"))
}

/// CLI > env > file, for string-valued options.
fn opt(args: &[String], key: &str, env: &str, file: Option<&str>) -> Option<String> {
    arg_value(args, key)
        .or_else(|| std::env::var(env).ok())
        .or_else(|| file.map(str::to_string))
}

fn parse_env_bool(s: &str) -> bool {
    matches!(s, "1" | "true" | "on" | "yes")
}

fn normalize_addrs(addrs: Vec<String>) -> Vec<String> {
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

/// Parse configuration from process args (excluding argv[0]).
pub fn parse(args: &[String]) -> Result<Config, String> {
    // The config file sits at the bottom of the precedence stack.
    let file: FileConfig = match opt(args, "config", "TURBOVEC_CONFIG", None) {
        Some(path) => {
            let text =
                std::fs::read_to_string(&path).map_err(|e| format!("read config {path}: {e}"))?;
            toml::from_str(&text).map_err(|e| format!("parse config {path}: {e}"))?
        }
        None => FileConfig::default(),
    };

    let role = match opt(args, "role", "TURBOVEC_ROLE", file.role.as_deref())
        .unwrap_or_else(|| "node".to_string())
        .as_str()
    {
        "node" => Role::Node,
        "coordinator" => Role::Coordinator,
        "both" => Role::Both,
        other => return Err(format!("unknown role {other:?} (node|coordinator|both)")),
    };

    let coord_listen = opt(
        args,
        "coord-listen",
        "TURBOVEC_COORD_LISTEN",
        file.coord_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50050".to_string())
    .parse::<SocketAddr>()
    .map_err(|e| format!("invalid coordinator listen address: {e}"))?;

    // Coordinator fan-out list: CLI (--nodes=a,b) > env > file.
    let node_addrs = match opt(args, "nodes", "TURBOVEC_NODES", None) {
        Some(s) => normalize_addrs(
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
        ),
        None => normalize_addrs(file.nodes.clone().unwrap_or_default()),
    };

    let dim = opt(
        args,
        "dim",
        "TURBOVEC_DIM",
        file.dim.map(|d| d.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "128".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid dim: {e}"))?;
    let bit_width = opt(
        args,
        "bit-width",
        "TURBOVEC_BIT_WIDTH",
        file.bit_width.map(|b| b.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "4".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid bit width: {e}"))?;

    // Shard set. CLI --index/--demo-vectors (with --node-listen and
    // --slot-offset) describes a single shard and overrides the file's
    // [[shards]] entirely; otherwise the file's shards are used.
    let node_listen_default = opt(
        args,
        "node-listen",
        "TURBOVEC_NODE_LISTEN",
        file.node_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50051".to_string());
    let cli_index = opt(args, "index", "TURBOVEC_INDEX", file.index.as_deref());
    let cli_demo = opt(
        args,
        "demo-vectors",
        "TURBOVEC_DEMO_VECTORS",
        file.demo_vectors.map(|d| d.to_string()).as_deref(),
    );
    let cli_offset = opt(
        args,
        "slot-offset",
        "TURBOVEC_SLOT_OFFSET",
        file.slot_offset.map(|o| o.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "0".to_string())
    .parse::<u64>()
    .map_err(|e| format!("invalid slot offset: {e}"))?;

    let shards: Vec<ShardConfig> = if cli_index.is_some() || cli_demo.is_some() {
        if cli_index.is_some() && cli_demo.is_some() {
            return Err("--index and --demo-vectors are mutually exclusive".to_string());
        }
        let listen = node_listen_default
            .parse::<SocketAddr>()
            .map_err(|e| format!("invalid node listen address: {e}"))?;
        let demo = cli_demo
            .map(|s| {
                s.parse::<usize>()
                    .map(|vectors| DemoConfig {
                        vectors,
                        dim,
                        bit_width,
                    })
                    .map_err(|e| format!("invalid demo vector count: {e}"))
            })
            .transpose()?;
        vec![ShardConfig {
            listen,
            index_path: cli_index.map(PathBuf::from),
            demo,
            slot_offset: cli_offset,
        }]
    } else {
        file.shards
            .iter()
            .enumerate()
            .map(|(i, shard)| {
                let listen = shard
                    .listen
                    .clone()
                    .unwrap_or_else(|| node_listen_default.clone())
                    .parse::<SocketAddr>()
                    .map_err(|e| format!("shards[{i}]: invalid listen address: {e}"))?;
                let demo = shard.demo_vectors.map(|vectors| DemoConfig {
                    vectors,
                    dim,
                    bit_width,
                });
                if shard.index.is_some() == demo.is_some() {
                    return Err(format!(
                        "shards[{i}]: exactly one of index / demo_vectors is required"
                    ));
                }
                Ok(ShardConfig {
                    listen,
                    index_path: shard.index.as_ref().map(PathBuf::from),
                    demo,
                    slot_offset: shard.slot_offset.unwrap_or(0),
                })
            })
            .collect::<Result<_, String>>()?
    };

    let chunk_blocks = opt(
        args,
        "chunk-blocks",
        "TURBOVEC_CHUNK_BLOCKS",
        file.chunk_blocks.map(|c| c.to_string()).as_deref(),
    )
    .unwrap_or_else(|| DEFAULT_CHUNK_BLOCKS.to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid chunk blocks: {e}"))?;

    let share_floors = match opt(args, "floor-sharing", "TURBOVEC_FLOOR_SHARING", None) {
        Some(s) => parse_env_bool(&s),
        None => file.floor_sharing.unwrap_or(true),
    };

    let max_message_bytes = opt(
        args,
        "max-message-mib",
        "TURBOVEC_MAX_MESSAGE_MIB",
        file.max_message_mib.map(|m| m.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map(|mib| mib * 1024 * 1024)
            .map_err(|e| format!("invalid max message MiB: {e}"))
    })
    .transpose()?
    .unwrap_or(MAX_MESSAGE_BYTES);

    let demo_query = flag_present(args, "demo-query")
        || std::env::var("TURBOVEC_DEMO_QUERY")
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.demo_query.unwrap_or(false);
    let query_dim = opt(
        args,
        "query-dim",
        "TURBOVEC_QUERY_DIM",
        file.query_dim.map(|d| d.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid query dim: {e}"))
    })
    .transpose()?
    .unwrap_or(dim);

    if matches!(role, Role::Node | Role::Both) && shards.is_empty() {
        return Err(
            "node/both role requires at least one shard (--index/--demo-vectors or [[shards]])"
                .to_string(),
        );
    }
    if matches!(role, Role::Coordinator | Role::Both) && node_addrs.is_empty() {
        return Err(
            "coordinator role requires --nodes (or `nodes` in the config file)".to_string(),
        );
    }
    if demo_query && role == Role::Node {
        return Err("--demo-query requires the coordinator or both role".to_string());
    }

    let save_on_shutdown = match opt(args, "save-on-shutdown", "TURBOVEC_SAVE_ON_SHUTDOWN", None) {
        Some(s) => parse_env_bool(&s),
        None => file.save_on_shutdown.unwrap_or(true),
    };

    Ok(Config {
        role,
        coord_listen,
        node_addrs,
        shards,
        chunk_blocks,
        share_floors,
        max_message_bytes,
        demo_query,
        query_dim,
        bit_width,
        save_on_shutdown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(pairs: &[&str]) -> Vec<String> {
        pairs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_node_role_flags() {
        let cfg = parse(&args(&[
            "--role=node",
            "--demo-vectors=1000",
            "--node-listen=127.0.0.1:9001",
            "--slot-offset=20000",
            "--chunk-blocks=8",
        ]))
        .unwrap();
        assert_eq!(cfg.role, Role::Node);
        assert_eq!(cfg.shards.len(), 1);
        assert_eq!(cfg.shards[0].listen.port(), 9001);
        assert_eq!(cfg.shards[0].slot_offset, 20000);
        assert_eq!(cfg.chunk_blocks, 8);
        assert_eq!(cfg.shards[0].demo.unwrap().vectors, 1000);
        assert!(cfg.share_floors);
        assert_eq!(cfg.max_message_bytes, MAX_MESSAGE_BYTES);
    }

    #[test]
    fn coordinator_requires_nodes() {
        assert!(parse(&args(&["--role=coordinator"])).is_err());
        let cfg = parse(&args(&[
            "--role=coordinator",
            "--nodes=127.0.0.1:50051,127.0.0.1:50052",
        ]))
        .unwrap();
        assert_eq!(cfg.node_addrs.len(), 2);
        assert!(cfg.node_addrs[0].starts_with("http://"));
    }

    #[test]
    fn node_requires_a_shard() {
        assert!(parse(&args(&["--role=node"])).is_err());
        assert!(parse(&args(&[
            "--role=node",
            "--index=/tmp/x.tv",
            "--demo-vectors=10"
        ]))
        .is_err());
    }

    #[test]
    fn toml_file_multi_shard() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("turbovec_search_cfg_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
role = "both"
coord_listen = "0.0.0.0:51050"
nodes = ["host-a:50051", "krick-1:50052"]
chunk_blocks = 16
floor_sharing = false
max_message_mib = 32

[[shards]]
listen = "0.0.0.0:50051"
index = "/data/shard-0.tv"
slot_offset = 0

[[shards]]
listen = "0.0.0.0:50052"
index = "/data/shard-1.tv"
slot_offset = 20000
"#,
        )
        .unwrap();
        let cfg = parse(&args(&[&format!("--config={}", path.display())])).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(cfg.role, Role::Both);
        assert_eq!(cfg.coord_listen.port(), 51050);
        assert_eq!(
            cfg.node_addrs,
            vec!["http://host-a:50051", "http://krick-1:50052"]
        );
        assert_eq!(cfg.shards.len(), 2);
        assert_eq!(cfg.shards[1].listen.port(), 50052);
        assert_eq!(cfg.shards[1].slot_offset, 20000);
        assert_eq!(
            cfg.shards[1].index_path.as_deref(),
            Some(std::path::Path::new("/data/shard-1.tv"))
        );
        assert_eq!(cfg.chunk_blocks, 16);
        assert!(!cfg.share_floors);
        assert_eq!(cfg.max_message_bytes, 32 * 1024 * 1024);
    }

    #[test]
    fn cli_overrides_file() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("turbovec_search_ovr_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            "role = \"node\"\nchunk_blocks = 16\n\n[[shards]]\nindex = \"/data/a.tv\"\n",
        )
        .unwrap();
        let cfg = parse(&args(&[
            &format!("--config={}", path.display()),
            "--chunk-blocks=99",
        ]))
        .unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(cfg.chunk_blocks, 99);
        assert_eq!(cfg.shards.len(), 1);
    }

    #[test]
    fn file_shard_needs_exactly_one_source() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("turbovec_search_bad_{}.toml", std::process::id()));
        std::fs::write(&path, "role = \"node\"\n\n[[shards]]\nslot_offset = 7\n").unwrap();
        let result = parse(&args(&[&format!("--config={}", path.display())]));
        let _ = std::fs::remove_file(&path);
        assert!(result.is_err());
    }

    #[test]
    fn floor_sharing_flag() {
        let cfg = parse(&args(&[
            "--role=node",
            "--demo-vectors=10",
            "--floor-sharing=false",
        ]))
        .unwrap();
        assert!(!cfg.share_floors);
    }

    #[test]
    fn demo_query_flag_and_dim() {
        let cfg = parse(&args(&[
            "--role=coordinator",
            "--nodes=a:1",
            "--demo-query",
            "--query-dim=256",
        ]))
        .unwrap();
        assert!(cfg.demo_query);
        assert_eq!(cfg.query_dim, 256);
    }
}
