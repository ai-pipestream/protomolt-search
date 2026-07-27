//! Process configuration: command-line args with environment fallbacks.
//!
//! Every option is `--key=value` on the command line; when absent, the
//! matching `TURBOVEC_*` environment variable is read; when that is absent
//! too, the default applies.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::chunked::DEFAULT_CHUNK_BLOCKS;

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

/// Full process configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Role(s) to serve.
    pub role: Role,
    /// Listen address for `NodeService` (roles node/both).
    pub node_listen: SocketAddr,
    /// Listen address for `SearchService` (roles coordinator/both).
    pub coord_listen: SocketAddr,
    /// Shard node addresses (`http://host:port`) for the coordinator.
    pub node_addrs: Vec<String>,
    /// Path to a `.tv` index file to load (node/both). Mutually exclusive
    /// with `demo`.
    pub index_path: Option<PathBuf>,
    /// Build a random demo index instead of loading one.
    pub demo: Option<DemoConfig>,
    /// This shard's global id base (added to local slots).
    pub slot_offset: u64,
    /// Scan chunk size in SIMD blocks.
    pub chunk_blocks: usize,
    /// Participate in floor sharing (publish + adopt floors).
    pub share_floors: bool,
}

fn arg_value(args: &[String], key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

fn opt(args: &[String], key: &str, env: &str) -> Option<String> {
    arg_value(args, key).or_else(|| std::env::var(env).ok())
}

/// Parse configuration from process args (excluding argv[0]).
pub fn parse(args: &[String]) -> Result<Config, String> {
    let role = match opt(args, "role", "TURBOVEC_ROLE")
        .unwrap_or_else(|| "node".to_string())
        .as_str()
    {
        "node" => Role::Node,
        "coordinator" => Role::Coordinator,
        "both" => Role::Both,
        other => return Err(format!("unknown role {other:?} (node|coordinator|both)")),
    };

    let node_listen = opt(args, "node-listen", "TURBOVEC_NODE_LISTEN")
        .unwrap_or_else(|| "0.0.0.0:50051".to_string())
        .parse::<SocketAddr>()
        .map_err(|e| format!("invalid node listen address: {e}"))?;
    let coord_listen = opt(args, "coord-listen", "TURBOVEC_COORD_LISTEN")
        .unwrap_or_else(|| "0.0.0.0:50050".to_string())
        .parse::<SocketAddr>()
        .map_err(|e| format!("invalid coordinator listen address: {e}"))?;

    let node_addrs: Vec<String> = opt(args, "nodes", "TURBOVEC_NODES")
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| {
                    if s.starts_with("http://") || s.starts_with("https://") {
                        s.to_string()
                    } else {
                        format!("http://{s}")
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let index_path = opt(args, "index", "TURBOVEC_INDEX").map(PathBuf::from);
    let demo = opt(args, "demo-vectors", "TURBOVEC_DEMO_VECTORS")
        .map(|s| {
            let vectors = s
                .parse::<usize>()
                .map_err(|e| format!("invalid demo vector count: {e}"))?;
            let dim = opt(args, "dim", "TURBOVEC_DIM")
                .unwrap_or_else(|| "128".to_string())
                .parse::<usize>()
                .map_err(|e| format!("invalid dim: {e}"))?;
            let bit_width = opt(args, "bit-width", "TURBOVEC_BIT_WIDTH")
                .unwrap_or_else(|| "4".to_string())
                .parse::<usize>()
                .map_err(|e| format!("invalid bit width: {e}"))?;
            Ok::<DemoConfig, String>(DemoConfig {
                vectors,
                dim,
                bit_width,
            })
        })
        .transpose()?;

    if index_path.is_some() == demo.is_some() && role != Role::Coordinator {
        return Err(
            "exactly one of --index / --demo-vectors is required for node/both roles".to_string(),
        );
    }

    let slot_offset = opt(args, "slot-offset", "TURBOVEC_SLOT_OFFSET")
        .unwrap_or_else(|| "0".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid slot offset: {e}"))?;
    let chunk_blocks = opt(args, "chunk-blocks", "TURBOVEC_CHUNK_BLOCKS")
        .unwrap_or_else(|| DEFAULT_CHUNK_BLOCKS.to_string())
        .parse::<usize>()
        .map_err(|e| format!("invalid chunk blocks: {e}"))?;
    let share_floors = opt(args, "floor-sharing", "TURBOVEC_FLOOR_SHARING")
        .map(|s| matches!(s.as_str(), "1" | "true" | "on" | "yes"))
        .unwrap_or(true);

    if matches!(role, Role::Coordinator | Role::Both) && node_addrs.is_empty() {
        return Err("coordinator role requires --nodes".to_string());
    }

    Ok(Config {
        role,
        node_listen,
        coord_listen,
        node_addrs,
        index_path,
        demo,
        slot_offset,
        chunk_blocks,
        share_floors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(pairs: &[&str]) -> Vec<String> {
        pairs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_node_role() {
        let cfg = parse(&args(&[
            "--role=node",
            "--demo-vectors=1000",
            "--node-listen=127.0.0.1:9001",
            "--slot-offset=20000",
            "--chunk-blocks=8",
        ]))
        .unwrap();
        assert_eq!(cfg.role, Role::Node);
        assert_eq!(cfg.node_listen.port(), 9001);
        assert_eq!(cfg.slot_offset, 20000);
        assert_eq!(cfg.chunk_blocks, 8);
        assert_eq!(cfg.demo.unwrap().vectors, 1000);
        assert!(cfg.share_floors);
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
    fn node_requires_exactly_one_index_source() {
        assert!(parse(&args(&["--role=node"])).is_err());
        assert!(parse(&args(&[
            "--role=node",
            "--index=/tmp/x.tv",
            "--demo-vectors=10"
        ]))
        .is_err());
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
}
