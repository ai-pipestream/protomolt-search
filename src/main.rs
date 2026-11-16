//! pipestream-search binary: one process, one or both roles, one or more
//! shards.
//!
//! Configuration: `--config cluster.toml` (TOML file) with `PIPESTREAM_SEARCH_*`
//! env overrides and `--key=value` flags on top (see `src/config.rs`).
//!
//! Subcommand: `pipestream-search configure-backend --fit-from=host:port
//! --apply-to=h1:p1,h2:p2` copies opaque provider construction state from
//! one configured node to empty peers.
//!
//! Examples:
//!
//! ```text
//! # Single-process demo: coordinator + node + random demo corpus,
//! # then one self-issued search against itself.
//! pipestream-search --role=both --demo-vectors=20000 --allow-plaintext \
//!     --nodes=127.0.0.1:50051 --demo-query
//!
//! # Static two-machine cluster (see README "Two-machine runbook").
//! # host-a:    pipestream-search --config /etc/turbovec/host-a.toml
//! # host-b:   pipestream-search --config /etc/turbovec/host-b.toml
//! ```

mod cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if matches!(
        argv.first().map(String::as_str),
        Some("configure-backend" | "calibrate")
    ) {
        return cli::configure_backend(&argv[1..]).await;
    }
    if argv.first().map(String::as_str) == Some("raft-prepare") {
        return cli::raft_prepare(&argv[1..]);
    }
    if matches!(
        argv.first().map(String::as_str),
        Some(
            "raft-status"
                | "raft-add-learner"
                | "raft-promote"
                | "raft-remove-member"
                | "raft-verify-image"
                | "raft-bootstrap"
        )
    ) {
        let command = argv[0].clone();
        return cli::raft_operator_cli(&command, &argv[1..]).await;
    }
    let cfg = pipestream_search::config::parse(&argv)?;
    cli::run(cfg).await
}
