//! Argument parsing of the `raft-*` operator subcommands. Exit codes:
//! 0 on success, 2 on a usage error (the invocation names no valid call,
//! so nothing is attempted), 1 on a rejected call with the status code
//! and message on stderr.

/// Report a usage error on stderr and exit with status 2.
pub(super) fn cli_usage(command: &str, detail: &str) -> ! {
    eprintln!("{command}: usage error: {detail}");
    std::process::exit(2);
}

/// The value of `--{key}=...` among a subcommand's args.
pub(super) fn cli_flag(args: &[String], key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

/// `raft-status --addr=<host:port>`: the member's address.
pub(super) fn parse_raft_status(args: &[String]) -> Result<String, String> {
    cli_flag(args, "addr").ok_or_else(|| "raft-status --addr=<host:port>".to_string())
}

/// `raft-add-learner --addr=<leader> --node-id=<n> --node-addr=<host:port>`:
/// the leader to ask, the learner's id, the address peers dial for it.
pub(super) fn parse_raft_add_learner(args: &[String]) -> Result<(String, u64, String), String> {
    const USAGE: &str = "raft-add-learner --addr=<leader> --node-id=<n> --node-addr=<host:port>";
    let addr = cli_flag(args, "addr").ok_or(USAGE)?;
    let raw = cli_flag(args, "node-id").ok_or(USAGE)?;
    let node_id = raw
        .parse::<u64>()
        .map_err(|_| format!("{USAGE}: --node-id={raw:?} is not a node id"))?;
    let node_addr = cli_flag(args, "node-addr").ok_or(USAGE)?;
    Ok((addr, node_id, node_addr))
}

/// `raft-promote --addr=<leader> --node-id=<n>[,<n>...]`: the leader to
/// ask and the learners to promote.
pub(super) fn parse_raft_promote(args: &[String]) -> Result<(String, Vec<u64>), String> {
    const USAGE: &str = "raft-promote --addr=<leader> --node-id=<n>[,<n>...]";
    let addr = cli_flag(args, "addr").ok_or(USAGE)?;
    let raw = cli_flag(args, "node-id").ok_or(USAGE)?;
    let mut ids = Vec::new();
    for part in raw.split(',') {
        ids.push(
            part.trim()
                .parse::<u64>()
                .map_err(|_| format!("{USAGE}: --node-id={raw:?} is not a learner list"))?,
        );
    }
    Ok((addr, ids))
}

/// `raft-remove-member --addr=<leader> --node-id=<n>`: the leader to ask
/// and the member to remove.
pub(super) fn parse_raft_remove_member(args: &[String]) -> Result<(String, u64), String> {
    const USAGE: &str = "raft-remove-member --addr=<leader> --node-id=<n>";
    let addr = cli_flag(args, "addr").ok_or(USAGE)?;
    let raw = cli_flag(args, "node-id").ok_or(USAGE)?;
    let node_id = raw
        .parse::<u64>()
        .map_err(|_| format!("{USAGE}: --node-id={raw:?} is not a node id"))?;
    Ok((addr, node_id))
}

/// `raft-verify-image --addr=<member>`: the member to ask.
pub(super) fn parse_raft_verify_image(args: &[String]) -> Result<String, String> {
    cli_flag(args, "addr").ok_or_else(|| "raft-verify-image --addr=<member>".to_string())
}

/// `raft-bootstrap`'s two JSON files: the policy and the limits the
/// first member is created with.
pub(super) fn parse_raft_bootstrap_files(args: &[String]) -> Result<(String, String), String> {
    const USAGE: &str = "raft-bootstrap --raft-dir=... --raft-node-id=... --raft-group-id=... \
        --raft-authority-incarnation=... --raft-listen=... --raft-peers=... \
        --policy=<file> --limits=<file>";
    let policy = cli_flag(args, "policy").ok_or(USAGE)?;
    let limits = cli_flag(args, "limits").ok_or(USAGE)?;
    Ok((policy, limits))
}
