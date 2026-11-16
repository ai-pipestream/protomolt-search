//! The `raft-*` operator subcommands: `raft-prepare`, and the thin
//! clients of the operator RPCs next to the local `raft-bootstrap`.

#[cfg(all(feature = "raft", feature = "tls"))]
mod args;
#[cfg(all(feature = "raft", feature = "tls"))]
mod bootstrap;
#[cfg(all(feature = "raft", feature = "tls"))]
mod rpc;
#[cfg(all(test, feature = "raft", feature = "tls"))]
mod tests;

use pipestream_search::config::parse;

#[cfg(all(feature = "raft", feature = "tls"))]
use args::cli_usage;

/// Lowercase hex of `bytes`.
#[cfg(all(feature = "raft", feature = "tls"))]
pub(in crate::cli) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
    }
    out
}

/// `raft-prepare --raft-dir=... --raft-node-id=... --raft-group-id=...
/// --raft-authority-incarnation=... --raft-listen=... --raft-peers=...`:
/// create the durable state of a member that will join by snapshot. The
/// genesis policy and limits are placeholders the group's image replaces;
/// no entry applies before that.
pub(crate) fn raft_prepare(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = parse(args)?;
    let Some(member) = &cfg.raft else {
        return Err("raft-prepare needs --raft-dir and the other --raft-* options".into());
    };
    #[cfg(all(feature = "raft", feature = "tls"))]
    {
        use pipestream_search::raft::{operator, RaftHost};
        let identity = operator::identity(member);
        let policy = pipestream_search::pb::AccessPolicy {
            format_version: 3,
            revision: 1,
            resources: Vec::new(),
            grants: Vec::new(),
        };
        let limits = pipestream_search::pb::storage::SourceAuthorityLimits {
            max_owners: 16,
            max_decisions: 256,
            max_payload_bytes: 16 << 20,
            max_command_bytes: 64 << 10,
        };
        RaftHost::prepare_member(&member.dir, &identity, member.node_id, &policy, &limits)
            .map_err(|status| format!("raft-prepare: {}", status.message()))?;
        println!(
            "prepared raft member {} of group {} in {}; add it as a learner from the leader",
            member.node_id,
            hex(&member.group_id),
            member.dir.display()
        );
        Ok(())
    }
    #[cfg(not(all(feature = "raft", feature = "tls")))]
    {
        let _ = member;
        Err("this build has no Raft support (features `raft` and `tls` are needed)".into())
    }
}

/// The `raft-*` operator subcommands on a build without Raft: parsed
/// nowhere, attempted nowhere.
#[cfg(not(all(feature = "raft", feature = "tls")))]
pub(crate) async fn raft_operator_cli(
    command: &str,
    _args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    Err(format!("{command} needs Raft support (features `raft` and `tls` are needed)").into())
}

/// The `raft-*` operator subcommands: status and membership over the
/// member's listener, bootstrap local. The RPC subcommands install the
/// client material their `--tls-*` flags name before they dial, so a
/// member's mTLS listener is reached the way the console reaches it; a
/// call without `--tls-ca` dials in plaintext and a TLS listener
/// rejects the handshake, which is the transport's own refusal.
#[cfg(all(feature = "raft", feature = "tls"))]
pub(crate) async fn raft_operator_cli(
    command: &str,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    if command != "raft-bootstrap" {
        let local = pipestream_search::config::parse_raft_local(args)
            .unwrap_or_else(|e| cli_usage(command, &e));
        pipestream_search::security::install_client_tls(local.client_tls);
    }
    match command {
        "raft-status" => rpc::raft_status(args).await,
        "raft-add-learner" => rpc::raft_add_learner(args).await,
        "raft-promote" => rpc::raft_promote(args).await,
        "raft-remove-member" => rpc::raft_remove_member(args).await,
        "raft-verify-image" => rpc::raft_verify_image(args).await,
        "raft-bootstrap" => bootstrap::raft_bootstrap(args).await,
        _ => cli_usage(command, "unknown raft subcommand"),
    }
}
