//! `raft-bootstrap`: the one-time local creation of the first member.

use super::args::{cli_usage, parse_raft_bootstrap_files};
use super::rpc::print_message_json;

/// Read one proto3 JSON file into its typed message through the shared
/// decoder, so the CLI rejects what the console rejects. A file that
/// does not decode is rejected naming the file and the decoder's
/// message.
fn read_message_json<M: prost::Message + Default>(
    path: &str,
    message_name: &str,
) -> Result<M, Box<dyn std::error::Error>> {
    let pool = pipestream_search::console::descriptor_pool();
    let descriptor = pool
        .get_message_by_name(message_name)
        .ok_or_else(|| format!("{message_name} is not in the compiled descriptor set"))?;
    let json = std::fs::read(path).map_err(|e| format!("raft-bootstrap: {path}: {e}"))?;
    let bytes = pipestream_search::console::json_to_message_bytes(&descriptor, &json)
        .map_err(|e| format!("raft-bootstrap: {path}: {e}"))?;
    let message = M::decode(bytes.as_slice())
        .map_err(|e| format!("raft-bootstrap: {path}: response bytes for {message_name}: {e}"))?;
    Ok(message)
}

/// `raft-bootstrap ... --policy=<file> --limits=<file>`: the one-time
/// creation of the first member. Local, not an RPC: it decodes the
/// policy and limits, refuses a directory that already has a store or a
/// log (each named with its own create's wording), bootstraps the
/// group, waits until the member leads, prints the status JSON, and
/// shuts the host down cleanly.
pub(super) async fn raft_bootstrap(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::raft::host::{LOG_FILE, STORE_FILE};
    use pipestream_search::raft::{operator, RaftHost};
    let local = pipestream_search::config::parse_raft_local(args)
        .unwrap_or_else(|e| cli_usage("raft-bootstrap", &e));
    let Some(member) = &local.member else {
        cli_usage(
            "raft-bootstrap",
            "raft-bootstrap needs --raft-dir and the other --raft-* options",
        );
    };
    let (policy_path, limits_path) =
        parse_raft_bootstrap_files(args).unwrap_or_else(|e| cli_usage("raft-bootstrap", &e));
    if member.dir.join(STORE_FILE).exists() {
        return Err("raft-bootstrap: source authority already exists".into());
    }
    if member.dir.join(LOG_FILE).exists() {
        return Err("raft-bootstrap: raft log store already exists".into());
    }
    std::fs::create_dir_all(&member.dir)
        .map_err(|e| format!("raft-bootstrap: member directory: {e}"))?;
    let policy = read_message_json::<pipestream_search::pb::AccessPolicy>(
        &policy_path,
        "ai.protomolt.search.v1.AccessPolicy",
    )?;
    let limits = read_message_json::<pipestream_search::pb::storage::SourceAuthorityLimits>(
        &limits_path,
        "ai.protomolt.search.storage.v1.SourceAuthorityLimits",
    )?;
    let identity = operator::identity(member);
    let host_config = operator::host_config(member).map_err(|e| format!("raft-bootstrap: {e}"))?;
    let transport =
        operator::cluster_transport(member, local.tls.as_ref(), local.client_tls.as_ref())
            .map_err(|e| format!("raft-bootstrap: {e}"))?;
    let host = RaftHost::bootstrap_cluster(
        &member.dir,
        &identity,
        member.node_id,
        &policy,
        &limits,
        &host_config,
        transport,
    )
    .await
    .map_err(|status| format!("raft-bootstrap: {:?}: {}", status.code(), status.message()))?;
    host.wait(Some(std::time::Duration::from_secs(30)))
        .state(openraft::ServerState::Leader, "raft-bootstrap leader")
        .await
        .map_err(|e| format!("raft-bootstrap: the member never led: {e}"))?;
    let status = pipestream_search::raft::operator_service::member_status(&host)
        .map_err(|status| format!("raft-bootstrap: {:?}: {}", status.code(), status.message()))?;
    print_message_json("ai.protomolt.search.v1.MemberStatus", &status)?;
    host.shutdown()
        .await
        .map_err(|status| format!("raft-bootstrap: {:?}: {}", status.code(), status.message()))?;
    Ok(())
}
