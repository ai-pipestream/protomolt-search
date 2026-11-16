//! The operator RPC subcommands: thin clients of the member's listener.

use super::args::{
    cli_usage, parse_raft_add_learner, parse_raft_promote, parse_raft_remove_member,
    parse_raft_status, parse_raft_verify_image,
};
use crate::cli::transport::secure_channel;

/// The operator RPC client over the member's listener, with the
/// process-wide client material, the way the console dials.
async fn operator_client(
    addr: &str,
) -> Result<
    pipestream_search::pb::raft_operator_service_client::RaftOperatorServiceClient<
        tonic::transport::Channel,
    >,
    Box<dyn std::error::Error>,
> {
    let channel = secure_channel(addr).await?;
    Ok(
        pipestream_search::pb::raft_operator_service_client::RaftOperatorServiceClient::new(
            channel,
        )
        .max_decoding_message_size(pipestream_search::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(pipestream_search::MAX_MESSAGE_BYTES),
    )
}

/// Print one proto message as proto3 JSON: the console's rendering
/// (every field, defaults included), one document, no other output on
/// stdout.
pub(super) fn print_message_json<M: prost::Message>(
    message_name: &str,
    message: &M,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = pipestream_search::console::descriptor_pool();
    let descriptor = pool
        .get_message_by_name(message_name)
        .ok_or_else(|| format!("{message_name} is not in the compiled descriptor set"))?;
    let json =
        pipestream_search::console::message_bytes_to_json(&descriptor, &message.encode_to_vec())?;
    println!(
        "{}",
        String::from_utf8(json).map_err(|e| format!("response JSON is not UTF-8: {e}"))?
    );
    Ok(())
}

pub(super) async fn raft_status(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::GetMemberStatusRequest;
    let addr = parse_raft_status(args).unwrap_or_else(|e| cli_usage("raft-status", &e));
    let status = operator_client(&addr)
        .await?
        .get_member_status(tonic::Request::new(GetMemberStatusRequest {}))
        .await
        .map_err(|status| format!("raft-status: {:?}: {}", status.code(), status.message()))?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MemberStatus", &status)
}

pub(super) async fn raft_add_learner(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::AddLearnerRequest;
    let (addr, node_id, node_addr) =
        parse_raft_add_learner(args).unwrap_or_else(|e| cli_usage("raft-add-learner", &e));
    let change = operator_client(&addr)
        .await?
        .add_learner(tonic::Request::new(AddLearnerRequest {
            node_id,
            addr: node_addr,
        }))
        .await
        .map_err(|status| {
            format!(
                "raft-add-learner: {:?}: {}",
                status.code(),
                status.message()
            )
        })?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MembershipChange", &change)
}

pub(super) async fn raft_promote(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::PromoteLearnersRequest;
    let (addr, learner_ids) =
        parse_raft_promote(args).unwrap_or_else(|e| cli_usage("raft-promote", &e));
    let change = operator_client(&addr)
        .await?
        .promote_learners(tonic::Request::new(PromoteLearnersRequest { learner_ids }))
        .await
        .map_err(|status| format!("raft-promote: {:?}: {}", status.code(), status.message()))?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MembershipChange", &change)
}

pub(super) async fn raft_remove_member(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::RemoveMemberRequest;
    let (addr, node_id) =
        parse_raft_remove_member(args).unwrap_or_else(|e| cli_usage("raft-remove-member", &e));
    let change = operator_client(&addr)
        .await?
        .remove_member(tonic::Request::new(RemoveMemberRequest { node_id }))
        .await
        .map_err(|status| {
            format!(
                "raft-remove-member: {:?}: {}",
                status.code(),
                status.message()
            )
        })?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MembershipChange", &change)
}

pub(super) async fn raft_verify_image(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::VerifyPublishedImageRequest;
    let addr = parse_raft_verify_image(args).unwrap_or_else(|e| cli_usage("raft-verify-image", &e));
    let verification = operator_client(&addr)
        .await?
        .verify_published_image(tonic::Request::new(VerifyPublishedImageRequest {}))
        .await
        .map_err(|status| {
            format!(
                "raft-verify-image: {:?}: {}",
                status.code(),
                status.message()
            )
        })?
        .into_inner();
    print_message_json(
        "ai.protomolt.search.v1.PublishedImageVerification",
        &verification,
    )
}
