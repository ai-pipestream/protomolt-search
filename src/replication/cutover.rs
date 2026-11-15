//! The atomic cutover of a live reshard to its children.

use std::path::Path;

use tonic::transport::Channel;

use crate::config::{ShardMap, ShardMapShard};
use crate::pb::search_service_client::SearchServiceClient;

use super::{catch_up_children_once, write_toml_atomic, LiveReshardState};

/// Freeze old-generation writes, catch up and verify the final WAL prefix,
/// durably publish the map file, atomically swap the live coordinator map,
/// and release the barrier. Queries continue on their frozen old snapshots.
pub async fn atomic_live_cutover(
    coordinator: &str,
    state: &LiveReshardState,
    state_path: &Path,
    shard_map_path: &Path,
) -> Result<LiveReshardState, String> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(coordinator))
            .map_err(|error| format!("invalid coordinator address: {error}"))?;
    let endpoint = crate::security::secure_endpoint(endpoint)?;
    let mut control = SearchServiceClient::new(endpoint.connect_lazy())
        .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
    let frozen = control
        .freeze_topology_writes(crate::pb::FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: state.old_topology_generation,
        })
        .await
        .map_err(|error| format!("freeze topology writes: {error}"))?
        .into_inner();
    let token = frozen.cutover_token;
    let mut durable_map_published = false;
    let result = async {
        let caught_up = catch_up_children_once(state).await?;
        caught_up.write(state_path)?;
        let map = children_map(&caught_up);
        write_toml_atomic(shard_map_path, &map, "shard map")?;
        durable_map_published = true;
        publish(&mut control, token, &map).await?;
        Ok(caught_up)
    }
    .await;
    if result.is_err() && !durable_map_published {
        let _ = control
            .abort_topology_cutover(crate::pb::AbortTopologyCutoverRequest {
                collection: String::new(),
                cutover_token: token,
            })
            .await;
    }
    match result {
        Err(error) if durable_map_published => Err(format!(
            "{error}; the new shard map is durable at {} and the coordinator remains write-frozen. Retry publication or restart the coordinator from that map before accepting writes",
            shard_map_path.display()
        )),
        other => other,
    }
}

/// The shard map the children serve under at the new generation.
fn children_map(state: &LiveReshardState) -> ShardMap {
    ShardMap {
        generation: state.new_topology_generation,
        shards: state
            .children
            .iter()
            .map(|child| ShardMapShard {
                addr: child.addr.clone(),
                replica: child.replica.clone(),
                slot_offset: child.slot_offset,
                hash_lo: Some(child.hash_lo),
                hash_hi: Some(child.hash_hi),
                placement: None,
            })
            .collect(),
        placement: None,
        derived: Vec::new(),
    }
}

/// Swap the coordinator's live map under the cutover token.
async fn publish(
    control: &mut SearchServiceClient<Channel>,
    token: u64,
    map: &ShardMap,
) -> Result<(), String> {
    let shards = map
        .shards
        .iter()
        .map(|shard| crate::pb::PublishedTopologyShard {
            addr: shard.addr.clone(),
            replica: shard.replica.clone().unwrap_or_default(),
            hash_lo: shard.hash_lo.unwrap_or_default(),
            hash_hi: shard.hash_hi.unwrap_or_default(),
            has_placement: shard.placement.is_some(),
            placement: shard.placement.unwrap_or_default(),
        })
        .collect();
    let published = control
        .publish_topology(crate::pb::PublishTopologyRequest {
            collection: String::new(),
            cutover_token: token,
            generation: map.generation,
            shards,
            placement: map.placement.as_ref().map(|tree| tree.to_proto()),
        })
        .await
        .map_err(|error| format!("publish topology: {error}"))?
        .into_inner();
    if published.topology_generation != map.generation {
        return Err("coordinator echoed the wrong topology generation".to_string());
    }
    Ok(())
}
