//! The BFF methods the console answers itself, from the coordinator's
//! health, the nodes' stored text, and the analysis endpoint. Each returns
//! proto3 JSON of its response message.

use prost_reflect::{DynamicMessage, Value as Field};

use super::*;

/// Ids one PreviewDocuments call may name.
const MAX_PREVIEW_IDS: usize = 100;
/// Snippet characters when the request names none, and at most.
const PREVIEW_CHARS: (usize, usize) = (600, 4000);
/// The longest text PreviewEmbedding embeds.
const MAX_EMBED_TEXT_BYTES: usize = 8192;

pub(super) async fn own_op(
    ctx: &Arc<Ctx>,
    op: Op,
    input: &DynamicMessage,
) -> Result<Value, BffError> {
    match op {
        Op::Config => console_config(ctx),
        Op::Health => health_summary(ctx, &string_field(input, "collection")).await,
        Op::Collections => list_collections(ctx).await,
        Op::Describe => describe_method(&string_field(input, "method")),
        Op::PreviewDocuments => preview_documents(ctx, input).await,
        Op::PreviewEmbedding => preview_embedding(ctx, &string_field(input, "text")).await,
        Op::Upstream => Err(BffError::new(
            Code::Internal,
            "an upstream method has no own answer",
        )),
    }
}

fn string_field(message: &DynamicMessage, name: &str) -> String {
    match message.get_field_by_name(name).as_deref() {
        Some(Field::String(s)) => s.clone(),
        _ => String::new(),
    }
}

fn u32_field(message: &DynamicMessage, name: &str) -> u32 {
    match message.get_field_by_name(name).as_deref() {
        Some(Field::U32(n)) => *n,
        _ => 0,
    }
}

fn u64_list(message: &DynamicMessage, name: &str) -> Vec<u64> {
    match message.get_field_by_name(name).as_deref() {
        Some(Field::List(values)) => values.iter().filter_map(Field::as_u64).collect(),
        _ => Vec::new(),
    }
}

/// A prost message as proto3 JSON, through its descriptor.
fn message_json(full_name: &str, bytes: &[u8]) -> Result<Value, BffError> {
    let internal = |e: String| BffError::new(Code::Internal, e);
    let desc = bff_pool()
        .get_message_by_name(full_name)
        .ok_or_else(|| internal(format!("{full_name} is not in the descriptor set")))?;
    let json = message_bytes_to_json(&desc, bytes).map_err(internal)?;
    serde_json::from_slice(&json).map_err(|e| internal(e.to_string()))
}

fn console_config(ctx: &Ctx) -> Result<Value, BffError> {
    let spec = crate::analyzer::body_spec().encode_to_vec();
    Ok(json!({
        "bodySpec": message_json(&format!("{PACKAGE}.AnalysisSpec"), &spec)?,
        "embeddingAvailable": ctx.analysis.is_some(),
        "rawExplorer": ctx.raw_explorer,
    }))
}

async fn cluster_health(ctx: &Ctx, collection: &str) -> Result<ClusterHealthResponse, BffError> {
    let unavailable = |_| BffError::upstream(&Status::unavailable(""));
    let mut client = ctx.search_client(&ctx.coordinator).map_err(unavailable)?;
    let request = ClusterHealthRequest {
        collection: collection.to_string(),
    };
    match client.cluster_health(request).await {
        Ok(r) => Ok(r.into_inner()),
        Err(status) => Err(BffError::upstream(&status)),
    }
}

/// Per-target counts with no address, probe error, or fingerprint.
async fn health_summary(ctx: &Ctx, collection: &str) -> Result<Value, BffError> {
    let health = cluster_health(ctx, collection).await?;
    let shards: Vec<Value> = health
        .targets
        .iter()
        .map(|t| {
            let h = t.health.as_ref();
            json!({
                "shard": t.shard,
                "replica": t.is_replica,
                "reachable": t.reachable,
                "liveDocs": h.map_or(0, |h| h.live_docs).to_string(),
                "deletedDocs": h.map_or(0, |h| h.deleted_docs).to_string(),
                "ingestActive": h.is_some_and(|h| h.ingest_active),
            })
        })
        .collect();
    Ok(json!({
        "shards": shards,
        "topologyGeneration": health.topology_generation.to_string(),
        "providerMismatch": !health.provider_mismatch.is_empty(),
        "collections": health.collections.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
    }))
}

fn collection_summary(name: &str, health: &ClusterHealthResponse) -> Value {
    let primaries = health.targets.iter().filter(|t| !t.is_replica);
    let live: u64 = primaries
        .clone()
        .filter_map(|t| t.health.as_ref().map(|h| h.live_docs))
        .sum();
    json!({ "name": name, "shards": primaries.count(), "liveDocs": live.to_string() })
}

async fn list_collections(ctx: &Ctx) -> Result<Value, BffError> {
    let health = cluster_health(ctx, "").await?;
    let collections: Vec<Value> = if health.collections.is_empty() {
        vec![collection_summary("", &health)]
    } else {
        health
            .collections
            .iter()
            .map(|c| collection_summary(&c.name, &c.health.clone().unwrap_or_default()))
            .collect()
    };
    Ok(json!({ "collections": collections }))
}

/// The shapes of one served BFF method, rendered as the explorer renders
/// the raw methods.
fn describe_method(name: &str) -> Result<Value, BffError> {
    let (method, _) = bff_method(name)?;
    let (mut messages, mut enums) = (serde_json::Map::new(), serde_json::Map::new());
    describe_message(&method.input(), &mut messages, &mut enums);
    describe_message(&method.output(), &mut messages, &mut enums);
    let schema = json!({
        "method": name,
        "server_streaming": method.is_server_streaming(),
        "request": method.input().full_name(),
        "response": method.output().full_name(),
        "messages": messages,
        "enums": enums,
    });
    Ok(json!({ "schemaJson": schema.to_string() }))
}

/// The first `max` characters of `text`, and whether anything was cut.
pub(super) fn snippet(text: &str, max: usize) -> (&str, bool) {
    match text.char_indices().nth(max) {
        Some((end, _)) => (&text[..end], true),
        None => (text, false),
    }
}

async fn preview_documents(ctx: &Arc<Ctx>, input: &DynamicMessage) -> Result<Value, BffError> {
    let ids = u64_list(input, "doc_ids");
    if ids.len() > MAX_PREVIEW_IDS {
        let message = format!(
            "at most {MAX_PREVIEW_IDS} doc_ids per call, got {}",
            ids.len()
        );
        return Err(BffError::new(Code::InvalidArgument, message));
    }
    let max = match u32_field(input, "max_chars") as usize {
        0 => PREVIEW_CHARS.0,
        n if n <= PREVIEW_CHARS.1 => n,
        n => {
            let message = format!("max_chars is at most {}, got {n}", PREVIEW_CHARS.1);
            return Err(BffError::new(Code::InvalidArgument, message));
        }
    };
    let (per_shard, unrouted) = read_documents(ctx, ids)
        .await
        .map_err(|s| BffError::upstream(&s))?;
    let documents: Vec<Value> = per_shard
        .into_iter()
        .flat_map(|(shard, docs)| docs.into_iter().map(move |doc| (shard, doc)))
        .map(|(shard, doc)| {
            let (text, truncated) = snippet(&doc.text, max);
            json!({
                "docId": doc.doc_id.to_string(),
                "shard": shard,
                "snippet": text,
                "truncated": truncated,
                "parentId": doc.lineage.map_or(0, |l| l.parent_id).to_string(),
            })
        })
        .collect();
    let unrouted: Vec<String> = unrouted.iter().map(u64::to_string).collect();
    Ok(json!({ "documents": documents, "unrouted": unrouted }))
}

async fn preview_embedding(ctx: &Ctx, text: &str) -> Result<Value, BffError> {
    if text.trim().is_empty() || text.len() > MAX_EMBED_TEXT_BYTES {
        let message = format!("text is required and at most {MAX_EMBED_TEXT_BYTES} bytes");
        return Err(BffError::new(Code::InvalidArgument, message));
    }
    let started = Instant::now();
    let vector = embed_vector(ctx, text)
        .await
        .map_err(|s| BffError::upstream(&s))?;
    Ok(json!({
        "dim": vector.len(),
        "vector": vector,
        "ms": started.elapsed().as_secs_f32() * 1e3,
    }))
}
