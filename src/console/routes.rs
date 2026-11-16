//! The UI's convenience routes: health, stored documents by slot range,
//! and the analysis endpoint's embedding.

use super::*;

pub(super) async fn health(ctx: &Ctx) -> Result<Value, String> {
    let mut client = ctx.search_client(&ctx.coordinator)?;
    let response = client
        .cluster_health(ClusterHealthRequest {
            collection: String::new(),
        })
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    let mut ranges = Vec::new();
    let targets: Vec<Value> = response
        .targets
        .iter()
        .map(|t| {
            if let (false, true, Some(h)) = (t.is_replica, t.reachable, t.health.as_ref()) {
                ranges.push(ShardRange {
                    shard: t.shard,
                    addr: ctx.security.url(&t.addr),
                    slot_offset: h.slot_offset,
                    rows: h.document_slots.max(h.num_vectors),
                });
            }
            json!({
                "shard": t.shard,
                "addr": t.addr,
                "is_replica": t.is_replica,
                "reachable": t.reachable,
                "error": t.error,
                "num_vectors": t.health.as_ref().map(|h| h.num_vectors),
                "bm25_docs": t.health.as_ref().map(|h| h.bm25_docs),
                "live_docs": t.health.as_ref().map(|h| h.live_docs),
                "deleted_docs": t.health.as_ref().map(|h| h.deleted_docs),
                "dim": t.health.as_ref().map(|h| h.dim),
                "slot_offset": t.health.as_ref().map(|h| h.slot_offset),
                "ingest_active": t.health.as_ref().map(|h| h.ingest_active),
                "vector_backend": t.health.as_ref().map(|h| h.vector_backend.clone()),
                "scoring_fingerprint": t.health.as_ref().map(|h| h.scoring_fingerprint.clone()),
                "exact_vectors_available": t.health.as_ref().map(|h| h.exact_vectors_available),
                "collection": t.health.as_ref().map(|h| h.collection.clone()),
            })
        })
        .collect();
    *ctx.health.lock().expect("health cache poisoned") = Some((Instant::now(), ranges));
    Ok(json!({
        "targets": targets,
        "topology_generation": response.topology_generation,
        "provider_mismatch": response.provider_mismatch,
        "collections": response.collections.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
    }))
}

pub(super) async fn shard_ranges(ctx: &Ctx) -> Result<Vec<ShardRange>, String> {
    if let Some((at, ranges)) = ctx.health.lock().expect("health cache poisoned").as_ref() {
        if at.elapsed() < HEALTH_TTL {
            return Ok(ranges.clone());
        }
    }
    health(ctx).await?;
    Ok(ctx
        .health
        .lock()
        .expect("health cache poisoned")
        .as_ref()
        .map(|(_, r)| r.clone())
        .unwrap_or_default())
}

#[derive(serde::Deserialize)]
pub(super) struct DocumentsBody {
    #[serde(default)]
    pub(super) doc_ids: Vec<Value>,
}

/// The request's ids as unsigned integers, numbers or decimal strings.
fn parse_doc_ids(values: &[Value]) -> Result<Vec<u64>, Reply> {
    let mut ids = Vec::with_capacity(values.len());
    for value in values {
        let id = match value {
            Value::Number(n) => n.as_u64(),
            Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        };
        let Some(id) = id else {
            return Err(Reply::error(
                400,
                "INVALID_ARGUMENT",
                format!("doc_ids entries are unsigned integers, got {value}"),
            ));
        };
        ids.push(id);
    }
    if ids.len() > 1000 {
        return Err(Reply::error(
            400,
            "INVALID_ARGUMENT",
            "at most 1000 doc_ids per call",
        ));
    }
    Ok(ids)
}

type ShardIds = BTreeMap<u32, (String, Vec<u64>)>;

/// The ids grouped by the shard whose slot range holds each, and the
/// ids no shard holds.
fn route_doc_ids(ranges: &[ShardRange], ids: Vec<u64>) -> (ShardIds, Vec<u64>) {
    let mut by_shard: ShardIds = BTreeMap::new();
    let mut unrouted = Vec::new();
    for id in ids {
        match ranges
            .iter()
            .find(|r| id >= r.slot_offset && id < r.slot_offset + r.rows)
        {
            Some(r) => by_shard
                .entry(r.shard)
                .or_insert_with(|| (r.addr.clone(), Vec::new()))
                .1
                .push(id),
            None => unrouted.push(id),
        }
    }
    (by_shard, unrouted)
}

/// One shard's stored documents as the UI's JSON rows.
async fn shard_documents(
    ctx: &Ctx,
    shard: u32,
    addr: &str,
    doc_ids: Vec<u64>,
) -> Result<Vec<Value>, Reply> {
    let mut client = ctx
        .node_client(addr)
        .map_err(|e| Reply::error(502, "UNAVAILABLE", e))?;
    let found = match client.get_documents(GetDocumentsRequest { doc_ids }).await {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return Err(Reply::error(
                http_status(status.code()),
                code_name(status.code()),
                format!("get_documents on shard {shard}: {}", status.message()),
            ))
        }
    };
    Ok(found
        .documents
        .into_iter()
        .map(|doc| {
            json!({
                "doc_id": doc.doc_id.to_string(),
                "shard": shard,
                "text": doc.text,
                "lineage": doc.lineage.map(|l| json!({
                    "parent_id": l.parent_id.to_string(),
                    "group_id": l.group_id.to_string(),
                    "span_start": l.span_start,
                    "span_end": l.span_end,
                })),
            })
        })
        .collect())
}

/// Stored text for global document ids, from the owning nodes: an id
/// belongs to the shard whose slot range holds it. The shards are read
/// concurrently and their rows returned in shard order; the first shard
/// to fail answers for the call and the other reads are cancelled.
pub(super) async fn documents(ctx: &Arc<Ctx>, body: &[u8]) -> Reply {
    let parsed: DocumentsBody = match serde_json::from_slice(body) {
        Ok(b) => b,
        Err(e) => return Reply::error(400, "INVALID_ARGUMENT", format!("bad request: {e}")),
    };
    let ids = match parse_doc_ids(&parsed.doc_ids) {
        Ok(ids) => ids,
        Err(reply) => return reply,
    };
    let ranges = match shard_ranges(ctx).await {
        Ok(r) => r,
        Err(e) => return Reply::error(502, "UNAVAILABLE", e),
    };
    let (by_shard, unrouted) = route_doc_ids(&ranges, ids);
    let mut reads = tokio::task::JoinSet::new();
    for (shard, (addr, doc_ids)) in by_shard {
        let ctx = Arc::clone(ctx);
        reads.spawn(async move {
            let rows = shard_documents(&ctx, shard, &addr, doc_ids).await;
            rows.map(|rows| (shard, rows))
        });
    }
    let mut per_shard = Vec::new();
    while let Some(joined) = reads.join_next().await {
        match joined {
            Ok(Ok(rows)) => per_shard.push(rows),
            Ok(Err(reply)) => return reply,
            Err(e) => return Reply::error(500, "INTERNAL", format!("shard read: {e}")),
        }
    }
    per_shard.sort_by_key(|(shard, _)| *shard);
    let documents: Vec<Value> = per_shard.into_iter().flat_map(|(_, rows)| rows).collect();
    Reply::json(
        200,
        json!({
            "documents": documents,
            "unrouted": unrouted.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        }),
    )
}

#[derive(serde::Deserialize)]
pub(super) struct EmbedBody {
    #[serde(default)]
    pub(super) text: String,
}

/// One query text embedded by the `--analysis` endpoint, dialed with the
/// console's client TLS material and bearer token (see `embed_options`).
pub(super) async fn embed(ctx: &Ctx, body: &[u8]) -> Reply {
    let Some(addr) = ctx.analysis.as_deref() else {
        return Reply::error(
            501,
            "UNIMPLEMENTED",
            "no --analysis sidecar configured; dense queries need a pasted vector",
        );
    };
    let parsed: EmbedBody = match serde_json::from_slice(body) {
        Ok(b) => b,
        Err(e) => return Reply::error(400, "INVALID_ARGUMENT", format!("bad request: {e}")),
    };
    if parsed.text.trim().is_empty() {
        return Reply::error(400, "INVALID_ARGUMENT", "text is required");
    }
    let started = Instant::now();
    match crate::analyzer::embed_text_with_options(addr, &parsed.text, &ctx.embed).await {
        Ok(vector) => Reply::json(
            200,
            json!({
                "dim": vector.len(),
                "vector": vector,
                "ms": started.elapsed().as_secs_f32() * 1e3,
            }),
        ),
        Err(status) => status_reply(&status),
    }
}
