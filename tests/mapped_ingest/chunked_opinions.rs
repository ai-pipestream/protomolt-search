//! Repeated-submessage chunk denormalization, parent collapse, and chunk error paths.

use super::*;

fn role(
    role: pipestream_search::pb::hints::BlockRole,
) -> pipestream_search::pb::hints::FieldIndexHint {
    pipestream_search::pb::hints::FieldIndexHint {
        block_role: role as i32,
        ..Default::default()
    }
}

/// law2.v1.Opinion { id, case_name (parent TEXT), court_code, year,
/// chunks[] } with law2.v1.Chunk { cid (CHUNK_ID), text (the body),
/// embedding, page }.
fn opinion_set() -> Vec<u8> {
    use prost_types::field_descriptor_proto::Label;
    let chunk = d_message(
        "Chunk",
        &[
            d_field(
                "cid",
                1,
                Label::Optional,
                Type::String,
                None,
                Some(&role(pipestream_search::pb::hints::BlockRole::ChunkId)),
            ),
            d_field("text", 2, Label::Optional, Type::String, None, None),
            d_field("embedding", 3, Label::Repeated, Type::Float, None, None),
            d_field("page", 4, Label::Optional, Type::Int32, None, None),
        ],
    );
    let opinion = d_message(
        "Opinion",
        &[
            d_field("id", 1, Label::Optional, Type::String, None, None),
            d_field("case_name", 2, Label::Optional, Type::String, None, None),
            d_field("court_code", 3, Label::Optional, Type::String, None, None),
            d_field("year", 4, Label::Optional, Type::Int64, None, None),
            d_field(
                "chunks",
                5,
                Label::Repeated,
                Type::Message,
                Some(".law2.v1.Chunk"),
                Some(&role(pipestream_search::pb::hints::BlockRole::Chunks)),
            ),
        ],
    );
    d_set("law2.v1", "opinion.proto", &[opinion, chunk])
}

struct ChunkDoc {
    cid: Option<String>,
    text: Option<String>,
    embedding: Vec<f32>,
    page: Option<u64>,
}

fn opinion_doc(
    id: Option<&str>,
    case_name: &str,
    court: &str,
    year: i64,
    chunks: &[ChunkDoc],
) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(id) = id {
        w_str(&mut out, 1, id);
    }
    w_str(&mut out, 2, case_name);
    w_str(&mut out, 3, court);
    w_varint(&mut out, 4, year as u64);
    for chunk in chunks {
        let mut c = Vec::new();
        if let Some(cid) = &chunk.cid {
            w_str(&mut c, 1, cid);
        }
        if let Some(text) = &chunk.text {
            w_str(&mut c, 2, text);
        }
        if !chunk.embedding.is_empty() {
            w_packed_floats(&mut c, 3, &chunk.embedding);
        }
        if let Some(page) = chunk.page {
            w_varint(&mut c, 4, page);
        }
        w_msg(&mut out, 5, &c);
    }
    out
}

fn chunk_of(parent: usize, ordinal: usize, row: usize) -> ChunkDoc {
    ChunkDoc {
        cid: Some(format!("op-{parent}-c{ordinal}")),
        text: Some(format!("chunk alpha {parent} {ordinal}")),
        embedding: unit_vectors(16, DIM, 99)[row * DIM..(row + 1) * DIM].to_vec(),
        page: Some(10 * parent as u64 + ordinal as u64),
    }
}

fn opinion_bind() -> MappedBind {
    let plan = derive_plan(&opinion_set(), "law2.v1.Opinion").expect("the Opinion plan derives");
    MappedBind {
        collection: String::new(),
        descriptor_set: opinion_set(),
        message_type: "law2.v1.Opinion".into(),
        expected_fingerprint: plan.fingerprint,
        // Empty: the scope's only TEXT field is the body by default.
        body_path: String::new(),
        analysis: None,
        materialize: None,
        field_analysis: Vec::new(),
        index_definition: None,
    }
}

fn opinion_node_config(analysis: String) -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(analysis),
        bm25_fields: vec!["body".into(), "case_name".into()],
        facet_fields: vec!["id".into(), "court_code".into(), "cid".into()],
        integer_fields: vec!["year".into(), "page".into()],
        ..Default::default()
    }
}

fn reduced(id: &str) -> u64 {
    u64::from_be_bytes(
        turbo_sha256::sha256(id.as_bytes()).0[..8]
            .try_into()
            .unwrap(),
    )
}

/// Chunk rows are the searchable rows: parent scalars and parent TEXT
/// denormalize onto every chunk, filters see parent and chunk fields
/// together with no join, the vector leg lands per chunk at the same
/// ids, and lineage carries the reduced parent id — so the engine's
/// existing parent-collapse groups mapped chunks with no new machinery.
#[tokio::test]
async fn chunked_ingest_denormalizes_and_collapses() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(opinion_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    // op-0: 2 chunks (rows 0, 1); op-1: 3 chunks (rows 2, 3, 4);
    // op-2: ZERO chunks — a legitimate empty document, zero rows.
    let mut row = 0;
    let mut docs = Vec::new();
    for (parent, n_chunks) in [(0usize, 2usize), (1, 3), (2, 0)] {
        let chunks: Vec<ChunkDoc> = (0..n_chunks)
            .map(|ordinal| {
                let c = chunk_of(parent, ordinal, row);
                row += 1;
                c
            })
            .collect();
        docs.push(opinion_doc(
            Some(&format!("op-{parent}")),
            &format!("case name {parent}"),
            if parent % 2 == 0 { "ca9" } else { "scotus" },
            1990 + parent as i64,
            &chunks,
        ));
    }
    let response = send(&addr, opinion_bind(), docs)
        .await
        .expect("chunked ingest");
    assert_eq!(response.added, 5, "rows = chunks");
    assert_eq!(
        response.parents, 3,
        "source documents, the chunkless one included"
    );
    assert_eq!(response.total, 5);

    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());

    // A filter mixing PARENT and CHUNK fields selects exact rows, and
    // projections read the denormalized parent id next to per-chunk
    // values.
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "alpha".into(),
            k: 10,
            filter: "year >= 1991 && page >= 11".into(),
            projections: ["id", "cid", "year", "page"]
                .iter()
                .map(|name| NamedProjection {
                    name: (*name).to_string(),
                    expression: (*name).to_string(),
                })
                .collect(),
            ..Default::default()
        }))
        .await
        .expect("mixed parent/chunk filter")
        .into_inner()
        .hits;
    let mut selected: Vec<(u64, Vec<Option<projected_value::Value>>)> = hits
        .iter()
        .map(|h| {
            (
                h.doc_id,
                h.projected.iter().map(|p| p.value.clone()).collect(),
            )
        })
        .collect();
    selected.sort_unstable_by_key(|(id, _)| *id);
    use projected_value::Value::{IntValue, StringValue};
    assert_eq!(
        selected,
        vec![
            (
                3,
                vec![
                    Some(StringValue("op-1".into())),
                    Some(StringValue("op-1-c1".into())),
                    Some(IntValue(1991)),
                    Some(IntValue(11)),
                ]
            ),
            (
                4,
                vec![
                    Some(StringValue("op-1".into())),
                    Some(StringValue("op-1-c2".into())),
                    Some(IntValue(1991)),
                    Some(IntValue(12)),
                ]
            ),
        ]
    );

    // Parent collapse over the vector leg: one hit per parent, keyed by
    // the reduced parent id the lineage carries.
    let hits = coordinator
        .search(Request::new(SearchRequest {
            k: 5,
            vector: unit_vectors(16, DIM, 99)[..DIM].to_vec(),
            collapse_parents: true,
            ..Default::default()
        }))
        .await
        .expect("collapsed vector search")
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 2, "one hit per parent that has chunks");
    assert_eq!(hits[0].vector_id, 0, "the queried chunk wins its parent");
    assert_eq!(hits[0].parent_id, reduced("op-0"));
    let parents: std::collections::HashSet<u64> = hits.iter().map(|h| h.parent_id).collect();
    assert_eq!(
        parents,
        [reduced("op-0"), reduced("op-1")].into_iter().collect()
    );
}

/// Chunk refusals name the document, the chunk ordinal, and the field.
#[tokio::test]
async fn chunked_refusals_name_document_chunk_and_field() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(opinion_node_config(analysis)).await;
    seed_calibration(&addr).await;

    // The body must live inside the CHUNKS scope.
    let mut parent_body = opinion_bind();
    parent_body.body_path = "case_name".into();
    expect_refusal(send(&addr, parent_body, vec![]).await, "CHUNKS-scope");

    let good = |row: usize| chunk_of(0, row, row);
    let doc_with =
        |chunks: &[ChunkDoc]| opinion_doc(Some("op-0"), "case name 0", "ca9", 1990, chunks);

    let mut no_vector = good(1);
    no_vector.embedding = Vec::new();
    expect_refusal(
        send(&addr, opinion_bind(), vec![doc_with(&[good(0), no_vector])]).await,
        "document 0: chunk 1:",
    );

    let mut no_cid = good(0);
    no_cid.cid = None;
    expect_refusal(
        send(&addr, opinion_bind(), vec![doc_with(&[no_cid])]).await,
        "the chunk has no id",
    );

    let mut no_text = good(0);
    no_text.text = None;
    expect_refusal(
        send(&addr, opinion_bind(), vec![doc_with(&[no_text])]).await,
        "no body text",
    );

    let orphan = opinion_doc(None, "case name 0", "ca9", 1990, &[good(0)]);
    expect_refusal(
        send(&addr, opinion_bind(), vec![orphan]).await,
        "identity is required",
    );

    // Nothing above poisoned the shard.
    let response = send(&addr, opinion_bind(), vec![doc_with(&[good(0), good(1)])])
        .await
        .expect("the shard ingests after refusals");
    assert_eq!(response.added, 2);
    assert_eq!(response.parents, 1);
}
