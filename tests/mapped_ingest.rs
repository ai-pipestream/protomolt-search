//! Descriptor-mapped protobuf-native ingest (`docs/descriptor-mappings.md`
//! increment 2): bind a derived plan by fingerprint, stream serialized
//! protobuf documents, and land them on the ordinary column planes and
//! both legs — no JSON, no intermediate document model, ids in lockstep
//! across BM25 and the vector index.
//!
//! Descriptor sets are built with prost-types (hint-free inference is
//! enough here); DOCUMENTS are hand-encoded at the wire level, because
//! that is exactly what the extractor consumes in production.

mod common;

use prost::Message as _;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use common::mock::start_mock_analysis;
use turbovec_search::harness::{fit_calibration, start_empty_node, unit_vectors};
use turbovec_search::mapping::derive_plan;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{
    ingest_mapped_request, projected_value, AddDocumentsRequest, Bm25SearchRequest,
    IngestMappedRequest, IngestMappedResponse, MappedBind, MaterializeKind, MaterializeSpec,
    MaterializedColumn, NamedProjection, SearchRequest, SetCalibrationRequest,
};

const DIM: usize = 8;
const BIT_WIDTH: usize = 4;

// ---------------------------------------------------------------------
// The corpus type: law.v1.Case
// ---------------------------------------------------------------------

fn scalar(name: &str, number: i32, typ: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(typ as i32),
        ..Default::default()
    }
}

fn message_field(name: &str, number: i32, type_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(Type::Message as i32),
        type_name: Some(type_name.to_string()),
        ..Default::default()
    }
}

fn timestamp_file() -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some("google/protobuf/timestamp.proto".to_string()),
        package: Some("google.protobuf".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("Timestamp".to_string()),
            field: vec![
                scalar("seconds", 1, Type::Int64, Label::Optional),
                scalar("nanos", 2, Type::Int32, Label::Optional),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// law.v1.Case:
///   id=1 string (KEYWORD, the doc id, facet "id")
///   title=2 string (TEXT, bound as the body)
///   price=3 double (f64 "price")
///   embedding=4 repeated float (the vector)
///   status=5 enum law.v1.Status (facet "status", value NAMES)
///   created_at=6 Timestamp (DATE, i64 "created_at" epoch micros)
///   meta=7 law.v1.Meta { author=1 string TEXT "meta_author",
///                        page_count=2 int32 i64 "meta_page_count" }
///   year=8 int64 (i64 "year")
///   tags=9 repeated string (FAMILY_NONE, visibly skipped)
///   published=10 bool (facet "published", "true"/"false")
fn case_set() -> Vec<u8> {
    let meta = DescriptorProto {
        name: Some("Meta".to_string()),
        field: vec![
            scalar("author", 1, Type::String, Label::Optional),
            scalar("page_count", 2, Type::Int32, Label::Optional),
        ],
        ..Default::default()
    };
    let status = EnumDescriptorProto {
        name: Some("Status".to_string()),
        value: [("STATUS_UNSPECIFIED", 0), ("OPEN", 1), ("CLOSED", 2)]
            .iter()
            .map(|(name, number)| EnumValueDescriptorProto {
                name: Some((*name).to_string()),
                number: Some(*number),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let case = DescriptorProto {
        name: Some("Case".to_string()),
        field: vec![
            scalar("id", 1, Type::String, Label::Optional),
            scalar("title", 2, Type::String, Label::Optional),
            scalar("price", 3, Type::Double, Label::Optional),
            scalar("embedding", 4, Type::Float, Label::Repeated),
            FieldDescriptorProto {
                name: Some("status".to_string()),
                number: Some(5),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Enum as i32),
                type_name: Some(".law.v1.Status".to_string()),
                ..Default::default()
            },
            message_field("created_at", 6, ".google.protobuf.Timestamp"),
            message_field("meta", 7, ".law.v1.Meta"),
            scalar("year", 8, Type::Int64, Label::Optional),
            scalar("tags", 9, Type::String, Label::Repeated),
            scalar("published", 10, Type::Bool, Label::Optional),
        ],
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![
            timestamp_file(),
            FileDescriptorProto {
                name: Some("case.proto".to_string()),
                package: Some("law.v1".to_string()),
                message_type: vec![case, meta],
                enum_type: vec![status],
                dependency: vec!["google/protobuf/timestamp.proto".to_string()],
                ..Default::default()
            },
        ],
    }
    .encode_to_vec()
}

// ---------------------------------------------------------------------
// Hand-encoded documents
// ---------------------------------------------------------------------

fn vint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn w_str(out: &mut Vec<u8>, field: u64, value: &str) {
    vint(out, field << 3 | 2);
    vint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn w_varint(out: &mut Vec<u8>, field: u64, value: u64) {
    vint(out, field << 3);
    vint(out, value);
}

fn w_double(out: &mut Vec<u8>, field: u64, value: f64) {
    vint(out, field << 3 | 1);
    out.extend_from_slice(&value.to_le_bytes());
}

fn w_packed_floats(out: &mut Vec<u8>, field: u64, values: &[f32]) {
    vint(out, field << 3 | 2);
    vint(out, (values.len() * 4) as u64);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn w_msg(out: &mut Vec<u8>, field: u64, body: &[u8]) {
    vint(out, field << 3 | 2);
    vint(out, body.len() as u64);
    out.extend_from_slice(body);
}

/// One Case document, absent-able per field. Everything mirrors the
/// deterministic value tables the assertions below recompute.
#[derive(Default, Clone)]
struct CaseDoc {
    id: Option<String>,
    title: Option<String>,
    price: Option<f64>,
    embedding: Vec<f32>,
    status: Option<u64>,
    created_at: Option<(i64, i32)>,
    author: Option<String>,
    page_count: Option<u64>,
    year: Option<i64>,
    published: bool,
}

impl CaseDoc {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if let Some(id) = &self.id {
            w_str(&mut out, 1, id);
        }
        if let Some(title) = &self.title {
            w_str(&mut out, 2, title);
        }
        if let Some(price) = self.price {
            w_double(&mut out, 3, price);
        }
        if !self.embedding.is_empty() {
            w_packed_floats(&mut out, 4, &self.embedding);
        }
        if let Some(status) = self.status {
            w_varint(&mut out, 5, status);
        }
        if let Some((seconds, nanos)) = self.created_at {
            let mut ts = Vec::new();
            w_varint(&mut ts, 1, seconds as u64);
            w_varint(&mut ts, 2, nanos as u64);
            w_msg(&mut out, 6, &ts);
        }
        if self.author.is_some() || self.page_count.is_some() {
            let mut meta = Vec::new();
            if let Some(author) = &self.author {
                w_str(&mut meta, 1, author);
            }
            if let Some(pages) = self.page_count {
                w_varint(&mut meta, 2, pages);
            }
            w_msg(&mut out, 7, &meta);
        }
        if let Some(year) = self.year {
            w_varint(&mut out, 8, year as u64);
        }
        if self.published {
            w_varint(&mut out, 10, 1);
        }
        out
    }
}

const N_DOCS: usize = 6;

fn embedding_of(i: usize) -> Vec<f32> {
    unit_vectors(N_DOCS, DIM, 42)[i * DIM..(i + 1) * DIM].to_vec()
}

fn doc(i: usize) -> CaseDoc {
    CaseDoc {
        id: Some(format!("case-{i}")),
        title: Some(format!("case number {i}")),
        price: (i != 3).then_some(1.5 * i as f64 + 0.5),
        embedding: embedding_of(i),
        status: (i != 4).then_some(if i.is_multiple_of(2) { 1 } else { 2 }),
        created_at: Some((1_600_000_000 + i as i64, 2_500)),
        author: i.is_multiple_of(2).then(|| format!("author {i}")),
        page_count: Some(10 * i as u64 + 5),
        year: (i != 2).then_some(1990 + i as i64),
        published: i.is_multiple_of(2),
    }
}

// ---------------------------------------------------------------------
// Node plumbing
// ---------------------------------------------------------------------

fn case_node_config(analysis: String) -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(analysis),
        bm25_fields: vec!["body".into(), "meta_author".into()],
        facet_fields: vec!["id".into(), "status".into(), "published".into()],
        integer_fields: vec![
            "created_at".into(),
            "meta_page_count".into(),
            "year".into(),
        ],
        numeric_fields: vec!["price".into(), "price2".into()],
        ..Default::default()
    }
}

async fn seed_calibration(addr: &str) {
    let sample = unit_vectors(64, DIM, 7);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
}

fn bind() -> MappedBind {
    let plan = derive_plan(&case_set(), "law.v1.Case").expect("the Case plan derives");
    MappedBind {
        descriptor_set: case_set(),
        message_type: "law.v1.Case".into(),
        expected_fingerprint: plan.fingerprint,
        body_path: "title".into(),
        analysis: None,
        materialize: None,
    }
}

async fn ingest(
    addr: &str,
    bind: MappedBind,
    docs: Vec<Vec<u8>>,
) -> Result<IngestMappedResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(16);
    let feeder = tokio::spawn(async move {
        let _ = tx
            .send(IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Bind(bind)),
            })
            .await;
        for d in docs {
            if tx
                .send(IngestMappedRequest {
                    payload: Some(ingest_mapped_request::Payload::Document(d)),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let out = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner());
    let _ = feeder.await;
    out
}

fn expect_refusal(result: Result<IngestMappedResponse, tonic::Status>, needle: &str) {
    let status = result.expect_err(&format!("expected a refusal mentioning {needle:?}"));
    assert!(
        status.message().contains(needle),
        "refusal {:?} does not mention {needle:?}",
        status.message()
    );
}

// ---------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------

/// Every plane at once: text body, multi-field text, string / enum /
/// bool facets, i64 (plain, timestamp-sourced, and nested), f64, and
/// the vector leg at the SAME ids — through one mapped stream.
#[tokio::test]
async fn mapped_ingest_lands_on_every_plane() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    let response = ingest(&addr, bind(), (0..N_DOCS).map(|i| doc(i).encode()).collect())
        .await
        .expect("mapped ingest succeeds");
    assert_eq!(response.added, N_DOCS as u64);
    assert_eq!(response.total, N_DOCS as u64);
    assert_eq!(response.first_id, 0);
    assert_eq!(
        response.fingerprint,
        derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint
    );

    let coordinator = CoordinatorServiceImpl::new(vec![addr])
        .with_bm25(Some(analysis), Default::default());

    // Projections read back every column, absences included.
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            projections: ["price", "year", "created_at", "meta_page_count", "id", "status", "published"]
                .iter()
                .map(|name| NamedProjection {
                    name: (*name).to_string(),
                    expression: (*name).to_string(),
                })
                .collect(),
            ..Default::default()
        }))
        .await
        .expect("projections over mapped columns")
        .into_inner()
        .hits;
    assert_eq!(hits.len(), N_DOCS);
    for hit in &hits {
        let i = hit.doc_id as usize;
        let d = doc(i);
        let values: Vec<Option<projected_value::Value>> =
            hit.projected.iter().map(|p| p.value.clone()).collect();
        use projected_value::Value::{DoubleValue, IntValue, StringValue};
        assert_eq!(values[0], d.price.map(DoubleValue), "price of doc {i}");
        assert_eq!(values[1], d.year.map(IntValue), "year of doc {i}");
        let (seconds, nanos) = d.created_at.unwrap();
        assert_eq!(
            values[2],
            Some(IntValue(seconds * 1_000_000 + i64::from(nanos) / 1_000)),
            "created_at of doc {i}"
        );
        assert_eq!(
            values[3],
            Some(IntValue(10 * i as i64 + 5)),
            "meta_page_count of doc {i}"
        );
        assert_eq!(
            values[4],
            Some(StringValue(format!("case-{i}"))),
            "id of doc {i}"
        );
        assert_eq!(
            values[5],
            d.status
                .map(|s| StringValue(if s == 1 { "OPEN" } else { "CLOSED" }.to_string())),
            "status of doc {i}"
        );
        assert_eq!(
            values[6],
            d.published.then(|| StringValue("true".to_string())),
            "published of doc {i}"
        );
    }

    // CEL selects over the mapped columns with the documented absence
    // rule: doc 2 (no year) and doc 3 (no price) never match.
    let filtered: Vec<u64> = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            filter: "year >= 1992 && price > 0.0".into(),
            ..Default::default()
        }))
        .await
        .expect("CEL filter over mapped columns")
        .into_inner()
        .hits
        .iter()
        .map(|h| h.doc_id)
        .collect();
    let mut filtered = filtered;
    filtered.sort_unstable();
    assert_eq!(filtered, vec![4, 5], "1994 and 1995, priced");

    // The vector leg landed at the SAME ids: querying doc 2's own
    // embedding returns doc 2 first.
    let top = coordinator
        .search(Request::new(SearchRequest {
            k: 1,
            vector: embedding_of(2),
            ..Default::default()
        }))
        .await
        .expect("vector search over mapped ingest")
        .into_inner()
        .hits;
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].vector_id, 2, "the vector leg shares the doc's id");
}

/// The bind stands or nothing streams: fingerprint discipline, declared
/// columns, body selection, protocol order.
#[tokio::test]
async fn bind_refusals_name_the_gap() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    // Fingerprint required: dry-run first, then bind what you saw.
    let mut unbound = bind();
    unbound.expected_fingerprint = String::new();
    expect_refusal(
        ingest(&addr, unbound, vec![]).await,
        "expected_fingerprint is required",
    );

    // A stale fingerprint names both sides.
    let real = derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint;
    let mut stale = bind();
    stale.expected_fingerprint = "deadbeef".into();
    expect_refusal(ingest(&addr, stale, vec![]).await, &real);

    // Two TEXT fields and no body_path: refused naming both.
    let mut ambiguous = bind();
    ambiguous.body_path = String::new();
    expect_refusal(ingest(&addr, ambiguous, vec![]).await, "meta.author");

    // body_path must be one of the plan's TEXT fields.
    let mut wrong_body = bind();
    wrong_body.body_path = "price".into();
    expect_refusal(ingest(&addr, wrong_body, vec![]).await, "TEXT fields");

    // A shard that does not declare the landing columns refuses at
    // bind, listing every gap with its flag.
    let (bare_addr, _bare) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        facet_fields: vec!["id".into(), "status".into(), "published".into()],
        integer_fields: vec!["created_at".into(), "meta_page_count".into(), "year".into()],
        ..Default::default()
    })
    .await;
    expect_refusal(
        ingest(&bare_addr, bind(), vec![]).await,
        "\"price\" (--numeric-fields)",
    );

    // Protocol: the bind comes first, exactly once.
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(IngestMappedRequest {
        payload: Some(ingest_mapped_request::Payload::Document(doc(0).encode())),
    })
    .await
    .unwrap();
    drop(tx);
    let status = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .expect_err("a document before the bind is refused");
    assert!(status.message().contains("must be a MappedBind"));

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    for payload in [
        ingest_mapped_request::Payload::Bind(bind()),
        ingest_mapped_request::Payload::Bind(bind()),
    ] {
        tx.send(IngestMappedRequest {
            payload: Some(payload),
        })
        .await
        .unwrap();
    }
    drop(tx);
    let status = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .expect_err("a second bind is refused");
    assert!(status.message().contains("bind repeats"));
}

/// Documents that cannot land refuse by position and by field; the
/// shard stays usable afterwards.
#[tokio::test]
async fn document_refusals_name_position_and_field() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis)).await;
    seed_calibration(&addr).await;

    let mut no_vector = doc(0);
    no_vector.embedding = Vec::new();
    expect_refusal(
        ingest(&addr, bind(), vec![no_vector.encode()]).await,
        "document 0",
    );
    expect_refusal(
        ingest(&addr, bind(), vec![no_vector.encode()]).await,
        "has no vector",
    );

    let mut no_id = doc(0);
    no_id.id = None;
    expect_refusal(ingest(&addr, bind(), vec![no_id.encode()]).await, "has no id");

    let mut no_body = doc(0);
    no_body.title = None;
    expect_refusal(
        ingest(&addr, bind(), vec![no_body.encode()]).await,
        "no body text",
    );

    let mut bad_enum = doc(0);
    bad_enum.status = Some(99);
    expect_refusal(
        ingest(&addr, bind(), vec![bad_enum.encode()]).await,
        "enum value 99",
    );

    let mut truncated = doc(0).encode();
    truncated.truncate(truncated.len() - 3);
    expect_refusal(ingest(&addr, bind(), vec![truncated]).await, "document 0");

    let mut short = doc(1);
    short.embedding = embedding_of(1)[..4].to_vec();
    expect_refusal(
        ingest(&addr, bind(), vec![doc(0).encode(), short.encode()]).await,
        "4 floats",
    );

    // Nothing above poisoned the shard: a clean stream still lands.
    // (The dim-mismatch stream above applied its first document before
    // refusing the second, exactly like ordinary ingest.)
    let response = ingest(&addr, bind(), vec![doc(2).encode()])
        .await
        .expect("the shard ingests after refusals");
    assert_eq!(response.added, 1);
}

/// A shard whose document leg ran ahead cannot take mapped documents:
/// the vector would land below its document. Refused by name.
#[tokio::test]
async fn lockstep_refusal_when_document_leg_is_ahead() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis)).await;
    seed_calibration(&addr).await;

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(AddDocumentsRequest {
        text: "a plain document with no vector".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();

    expect_refusal(
        ingest(&addr, bind(), vec![doc(0).encode()]).await,
        "lockstep",
    );
}

/// CEL materialization is a bind property: derived columns compute from
/// the document's own mapped values and become ordinary columns.
#[tokio::test]
async fn materialized_columns_ride_the_bind() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    let mut with_spec = bind();
    with_spec.materialize = Some(MaterializeSpec {
        columns: vec![MaterializedColumn {
            name: "price2".into(),
            expression: "price * 2.0".into(),
            kind: MaterializeKind::F64 as i32,
        }],
    });
    ingest(&addr, with_spec, (0..N_DOCS).map(|i| doc(i).encode()).collect())
        .await
        .expect("mapped ingest with materialization");

    let coordinator = CoordinatorServiceImpl::new(vec![addr])
        .with_bm25(Some(analysis), Default::default());
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            filter: "price2 >= 3.0".into(),
            projections: vec![NamedProjection {
                name: "price2".into(),
                expression: "price2".into(),
            }],
            ..Default::default()
        }))
        .await
        .expect("filter and project the materialized column")
        .into_inner()
        .hits;
    let mut selected: Vec<(u64, Option<projected_value::Value>)> = hits
        .iter()
        .map(|h| (h.doc_id, h.projected[0].value.clone()))
        .collect();
    selected.sort_unstable_by_key(|(id, _)| *id);
    // price2 = 2 * (1.5 i + 0.5) >= 3.0 selects i >= 1; doc 3 has no
    // price, so no price2 — absence propagates, it never matches.
    let expected: Vec<(u64, Option<projected_value::Value>)> = [1usize, 2, 4, 5]
        .iter()
        .map(|&i| {
            (
                i as u64,
                Some(projected_value::Value::DoubleValue(
                    2.0 * (1.5 * i as f64 + 0.5),
                )),
            )
        })
        .collect();
    assert_eq!(selected, expected);
}
