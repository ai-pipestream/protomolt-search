//! Shared protobuf descriptor builders, wire encoders, and mapped-ingest helpers.

use super::*;

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
pub(crate) fn case_set() -> Vec<u8> {
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

// Hand-encoded documents

pub(crate) fn vint(out: &mut Vec<u8>, mut v: u64) {
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

pub(crate) fn w_str(out: &mut Vec<u8>, field: u64, value: &str) {
    vint(out, field << 3 | 2);
    vint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

pub(crate) fn w_varint(out: &mut Vec<u8>, field: u64, value: u64) {
    vint(out, field << 3);
    vint(out, value);
}

pub(crate) fn w_double(out: &mut Vec<u8>, field: u64, value: f64) {
    vint(out, field << 3 | 1);
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn w_packed_floats(out: &mut Vec<u8>, field: u64, values: &[f32]) {
    vint(out, field << 3 | 2);
    vint(out, (values.len() * 4) as u64);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

pub(crate) fn w_msg(out: &mut Vec<u8>, field: u64, body: &[u8]) {
    vint(out, field << 3 | 2);
    vint(out, body.len() as u64);
    out.extend_from_slice(body);
}

/// One Case document, absent-able per field. Everything mirrors the
/// deterministic value tables the assertions below recompute.
#[derive(Default, Clone)]
pub(crate) struct CaseDoc {
    pub(crate) id: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) price: Option<f64>,
    pub(crate) embedding: Vec<f32>,
    pub(crate) status: Option<u64>,
    pub(crate) created_at: Option<(i64, i32)>,
    pub(crate) author: Option<String>,
    pub(crate) page_count: Option<u64>,
    pub(crate) year: Option<i64>,
    pub(crate) published: bool,
}

impl CaseDoc {
    pub(crate) fn encode(&self) -> Vec<u8> {
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

pub(crate) const N_DOCS: usize = 6;

pub(crate) fn embedding_of(i: usize) -> Vec<f32> {
    unit_vectors(N_DOCS, DIM, 42)[i * DIM..(i + 1) * DIM].to_vec()
}

pub(crate) fn doc(i: usize) -> CaseDoc {
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

// Node plumbing

pub(crate) fn case_node_config(analysis: String) -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(analysis),
        bm25_fields: vec!["body".into(), "meta_author".into()],
        facet_fields: vec!["id".into(), "status".into(), "published".into()],
        integer_fields: vec!["created_at".into(), "meta_page_count".into(), "year".into()],
        numeric_fields: vec!["price".into(), "price2".into()],
        ..Default::default()
    }
}

pub(crate) async fn seed_calibration(addr: &str) {
    let sample = unit_vectors(64, DIM, 7);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, BIT_WIDTH, &shift, &scale,
        ))
        .await
        .unwrap();
}

pub(crate) fn bind() -> MappedBind {
    let plan = derive_plan(&case_set(), "law.v1.Case").expect("the Case plan derives");
    MappedBind {
        collection: String::new(),
        descriptor_set: case_set(),
        message_type: "law.v1.Case".into(),
        expected_fingerprint: plan.fingerprint,
        body_path: "title".into(),
        analysis: None,
        materialize: None,
        field_analysis: Vec::new(),
        index_definition: None,
    }
}

pub(crate) fn explicit_bind() -> MappedBind {
    let mut binding = bind();
    binding.field_analysis = derive_plan(&case_set(), "law.v1.Case")
        .unwrap()
        .fields
        .into_iter()
        .filter(|field| field.family == pipestream_search::pb::ColumnFamily::TextField as i32)
        .map(|field| pipestream_search::pb::MappedFieldAnalysis {
            path: field.path,
            analysis: Some(pipestream_search::analyzer::body_spec()),
        })
        .collect();
    binding
}

pub(crate) async fn send(
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

pub(crate) fn expect_refusal(result: Result<IngestMappedResponse, tonic::Status>, needle: &str) {
    let status = result.expect_err(&format!("expected a refusal mentioning {needle:?}"));
    assert!(
        status.message().contains(needle),
        "refusal {:?} does not mention {needle:?}",
        status.message()
    );
}

pub(crate) fn expected_binding() -> pipestream_search::postings::StoredBinding {
    pipestream_search::postings::StoredBinding {
        index_contract: Vec::new(),
        plan_fingerprint: derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint,
        body_path: "title".into(),
        materialize_sha: String::new(),
        analysis_sha: String::new(),
        analysis_contract: Vec::new(),
        vector_binding: derive_plan(&case_set(), "law.v1.Case")
            .unwrap()
            .vector_binding
            .unwrap()
            .encode_to_vec(),
    }
}

// Chunk-scope ingest

// Descriptor wire helpers for HINT-BEARING sets (prost drops extension
// fields, so these are hand-encoded like production reads them).
pub(crate) fn d_field(
    name: &str,
    number: u64,
    label: prost_types::field_descriptor_proto::Label,
    typ: Type,
    type_name: Option<&str>,
    hint: Option<&pipestream_search::pb::hints::FieldIndexHint>,
) -> Vec<u8> {
    let mut f = Vec::new();
    w_str(&mut f, 1, name);
    w_varint(&mut f, 3, number);
    w_varint(&mut f, 4, label as u64);
    w_varint(&mut f, 5, typ as u64);
    if let Some(tn) = type_name {
        w_str(&mut f, 6, tn);
    }
    if let Some(hint) = hint {
        let mut options = Vec::new();
        w_msg(&mut options, 59_100_471, &hint.encode_to_vec());
        w_msg(&mut f, 8, &options);
    }
    f
}

pub(crate) fn d_message(name: &str, fields: &[Vec<u8>]) -> Vec<u8> {
    let mut m = Vec::new();
    w_str(&mut m, 1, name);
    for f in fields {
        w_msg(&mut m, 2, f);
    }
    m
}

pub(crate) fn d_set(package: &str, file: &str, messages: &[Vec<u8>]) -> Vec<u8> {
    let mut f = Vec::new();
    w_str(&mut f, 1, file);
    w_str(&mut f, 2, package);
    for m in messages {
        w_msg(&mut f, 4, m);
    }
    let mut s = Vec::new();
    w_msg(&mut s, 1, &f);
    s
}
