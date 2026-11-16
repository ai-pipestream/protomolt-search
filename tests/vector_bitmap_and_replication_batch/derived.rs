//! Follower catch-up of logged derived values: index-declared columns and
//! pre-materialized NLP columns replay byte for byte under the WAL replay
//! mark, and only there.

use crate::pair::{appended, key, Pair};
use pipestream_search::derived::Declaration;
use pipestream_search::pb::wal::wal_record::Op;
use pipestream_search::pb::{
    self, node_service_client::NodeServiceClient, DerivedColumn, DerivedColumns, DerivedDisclosure,
    MaterializeKind,
};
use pipestream_search::replication::{
    encode_replay_first_id, encode_stable_keys, STABLE_KEYS_METADATA, WAL_REPLAY_METADATA,
};
use pipestream_search::values::fnv1a64_bytes;
use std::sync::Arc;

/// The NLP materialization id a client that ran NLP upstream stamps.
const NLP_MARK: &str = "v1:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

fn declaration() -> Arc<Declaration> {
    let column = |name: &str, kind: MaterializeKind, expression: &str| DerivedColumn {
        name: name.into(),
        expression: expression.into(),
        kind: kind as i32,
        disclosure: DerivedDisclosure::Inputs as i32,
    };
    Arc::new(
        Declaration::compile(&DerivedColumns {
            columns: vec![
                column(
                    "key_bucket",
                    MaterializeKind::U64,
                    "hash.fnv64(stable_key()) % 64u",
                ),
                column("usd_cents", MaterializeKind::F64, "money['USD'] * 100.0"),
            ],
        })
        .unwrap(),
    )
}

/// A document carrying an upstream NLP money column and its mark.
fn priced(i: usize) -> pb::AddDocumentsRequest {
    pb::AddDocumentsRequest {
        text: format!("invoice {i} paid in dollars"),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        map_numerics: vec![pb::MapNumericEntry {
            field: "money".into(),
            key: "USD".into(),
            value: i as f64 + 0.25,
        }],
        nlp_materialization: NLP_MARK.into(),
        ..Default::default()
    }
}

fn unsigned(doc: &pb::AddDocumentsRequest, field: &str) -> Option<u64> {
    doc.unsigned_integers
        .iter()
        .find(|value| value.field == field)
        .map(|value| value.value)
}

fn numeric(doc: &pb::AddDocumentsRequest, field: &str) -> Option<f64> {
    doc.numerics
        .iter()
        .find(|value| value.field == field)
        .map(|value| value.value)
}

#[tokio::test]
async fn logged_derived_and_nlp_columns_replay_byte_for_byte_in_two_streams() {
    const ROWS: usize = 520;
    let declaration = declaration();
    let mut pair = Pair::start_derived("derived", declaration.clone()).await;
    let keys: Vec<Vec<u8>> = (0..ROWS).map(key).collect();
    pair.keyed((0..ROWS).map(priced).collect(), &keys).await;
    let start = pair.cursor.clone();
    pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddDocuments", "AddDocuments"]);
    let replicated = appended(&pair.cursor.replica).await;
    assert_eq!(replicated.len(), ROWS);
    for (row, op) in replicated.into_iter().enumerate() {
        let Op::AddDocuments(add) = op else {
            panic!("row {row} is not a document record")
        };
        assert_eq!(add.stable_routing_keys, [key(row)]);
        let doc = &add.documents[0];
        assert_eq!(doc.derived_fingerprint, declaration.fingerprint());
        assert_eq!(doc.nlp_materialization, NLP_MARK);
        assert_eq!(
            unsigned(doc, "key_bucket"),
            Some(fnv1a64_bytes(&key(row)) % 64),
            "row {row}"
        );
        assert_eq!(numeric(doc, "usd_cents"), Some((row as f64 + 0.25) * 100.0));
    }
}

/// Send `doc` straight to `addr` under `key`, marked as WAL replay from
/// `first_id` when one is given.
async fn send(
    addr: &str,
    doc: pb::AddDocumentsRequest,
    key: &[u8],
    first_id: Option<u64>,
) -> Result<pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let mut request = tonic::Request::new(tokio_stream::iter([doc]));
    let keys = encode_stable_keys(&[key.to_vec()]).unwrap();
    let metadata = request.metadata_mut();
    metadata.insert_bin(
        STABLE_KEYS_METADATA,
        tonic::metadata::MetadataValue::from_bytes(&keys),
    );
    if let Some(first_id) = first_id {
        metadata.insert_bin(
            WAL_REPLAY_METADATA,
            tonic::metadata::MetadataValue::from_bytes(&encode_replay_first_id(first_id)),
        );
    }
    client.add_documents(request).await.map(|r| r.into_inner())
}

#[tokio::test]
async fn a_logged_row_outside_a_replay_stream_or_off_the_tip_is_refused() {
    let mut pair = Pair::start_derived("derived-refused", declaration()).await;
    pair.keyed(vec![priced(0), priced(1)], &[key(0), key(1)])
        .await;
    pair.source.flush(pb::FlushRequest {}).await.unwrap();
    let Some(Op::AddDocuments(logged)) = appended(&pair.cursor.primary).await.into_iter().next()
    else {
        panic!("the primary logged no document")
    };
    let row = logged.documents[0].clone();
    let replica = pair.cursor.replica.clone();
    let forged = send(&replica, row.clone(), &key(0), None)
        .await
        .unwrap_err();
    assert_eq!(forged.code(), tonic::Code::InvalidArgument);
    assert!(
        forged.message().contains("stamps derived fingerprint"),
        "{forged}"
    );
    let off_tip = send(&replica, row.clone(), &key(0), Some(1))
        .await
        .unwrap_err();
    assert_eq!(off_tip.code(), tonic::Code::FailedPrecondition);
    assert!(
        off_tip
            .message()
            .contains("WAL replay starts at document 1"),
        "{off_tip}"
    );
    let mut health = NodeServiceClient::connect(replica.clone()).await.unwrap();
    let before = health
        .health(pb::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(before.document_slots, 0, "no refused row was allocated");
    assert!(before.replication_version >= 1);
    let start = pair.cursor.clone();
    pair.sync(&start).await;
}
