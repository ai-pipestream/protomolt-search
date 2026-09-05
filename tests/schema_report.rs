use pipestream_search::mapping::{derive_plan, describe_schema};
use pipestream_search::pb::{
    MappedQueryRepresentation as Query, ProjectionUse as Use, SchemaField, SchemaReport,
    SourcePreservation,
};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};

#[tokio::test]
async fn describe_schema_is_available_over_the_public_grpc_contract() {
    let (addr, server) = pipestream_search::harness::start_coordinator(vec![]).await;
    let mut client =
        pipestream_search::pb::search_service_client::SearchServiceClient::connect(addr)
            .await
            .unwrap();
    let request = pipestream_search::pb::DescribeSchemaRequest {
        descriptor_set: include_bytes!("fixtures/schema-report/source-only.bin").to_vec(),
        message_type: "source_report.AllShapes".into(),
        ..Default::default()
    };
    let expected = describe_schema(&request.descriptor_set, &request.message_type).unwrap();
    let actual = client
        .describe_schema(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(actual, expected);
    let mut invalid = request;
    let mut set =
        prost_types::FileDescriptorSet::decode(invalid.descriptor_set.as_slice()).unwrap();
    set.file[0].syntax = Some("editions".into());
    invalid.descriptor_set = set.encode_to_vec();
    let error = client.describe_schema(invalid).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("unsupported protobuf syntax"));
    server.abort();
    let _ = server.await;
}

fn field<'a>(report: &'a SchemaReport, full_name: &str) -> &'a SchemaField {
    report
        .messages
        .iter()
        .flat_map(|m| &m.fields)
        .find(|f| f.full_name == full_name)
        .unwrap()
}

#[test]
fn source_description_covers_all_field_types_without_a_search_plan() {
    let bytes = include_bytes!("fixtures/schema-report/source-only.bin");
    assert!(derive_plan(bytes, "source_report.AllShapes").is_err());
    let described = describe_schema(bytes, "source_report.AllShapes").unwrap();
    assert_eq!(
        described.descriptor_sha256,
        pipestream_search::sha256::hex_digest(bytes)
    );
    let report = described.report.unwrap();
    assert!(!report.requires_index_rows_for_preservation);
    assert_eq!(report.root_message, "source_report.AllShapes");
    let root = report
        .messages
        .iter()
        .find(|m| m.full_name == report.root_message)
        .unwrap();
    let types = root
        .fields
        .iter()
        .map(|f| f.descriptor.as_ref().unwrap().r#type())
        .collect::<std::collections::HashSet<_>>();
    for number in 1..=18 {
        let kind = prost_types::field_descriptor_proto::Type::try_from(number).unwrap();
        assert!(types.contains(&kind), "missing protobuf type {kind:?}");
    }
    for message in &report.messages {
        for f in &message.fields {
            assert_eq!(f.preservation, SourcePreservation::OriginalBytes as i32);
            assert!(
                f.projections.is_empty(),
                "{} acquired a projection",
                f.full_name
            );
            assert!(!f.excluded_by_hint);
        }
    }
    assert!(field(&report, "source_report.extra").extension);
    assert!(field(&report, "source_report.legacy_entry").extension);
    assert!(field(&report, "source_report.AllShapes.map_value").map);
    assert!(field(&report, "source_report.AllShapes.uint64_value").supports_presence);
    assert_eq!(
        field(&report, "source_report.AllShapes.enum_value")
            .descriptor
            .as_ref()
            .unwrap()
            .default_value(),
        "READY"
    );
    assert_eq!(
        field(&report, "source_report.AllShapes.Legacy.required_value")
            .descriptor
            .as_ref()
            .unwrap()
            .label(),
        prost_types::field_descriptor_proto::Label::Required
    );
    assert!(
        report
            .messages
            .iter()
            .find(|m| m.full_name == "source_report.LegacySet")
            .unwrap()
            .message_set_wire_format
    );
    assert!(report
        .messages
        .iter()
        .any(|m| m.full_name == "google.protobuf.Value"));
    assert!(report
        .messages
        .iter()
        .any(|m| m.full_name == "google.protobuf.Any"));
    assert_eq!(
        report
            .messages
            .iter()
            .filter(|m| m.full_name == report.root_message)
            .count(),
        1
    );
    assert_eq!(root.oneofs[0].name(), "choice");
    let closed = report
        .enums
        .iter()
        .find(|e| e.full_name == "source_report.State")
        .unwrap();
    assert!(!closed.open);
    assert_eq!(closed.descriptor.as_ref().unwrap().value.len(), 3);

    let empty = describe_schema(bytes, "source_report.Empty")
        .unwrap()
        .report
        .unwrap();
    assert_eq!(empty.messages.len(), 1);
    assert!(empty.messages[0].fields.is_empty());
}

#[test]
fn source_description_does_not_apply_hints_or_change_projection_fingerprints() {
    let bytes = include_bytes!("fixtures/schema-report/descriptor.bin");
    let before = derive_plan(bytes, "report_fixture.Record").unwrap();
    let source = describe_schema(bytes, "report_fixture.Record")
        .unwrap()
        .report
        .unwrap();
    let mut mapped = before.schema_report.clone().unwrap();
    mapped.requires_index_rows_for_preservation = false;
    for f in mapped.messages.iter_mut().flat_map(|m| &mut m.fields) {
        f.projections.clear();
        f.excluded_by_hint = false;
        f.disposition = "Retained in the original; no indexing projection was requested.".into();
    }
    assert_eq!(source, mapped);
    assert_eq!(derive_plan(bytes, "report_fixture.Record").unwrap(), before);
    let mut set = prost_types::FileDescriptorSet::decode(bytes.as_slice()).unwrap();
    set.file.reverse();
    let reordered = describe_schema(&set.encode_to_vec(), "report_fixture.Record").unwrap();
    assert_eq!(reordered.report.unwrap(), source);
    assert_ne!(reordered.descriptor_sha256, before.descriptor_sha256);
}

#[test]
fn source_description_rejects_invalid_descriptors_and_unknown_roots() {
    let bytes = include_bytes!("fixtures/schema-report/source-only.bin");
    for (descriptor, root) in [
        (&b""[..], "source_report.Empty"),
        (&b"invalid"[..], "source_report.Empty"),
        (&bytes[..], ""),
        (&bytes[..], "source_report.Missing"),
        (&bytes[..], ".source_report.Empty"),
    ] {
        assert_eq!(
            describe_schema(descriptor, root).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    let mut missing_import = prost_types::FileDescriptorSet::decode(bytes.as_slice()).unwrap();
    missing_import
        .file
        .retain(|f| f.package() == "source_report");
    assert_eq!(
        describe_schema(&missing_import.encode_to_vec(), "source_report.AllShapes")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let set = prost_types::FileDescriptorSet::decode(bytes.as_slice()).unwrap();
    for index in 0..set.file.len() {
        for syntax in ["editions", "unknown", ""] {
            let mut unsupported = set.clone();
            unsupported.file[index].syntax = Some(syntax.into());
            let bytes = unsupported.encode_to_vec();
            let description = describe_schema(&bytes, "source_report.AllShapes").unwrap_err();
            let planning = derive_plan(&bytes, "source_report.AllShapes").unwrap_err();
            for error in [description, planning] {
                assert_eq!(error.code(), tonic::Code::InvalidArgument);
                assert!(error.message().contains("unsupported protobuf syntax"));
            }
        }
    }
}

#[test]
fn graph_covers_skipped_recursive_repeated_map_and_well_known_fields() {
    let bytes = include_bytes!("fixtures/schema-report/descriptor.bin");
    let plan = derive_plan(bytes, "report_fixture.Record").unwrap();
    let report = plan.schema_report.as_ref().unwrap();
    assert_eq!(report.report_version, 1);
    assert!(report.requires_index_rows_for_preservation);
    assert_eq!(
        report.unknown_fields,
        SourcePreservation::OriginalBytes as i32
    );
    assert_eq!(
        report
            .messages
            .iter()
            .map(|m| m.full_name.as_str())
            .collect::<Vec<_>>(),
        [
            "google.protobuf.Any",
            "google.protobuf.Int64Value",
            "google.protobuf.ListValue",
            "google.protobuf.Struct",
            "google.protobuf.Struct.FieldsEntry",
            "google.protobuf.Timestamp",
            "google.protobuf.Value",
            "report_fixture.Hidden",
            "report_fixture.Node",
            "report_fixture.Record",
            "report_fixture.Record.NodesByKeyEntry"
        ]
    );
    assert!(report
        .messages
        .iter()
        .all(|m| !m.full_name.contains("FieldIndexHint")));
    let secret = field(report, "report_fixture.Record.secret");
    assert_eq!(
        secret.preservation,
        SourcePreservation::OriginalBytes as i32
    );
    assert!(secret.projections.is_empty());
    assert!(secret.excluded_by_hint);
    assert!(field(report, "report_fixture.Node.hidden").excluded_by_hint);
    assert!(field(report, "report_fixture.Hidden.leaf")
        .projections
        .is_empty());
    let caption = field(report, "report_fixture.Node.caption");
    assert_eq!(caption.projections.len(), 1);
    assert_eq!(caption.projections[0].path, "head.caption");
    assert_eq!(caption.projections[0].field_numbers, [6, 1]);
    assert_eq!(caption.projections[0].r#use, Use::Value as i32);
    assert_eq!(
        caption.projections[0].query_representation,
        Query::AnalyzedText as i32
    );
    let next = field(report, "report_fixture.Node.next");
    assert_eq!(
        next.descriptor.as_ref().unwrap().type_name(),
        ".report_fixture.Node"
    );
    assert_eq!(next.projections[0].r#use, Use::SourceOnly as i32);
    assert!(field(report, "report_fixture.Record.nodes_by_key").map);
    assert!(field(report, "report_fixture.Record.counter").supports_presence);
    assert!(!field(report, "report_fixture.Record.id").supports_presence);
    let root = report
        .messages
        .iter()
        .find(|m| m.full_name == "report_fixture.Record")
        .unwrap();
    assert!(root.oneofs.iter().any(|o| o.name() == "selected"));
    let created = &field(report, "report_fixture.Record.created").projections[0];
    assert_eq!(created.query_representation, Query::SignedInteger as i32);
    assert!(created
        .constraints
        .iter()
        .any(|c| c.contains("epoch microseconds")));
    let counter = &field(report, "report_fixture.Record.counter").projections[0];
    assert_eq!(counter.query_representation, Query::UnsignedInteger as i32);
    assert!(!counter.constraints.iter().any(|c| c.contains("i64::MAX")));
    assert!(field(report, "report_fixture.Record.label")
        .descriptor
        .as_ref()
        .unwrap()
        .oneof_index
        .is_some());
    assert!(report
        .messages
        .iter()
        .any(|m| m.full_name == "google.protobuf.Value"));
    assert!(field(report, "google.protobuf.Value.struct_value")
        .projections
        .is_empty());
    assert!(field(report, "google.protobuf.Any.value")
        .projections
        .iter()
        .all(|p| p.r#use == Use::SourceOnly as i32));
}

#[test]
fn graph_retains_extension_group_required_default_and_enum_declarations() {
    let plan = derive_plan(
        include_bytes!("fixtures/protobuf-semantics/descriptor.bin"),
        "semantics.Doc",
    )
    .unwrap();
    let report = plan.schema_report.as_ref().unwrap();
    let doc = report
        .messages
        .iter()
        .find(|m| m.full_name == "semantics.Doc")
        .unwrap();
    assert_eq!(doc.fields.len(), 18);
    assert_eq!(doc.syntax, "proto2");
    let extra = field(report, "semantics.extra");
    assert!(extra.extension);
    assert!(extra.projections.is_empty());
    assert_eq!(extra.descriptor.as_ref().unwrap().number(), 100);
    assert_eq!(
        field(report, "semantics.Doc.status")
            .descriptor
            .as_ref()
            .unwrap()
            .default_value(),
        "READY"
    );
    assert_eq!(
        field(report, "semantics.Doc.required_token")
            .descriptor
            .as_ref()
            .unwrap()
            .label(),
        prost_types::field_descriptor_proto::Label::Required
    );
    assert_eq!(
        field(report, "semantics.Doc.legacy")
            .descriptor
            .as_ref()
            .unwrap()
            .r#type(),
        prost_types::field_descriptor_proto::Type::Group
    );
    let detail = field(report, "semantics.Detail.left");
    assert_eq!(
        detail
            .projections
            .iter()
            .map(|p| p.path.as_str())
            .collect::<Vec<_>>(),
        ["detail.left", "metadata.left"]
    );
    assert!(
        !report
            .enums
            .iter()
            .find(|e| e.full_name == "semantics.State")
            .unwrap()
            .open
    );
    assert!(
        report
            .enums
            .iter()
            .find(|e| e.full_name == "semantics.OpenState")
            .unwrap()
            .open
    );
    let state = report
        .enums
        .iter()
        .find(|e| e.full_name == "semantics.State")
        .unwrap();
    assert_eq!(
        state
            .descriptor
            .as_ref()
            .unwrap()
            .value
            .iter()
            .map(|v| v.name())
            .collect::<Vec<_>>(),
        ["UNKNOWN", "READY", "AVAILABLE"]
    );
}

#[test]
fn report_is_independent_of_descriptor_file_order_and_does_not_change_binding() {
    let bytes = include_bytes!("fixtures/schema-report/descriptor.bin");
    let plan = derive_plan(bytes, "report_fixture.Record").unwrap();
    // Reflected decode retains the custom FieldOptions bytes. prost_types
    // alone would discard those options and change the projection policy.
    let descriptor = DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .unwrap();
    let mut set = DynamicMessage::decode(descriptor, bytes.as_slice()).unwrap();
    set.get_field_by_name_mut("file")
        .unwrap()
        .as_list_mut()
        .unwrap()
        .reverse();
    let reordered = derive_plan(&set.encode_to_vec(), "report_fixture.Record").unwrap();
    assert_eq!(plan.schema_report, reordered.schema_report);
    assert_eq!(plan.fingerprint, reordered.fingerprint);
    assert_ne!(plan.descriptor_sha256, reordered.descriptor_sha256);
}

#[test]
fn impossible_value_projections_refuse_during_planning() {
    let bytes = include_bytes!("fixtures/schema-report/descriptor.bin");
    for (root, reason) in [
        (
            "report_fixture.InvalidText",
            "a TEXT field must be a string",
        ),
        (
            "report_fixture.InvalidDate",
            "a DATE hint requires a google.protobuf.Timestamp",
        ),
    ] {
        let error = derive_plan(bytes, root).unwrap_err();
        assert!(error.message().contains(reason), "{error}");
    }
}
