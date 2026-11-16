use super::{args, with};
use crate::config::file::FileConfig;
use crate::config::parse;

const NODE: [&str; 3] = [
    "--role=node",
    "--demo-vectors=10",
    "--node-listen=127.0.0.1:9001",
];

#[test]
fn unsigned_columns_keep_their_own_configured_type() {
    let base = ["--role=node", "--index=/tmp/unsigned.tv"];
    let config = parse(&with(
        &base,
        &[
            "--unsigned-integer-fields=count,maximum",
            "--integer-fields=signed",
        ],
    ))
    .unwrap();
    assert_eq!(config.unsigned_integer_fields, ["count", "maximum"]);
    assert_eq!(config.integer_fields, ["signed"]);
    for conflict in [
        "--integer-fields=count",
        "--numeric-fields=count",
        "--facet-fields=count",
        "--placement-column=count",
    ] {
        let error =
            parse(&with(&base, &["--unsigned-integer-fields=count", conflict])).unwrap_err();
        assert!(
            error.contains("more than one column kind"),
            "{conflict}: {error}"
        );
    }
    let file: FileConfig = toml::from_str("unsigned_integer_fields = ['counter']").unwrap();
    assert_eq!(file.unsigned_integer_fields, Some(vec!["counter".into()]));
    let empty: FileConfig = toml::from_str("").unwrap();
    assert!(empty.unsigned_integer_fields.is_none());
}

#[test]
fn map_integer_columns_read_their_flags() {
    let config = parse(&with(
        &NODE,
        &[
            "--map-integer-fields=counts",
            "--map-unsigned-integer-fields=sizes",
        ],
    ))
    .unwrap();
    assert_eq!(config.map_integer_fields, ["counts"]);
    assert_eq!(config.map_unsigned_integer_fields, ["sizes"]);
}

#[test]
fn facet_and_numeric_tables_refuse_repeats_and_overlap() {
    for (extra, expect) in [
        (
            &["--facet-fields=court,court"][..],
            "repeats in the facet table",
        ),
        (
            &["--numeric-fields=year,year"][..],
            "repeats in the numeric table",
        ),
        (
            &["--facet-fields=year", "--numeric-fields=year"][..],
            "both a facet and a numeric",
        ),
    ] {
        let error = parse(&with(&NODE, extra)).unwrap_err();
        assert!(error.contains(expect), "{extra:?}: {error}");
    }
}

#[test]
fn bm25_fields_default_body_and_validation() {
    assert_eq!(parse(&args(&NODE)).unwrap().bm25_fields, ["body"]);
    let cfg = parse(&with(&NODE, &["--bm25-fields=body, case_name"])).unwrap();
    assert_eq!(cfg.bm25_fields, ["body", "case_name"]);
    let bad = parse(&with(&NODE, &["--bm25-fields=case_name,body"]));
    assert!(bad.is_err(), "body must come first");
    let dup = parse(&with(&NODE, &["--bm25-fields=body,case_name,case_name"]));
    assert!(dup.is_err(), "duplicates are refused");
}

#[test]
fn phrase_configuration_requires_explicit_storage_fields() {
    let base = with(&NODE, &["--phrase-glossary=/tmp/concepts.tsv"]);
    assert!(parse(&base).unwrap_err().contains("phrase field"));
    let mut configured: Vec<&str> = base.iter().map(String::as_str).collect();
    configured.extend([
        "--bm25-fields=body,phrases",
        "--map-facet-fields=entities",
        "--entity-map-field=entities",
        "--phrase-ner",
    ]);
    let cfg = parse(&args(&configured)).unwrap();
    assert_eq!(cfg.phrase_field, "phrases");
    assert_eq!(cfg.entity_map_field.as_deref(), Some("entities"));
    assert!(cfg.phrase_ignore_case);
    assert!(cfg.phrase_ner);
}

#[test]
fn proximity_payloads_are_declared_over_the_field_table() {
    let cfg = parse(&with(
        &NODE,
        &[
            "--bm25-fields=body,body.bigrams",
            "--position-fields=body",
            "--bigram-fields=body",
            "--sentence-fields=body",
        ],
    ))
    .unwrap();
    assert_eq!(cfg.position_fields, ["body"]);
    assert_eq!(cfg.bigram_fields, ["body"]);
    assert_eq!(cfg.sentence_fields, ["body"]);
    for (extra, expect) in [
        (
            &["--position-fields=title"][..],
            "must be declared in --bm25-fields",
        ),
        (
            &["--bigram-fields=body"][..],
            "must be declared in --bm25-fields",
        ),
        (
            &["--bm25-fields=body,title", "--sentence-fields=title"][..],
            "body only",
        ),
        (
            &["--entity-map-field=entities"][..],
            "require --phrase-glossary",
        ),
    ] {
        let error = parse(&with(&NODE, extra)).unwrap_err();
        assert!(error.contains(expect), "{extra:?}: {error}");
    }
}
