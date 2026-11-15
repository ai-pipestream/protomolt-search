use super::*;

#[test]
fn names_are_operator_safe() {
    assert!(validate_name("court-opinions_v2.2026").is_ok());
    assert!(validate_name("").is_err());
    assert!(validate_name("has space").is_err());
    assert!(validate_name("a/b").is_err());
    assert!(validate_name("a:b").is_err());
    assert!(validate_name("naïve").is_err());
    assert!(validate_name(&"x".repeat(129)).is_err());
}

#[test]
fn resolution_never_picks_silently() {
    let unnamed: Members<u8> = Members::single(1);
    assert_eq!(unnamed.resolve("").unwrap(), (String::new(), &1));
    assert!(unnamed
        .resolve("a")
        .unwrap_err()
        .message()
        .contains("one unnamed dataset"));

    let named: Members<u8> =
        Members::named(vec![("a".into(), 1), ("b".into(), 2)], None, "t").unwrap();
    assert_eq!(named.resolve("a").unwrap(), ("a".to_string(), &1));
    assert_eq!(named.resolve("b").unwrap(), ("b".to_string(), &2));
    let error = named.resolve("").unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("[\"a\", \"b\"]") && error.message().contains("no default"));
    let error = named.resolve("c").unwrap_err();
    assert!(error.message().contains("unknown collection \"c\""));

    let with_default: Members<u8> = Members::named(
        vec![("a".into(), 1), ("b".into(), 2)],
        Some("b".into()),
        "t",
    )
    .unwrap();
    assert_eq!(with_default.resolve("").unwrap(), ("b".to_string(), &2));

    assert!(
        Members::<u8>::named(vec![("a".into(), 1), ("a".into(), 2)], None, "t")
            .unwrap_err()
            .contains("declared twice")
    );
    assert!(
        Members::<u8>::named(vec![("a".into(), 1)], Some("z".into()), "t")
            .unwrap_err()
            .contains("not one of")
    );
    assert!(Members::<u8>::named(Vec::new(), None, "t").is_err());
}
