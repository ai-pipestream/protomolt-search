use super::option::{env_name, invalid, split_list, Options};

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|arg| (*arg).to_string()).collect()
}

#[test]
fn the_environment_name_is_the_flag_under_the_one_prefix() {
    assert_eq!(env_name("coord-listen"), "PIPESTREAM_SEARCH_COORD_LISTEN");
    assert_eq!(
        env_name("map-unsigned-integer-fields"),
        "PIPESTREAM_SEARCH_MAP_UNSIGNED_INTEGER_FIELDS"
    );
    for (flag, name) in [
        ("turbovec-coordinator", "PIPESTREAM_SEARCH_COORDINATOR"),
        ("turbovec-cluster-nodes", "PIPESTREAM_SEARCH_CLUSTER_NODES"),
        ("turbovec-cluster-state", "PIPESTREAM_SEARCH_CLUSTER_STATE"),
        (
            "allow-ephemeral-turbovec-cluster",
            "PIPESTREAM_SEARCH_ALLOW_EPHEMERAL_CLUSTER",
        ),
    ] {
        assert_eq!(env_name(flag), name);
    }
}

#[test]
fn a_typed_value_parses_the_flag_and_passes_the_file_value_through() {
    let both = args(&["--wave2-test-k=7", "--wave2-test-j", "9"]);
    let o = Options::new(&both);
    assert_eq!(o.value::<u32>("wave2-test-k", Some(3)).unwrap(), Some(7));
    assert_eq!(o.value::<u32>("wave2-test-j", None).unwrap(), Some(9));
    assert_eq!(o.value::<u32>("wave2-test-none", Some(3)).unwrap(), Some(3));
    assert_eq!(o.value::<u32>("wave2-test-none", None).unwrap(), None);
}

#[test]
fn every_unparsable_value_reports_the_flag_the_text_and_the_cause() {
    let bad = args(&["--wave2-test-k=seven"]);
    let err = Options::new(&bad)
        .value::<u32>("wave2-test-k", None)
        .unwrap_err();
    assert_eq!(
        err,
        invalid("wave2-test-k", "seven", "invalid digit found in string")
    );
    assert!(
        err.starts_with("invalid --wave2-test-k \"seven\": "),
        "{err}"
    );
    let err = Options::new(&bad)
        .parsed::<std::net::SocketAddr>("wave2-test-k", None)
        .unwrap_err();
    assert!(
        err.starts_with("invalid --wave2-test-k \"seven\": "),
        "{err}"
    );
}

#[test]
fn the_three_switch_shapes_keep_their_own_rules() {
    let off = args(&["--wave2-test-s=false"]);
    let bare = args(&["--wave2-test-s"]);
    let none = args(&[]);
    // A valued switch: the flag overrides the file in both directions.
    assert!(!Options::new(&off).switch("wave2-test-s", Some(true), true));
    assert!(Options::new(&none).switch("wave2-test-s", Some(true), false));
    assert!(Options::new(&none).switch("wave2-test-s", None, true));
    // The bare flag turns a valued switch on.
    assert!(!Options::new(&bare).switch("wave2-test-s", None, false));
    assert!(Options::new(&bare).switch_or_bare("wave2-test-s", None, false));
    // A raised switch reads the bare flag and the file, never `--flag=value`.
    assert!(Options::new(&bare).raised("wave2-test-s", None));
    assert!(Options::new(&off).raised("wave2-test-s", Some(true)));
    assert!(!Options::new(&args(&["--wave2-test-s=true"])).raised("wave2-test-s", None));
}

#[test]
fn lists_trim_entries_and_blank_text_is_absent() {
    assert_eq!(split_list(" a, ,b ,"), ["a", "b"]);
    let given = args(&["--wave2-test-l= x , y", "--wave2-test-p=  "]);
    let o = Options::new(&given);
    let file = vec!["f".to_string()];
    assert_eq!(o.list("wave2-test-l", Some(&file)), ["x", "y"]);
    assert_eq!(o.list("wave2-test-none", Some(&file)), ["f"]);
    assert_eq!(o.path("wave2-test-p", Some("/from/file")), None);
    assert_eq!(
        o.trimmed("wave2-test-none", Some(" v ")),
        Some("v".to_string())
    );
    assert_eq!(o.arguments("wave2-test-l"), [" x , y"]);
}
