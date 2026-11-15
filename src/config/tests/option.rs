use super::args_raw;
use crate::config::option::{env_name, parse_bool, split_list, Options, ENV_PREFIX};

#[test]
fn every_flag_reads_one_environment_variable_under_the_product_prefix() {
    assert_eq!(ENV_PREFIX, "PIPESTREAM_SEARCH_");
    for (flag, env) in [
        ("role", "PIPESTREAM_SEARCH_ROLE"),
        (
            "shard-map-reload-ms",
            "PIPESTREAM_SEARCH_SHARD_MAP_RELOAD_MS",
        ),
        ("map-integer-fields", "PIPESTREAM_SEARCH_MAP_INTEGER_FIELDS"),
        (
            "turbovec-coordinator",
            "PIPESTREAM_SEARCH_TURBOVEC_COORDINATOR",
        ),
        ("raft-node-id", "PIPESTREAM_SEARCH_RAFT_NODE_ID"),
        ("config", "PIPESTREAM_SEARCH_CONFIG"),
    ] {
        assert_eq!(env_name(flag), env);
    }
}

#[test]
fn values_read_both_spellings_and_the_first_wins() {
    let args = args_raw(&["--dim=64", "--dim", "32", "--tag", "a", "--tag=b", "--flag"]);
    let o = Options::new(&args);
    assert_eq!(o.value::<usize>("dim", Some(8)).unwrap(), Some(64));
    assert_eq!(o.arguments("tag"), ["b", "a"]);
    assert!(o.bare("flag"));
    assert!(!o.bare("absent"));
    let error = o.value::<u8>("tag", None).unwrap_err();
    assert!(error.starts_with("invalid --tag \"b\""), "{error}");
}

#[test]
fn absent_options_fall_back_to_the_file_then_the_default() {
    let args = args_raw(&["--blank=  ", "--padded= x "]);
    let o = Options::new(&args);
    assert_eq!(o.value_or("knob", Some(3u32), 9).unwrap(), 3);
    assert_eq!(o.value_or("knob", None, 9u32).unwrap(), 9);
    assert_eq!(o.parsed::<u16>("knob", Some("7")).unwrap(), Some(7));
    assert_eq!(o.text("knob", Some("file")).as_deref(), Some("file"));
    assert_eq!(o.nonblank("blank", Some("file")), None);
    assert_eq!(o.trimmed("padded", None).as_deref(), Some("x"));
    assert_eq!(o.path("blank", None), None);
    let file_list = vec!["f".to_string()];
    assert_eq!(o.list("knob", Some(&file_list)), ["f"]);
}

#[test]
fn switches_take_a_value_and_some_take_the_bare_flag() {
    let args = args_raw(&["--off=false", "--on=yes", "--bare"]);
    let o = Options::new(&args);
    assert!(!o.switch("off", Some(true), true));
    assert!(o.switch("on", Some(false), false));
    assert!(o.switch("absent", Some(true), false));
    assert!(o.switch("absent", None, true));
    assert!(o.switch_or_bare("bare", Some(false), false));
    assert!(
        !o.switch("bare", Some(false), false),
        "a bare flag is no value"
    );
}

#[test]
fn lists_and_truth_values_have_one_spelling_each() {
    assert_eq!(split_list(" a, ,b ,"), ["a", "b"]);
    assert!(split_list("").is_empty());
    for truthy in ["1", "true", "on", "yes"] {
        assert!(parse_bool(truthy), "{truthy}");
    }
    for falsy in ["0", "false", "off", "no", "TRUE", ""] {
        assert!(!parse_bool(falsy), "{falsy}");
    }
}
