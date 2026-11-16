use super::args::*;

fn args(words: &[&str]) -> Vec<String> {
    words.iter().map(|w| (*w).to_string()).collect()
}

#[test]
fn status_parses_its_addr() {
    assert_eq!(
        parse_raft_status(&args(&["--addr=127.0.0.1:50051"])),
        Ok("127.0.0.1:50051".to_string())
    );
    assert!(parse_raft_status(&args(&[])).is_err());
}

#[test]
fn add_learner_parses_its_target() {
    assert_eq!(
        parse_raft_add_learner(&args(&[
            "--addr=127.0.0.1:50051",
            "--node-id=4",
            "--node-addr=127.0.0.1:50151"
        ])),
        Ok((
            "127.0.0.1:50051".to_string(),
            4,
            "127.0.0.1:50151".to_string()
        ))
    );
    assert!(parse_raft_add_learner(&args(&["--addr=127.0.0.1:50051", "--node-id=4"])).is_err());
    assert!(parse_raft_add_learner(&args(&[
        "--addr=127.0.0.1:50051",
        "--node-id=four",
        "--node-addr=127.0.0.1:50151"
    ]))
    .is_err());
}

#[test]
fn promote_parses_a_learner_list() {
    assert_eq!(
        parse_raft_promote(&args(&["--addr=127.0.0.1:50051", "--node-id=4,5"])),
        Ok(("127.0.0.1:50051".to_string(), vec![4, 5]))
    );
    assert!(parse_raft_promote(&args(&["--addr=127.0.0.1:50051"])).is_err());
    assert!(parse_raft_promote(&args(&["--addr=127.0.0.1:50051", "--node-id=4,x"])).is_err());
}

#[test]
fn remove_member_parses_its_target() {
    assert_eq!(
        parse_raft_remove_member(&args(&["--addr=127.0.0.1:50051", "--node-id=4"])),
        Ok(("127.0.0.1:50051".to_string(), 4))
    );
    assert!(parse_raft_remove_member(&args(&["--addr=127.0.0.1:50051"])).is_err());
}

#[test]
fn verify_image_parses_its_member() {
    assert_eq!(
        parse_raft_verify_image(&args(&["--addr=127.0.0.1:50051"])),
        Ok("127.0.0.1:50051".to_string())
    );
    assert!(parse_raft_verify_image(&args(&[])).is_err());
}

#[test]
fn bootstrap_parses_its_two_files() {
    assert_eq!(
        parse_raft_bootstrap_files(&args(&[
            "--raft-dir=/tmp/raft0",
            "--policy=/tmp/policy.json",
            "--limits=/tmp/limits.json"
        ])),
        Ok((
            "/tmp/policy.json".to_string(),
            "/tmp/limits.json".to_string()
        ))
    );
    assert!(parse_raft_bootstrap_files(&args(&["--policy=/tmp/policy.json"])).is_err());
}
