use std::path::PathBuf;

use super::{args, with};
use crate::config::{parse, ClusteredTurboVecConfig, Role};
use crate::MAX_MESSAGE_BYTES;

const COORDINATOR: [&str; 2] = ["--role=coordinator", "--nodes=127.0.0.1:50051"];
const INDEXED: [&str; 2] = ["--role=node", "--index=/tmp/x.tv"];

#[test]
fn parses_node_role_flags() {
    let cfg = parse(&args(&[
        "--role=node",
        "--demo-vectors=1000",
        "--node-listen=127.0.0.1:9001",
        "--slot-offset=20000",
        "--chunk-blocks=8",
    ]))
    .unwrap();
    assert_eq!(cfg.role, Role::Node);
    assert_eq!(cfg.shards.len(), 1);
    assert_eq!(cfg.shards[0].listen.port(), 9001);
    assert_eq!(cfg.shards[0].slot_offset, 20000);
    assert_eq!(cfg.chunk_blocks, 8);
    assert_eq!(cfg.shards[0].demo.unwrap().vectors, 1000);
    assert_eq!(
        cfg.shards[0].vector_backend,
        crate::vector::EMBEDDED_TURBOVEC
    );
    assert!(cfg.share_floors);
    assert_eq!(cfg.max_message_bytes, MAX_MESSAGE_BYTES);
}

#[test]
fn native_analysis_backend_is_not_rewritten_as_http() {
    let cfg = parse(&args(&[
        "--role=both",
        "--demo-vectors=10",
        "--nodes=127.0.0.1:9001",
        "--node-listen=127.0.0.1:9001",
        "--analysis-addr=native://",
    ]))
    .unwrap();
    assert_eq!(cfg.analysis_addr.as_deref(), Some("native"));
    assert_eq!(cfg.shards[0].analysis_addr.as_deref(), Some("native"));
}

#[test]
fn each_role_needs_something_to_serve() {
    assert!(parse(&args(&["--role=coordinator"])).is_err());
    let cfg = parse(&args(&[
        "--role=coordinator",
        "--nodes=127.0.0.1:50051,127.0.0.1:50052",
    ]))
    .unwrap();
    assert_eq!(cfg.node_addrs.len(), 2);
    assert!(cfg.node_addrs[0].starts_with("http://"));
    assert!(parse(&args(&["--role=node"])).is_err());
    assert!(parse(&with(&INDEXED, &["--demo-vectors=10"])).is_err());
    let error = parse(&args(&["--role=leader"])).unwrap_err();
    assert!(error.contains("unknown role"), "{error}");
}

#[test]
fn clustered_turbovec_transports_are_explicit_and_exclusive() {
    let external = parse(&with(
        &COORDINATOR,
        &["--turbovec-coordinator=127.0.0.1:51050"],
    ))
    .unwrap();
    assert_eq!(
        external.clustered_turbovec,
        Some(ClusteredTurboVecConfig::External {
            endpoint: "http://127.0.0.1:51050".to_string(),
        })
    );
    let nodes = "--turbovec-cluster-nodes=127.0.0.1:52051 shard-a 7";
    let error = parse(&with(&COORDINATOR, &[nodes])).unwrap_err();
    assert!(error.contains("requires turbovec_cluster_state"), "{error}");
    let embedded = parse(&with(
        &COORDINATOR,
        &[nodes, "--allow-ephemeral-turbovec-cluster"],
    ))
    .unwrap();
    assert_eq!(
        embedded.clustered_turbovec,
        Some(ClusteredTurboVecConfig::InProcess {
            nodes: vec!["127.0.0.1:52051 shard-a 7".to_string()],
            state: None,
            allow_ephemeral: true,
        })
    );
    let both = parse(&with(
        &COORDINATOR,
        &[
            "--turbovec-coordinator=127.0.0.1:51050",
            "--turbovec-cluster-nodes=127.0.0.1:52051",
        ],
    ));
    assert!(both.unwrap_err().contains("not both"));
}

#[test]
fn switches_default_and_turn_off() {
    let node = ["--role=node", "--demo-vectors=10"];
    assert!(
        !parse(&with(&node, &["--floor-sharing=false"]))
            .unwrap()
            .share_floors
    );
    assert!(parse(&args(&COORDINATOR)).unwrap().bm25_stream);
    assert!(
        !parse(&with(&COORDINATOR, &["--bm25-stream=false"]))
            .unwrap()
            .bm25_stream
    );
    let cfg = parse(&with(&COORDINATOR, &["--demo-query", "--query-dim=256"])).unwrap();
    assert!(cfg.demo_query);
    assert_eq!(cfg.query_dim, 256);
    let error = parse(&with(&node, &["--demo-query"])).unwrap_err();
    assert!(error.contains("--demo-query requires"), "{error}");
}

#[test]
fn wal_defaults_on_for_index_off_for_demo() {
    assert!(
        !parse(&args(&["--role=node", "--demo-vectors=10"]))
            .unwrap()
            .shards[0]
            .wal
    );
    assert!(parse(&args(&INDEXED)).unwrap().shards[0].wal);
    assert!(!parse(&with(&INDEXED, &["--wal=false"])).unwrap().shards[0].wal);
    let error = parse(&with(&INDEXED, &["--wal-buckets=3"])).unwrap_err();
    assert!(error.contains("power of two"), "{error}");
}

#[test]
fn vocab_defaults_off_and_parses_knobs() {
    let defaults = parse(&args(&INDEXED)).unwrap();
    assert!(!defaults.shards[0].vocab);
    assert_eq!(defaults.vocab_window_docs, 1_000_000);
    assert_eq!(defaults.vocab_top_k, 1024);
    let on = parse(&with(
        &INDEXED,
        &[
            "--vocab=true",
            "--vocab-window-docs=500000",
            "--vocab-top-k=256",
        ],
    ))
    .unwrap();
    assert!(on.shards[0].vocab);
    assert_eq!(on.vocab_window_docs, 500_000);
    assert_eq!(on.vocab_top_k, 256);
    // Demo shards have no index path to snapshot next to.
    let demo = parse(&args(&["--role=node", "--demo-vectors=10", "--vocab=true"])).unwrap();
    assert!(!demo.shards[0].vocab);
    assert!(parse(&with(&INDEXED, &["--vocab-window-docs=0"])).is_err());
    assert!(parse(&with(&INDEXED, &["--vocab-top-k=0"])).is_err());
}

#[test]
fn maturity_knobs_parse_with_safe_defaults() {
    let node = ["--role=node", "--demo-vectors=10"];
    let defaults = parse(&args(&node)).unwrap();
    assert_eq!(defaults.floor_delta, 0.0);
    assert_eq!(defaults.shard_deadline_ms, 0);
    assert_eq!(defaults.hedge_delay_ms, 0);
    assert!(defaults.replica_addrs.is_empty());
    assert_eq!(defaults.rerank_parallel, 0);
    assert_eq!(
        defaults.max_rerank_bytes,
        crate::coordinator::DEFAULT_MAX_RERANK_BYTES
    );
    let cfg = parse(&with(
        &node,
        &[
            "--floor-delta=0.005",
            "--shard-deadline-ms=1500",
            "--hedge-delay-ms=200",
            "--rerank-parallel=8",
            "--max-rerank-mib=32",
        ],
    ))
    .unwrap();
    assert_eq!(cfg.floor_delta, 0.005);
    assert_eq!(cfg.shard_deadline_ms, 1500);
    assert_eq!(cfg.hedge_delay_ms, 200);
    assert_eq!(cfg.rerank_parallel, 8);
    assert_eq!(cfg.max_rerank_bytes, 32 * 1024 * 1024);
    for refused in [
        "--floor-delta=-0.1",
        "--rerank-parallel=65",
        "--max-k=0",
        "--signal-batch=0",
        "--max-rerank-mib=0",
        "--chunk-blocks=many",
    ] {
        assert!(parse(&with(&node, &[refused])).is_err(), "{refused}");
    }
}

#[test]
fn membership_flags_parse_and_shards_carry_their_identity() {
    let cfg = parse(&with(
        &INDEXED,
        &[
            "--node-id=b",
            "--control-addr=10.0.0.1:50050",
            "--failure-domain=rack-2",
            "--data-dir=/var/lib/placed",
            "--advertise-addr=10.0.0.2:50051",
            "--replica-listen=10.0.0.2:0",
            "--node-report-ms=500",
            "--shard-id=s7",
            "--hash-lo=0",
            "--hash-hi=100",
        ],
    ))
    .unwrap();
    let membership = cfg.membership.as_ref().unwrap();
    assert_eq!(membership.node_id, "b");
    assert_eq!(membership.control_addr, "http://10.0.0.1:50050");
    assert_eq!(membership.failure_domain, "rack-2");
    assert_eq!(membership.data_dir, PathBuf::from("/var/lib/placed"));
    assert_eq!(membership.advertise_addr.as_deref(), Some("10.0.0.2:50051"));
    let replica_listen = membership.replica_listen.map(|a| a.to_string());
    assert_eq!(replica_listen.as_deref(), Some("10.0.0.2:0"));
    assert_eq!(
        (membership.report_ms, membership.reconcile_ms),
        (500, 2_000)
    );
    assert_eq!((membership.lease_ms, membership.lag_bound), (0, 0));
    assert_eq!(cfg.shards[0].shard_id.as_deref(), Some("s7"));
    assert_eq!(cfg.shards[0].hash_range, Some((0, 100)));
    let plain = parse(&args(&INDEXED)).unwrap();
    assert!(plain.membership.is_none());
    assert!(plain.shards[0].shard_id.is_none() && plain.shards[0].hash_range.is_none());
}

#[test]
fn membership_flags_need_each_other() {
    let registered = ["--node-id=b", "--control-addr=c:1", "--data-dir=/d"];
    for (extra, needle) in [
        (&["--node-id=b"][..], "needs --control-addr"),
        (&registered[..2], "needs --data-dir"),
        (&["--data-dir=/d"][..], "pass --node-id"),
        (
            &[
                registered[0],
                registered[1],
                registered[2],
                "--advertise-addr=nowhere",
            ][..],
            "is not host:port",
        ),
        (&["--hash-lo=5"][..], "both bounds or neither"),
        (&["--hash-lo=5", "--hash-hi=4"][..], "is inverted"),
    ] {
        let error = parse(&with(&INDEXED, extra)).unwrap_err();
        assert!(error.contains(needle), "{extra:?}: {error}");
    }
    let error = parse(&with(&COORDINATOR, &registered)).unwrap_err();
    assert!(error.contains("node or both role"), "{error}");
}
