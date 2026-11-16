use super::{args, config_flag, temp_file};
use crate::config::{parse, Role};

#[test]
fn vector_backend_is_provider_neutral_and_can_vary_per_shard() {
    let path = temp_file(
        "pipestream_search_backends",
        r#"
role = "node"
vector_backend = "default-provider"

[[shards]]
index = "/data/a.vector"

[[shards]]
index = "/data/b.vector"
vector_backend = "shard-provider"
"#,
    );
    let cfg = parse(&args(&[&config_flag(&path)])).unwrap();
    let _ = std::fs::remove_file(path);
    assert_eq!(cfg.shards[0].vector_backend, "default-provider");
    assert_eq!(cfg.shards[1].vector_backend, "shard-provider");
    let cli = parse(&args(&[
        "--role=node",
        "--index=/data/c.vector",
        "--vector-backend=cli-provider",
    ]))
    .unwrap();
    assert_eq!(cli.shards[0].vector_backend, "cli-provider");
}

#[test]
fn toml_file_multi_shard() {
    let path = temp_file(
        "pipestream_search_cfg",
        r#"
role = "both"
coord_listen = "0.0.0.0:51050"
nodes = ["host-a:50051", "host-b:50052"]
chunk_blocks = 16
floor_sharing = false
max_message_mib = 32

[[shards]]
listen = "0.0.0.0:50051"
index = "/data/shard-0.tv"
slot_offset = 0

[[shards]]
listen = "0.0.0.0:50052"
index = "/data/shard-1.tv"
slot_offset = 20000
"#,
    );
    let cfg = parse(&args(&[&config_flag(&path)])).unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(cfg.role, Role::Both);
    assert_eq!(cfg.coord_listen.port(), 51050);
    assert_eq!(
        cfg.node_addrs,
        ["http://host-a:50051", "http://host-b:50052"]
    );
    assert_eq!(cfg.shards.len(), 2);
    assert_eq!(cfg.shards[1].listen.port(), 50052);
    assert_eq!(cfg.shards[1].slot_offset, 20000);
    assert_eq!(
        cfg.shards[1].index_path.as_deref(),
        Some(std::path::Path::new("/data/shard-1.tv"))
    );
    assert_eq!(cfg.chunk_blocks, 16);
    assert!(!cfg.share_floors);
    assert_eq!(cfg.max_message_bytes, 32 * 1024 * 1024);
}

#[test]
fn cli_overrides_file() {
    let path = temp_file(
        "pipestream_search_ovr",
        "role = \"node\"\nchunk_blocks = 16\n\n[[shards]]\nindex = \"/data/a.tv\"\n",
    );
    let cfg = parse(&args(&[&config_flag(&path), "--chunk-blocks=99"])).unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(cfg.chunk_blocks, 99);
    assert_eq!(cfg.shards.len(), 1);
}

#[test]
fn file_shard_needs_exactly_one_source() {
    let path = temp_file(
        "pipestream_search_bad",
        "role = \"node\"\n\n[[shards]]\nslot_offset = 7\n",
    );
    let result = parse(&args(&[&config_flag(&path)]));
    let _ = std::fs::remove_file(&path);
    assert!(result
        .unwrap_err()
        .contains("exactly one of index / demo_vectors"));
}

#[test]
fn durable_control_requires_a_coordinator_map_and_parses_policy() {
    let map = temp_file(
        "control-map",
        "generation = 4\n\n[[shards]]\naddr = \"node:50051\"\nslot_offset = 0\n",
    );
    let state = map.with_file_name(format!("control-state-{}.json", std::process::id()));
    let map_flag = format!("--shard-map={}", map.display());
    let state_flag = format!("--control-state={}", state.display());
    let cfg = parse(&args(&[
        "--role=coordinator",
        &map_flag,
        &state_flag,
        "--control-reconcile-ms=250",
        "--control-lease-ms=5000",
        "--control-replication-factor=3",
        "--control-split-rows=1000",
        "--control-merge-rows=100",
        "--control-compact-segments=6",
        "--control-compact-tombstone-ppm=50000",
    ]))
    .unwrap();
    assert_eq!(cfg.control_state_path.as_deref(), Some(state.as_path()));
    assert_eq!(cfg.control_reconcile_ms, 250);
    assert_eq!(cfg.control_lease_ms, 5_000);
    assert_eq!(cfg.control_replication_factor, 3);
    assert_eq!(cfg.control_split_rows, 1_000);
    assert_eq!(cfg.control_merge_rows, 100);
    assert_eq!(cfg.control_compact_segments, 6);
    assert_eq!(cfg.control_compact_tombstone_ppm, 50_000);
    let without_map = ["--role=coordinator", "--nodes=node:50051", &state_flag];
    assert!(parse(&args(&without_map)).is_err());
    let too_wide = "--control-compact-segments=4294967296";
    assert!(parse(&args(&[
        "--role=coordinator",
        &map_flag,
        &state_flag,
        too_wide
    ]))
    .is_err());
    let lease = "--control-lease-ms=10";
    assert!(parse(&args(&[
        "--role=coordinator",
        &map_flag,
        &state_flag,
        lease
    ]))
    .is_err());
    std::fs::remove_file(map).unwrap();
}

#[test]
fn shard_map_parses_replicas() {
    let path = temp_file(
        "pipestream_search_replicas",
        "[[shards]]\naddr = \"host-a:50051\"\nreplica = \"host-a2:50051\"\n\n\
         [[shards]]\naddr = \"host-b:50051\"\n",
    );
    let flag = format!("--shard-map={}", path.display());
    let cfg = parse(&args(&["--role=coordinator", &flag])).unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        cfg.replica_addrs,
        [Some("http://host-a2:50051".to_string()), None]
    );
    assert_eq!(cfg.replica_sync_ms, 1_000, "a replica turns catch-up on");
    assert_eq!(cfg.shard_map_reload_ms, 1_000, "a shard map is polled");
}

#[test]
fn shard_map_replaces_nodes() {
    let path = temp_file(
        "pipestream_search_shardmap",
        r#"
generation = 7

[[shards]]
addr = "host-a:50051"
slot_offset = 0
hash_lo = 0
hash_hi = 9223372036854775807

[[shards]]
addr = "host-b:50051"
slot_offset = 25000000
"#,
    );
    let flag = format!("--shard-map={}", path.display());
    let cfg = parse(&args(&["--role=coordinator", &flag])).unwrap();
    let map = cfg.shard_map.expect("shard map parsed");
    assert_eq!(map.generation, 7);
    assert_eq!(map.shards.len(), 2);
    assert_eq!(map.shards[1].slot_offset, 25_000_000);
    assert_eq!(map.shards[1].hash_lo, None);
    assert_eq!(
        cfg.node_addrs,
        ["http://host-a:50051", "http://host-b:50051"]
    );
    let error = parse(&args(&["--role=coordinator", &flag, "--nodes=a:1"])).unwrap_err();
    assert!(error.contains("replaces --nodes"), "{error}");
    let _ = std::fs::remove_file(&path);
    let error = parse(&args(&[
        "--role=coordinator",
        "--nodes=a:1",
        "--shard-map-reload-ms=5",
    ]))
    .unwrap_err();
    assert!(error.contains("requires --shard-map"), "{error}");
}

#[test]
fn named_collections_replace_the_top_level_dataset() {
    let path = temp_file(
        "pipestream_search_collections",
        r#"
role = "coordinator"
default_collection = "books"

[[collections]]
name = "books"
nodes = ["host-a:50051"]
bm25_k1 = 0.9

[[collections]]
name = "papers"
nodes = ["host-b:50051"]
"#,
    );
    let flag = config_flag(&path);
    let cfg = parse(&args(&[&flag, "--bm25-b=0.5"])).unwrap();
    assert_eq!(cfg.collections.len(), 2);
    assert_eq!(cfg.collections[0].node_addrs, ["http://host-a:50051"]);
    assert_eq!(
        (cfg.collections[0].bm25_k1, cfg.collections[0].bm25_b),
        (0.9, 0.5)
    );
    assert_eq!(cfg.collections[1].bm25_k1, 1.2);
    assert_eq!(cfg.default_collection.as_deref(), Some("books"));
    let error = parse(&args(&[&flag, "--nodes=host-c:50051"])).unwrap_err();
    assert!(error.contains("`collections` replaces --nodes"), "{error}");
    let error = parse(&args(&[&flag, "--relay"])).unwrap_err();
    assert!(error.contains("not multiplexed through a relay"), "{error}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn dense_execution_policy_parses_v2_paths_and_refuses_v1_toml_at_load() {
    let top_file = temp_file(
        "pipestream_search_dense_policy_top",
        r#"
role = "coordinator"
nodes = ["host-a:50051"]
dense_execution_policy = "/etc/pipestream/policy.binpb"
"#,
    );
    let top_cfg = parse(&args(&[&config_flag(&top_file)])).unwrap();
    let _ = std::fs::remove_file(&top_file);
    assert_eq!(
        top_cfg.dense_execution_policy.as_deref(),
        Some(std::path::Path::new("/etc/pipestream/policy.binpb"))
    );

    let col_file = temp_file(
        "pipestream_search_dense_policy_collection",
        r#"
role = "coordinator"

[[collections]]
name = "books"
nodes = ["host-a:50051"]
dense_execution_policy = "/etc/pipestream/books-policy.binpb"
"#,
    );
    let col_cfg = parse(&args(&[&config_flag(&col_file)])).unwrap();
    let _ = std::fs::remove_file(&col_file);
    assert_eq!(
        col_cfg.collections[0].dense_execution_policy.as_deref(),
        Some(std::path::Path::new("/etc/pipestream/books-policy.binpb"))
    );

    let v1_toml = temp_file(
        "pipestream_search_dense_policy_v1_toml",
        "format_version = 1\npolicy_id = \"retired-v1\"\n",
    );
    let cli_cfg = parse(&args(&[
        "--role=coordinator",
        "--nodes=host-a:50051",
        &format!("--dense-execution-policy={}", v1_toml.display()),
    ]))
    .unwrap();
    let load_err = crate::dense_policy::DenseExecutionPolicy::load(
        cli_cfg.dense_execution_policy.as_deref().unwrap(),
    )
    .unwrap_err();
    let _ = std::fs::remove_file(&v1_toml);
    assert!(load_err.contains("version-2"), "{load_err}");
}
