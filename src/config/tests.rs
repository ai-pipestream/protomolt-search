use super::*;

/// The arguments of one test plus `--allow-plaintext`: these tests
/// bind the non-loopback defaults and are about other knobs, and
/// plaintext off loopback needs the explicit flag (docs/security.md).
fn args(pairs: &[&str]) -> Vec<String> {
    let mut v = args_raw(pairs);
    if !pairs
        .iter()
        .any(|p| p.starts_with("--tls-") || *p == "--allow-plaintext")
    {
        v.push("--allow-plaintext".to_string());
    }
    v
}

fn args_raw(pairs: &[&str]) -> Vec<String> {
    pairs.iter().map(|s| s.to_string()).collect()
}

#[test]
fn raft_member_options_parse_together_and_partial_ones_refuse() {
    // Extras replace a base option of the same key: the first value of
    // a key wins in `arg_value`.
    let member = |extra: &[&str]| {
        let key = |pair: &str| pair.split('=').next().unwrap_or(pair).to_string();
        let overridden: Vec<String> = extra.iter().map(|p| key(p)).collect();
        let mut pairs: Vec<&str> = [
            "--role=coordinator",
            "--nodes=127.0.0.1:1",
            "--raft-dir=/tmp/member-1",
            "--raft-node-id=1",
            "--raft-group-id=0102030405060708090a0b0c0d0e0f10",
            "--raft-authority-incarnation=ffffffffffffffffffffffffffffffff",
            "--raft-listen=127.0.0.1:19500",
            "--raft-peers=/tmp/peers.toml",
        ]
        .into_iter()
        .filter(|p| !overridden.contains(&key(p)))
        .collect();
        pairs.extend_from_slice(extra);
        parse(&args(&pairs))
    };
    let cfg = member(&["--raft-advertise=krick-1:19500", "--raft-lease-ms=400"]).unwrap();
    let raft = cfg.raft.unwrap();
    assert_eq!(raft.node_id, 1);
    assert_eq!(raft.group_id, (1..=16).collect::<Vec<u8>>());
    assert_eq!(raft.authority_incarnation, vec![255; 16]);
    assert_eq!(raft.listen.port(), 19500);
    assert_eq!(raft.advertise.as_deref(), Some("krick-1:19500"));
    assert_eq!(raft.peers, PathBuf::from("/tmp/peers.toml"));
    assert_eq!(raft.lease_ms, 400);
    assert_eq!(raft.election_min_ms, 1_000);
    assert!(raft.map.is_none());
    assert!(parse(&args(&["--role=node", "--index=/tmp/x.tv"]))
        .unwrap()
        .raft
        .is_none());
    // A member is its directory: node id or peers without it refuse.
    let error = parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--raft-node-id=1",
    ]))
    .unwrap_err();
    assert!(error.contains("needs --raft-dir"), "{error}");
    for (missing, expect) in [
        ("--raft-node-id=", "needs --raft-node-id"),
        ("--raft-group-id=", "needs --raft-group-id"),
        ("--raft-peers=", "needs --raft-peers"),
    ] {
        let pairs: Vec<&str> = [
            "--role=coordinator",
            "--nodes=127.0.0.1:1",
            "--raft-dir=/tmp/member-1",
            "--raft-node-id=1",
            "--raft-group-id=0102030405060708090a0b0c0d0e0f10",
            "--raft-authority-incarnation=ffffffffffffffffffffffffffffffff",
            "--raft-listen=127.0.0.1:19500",
            "--raft-peers=/tmp/peers.toml",
        ]
        .into_iter()
        .filter(|p| !p.starts_with(missing))
        .collect();
        let error = parse(&args(&pairs)).unwrap_err();
        assert!(error.contains(expect), "{missing}: {error}");
    }
    // Malformed values are refused by name.
    let error = member(&["--raft-group-id=0102"]).unwrap_err();
    assert!(error.contains("32 hex digits"), "{error}");
    let error = member(&["--raft-group-id=zz02030405060708090a0b0c0d0e0f10"]).unwrap_err();
    assert!(error.contains("non-hex"), "{error}");
    let error =
        member(&["--raft-authority-incarnation=00000000000000000000000000000000"]).unwrap_err();
    assert!(error.contains("nonzero"), "{error}");
    let error = member(&["--raft-node-id=0"]).unwrap_err();
    assert!(error.contains("nonzero"), "{error}");
    let error = member(&["--raft-lease-ms=soon"]).unwrap_err();
    assert!(error.contains("--raft-lease-ms"), "{error}");
    // The map options go together and need a relay.
    let error = member(&["--raft-map-principal=relay"]).unwrap_err();
    assert!(error.contains("go together"), "{error}");
    let error = member(&[
        "--raft-map-principal=relay",
        "--raft-map-workspace=workspace-a",
        "--raft-map-collection=books",
    ])
    .unwrap_err();
    assert!(error.contains("needs --relay"), "{error}");
    let cfg = member(&[
        "--relay",
        "--raft-map-principal=relay",
        "--raft-map-workspace=workspace-a",
        "--raft-map-collection=books",
    ])
    .unwrap();
    let map = cfg.raft.unwrap().map.unwrap();
    assert_eq!(
        (
            map.principal.as_str(),
            map.workspace.as_str(),
            map.collection.as_str()
        ),
        ("relay", "workspace-a", "books")
    );
}

#[test]
fn raft_local_parse_serves_the_directory_commands_without_shards() {
    // The local commands create a directory but serve nothing: raft
    // flags alone are a member, with no shard or collection flags.
    let local = parse_raft_local(&args_raw(&[
        "--raft-dir=/tmp/member-1",
        "--raft-node-id=1",
        "--raft-group-id=0102030405060708090a0b0c0d0e0f10",
        "--raft-authority-incarnation=ffffffffffffffffffffffffffffffff",
        "--raft-listen=127.0.0.1:19500",
        "--raft-peers=/tmp/peers.toml",
    ]))
    .unwrap();
    assert_eq!(
        local
            .member
            .as_ref()
            .expect("raft flags alone are a member")
            .node_id,
        1
    );
    assert!(local.tls.is_none());
    assert!(local.client_tls.is_none());
    // No raft flags is no member, not an error.
    let local = parse_raft_local(&args_raw(&[])).unwrap();
    assert!(local.member.is_none());
    // A partial member is refused by name.
    let error = parse_raft_local(&args_raw(&["--raft-node-id=1"]))
        .err()
        .expect("a member without its directory refuses");
    assert!(error.contains("needs --raft-dir"), "{error}");
}

#[test]
#[cfg(all(feature = "raft", feature = "tls"))]
fn raft_managed_catalog_options_parse_repeatable_and_refuse_without_member() {
    let base = || {
        vec![
            "--role=coordinator",
            "--nodes=127.0.0.1:1",
            "--raft-dir=/tmp/member-1",
            "--raft-node-id=1",
            "--raft-group-id=0102030405060708090a0b0c0d0e0f10",
            "--raft-authority-incarnation=ffffffffffffffffffffffffffffffff",
            "--raft-listen=127.0.0.1:19500",
            "--raft-peers=/tmp/peers.toml",
        ]
    };
    let member = |extra: &[&str]| {
        let mut pairs: Vec<&str> = base();
        pairs.extend_from_slice(extra);
        parse(&args(&pairs))
    };
    // Repeatable: two catalogs and their recovery actor parse together.
    let cfg = member(&[
        "--raft-managed-catalog=books=/tmp/books.redb",
        "--raft-managed-catalog=papers=/tmp/papers.redb",
        "--raft-managed-principal=alice",
    ])
    .unwrap();
    let raft = cfg.raft.unwrap();
    assert_eq!(raft.managed_catalogs.len(), 2);
    assert_eq!(raft.managed_catalogs[0].collection, "books");
    assert_eq!(
        raft.managed_catalogs[0].path,
        PathBuf::from("/tmp/books.redb")
    );
    assert_eq!(raft.managed_catalogs[1].collection, "papers");
    assert_eq!(raft.managed_principal.as_deref(), Some("alice"));
    // The service limits default to one MiB per write, sixteen MiB
    // admitted and sixty-four operations; each is an option.
    assert_eq!(
        (
            raft.managed_max_request_bytes,
            raft.managed_max_pending_bytes,
            raft.managed_max_in_flight
        ),
        (1024 * 1024, 16 * 1024 * 1024, 64)
    );
    let cfg = member(&[
        "--raft-managed-catalog=books=/tmp/books.redb",
        "--raft-managed-principal=alice",
        "--raft-managed-max-request-bytes=4096",
        "--raft-managed-max-pending-bytes=65536",
        "--raft-managed-max-in-flight=1",
    ])
    .unwrap()
    .raft
    .unwrap();
    assert_eq!(
        (
            cfg.managed_max_request_bytes,
            cfg.managed_max_pending_bytes,
            cfg.managed_max_in_flight
        ),
        (4096, 65536, 1)
    );
    let error = member(&["--raft-managed-max-in-flight=many"]).unwrap_err();
    assert!(error.contains("--raft-managed-max-in-flight"), "{error}");
    // A catalog without its recovery actor refuses.
    let error = member(&["--raft-managed-catalog=books=/tmp/books.redb"]).unwrap_err();
    assert!(error.contains("--raft-managed-principal"), "{error}");
    // A malformed entry refuses by name.
    let error = member(&[
        "--raft-managed-catalog=books",
        "--raft-managed-principal=alice",
    ])
    .unwrap_err();
    assert!(error.contains("--raft-managed-catalog"), "{error}");
    // Managed options on a non-member refuse by name.
    let error = parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--raft-managed-catalog=books=/tmp/books.redb",
        "--raft-managed-principal=alice",
    ]))
    .unwrap_err();
    assert!(error.contains("needs --raft-dir"), "{error}");
    let error = parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--raft-managed-principal=alice",
    ]))
    .unwrap_err();
    assert!(error.contains("needs --raft-dir"), "{error}");
}

#[test]
#[cfg(not(all(feature = "raft", feature = "tls")))]
fn raft_managed_catalog_options_refuse_without_raft_support() {
    let error = parse(&args(&[
        "--role=coordinator",
        "--nodes=127.0.0.1:1",
        "--raft-dir=/tmp/member-1",
        "--raft-node-id=1",
        "--raft-group-id=0102030405060708090a0b0c0d0e0f10",
        "--raft-authority-incarnation=ffffffffffffffffffffffffffffffff",
        "--raft-listen=127.0.0.1:19500",
        "--raft-peers=/tmp/peers.toml",
        "--raft-managed-catalog=books=/tmp/books.redb",
        "--raft-managed-principal=alice",
    ]))
    .unwrap_err();
    assert!(error.contains("has no Raft support"), "{error}");
}

#[test]
fn unsigned_columns_keep_their_own_configured_type() {
    let config = parse(&args(&[
        "--role=node",
        "--index=/tmp/unsigned.tv",
        "--unsigned-integer-fields=count,maximum",
        "--integer-fields=signed",
    ]))
    .unwrap();
    assert_eq!(config.unsigned_integer_fields, ["count", "maximum"]);
    assert_eq!(config.integer_fields, ["signed"]);
    for conflict in [
        "--integer-fields=count",
        "--numeric-fields=count",
        "--facet-fields=count",
        "--placement-column=count",
    ] {
        assert!(parse(&args(&[
            "--role=node",
            "--index=/tmp/unsigned.tv",
            "--unsigned-integer-fields=count",
            conflict
        ]))
        .unwrap_err()
        .contains("more than one column kind"));
    }
    let file: FileConfig = toml::from_str("unsigned_integer_fields = ['counter']").unwrap();
    assert_eq!(file.unsigned_integer_fields, Some(vec!["counter".into()]));
    let old: FileConfig = toml::from_str("").unwrap();
    assert!(old.unsigned_integer_fields.is_none());
}

#[test]
fn allow_missing_bm25_is_off_unless_asked_for() {
    // The default has to be the strict one: a shard silently serving
    // no postings is the failure this exists to catch, and a default
    // of "permit" would mean the check never fires where it matters.
    let base = [
        "--role=node",
        "--demo-vectors=10",
        "--node-listen=127.0.0.1:9001",
    ];
    assert!(!parse(&args(&base)).unwrap().allow_missing_bm25);
    let mut bare = base.to_vec();
    bare.push("--allow-missing-bm25");
    assert!(
        parse(&args(&bare)).unwrap().allow_missing_bm25,
        "the bare flag must work; an operator will not write =true"
    );
    let mut valued = base.to_vec();
    valued.push("--allow-missing-bm25=true");
    assert!(parse(&args(&valued)).unwrap().allow_missing_bm25);
    let mut off = base.to_vec();
    off.push("--allow-missing-bm25=false");
    assert!(!parse(&args(&off)).unwrap().allow_missing_bm25);
}

#[test]
fn bm25_fields_default_body_and_validation() {
    let base = [
        "--role=node",
        "--demo-vectors=10",
        "--node-listen=127.0.0.1:9001",
    ];
    let cfg = parse(&args(&base)).unwrap();
    assert_eq!(cfg.bm25_fields, vec!["body".to_string()]);

    let mut with = base.to_vec();
    with.push("--bm25-fields=body, case_name");
    let cfg = parse(&args(&with)).unwrap();
    assert_eq!(
        cfg.bm25_fields,
        vec!["body".to_string(), "case_name".to_string()]
    );

    let mut bad = base.to_vec();
    bad.push("--bm25-fields=case_name,body");
    assert!(parse(&args(&bad)).is_err(), "body must come first");
    let mut dup = base.to_vec();
    dup.push("--bm25-fields=body,case_name,case_name");
    assert!(parse(&args(&dup)).is_err(), "duplicates are refused");
}

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
fn coordinator_requires_nodes() {
    assert!(parse(&args(&["--role=coordinator"])).is_err());
    let cfg = parse(&args(&[
        "--role=coordinator",
        "--nodes=127.0.0.1:50051,127.0.0.1:50052",
    ]))
    .unwrap();
    assert_eq!(cfg.node_addrs.len(), 2);
    assert!(cfg.node_addrs[0].starts_with("http://"));
}

#[test]
fn clustered_turbovec_transports_are_explicit_and_exclusive() {
    let external = parse(&args(&[
        "--role=coordinator",
        "--nodes=127.0.0.1:50051",
        "--turbovec-coordinator=127.0.0.1:51050",
    ]))
    .unwrap();
    assert_eq!(
        external.clustered_turbovec,
        Some(ClusteredTurboVecConfig::External {
            endpoint: "http://127.0.0.1:51050".to_string(),
        })
    );

    let missing_state = parse(&args(&[
        "--role=coordinator",
        "--nodes=127.0.0.1:50051",
        "--turbovec-cluster-nodes=127.0.0.1:52051 shard-a 7",
    ]));
    assert!(missing_state
        .unwrap_err()
        .contains("requires turbovec_cluster_state"));

    let embedded = parse(&args(&[
        "--role=coordinator",
        "--nodes=127.0.0.1:50051",
        "--turbovec-cluster-nodes=127.0.0.1:52051 shard-a 7",
        "--allow-ephemeral-turbovec-cluster",
    ]))
    .unwrap();
    assert_eq!(
        embedded.clustered_turbovec,
        Some(ClusteredTurboVecConfig::InProcess {
            nodes: vec!["127.0.0.1:52051 shard-a 7".to_string()],
            state: None,
            allow_ephemeral: true,
        })
    );

    let both = parse(&args(&[
        "--role=coordinator",
        "--nodes=127.0.0.1:50051",
        "--turbovec-coordinator=127.0.0.1:51050",
        "--turbovec-cluster-nodes=127.0.0.1:52051",
    ]));
    assert!(both.unwrap_err().contains("not both"));
}

#[test]
fn vector_backend_is_provider_neutral_and_can_vary_per_shard() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    let path = dir.join(format!(
        "pipestream_search_backends_{}.toml",
        std::process::id()
    ));
    std::fs::write(
        &path,
        r#"
role = "node"
vector_backend = "default-provider"

[[shards]]
index = "/data/a.vector"

[[shards]]
index = "/data/b.vector"
vector_backend = "shard-provider"
"#,
    )
    .unwrap();
    let cfg = parse(&args(&[&format!("--config={}", path.display())])).unwrap();
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
fn node_requires_a_shard() {
    assert!(parse(&args(&["--role=node"])).is_err());
    assert!(parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--demo-vectors=10"
    ]))
    .is_err());
}

#[test]
fn toml_file_multi_shard() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    let path = dir.join(format!("pipestream_search_cfg_{}.toml", std::process::id()));
    std::fs::write(
        &path,
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
    )
    .unwrap();
    let cfg = parse(&args(&[&format!("--config={}", path.display())])).unwrap();
    let _ = std::fs::remove_file(&path);

    assert_eq!(cfg.role, Role::Both);
    assert_eq!(cfg.coord_listen.port(), 51050);
    assert_eq!(
        cfg.node_addrs,
        vec!["http://host-a:50051", "http://host-b:50052"]
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
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("pipestream_search_ovr_{}.toml", std::process::id()));
    std::fs::write(
        &path,
        "role = \"node\"\nchunk_blocks = 16\n\n[[shards]]\nindex = \"/data/a.tv\"\n",
    )
    .unwrap();
    let cfg = parse(&args(&[
        &format!("--config={}", path.display()),
        "--chunk-blocks=99",
    ]))
    .unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(cfg.chunk_blocks, 99);
    assert_eq!(cfg.shards.len(), 1);
}

#[test]
fn file_shard_needs_exactly_one_source() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("pipestream_search_bad_{}.toml", std::process::id()));
    std::fs::write(&path, "role = \"node\"\n\n[[shards]]\nslot_offset = 7\n").unwrap();
    let result = parse(&args(&[&format!("--config={}", path.display())]));
    let _ = std::fs::remove_file(&path);
    assert!(result.is_err());
}

#[test]
fn floor_sharing_flag() {
    let cfg = parse(&args(&[
        "--role=node",
        "--demo-vectors=10",
        "--floor-sharing=false",
    ]))
    .unwrap();
    assert!(!cfg.share_floors);
}

#[test]
fn bm25_candidate_stream_defaults_on_and_can_be_disabled() {
    let defaults = parse(&args(&["--role=coordinator", "--nodes=a:1"])).unwrap();
    assert!(defaults.bm25_stream);

    let unary = parse(&args(&[
        "--role=coordinator",
        "--nodes=a:1",
        "--bm25-stream=false",
    ]))
    .unwrap();
    assert!(!unary.bm25_stream);
}

#[test]
fn demo_query_flag_and_dim() {
    let cfg = parse(&args(&[
        "--role=coordinator",
        "--nodes=a:1",
        "--demo-query",
        "--query-dim=256",
    ]))
    .unwrap();
    assert!(cfg.demo_query);
    assert_eq!(cfg.query_dim, 256);
}

#[test]
fn wal_defaults_on_for_index_off_for_demo() {
    let demo = parse(&args(&["--role=node", "--demo-vectors=10"])).unwrap();
    assert!(!demo.shards[0].wal);
    let persisted = parse(&args(&["--role=node", "--index=/tmp/x.tv"])).unwrap();
    assert!(persisted.shards[0].wal);
    let off = parse(&args(&["--role=node", "--index=/tmp/x.tv", "--wal=false"])).unwrap();
    assert!(!off.shards[0].wal);
}

#[test]
fn vocab_defaults_off_and_parses_knobs() {
    let defaults = parse(&args(&["--role=node", "--index=/tmp/x.tv"])).unwrap();
    assert!(!defaults.shards[0].vocab);
    assert_eq!(defaults.vocab_window_docs, 1_000_000);
    assert_eq!(defaults.vocab_top_k, 1024);

    let on = parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--vocab=true",
        "--vocab-window-docs=500000",
        "--vocab-top-k=256",
    ]))
    .unwrap();
    assert!(on.shards[0].vocab);
    assert_eq!(on.vocab_window_docs, 500_000);
    assert_eq!(on.vocab_top_k, 256);

    // Demo shards have no index path to snapshot next to.
    let demo = parse(&args(&["--role=node", "--demo-vectors=10", "--vocab=true"])).unwrap();
    assert!(!demo.shards[0].vocab);

    assert!(parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--vocab-window-docs=0"
    ]))
    .is_err());
    assert!(parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--vocab-top-k=0"
    ]))
    .is_err());
}

#[test]
fn maturity_knobs_parse_with_safe_defaults() {
    let defaults = parse(&args(&["--role=node", "--demo-vectors=10"])).unwrap();
    assert_eq!(defaults.floor_delta, 0.0);
    assert_eq!(defaults.shard_deadline_ms, 0);
    assert_eq!(defaults.hedge_delay_ms, 0);
    assert!(defaults.replica_addrs.is_empty());
    assert_eq!(defaults.rerank_parallel, 0);
    assert_eq!(
        defaults.max_rerank_bytes,
        crate::coordinator::DEFAULT_MAX_RERANK_BYTES
    );

    let cfg = parse(&args(&[
        "--role=node",
        "--demo-vectors=10",
        "--floor-delta=0.005",
        "--shard-deadline-ms=1500",
        "--hedge-delay-ms=200",
        "--rerank-parallel=8",
        "--max-rerank-mib=32",
    ]))
    .unwrap();
    assert_eq!(cfg.floor_delta, 0.005);
    assert_eq!(cfg.shard_deadline_ms, 1500);
    assert_eq!(cfg.hedge_delay_ms, 200);
    assert_eq!(cfg.rerank_parallel, 8);
    assert_eq!(cfg.max_rerank_bytes, 32 * 1024 * 1024);

    // Negative or non-finite deltas are rejected.
    assert!(parse(&args(&[
        "--role=node",
        "--demo-vectors=10",
        "--floor-delta=-0.1"
    ]))
    .is_err());
    assert!(parse(&args(&[
        "--role=node",
        "--demo-vectors=10",
        "--rerank-parallel=65"
    ]))
    .is_err());
}

#[test]
fn membership_flags_need_each_other_and_shards_carry_their_identity() {
    let cfg = parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
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
    ]))
    .unwrap();
    let membership = cfg.membership.as_ref().unwrap();
    assert_eq!(membership.node_id, "b");
    assert_eq!(membership.control_addr, "http://10.0.0.1:50050");
    assert_eq!(membership.failure_domain, "rack-2");
    assert_eq!(membership.data_dir, PathBuf::from("/var/lib/placed"));
    assert_eq!(membership.advertise_addr.as_deref(), Some("10.0.0.2:50051"));
    assert_eq!(
        membership.replica_listen.map(|a| a.to_string()),
        Some("10.0.0.2:0".to_string())
    );
    assert_eq!(
        (membership.report_ms, membership.reconcile_ms),
        (500, 2_000)
    );
    assert_eq!((membership.lease_ms, membership.lag_bound), (0, 0));
    assert_eq!(cfg.shards[0].shard_id.as_deref(), Some("s7"));
    assert_eq!(cfg.shards[0].hash_range, Some((0, 100)));
    let plain = parse(&args(&["--role=node", "--index=/tmp/x.tv"])).unwrap();
    assert!(plain.membership.is_none());
    assert!(plain.shards[0].shard_id.is_none() && plain.shards[0].hash_range.is_none());
    for (flags, needle) in [
        (
            vec!["--role=node", "--index=/tmp/x.tv", "--node-id=b"],
            "needs --control-addr",
        ),
        (
            vec![
                "--role=node",
                "--index=/tmp/x.tv",
                "--node-id=b",
                "--control-addr=c:1",
            ],
            "needs --data-dir",
        ),
        (
            vec!["--role=node", "--index=/tmp/x.tv", "--data-dir=/d"],
            "pass --node-id",
        ),
        (
            vec![
                "--role=coordinator",
                "--nodes=a:1",
                "--node-id=b",
                "--control-addr=c:1",
                "--data-dir=/d",
            ],
            "node or both role",
        ),
        (
            vec![
                "--role=node",
                "--index=/tmp/x.tv",
                "--node-id=b",
                "--control-addr=c:1",
                "--data-dir=/d",
                "--advertise-addr=nowhere",
            ],
            "is not host:port",
        ),
        (
            vec!["--role=node", "--index=/tmp/x.tv", "--hash-lo=5"],
            "both bounds or neither",
        ),
        (
            vec![
                "--role=node",
                "--index=/tmp/x.tv",
                "--hash-lo=5",
                "--hash-hi=4",
            ],
            "is inverted",
        ),
    ] {
        let error = parse(&args(&flags)).unwrap_err();
        assert!(error.contains(needle), "{flags:?}: {error}");
    }
}

#[test]
fn durable_control_requires_a_coordinator_map_and_parses_policy() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&dir).unwrap();
    let map = dir.join(format!("control-map-{}.toml", std::process::id()));
    let state = dir.join(format!("control-state-{}.json", std::process::id()));
    std::fs::write(
        &map,
        "generation = 4\n\n[[shards]]\naddr = \"node:50051\"\nslot_offset = 0\n",
    )
    .unwrap();
    let cfg = parse(&args(&[
        "--role=coordinator",
        &format!("--shard-map={}", map.display()),
        &format!("--control-state={}", state.display()),
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
    assert!(parse(&args(&[
        "--role=coordinator",
        "--nodes=node:50051",
        &format!("--control-state={}", state.display()),
    ]))
    .is_err());
    assert!(parse(&args(&[
        "--role=coordinator",
        &format!("--shard-map={}", map.display()),
        &format!("--control-state={}", state.display()),
        "--control-compact-segments=4294967296",
    ]))
    .is_err());
    std::fs::remove_file(map).unwrap();
}

#[test]
fn shard_map_parses_replicas() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&dir).ok();
    let path = dir.join(format!("turbovec_replicas_{}.toml", std::process::id()));
    std::fs::write(
        &path,
        r#"
[[shards]]
addr = "host-a:50051"
replica = "host-a2:50051"

[[shards]]
addr = "host-b:50051"
"#,
    )
    .unwrap();
    let cfg = parse(&args(&[
        "--role=coordinator",
        &format!("--shard-map={}", path.display()),
    ]))
    .unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        cfg.replica_addrs,
        vec![Some("http://host-a2:50051".to_string()), None]
    );
}

#[test]
fn shard_map_replaces_nodes() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&dir).ok();
    let path = dir.join(format!("turbovec_shardmap_{}.toml", std::process::id()));
    std::fs::write(
        &path,
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
    )
    .unwrap();
    let cfg = parse(&args(&[
        "--role=coordinator",
        &format!("--shard-map={}", path.display()),
    ]))
    .unwrap();
    let map = cfg.shard_map.expect("shard map parsed");
    assert_eq!(map.generation, 7);
    assert_eq!(map.shards.len(), 2);
    assert_eq!(map.shards[1].slot_offset, 25_000_000);
    assert_eq!(map.shards[1].hash_lo, None);
    assert_eq!(
        cfg.node_addrs,
        vec!["http://host-a:50051", "http://host-b:50051"]
    );
    // Both --nodes and --shard-map is an error.
    assert!(parse(&args(&[
        "--role=coordinator",
        &format!("--shard-map={}", path.display()),
        "--nodes=a:1",
    ]))
    .is_err());
    let _ = std::fs::remove_file(&path);
}

#[test]
fn phrase_configuration_requires_explicit_storage_fields() {
    let base = [
        "--role=node",
        "--demo-vectors=10",
        "--node-listen=127.0.0.1:9001",
        "--phrase-glossary=/tmp/concepts.tsv",
    ];
    assert!(parse(&args(&base)).unwrap_err().contains("phrase field"));

    let mut configured = base.to_vec();
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
