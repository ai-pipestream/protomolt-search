//! Collections (`docs/collections.md`): one cluster, many datasets, no
//! bleed. Pins the naming table, per-collection statistics (same term,
//! different df), set == single coordinator per collection, the empty
//! collection, node-side refusals and writing, the WAL manifest, the
//! control plane, cluster health, and configuration parsing.

mod common;

use std::path::PathBuf;

use common::start_empty_node;
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::collections::{ClusterControlSet, CollectionSet};
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::cluster_control_server::ClusterControl;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::wal::wal_record;
use pipestream_search::pb::{
    AddDocumentsRequest, ApplyWalBindingRequest, Bm25SearchRequest, Bm25SearchResponse,
    ClusterHealthRequest, GetClusterPlanRequest, HealthRequest, NodeCapacity, RegisterNodeRequest,
    ReportShardRequest, ShardReplicaState,
};
use pipestream_search::wal::{self, WalManifest};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

const CORPUS_A: [&str; 3] = ["court one", "court two", "court three"];
const CORPUS_B: [&str; 3] = ["court only", "other doc", "third doc"];

fn config(collection: &str) -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        collection: collection.to_string(),
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: &[&str], collection: &str) -> Result<u64, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in docs {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            collection: collection.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner().added)
}

fn coordinator(addr: &str, collection: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_collection(collection)
}

fn request(text: &str, collection: &str) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.to_string(),
        k: 10,
        analysis: Some(body_spec()),
        collection: collection.to_string(),
        ..Default::default()
    }
}

async fn bm25<S: SearchService>(s: &S, req: Bm25SearchRequest) -> Bm25SearchResponse {
    SearchService::bm25_search(s, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

async fn refused<S: SearchService>(s: &S, req: Bm25SearchRequest) -> tonic::Status {
    SearchService::bm25_search(s, Request::new(req))
        .await
        .unwrap_err()
}

fn signature(resp: &Bm25SearchResponse) -> Vec<(u64, u32)> {
    resp.hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect()
}

/// Two named collections, one node each, plus an empty third.
struct Fleet {
    a: String,
    b: String,
    empty: String,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

async fn fleet() -> Fleet {
    let (a, ha) = start_empty_node(config("a")).await;
    let (b, hb) = start_empty_node(config("b")).await;
    let (empty, he) = start_empty_node(config("empty")).await;
    assert_eq!(ingest(&a, &CORPUS_A, "").await.unwrap(), 3);
    assert_eq!(ingest(&b, &CORPUS_B, "b").await.unwrap(), 3);
    Fleet {
        a,
        b,
        empty,
        handles: vec![ha, hb, he],
    }
}

fn named_set(f: &Fleet, default: Option<&str>) -> CollectionSet {
    CollectionSet::named(
        vec![
            ("a".to_string(), coordinator(&f.a, "a")),
            ("b".to_string(), coordinator(&f.b, "b")),
            ("empty".to_string(), coordinator(&f.empty, "empty")),
        ],
        default.map(str::to_string),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_term_has_a_different_df_in_each_collection() {
    let f = fleet().await;
    let set = named_set(&f, None);
    let a = bm25(&set, request("court", "a")).await;
    let b = bm25(&set, request("court", "b")).await;
    assert_eq!(a.hits.len(), 3, "every document of a contains the term");
    assert_eq!(b.hits.len(), 1, "one document of b does");
    // df 3 of 3 in a, 1 of 3 in b: the idf, and so the score, differ.
    assert_ne!(a.hits[0].score.to_bits(), b.hits[0].score.to_bits());
    assert!(
        b.hits[0].score > a.hits[0].score,
        "rarer term scores higher"
    );
    // The set returns precisely what a single coordinator over the same
    // collection returns: routing adds nothing and removes nothing.
    let lone_a = coordinator(&f.a, "a");
    let lone_b = coordinator(&f.b, "b");
    assert_eq!(
        signature(&a),
        signature(&bm25(&lone_a, request("court", "a")).await)
    );
    assert_eq!(
        signature(&b),
        signature(&bm25(&lone_b, request("court", "b")).await)
    );
    // The empty collection is empty, not the other dataset.
    let empty = bm25(&set, request("court", "empty")).await;
    assert!(empty.hits.is_empty());
    for h in f.handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn naming_never_picks_silently() {
    let f = fleet().await;
    // Named set, no default: an unnamed request refuses, naming the
    // collections; an unknown name refuses, naming it.
    let set = named_set(&f, None);
    let error = refused(&set, request("court", "")).await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("[\"a\", \"b\", \"empty\"]")
            && error.message().contains("no default"),
        "{}",
        error.message()
    );
    let error = refused(&set, request("court", "zzz")).await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("unknown collection \"zzz\""),
        "{}",
        error.message()
    );
    assert_eq!(set.names(), vec!["a", "b", "empty"]);
    // With a default, the unnamed request gets to it — and only it.
    let with_default = named_set(&f, Some("b"));
    let unnamed = bm25(&with_default, request("court", "")).await;
    assert_eq!(unnamed.hits.len(), 1, "the default is b");
    // The unnamed set serves unnamed requests and refuses any name.
    let single = CollectionSet::single(coordinator(&f.a, ""));
    assert_eq!(bm25(&single, request("court", "")).await.hits.len(), 3);
    let error = refused(&single, request("court", "a")).await;
    assert!(
        error.message().contains("one unnamed dataset"),
        "{}",
        error.message()
    );
    // A single coordinator built for a refuses b even when reached directly.
    let single = coordinator(&f.a, "a");
    assert_eq!(bm25(&single, request("court", "")).await.hits.len(), 3);
    assert_eq!(bm25(&single, request("court", "a")).await.hits.len(), 3);
    let error = refused(&single, request("court", "b")).await;
    assert!(
        error
            .message()
            .contains("serves collection \"a\", not \"b\""),
        "{}",
        error.message()
    );
    // Building a set with a coordinator under the wrong name, a node in
    // two collections, or a default outside the set refuses.
    let error = CollectionSet::named(vec![("a".to_string(), coordinator(&f.a, "b"))], None)
        .err()
        .expect("refused");
    assert!(error.contains("was built for \"b\""), "{error}");
    let error = CollectionSet::named(
        vec![
            ("a".to_string(), coordinator(&f.a, "a")),
            ("b".to_string(), coordinator(&f.a, "b")),
        ],
        None,
    )
    .err()
    .expect("refused");
    assert!(error.contains("only one collection"), "{error}");
    let error = CollectionSet::named(
        vec![("a".to_string(), coordinator(&f.a, "a"))],
        Some("b".to_string()),
    )
    .err()
    .expect("refused");
    assert!(error.contains("not one of"), "{error}");
    for h in f.handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_serves_one_collection_and_says_so() {
    let f = fleet().await;
    let mut client = NodeServiceClient::connect(f.a.clone()).await.unwrap();
    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.collection, "a");
    // A document naming another collection refuses; the node's own name
    // and no name are both accepted.
    let error = ingest(&f.a, &["stray"], "b").await.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("serves collection \"a\", not \"b\""),
        "{}",
        error.message()
    );
    assert_eq!(ingest(&f.a, &["court four"], "a").await.unwrap(), 1);
    assert_eq!(ingest(&f.a, &["court five"], "").await.unwrap(), 1);
    // A node outside any collection refuses a named document.
    let (unnamed, hu) = start_empty_node(config("")).await;
    let error = ingest(&unnamed, &["stray"], "a").await.unwrap_err();
    assert!(
        error.message().contains("serves no named collection"),
        "{}",
        error.message()
    );
    // A WAL binding for another collection refuses before anything binds.
    let error = client
        .apply_wal_binding(ApplyWalBindingRequest {
            plan_fingerprint: "fp".into(),
            body_path: "body".into(),
            materialize_sha: String::new(),
            analysis_sha: String::new(),
            analysis_contract: Vec::new(),
            vector_binding: Vec::new(),
            collection: "b".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("not \"b\""), "{}", error.message());
    hu.abort();
    for h in f.handles {
        h.abort();
    }
}

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("collections-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn manifest(collection: &str) -> WalManifest {
    WalManifest {
        collection: collection.to_string(),
        dim: 0,
        vector_backend: String::new(),
        vector_config_format: String::new(),
        vector_config_payload: Vec::new(),
        bit_width: 4,
        calibration_shift: Vec::new(),
        calibration_scale: Vec::new(),
        slot_offset: 0,
        generation: 0,
        bucket_bits: 2,
        bucket_count: 4,
        preexisting_vectors: 0,
        preexisting_documents: 0,
        format_version: wal::FORMAT_VERSION,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_wal_manifest_carries_the_collection_and_refuses_a_foreign_one() {
    let dir = tempdir("wal");
    // A node of collection a with a log: the manifest is written at
    // creation, and every logged document carries the name.
    let index_path = dir.join("a.tv");
    let (addr, handle) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        wal: true,
        wal_buckets: 4,
        ..config("a")
    })
    .await;
    assert_eq!(ingest(&addr, &CORPUS_A, "").await.unwrap(), 3);
    // Flush is the durability point: the log is fsynced before the
    // index images are written, so its records are readable after.
    NodeServiceClient::connect(addr.clone())
        .await
        .unwrap()
        .flush(pipestream_search::pb::FlushRequest {})
        .await
        .unwrap();
    let wal_dir = wal::wal_dir(&index_path);
    let (_, gen_dir) = wal::latest_gen(&wal_dir).unwrap().expect("a generation");
    assert_eq!(wal::read_manifest(&gen_dir).unwrap().collection, "a");
    let mut logged: Vec<String> = Vec::new();
    for bucket in 0..4 {
        let path = wal::bucket_path(&gen_dir, bucket);
        if !path.exists() {
            continue;
        }
        let mut reader = wal::RecordReader::open(&path).unwrap();
        while let Some(record) = reader.next_record().unwrap() {
            if let Some(wal_record::Op::AddDocuments(add)) = record.op {
                logged.extend(add.documents.iter().map(|d| d.collection.clone()));
            }
        }
    }
    assert_eq!(logged.len(), 3);
    assert!(logged.iter().all(|c| c == "a"), "{logged:?}");
    handle.abort();

    // A log from before collections is adopted and written by the node
    // that opens it under a name.
    let legacy_path = dir.join("legacy.tv");
    let legacy_wal = wal::wal_dir(&legacy_path);
    drop(wal::open_or_create(&legacy_wal, 0, manifest("")).unwrap());
    let node = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(legacy_path.clone()),
            wal: true,
            wal_buckets: 4,
            ..config("a")
        },
        None,
        true,
    )
    .unwrap();
    drop(node);
    let (_, gen_dir) = wal::latest_gen(&legacy_wal).unwrap().unwrap();
    assert_eq!(wal::read_manifest(&gen_dir).unwrap().collection, "a");

    // A log of another collection refuses to open here, naming both.
    let foreign_path = dir.join("foreign.tv");
    let foreign_wal = wal::wal_dir(&foreign_path);
    drop(wal::open_or_create(&foreign_wal, 0, manifest("b")).unwrap());
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        NodeServiceImpl::open(
            NodeConfig {
                index_path: Some(foreign_path.clone()),
                wal: true,
                wal_buckets: 4,
                ..config("a")
            },
            None,
            true,
        )
    }));
    let message = match outcome {
        Err(payload) => payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default(),
        Ok(_) => String::from("opened"),
    };
    assert!(
        message.contains("belongs to collection \"b\"") && message.contains("serves \"a\""),
        "{message}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn policy() -> ControlPolicy {
    ControlPolicy {
        lease_ms: 60_000,
        replication_factor: 1,
        split_rows: 1_000_000,
        merge_rows: 1_000,
        compact_segments: 8,
        compact_tombstone_ppm: 100_000,
        history_limit: 8,
    }
}

fn register(collection: &str) -> RegisterNodeRequest {
    RegisterNodeRequest {
        node_id: "n1".into(),
        addr: "10.0.0.1:1".into(),
        capacity: Some(NodeCapacity {
            disk_bytes: 1_000,
            failure_domain: "z1".into(),
            ..Default::default()
        }),
        lease_ms: 60_000,
        collection: collection.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_control_plane_governs_one_collection() {
    let plane = DurableControlPlane::in_memory(policy())
        .with_collection("a")
        .unwrap();
    let control = ClusterControlService::new(plane);
    assert_eq!(control.collection(), "a");
    let error = ClusterControl::register_node(&control, Request::new(register("b")))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("governs collection \"a\", not \"b\""),
        "{}",
        error.message()
    );
    let lease = ClusterControl::register_node(&control, Request::new(register("a")))
        .await
        .unwrap()
        .into_inner();
    // A shard report whose replica names another collection refuses.
    let report = |collection: &str| ReportShardRequest {
        node_id: "n1".into(),
        lease_token: lease.lease_token,
        replica: Some(ShardReplicaState {
            shard_id: "s0".into(),
            node_id: "n1".into(),
            addr: "10.0.0.1:1".into(),
            hash_lo: 0,
            hash_hi: u64::MAX,
            ready: true,
            role: pipestream_search::pb::ShardReplicaRole::Primary as i32,
            collection: collection.to_string(),
            ..Default::default()
        }),
        collection: "a".into(),
    };
    let error = ClusterControl::report_shard(&control, Request::new(report("b")))
        .await
        .unwrap_err();
    assert!(error.message().contains("not \"b\""), "{}", error.message());
    ClusterControl::report_shard(&control, Request::new(report("a")))
        .await
        .unwrap();
    // Every record of the plan carries the collection.
    let plan = ClusterControl::get_cluster_plan(
        &control,
        Request::new(GetClusterPlanRequest {
            collection: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(plan.collection, "a");
    assert!(plan.nodes.iter().all(|n| n.collection == "a") && !plan.nodes.is_empty());
    assert!(plan.replicas.iter().all(|r| r.collection == "a") && !plan.replicas.is_empty());
    assert!(plan.actions.iter().all(|x| x.collection == "a"));

    // Durable state remembers its collection and refuses another name.
    let dir = tempdir("control");
    let path = dir.join("control.json");
    drop(
        DurableControlPlane::open(&path, policy())
            .unwrap()
            .with_collection("a")
            .unwrap(),
    );
    let reopened = DurableControlPlane::open(&path, policy()).unwrap();
    assert_eq!(reopened.collection(), "a");
    let error = reopened.with_collection("b").err().expect("refused");
    assert!(
        error.contains("governs collection \"a\", not \"b\""),
        "{error}"
    );
    let _ = std::fs::remove_dir_all(&dir);

    // The control set applies the naming rules of the search set.
    let a = ClusterControlService::new(
        DurableControlPlane::in_memory(policy())
            .with_collection("a")
            .unwrap(),
    );
    let b = ClusterControlService::new(
        DurableControlPlane::in_memory(policy())
            .with_collection("b")
            .unwrap(),
    );
    let set = ClusterControlSet::named(vec![("a".into(), a), ("b".into(), b)], None).unwrap();
    let error = ClusterControl::get_cluster_plan(
        &set,
        Request::new(GetClusterPlanRequest {
            collection: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert!(
        error.message().contains("no default"),
        "{}",
        error.message()
    );
    let plan = ClusterControl::get_cluster_plan(
        &set,
        Request::new(GetClusterPlanRequest {
            collection: "b".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(plan.collection, "b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_lists_collections_without_mixing_and_flags_a_foreign_node() {
    let f = fleet().await;
    let set = named_set(&f, None);
    let health = SearchService::cluster_health(
        &set,
        Request::new(ClusterHealthRequest {
            collection: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(health.targets.is_empty(), "no summed target list");
    assert_eq!(health.collections.len(), 3);
    for entry in &health.collections {
        let inner = entry.health.as_ref().unwrap();
        assert_eq!(inner.targets.len(), 1, "{}", entry.name);
        let target = &inner.targets[0];
        assert!(target.reachable && target.error.is_empty(), "{target:?}");
        let node = target.health.as_ref().unwrap();
        assert_eq!(node.collection, entry.name);
        let expected = match entry.name.as_str() {
            "a" | "b" => 3,
            _ => 0,
        };
        assert_eq!(node.bm25_docs, expected, "{}", entry.name);
    }
    // A named health request is that collection's alone.
    let only_b = SearchService::cluster_health(
        &set,
        Request::new(ClusterHealthRequest {
            collection: "b".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(only_b.targets.len(), 1);
    assert!(only_b.collections.is_empty());
    // A coordinator for a that lists b's node: membership refuses, and
    // health names the node rather than counting it.
    let wrong = CoordinatorServiceImpl::new(vec![f.b.clone()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_collection("a");
    let error = wrong.verify_collection_membership().await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("serves collection \"b\", but this coordinator is \"a\""),
        "{}",
        error.message()
    );
    let health = SearchService::cluster_health(
        &wrong,
        Request::new(ClusterHealthRequest {
            collection: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(
        health.targets[0].error.contains("serves collection \"b\""),
        "{:?}",
        health.targets[0]
    );
    let right = named_set(&f, None);
    right.verify_membership().await.unwrap();
    for h in f.handles {
        h.abort();
    }
}

#[test]
fn configuration_declares_collections_and_refuses_ambiguity() {
    let dir = tempdir("config");
    // The defaults bind off loopback; plaintext there needs the explicit
    // flag (docs/security.md), which is not what these cases are about.
    let write = |name: &str, body: &str| {
        let path = dir.join(name);
        std::fs::write(&path, format!("allow_plaintext = true\n{body}")).unwrap();
        path.display().to_string()
    };
    let good = write(
        "good.toml",
        r#"
role = "coordinator"
default_collection = "opinions"

[[collections]]
name = "opinions"
nodes = ["10.0.0.1:59300", "10.0.0.2:59300"]
bm25_k1 = 1.5

[[collections]]
name = "dockets"
nodes = ["10.0.0.3:59300"]
analysis_addr = "native"
"#,
    );
    let cfg = pipestream_search::config::parse(&[format!("--config={good}")]).unwrap();
    assert!(cfg.node_addrs.is_empty());
    assert_eq!(cfg.default_collection.as_deref(), Some("opinions"));
    let names: Vec<&str> = cfg.collections.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["opinions", "dockets"]);
    assert_eq!(cfg.collections[0].node_addrs.len(), 2);
    assert_eq!(cfg.collections[0].bm25_k1, 1.5);
    assert_eq!(cfg.collections[1].node_addrs.len(), 1);
    assert_eq!(cfg.collections[1].analysis_addr.as_deref(), Some("native"));

    let cases = [
        (
            "dup.toml",
            "role = \"coordinator\"\n[[collections]]\nname = \"a\"\nnodes = [\"10.0.0.1:1\"]\n[[collections]]\nname = \"a\"\nnodes = [\"10.0.0.2:1\"]\n",
            "declared twice",
        ),
        (
            "shared.toml",
            "role = \"coordinator\"\n[[collections]]\nname = \"a\"\nnodes = [\"10.0.0.1:1\"]\n[[collections]]\nname = \"b\"\nnodes = [\"10.0.0.1:1\"]\n",
            "only one collection",
        ),
        (
            "default.toml",
            "role = \"coordinator\"\ndefault_collection = \"zz\"\n[[collections]]\nname = \"a\"\nnodes = [\"10.0.0.1:1\"]\n",
            "not a declared collection",
        ),
        (
            "both.toml",
            "role = \"coordinator\"\nnodes = [\"10.0.0.9:1\"]\n[[collections]]\nname = \"a\"\nnodes = [\"10.0.0.1:1\"]\n",
            "replaces --nodes",
        ),
        (
            "name.toml",
            "role = \"coordinator\"\n[[collections]]\nname = \"a b\"\nnodes = [\"10.0.0.1:1\"]\n",
            "printable ASCII",
        ),
        (
            "empty.toml",
            "role = \"coordinator\"\n[[collections]]\nname = \"a\"\n",
            "needs nodes or a shard_map",
        ),
        (
            "shard.toml",
            "role = \"both\"\n[[collections]]\nname = \"a\"\nnodes = [\"127.0.0.1:1\"]\n[[shards]]\ncollection = \"zz\"\nlisten = \"127.0.0.1:0\"\nindex = \"/tmp/x.tv\"\n",
            "does not declare",
        ),
    ];
    for (file, body, needle) in cases {
        let path = write(file, body);
        let error = pipestream_search::config::parse(&[format!("--config={path}")]).unwrap_err();
        assert!(error.contains(needle), "{file}: {error}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
