//! End-to-end regression coverage for lazy WAL manifest locking on mapped ingest.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pipestream_search::analyzer::*;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::ingest_mapped_request::Payload;
use pipestream_search::pb::*;
use pipestream_search::reshard::{self, SplitRequest};
use pipestream_search::vector::{legacy_calibration_config, VectorIndex, EMBEDDED_TURBOVEC};
use pipestream_search::{
    harness::unit_vectors, hnsw::HNSW, link::NodeLink, mapping::derive_plan, wal,
};
use prost::Message as _;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};

const DIM: usize = 8;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped_lazy_wal_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn mapped_stream(vectors: &[f32], count: usize) -> Vec<IngestMappedRequest> {
    let fields = [
        ("id", 1, Type::Int64, Label::Optional),
        ("body", 2, Type::String, Label::Optional),
        ("embedding", 3, Type::Float, Label::Repeated),
    ]
    .map(|(n, num, t, l)| FieldDescriptorProto {
        name: Some(n.into()),
        number: Some(num),
        label: Some(l as i32),
        r#type: Some(t as i32),
        ..Default::default()
    });
    let msg_proto = DescriptorProto {
        name: Some("Article".into()),
        field: fields.into(),
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("article.proto".into()),
        package: Some("lazy.v1".into()),
        message_type: vec![msg_proto],
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![file] }.encode_to_vec();
    let plan = derive_plan(&fds, "lazy.v1.Article").unwrap();
    let bind = MappedBind {
        descriptor_set: fds,
        message_type: "lazy.v1.Article".into(),
        expected_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        field_analysis: vec![MappedFieldAnalysis {
            path: "body".into(),
            analysis: Some(body_spec()),
        }],
        ..Default::default()
    };
    let msg = |p| IngestMappedRequest { payload: Some(p) };
    std::iter::once(msg(Payload::Bind(bind)))
        .chain((0..count).map(|i| {
            let text = format!("lazy token{i} shared");
            let mut raw = vec![1 << 3, (i + 1) as u8, (2 << 3) | 2, text.len() as u8];
            raw.extend_from_slice(text.as_bytes());
            raw.extend([(3 << 3) | 2, (DIM * 4) as u8]);
            for &c in &vectors[i * DIM..(i + 1) * DIM] {
                raw.extend_from_slice(&c.to_le_bytes());
            }
            msg(Payload::Document(raw))
        }))
        .collect()
}

fn node_config(index_path: &Path, layout: Layout, backend: &str) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path.to_path_buf()),
        layout,
        vector_backend: backend.into(),
        analysis_addr: Some("native".into()),
        integer_fields: vec!["id".into()],
        wal: true,
        wal_buckets: 4,
        ..Default::default()
    }
}

fn assert_locked_manifest_and_split(gen_dir: &Path, split_out: &Path, backend: &str) {
    let locked = wal::read_manifest(gen_dir).unwrap();
    assert_eq!(locked.dim, DIM as u32);
    let locked_cfg = locked.backend_config().expect("manifest must lock config");
    assert_eq!(locked_cfg.backend_kind, backend);
    VectorIndex::from_backend_config(DIM, &locked_cfg).expect("locked config builds index");
    if backend == EMBEDDED_TURBOVEC {
        let cal = legacy_calibration_config(&locked_cfg).unwrap().unwrap();
        assert_eq!(cal.bits_per_dimension, 4);
        return;
    }
    let rt = tokio::runtime::Handle::current();
    let mut analyze = |docs: &[(&str, Option<&AnalysisSpec>, SessionLayers)]| {
        tokio::task::block_in_place(|| {
            rt.block_on(analyze_batch_streams("native", docs, 1))
                .map_err(|e| e.to_string())
        })
    };
    let gens = [gen_dir.to_path_buf()];
    let req = SplitRequest::new(&gens, 1, split_out);
    let split = reshard::split_logs_with(&req, &mut analyze).expect("WAL must replay cleanly");
    let c = &split.children[0];
    assert_eq!((c.num_vectors, c.num_documents), (4, 4));
}

async fn verify_lazy_case(root: &Path, layout: Layout, backend: &str) {
    let shard = root.join("shard.tv");
    let cfg = node_config(&shard, layout, backend);
    let open = |c| NodeLink::local(Arc::new(NodeServiceImpl::open(c, None, false).unwrap()));
    let mut link = open(cfg.clone());
    let gen_dir = reshard::resolve_gen(&wal::wal_dir(&shard)).unwrap();
    let initial = wal::read_manifest(&gen_dir).unwrap();
    assert!(initial.dim == 0 && initial.backend_config().is_err());
    let corpus = unit_vectors(4, DIM, 0x1840_C0DE);
    let batch = tokio_stream::iter(mapped_stream(&corpus, 4));
    let res = link.ingest_mapped(batch).await.unwrap().into_inner();
    assert_eq!(res.added, 4);
    assert_eq!(wal::read_manifest(&gen_dir).unwrap().dim, 8);
    link.flush(FlushRequest {}).await.unwrap();
    drop(link);
    assert_locked_manifest_and_split(&gen_dir, &root.join("split"), backend);
    let mut reopened = open(cfg);
    let req = ShardLegsRequest {
        vector: corpus[0..DIM].to_vec(),
        terms: vec!["token2".into()],
        global_doc_count: 4,
        global_total_doc_length: 12,
        global_doc_frequencies: vec![1],
        analysis_fingerprint: analysis_fingerprint(Some(&body_spec())),
        k: 4,
        ..Default::default()
    };
    let legs = reopened.shard_legs(req).await.unwrap().into_inner();
    assert_eq!(legs.vector_hits.first().map(|h| h.doc_id), Some(0));
    assert_eq!(legs.bm25_hits.first().map(|h| h.doc_id), Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_mapped_ingest_locks_wal_manifest_and_recovers_cleanly() {
    for (tag, layout, backend) in [
        ("single_tv", Layout::SingleImage, EMBEDDED_TURBOVEC),
        ("seg_tv", Layout::Segments, EMBEDDED_TURBOVEC),
        ("single_hnsw", Layout::SingleImage, HNSW),
        ("seg_hnsw", Layout::Segments, HNSW),
    ] {
        let dir = tempdir(tag);
        verify_lazy_case(&dir, layout, backend).await;
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mapped_ingest_retires_wal_when_initial_manifest_rewrite_fails() {
    use std::os::unix::fs::PermissionsExt as _;
    let chmod = |p, m| std::fs::set_permissions(p, std::fs::Permissions::from_mode(m)).unwrap();
    let root = tempdir("retire_broken");
    let shard = root.join("shard.tv");
    let cfg = node_config(&shard, Layout::SingleImage, EMBEDDED_TURBOVEC);
    let mut link = NodeLink::local(Arc::new(NodeServiceImpl::open(cfg, None, false).unwrap()));
    let gen_dir = reshard::resolve_gen(&wal::wal_dir(&shard)).unwrap();
    for b in 0..4 {
        std::fs::File::create(gen_dir.join(format!("bucket-{b:04}.wal"))).unwrap();
    }
    chmod(&gen_dir, 0o555);
    if std::fs::File::create(gen_dir.join(".root_probe")).is_ok() {
        chmod(&gen_dir, 0o755);
        std::fs::remove_dir_all(&root).ok();
        return;
    }
    let batch = tokio_stream::iter(mapped_stream(&unit_vectors(1, DIM, 9), 1));
    let res = link.ingest_mapped(batch).await.unwrap().into_inner();
    assert_eq!(res.added, 1);

    let broken = gen_dir.with_extension("broken");
    assert!(!gen_dir.exists() && broken.exists());
    for b in 0..4 {
        let path = broken.join(format!("bucket-{b:04}.wal"));
        assert_eq!(path.metadata().unwrap().len(), 0);
    }
    chmod(&broken, 0o755);
    std::fs::remove_dir_all(&root).ok();
}
