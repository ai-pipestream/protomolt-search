use std::cell::Cell;
use std::path::PathBuf;
use std::sync::Arc;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    authorization::PolicyAuthority,
    bm25::{self, Bm25Params, CorpusStats, FieldQuery, TopK},
    collections::CollectionSet,
    coordinator::CoordinatorServiceImpl,
    link::NodeLink,
    node::{NodeConfig, NodeServiceImpl},
    pb::{search_service_server::SearchService, *},
    postings::{
        AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, DocLineage, ImpactCursor, PostingCallback,
    },
    security::{PrincipalConfig, Principals},
};
use tonic::Request;

const READER_TOKEN: &str = "reader-token-0123456789012345";
const COMMON_TERMS: usize = 160;
const WIDE_DOCS: u32 = 2_048;
const PARAMS: Bm25Params = Bm25Params { k1: 1.2, b: 0.75 };

fn stats_for(store: &Bm25Store, terms: &[String]) -> CorpusStats {
    CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.total_doc_length(),
        dfs: terms.iter().map(|t| store.df(t)).collect(),
    }
}

fn req_topk<'a>(k: usize, floor: f64, offsets: bool) -> TopK<'a> {
    TopK::new(k).floor(floor).include_offsets(offsets)
}

fn test_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "bm25_wide_query_pruning_{name}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn wide_fixture() -> (Bm25Store, Vec<String>, CorpusStats) {
    let terms: Vec<String> = (0..COMMON_TERMS)
        .map(|term| format!("common{term}"))
        .chain(std::iter::once("rare-anchor".to_string()))
        .collect();
    let mut store = Bm25Store::new();
    for doc in 0..WIDE_DOCS {
        let mut postings: Vec<_> = terms[..COMMON_TERMS]
            .iter()
            .map(|term| (term.clone(), 1, vec![(0, 1)]))
            .collect();
        if doc == WIDE_DOCS - 1 {
            postings.push(("rare-anchor".to_string(), 1, vec![(160, 166)]));
        }
        let length = postings.iter().map(|(_, tf, _)| *tf).sum();
        store.add_document(
            doc,
            format!("doc {doc}"),
            AnalyzedDoc::body(postings, length),
        );
    }
    let stats = stats_for(&store, &terms);
    (store, terms, stats)
}

#[test]
fn wide_query_prunes_and_preserves_every_matched_term() {
    let dir = test_dir("wide");
    let path = dir.join("wide.bm25");
    let (store, terms, stats) = wide_fixture();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let unseeded = bm25::top_k_exhaustive(
        &store,
        &terms,
        &stats,
        PARAMS,
        req_topk(1, f64::NEG_INFINITY, false),
    );
    assert_eq!(unseeded[0].doc_id, WIDE_DOCS - 1);
    let floor = unseeded[0].score;
    let expected =
        bm25::top_k_exhaustive(&store, &terms, &stats, PARAMS, req_topk(1, floor, false));
    let mut prune = bm25::PruneStats::default();
    let actual = bm25::top_k(
        &reader,
        &terms,
        &stats,
        PARAMS,
        req_topk(1, floor, false).prune_stats(&mut prune),
    );
    assert_eq!(actual, expected);
    assert!(prune.blocks_total > 0);
    assert!(prune.blocks_skipped > 0, "wide query did not skip blocks");
    assert!(
        prune.candidates_evaluated < u64::from(WIDE_DOCS),
        "wide query exhaustively evaluated every document"
    );

    let expected = bm25::top_k_exhaustive(&store, &terms, &stats, PARAMS, req_topk(1, floor, true));
    let actual = bm25::top_k(&reader, &terms, &stats, PARAMS, req_topk(1, floor, true));
    assert_eq!(actual, expected);
    assert!(actual[0]
        .term_offsets
        .iter()
        .any(|(term, offsets)| *term == COMMON_TERMS && *offsets == vec![(160, 166)]));
    let _ = std::fs::remove_dir_all(&dir);
}

struct CountingIndex<'a> {
    inner: &'a Bm25Reader,
    offset_reads: &'a Cell<usize>,
    posting_walks: &'a Cell<usize>,
}

impl Bm25Index for CountingIndex<'_> {
    fn doc_count(&self) -> u64 {
        self.inner.doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.inner.total_doc_length()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.inner.doc_length(doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.inner.df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback<'_>) {
        self.posting_walks.set(self.posting_walks.get() + 1);
        self.inner.for_each_posting(term, f);
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.inner.for_each_doc_tf(term, f);
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.offset_reads.set(self.offset_reads.get() + 1);
        self.inner.posting_offsets(term, doc_id)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        self.inner.impacts(term)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.inner.text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.inner.lineage(doc_id)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.inner.expand_prefix(prefix, cap)
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.inner.suggest_prefix(prefix, max_scan)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.inner.prefix_terms(prefix)
    }
}

fn small_fixture() -> (Bm25Store, Vec<String>, CorpusStats) {
    let terms = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
    let mut store = Bm25Store::new();
    for doc in 0..512u32 {
        let mut postings = vec![("alpha".to_string(), 1, vec![(0, 5)])];
        if doc % 3 == 0 {
            postings.push(("beta".to_string(), 2, vec![(6, 10), (11, 15)]));
        }
        if doc == 511 {
            postings.push(("gamma".to_string(), 1, vec![(16, 21)]));
        }
        let length = postings.iter().map(|(_, tf, _)| *tf).sum();
        store.add_document(
            doc,
            format!("doc {doc}"),
            AnalyzedDoc::body(postings, length),
        );
    }
    let stats = stats_for(&store, &terms);
    (store, terms, stats)
}

fn field<'a>(index: &'a dyn Bm25Index, terms: &'a [String], stats: &CorpusStats) -> FieldQuery<'a> {
    FieldQuery {
        index,
        terms,
        stats: stats.clone(),
        params: PARAMS,
        weight: 1.0,
    }
}

#[test]
fn disabled_offsets_avoid_positional_reads_on_every_scorer() {
    let dir = test_dir("offsets");
    let path = dir.join("offsets.bm25");
    let (store, terms, stats) = small_fixture();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let (offset_reads, posting_walks) = (Cell::new(0), Cell::new(0));
    let index = CountingIndex {
        inner: &reader,
        offset_reads: &offset_reads,
        posting_walks: &posting_walks,
    };
    let no_off = || TopK::new(4).include_offsets(false);
    let expected = bm25::top_k_exhaustive(&store, &terms, &stats, PARAMS, no_off());
    assert_eq!(
        bm25::top_k(&index, &terms, &stats, PARAMS, no_off()),
        expected
    );
    let expected = bm25::top_k_fused_exhaustive(&[field(&store, &terms, &stats)], no_off(), None);
    let query = [field(&index, &terms, &stats)];
    assert_eq!(bm25::top_k_fused(&query, no_off()), expected);
    let actual = bm25::top_k_fused_exhaustive(&query, no_off(), None);
    assert_eq!(actual, expected);
    assert!(actual.iter().all(|doc| doc.term_offsets.is_empty()));
    assert_eq!((offset_reads.get(), posting_walks.get()), (0, 0));

    let with_off = || TopK::new(4).include_offsets(true);
    let with_offsets = bm25::top_k(&index, &terms, &stats, PARAMS, with_off());
    assert!(with_offsets.iter().any(|doc| !doc.term_offsets.is_empty()));
    assert!(offset_reads.get() > 0);
    let _ = bm25::top_k_fused_exhaustive(&query, with_off(), None);
    assert!(posting_walks.get() > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

fn varied_wide_fixture() -> (Bm25Store, Vec<String>, CorpusStats) {
    let terms: Vec<String> = (0..192).map(|t| format!("tier{t}")).collect();
    let mut store = Bm25Store::new();
    for doc in 0..256u32 {
        let mut postings = Vec::new();
        for (idx, term) in terms.iter().enumerate() {
            if doc % (1 + (idx as u32 % 32)) == 0 {
                let tf = 1 + ((doc + idx as u32) % 4);
                let start = idx as u32 * 4;
                postings.push((term.clone(), tf, vec![(start, start + 3)]));
            }
        }
        let length = postings.iter().map(|(_, tf, _)| *tf).sum::<u32>().max(1);
        store.add_document(
            doc,
            format!("doc {doc}"),
            AnalyzedDoc::body(postings, length),
        );
    }
    let stats = stats_for(&store, &terms);
    (store, terms, stats)
}

#[test]
fn wide_varied_impacts_match_exhaustive_across_partition_floors() {
    let dir = test_dir("varied_wide");
    let path = dir.join("varied.bm25");
    let (store, terms, stats) = varied_wide_fixture();
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let best = bm25::top_k_exhaustive(&store, &terms, &stats, PARAMS, 3)[0].score;
    for floor in [
        f64::NEG_INFINITY,
        0.0,
        best * 0.25,
        best * 0.6,
        best,
        best + 10.0,
    ] {
        for off in [false, true] {
            let want =
                bm25::top_k_exhaustive(&store, &terms, &stats, PARAMS, req_topk(3, floor, off));
            let got = bm25::top_k(&reader, &terms, &stats, PARAMS, req_topk(3, floor, off));
            assert_eq!(got.len(), want.len());
            for (g, w) in got.iter().zip(&want) {
                assert_eq!((g.doc_id, g.score.to_bits()), (w.doc_id, w.score.to_bits()));
                assert_eq!(g.term_offsets, w.term_offsets);
            }
            let want_f = bm25::top_k_fused_exhaustive(
                &[field(&store, &terms, &stats)],
                req_topk(3, floor, off),
                None,
            );
            let got_f =
                bm25::top_k_fused(&[field(&reader, &terms, &stats)], req_topk(3, floor, off));
            assert_eq!(got_f.len(), want_f.len());
            for (g, w) in got_f.iter().zip(&want_f) {
                assert_eq!((g.doc_id, g.score.to_bits()), (w.doc_id, w.score.to_bits()));
                assert_eq!(g.term_offsets, w.term_offsets);
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn use_only_collection(coordinator: CoordinatorServiceImpl) -> CollectionSet {
    let grants = ["body", "title"]
        .into_iter()
        .map(|field| FieldGrant {
            field: field.into(),
            actions: vec![FieldAction::Use as i32],
        })
        .collect();
    let policy = AccessPolicy {
        format_version: 3,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "work".into(),
            collection: String::new(),
        }],
        grants: vec![CollectionGrant {
            principal: "reader".into(),
            workspace: "work".into(),
            collection: String::new(),
            actions: vec![AccessAction::Search as i32],
            field_permissions: Some(FieldPermissions {
                grants,
                disclose_document_identity: true,
            }),
            document_visibility: None,
        }],
    };
    let principals = Principals::from_configs(&[PrincipalConfig {
        name: "reader".into(),
        token: READER_TOKEN.into(),
        ..Default::default()
    }])
    .unwrap()
    .with_authorizer(Arc::new(PolicyAuthority::new(policy).unwrap()));
    CollectionSet::single(coordinator).with_principals(Arc::new(principals))
}

async fn seeded_coordinator(streaming: bool) -> CoordinatorServiceImpl {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            bm25_fields: vec!["body".into(), "title".into()],
            position_fields: vec!["body".into(), "title".into()],
            ..Default::default()
        },
    ));
    NodeLink::local(node.clone())
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "alpha beta".into(),
            analysis: Some(body_spec()),
            fields: vec![DocumentField {
                field: "title".into(),
                text: "alpha title".into(),
                analysis: Some(body_spec()),
            }],
            ..Default::default()
        }]))
        .await
        .unwrap();
    CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
        .with_bm25_stream(streaming)
}

#[tokio::test]
async fn node_flat_and_fused_queries_emit_terms_and_redact_use_only_fields() {
    let flat = Bm25SearchRequest {
        text: "alpha".into(),
        k: 5,
        analysis: Some(body_spec()),
        ..Default::default()
    };
    let fused = Bm25SearchRequest {
        text: "alpha".into(),
        k: 5,
        fields: ["body", "title"]
            .into_iter()
            .map(|field| QueryField {
                field: field.into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    for streaming in [false, true] {
        let coordinator = seeded_coordinator(streaming).await;
        let reader = use_only_collection(coordinator.clone());
        for req in [&flat, &fused] {
            let full = SearchService::bm25_search(&coordinator, Request::new(req.clone()))
                .await
                .unwrap()
                .into_inner();
            assert!(!full.field_details_redacted);
            assert_eq!(full.hits.len(), 1);
            assert!(!full.hits[0].terms.is_empty());
            assert!(!full.hits[0].terms[0].offsets.is_empty());

            let mut auth_req = Request::new(req.clone());
            auth_req.metadata_mut().insert(
                "authorization",
                format!("Bearer {READER_TOKEN}").parse().unwrap(),
            );
            let redacted = SearchService::bm25_search(&reader, auth_req)
                .await
                .unwrap()
                .into_inner();
            assert!(redacted.field_details_redacted);
            assert_eq!(redacted.hits.len(), 1);
            assert!(redacted.hits[0].terms.is_empty());
            assert_eq!(
                redacted.hits[0].score.to_bits(),
                full.hits[0].score.to_bits()
            );
        }
    }
}
