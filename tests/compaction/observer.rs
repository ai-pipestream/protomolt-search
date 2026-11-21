//! Full-surface shard read observer and monolithic reference builder.

use super::*;

/// Everything a reader can observe on one shard, keyed by stable text
/// so two shards with different positional ids compare directly. Scores
/// are exact bits.
#[derive(Debug, PartialEq)]
pub(crate) struct Reads {
    pub(crate) lexical: Vec<(String, Vec<(String, u32)>)>,
    pub(crate) facets: Vec<(String, Vec<(String, u64)>)>,
    pub(crate) dense: Vec<Vec<(String, u32)>>,
    pub(crate) hybrid: Vec<Vec<(String, u32)>>,
    pub(crate) browse_by_num: Vec<String>,
    pub(crate) parents: Vec<(String, u64)>,
}

pub(crate) const LEXICAL_PROBES: [&str; 5] =
    ["revised", "four", "blue three", "row17 green", "red one"];

pub(crate) async fn texts_of(
    node: &mut NodeServiceClient<Channel>,
    ids: &[u64],
) -> BTreeMap<u64, (String, Option<DocLineage>)> {
    if ids.is_empty() {
        return BTreeMap::new();
    }
    node.get_documents(GetDocumentsRequest {
        doc_ids: ids.to_vec(),
    })
    .await
    .unwrap()
    .into_inner()
    .documents
    .into_iter()
    .map(|d| {
        let expected = identity_for_text(&d.text);
        assert_eq!(d.identity, expected);
        (d.doc_id, (d.text, d.lineage))
    })
    .collect()
}

pub(crate) fn identity_for_text(text: &str) -> Option<pipestream_search::pb::DocumentIdentity> {
    // The unkeyed partition fixture also includes legacy rows without source metadata.
    if text.starts_with("bare") {
        return None;
    }
    let number = text
        .split_whitespace()
        .next()
        .unwrap()
        .strip_prefix("row")
        .unwrap()
        .parse()
        .unwrap();
    Some(logical_identity(&row(number, text.contains("revised"))))
}

pub(crate) fn sorted(mut hits: Vec<(String, u32)>) -> Vec<(String, u32)> {
    hits.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    hits
}

/// Observe `addrs` through one coordinator (and each node's own browse
/// and parent routes). `browse_addr` is the node whose sorted browse and
/// parents are read; the coordinator routes cover every shard.
pub(crate) async fn observe(addrs: &[String], analysis: &str, queries: &[Vec<f32>]) -> Reads {
    let coordinator = CoordinatorServiceImpl::new(addrs.to_vec())
        .with_bm25(Some(analysis.to_string()), Default::default());
    let mut nodes = Vec::new();
    for addr in addrs {
        nodes.push(client(addr).await);
    }
    // Which node owns a global id: slot offsets are 0 and 1_000_000.
    async fn resolve(
        nodes: &mut [NodeServiceClient<Channel>],
        ids: Vec<(u64, u32)>,
        identities: Option<&BTreeMap<u64, Option<pipestream_search::pb::DocumentIdentity>>>,
    ) -> Vec<(String, u32)> {
        let mut out = Vec::with_capacity(ids.len());
        for (id, bits) in ids {
            let owner = if id >= 1_000_000 { 1 } else { 0 }.min(nodes.len() - 1);
            let mut texts = texts_of(&mut nodes[owner], &[id]).await;
            let (text, _) = texts
                .remove(&id)
                .expect("hit resolves to a stored document");
            if let Some(identities) = identities {
                assert_eq!(identities[&id], identity_for_text(&text));
            }
            out.push((text, bits));
        }
        sorted(out)
    }
    // Public result identities resolve to the logical source after cutover,
    // reopen and WAL replay, even though compaction has renumbered physical IDs.
    let lexical_selection = SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lex".into(),
            query: Some(search_query::Query::Lexical(
                pipestream_search::pb::LexicalQuery {
                    text: "common".into(),
                    ..Default::default()
                },
            )),
        })),
    };
    let browse_selection = SelectionQuery {
        node: Some(selection_query::Node::Filter(
            pipestream_search::pb::FilterQuery {
                id: "rows".into(),
                predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                    "num >= 0".into(),
                )),
            },
        )),
    };
    let boolean_selection = SelectionQuery {
        node: Some(selection_query::Node::Boolean(
            pipestream_search::pb::BooleanQuery {
                must: vec![lexical_selection.clone()],
                ..Default::default()
            },
        )),
    };
    for selection in [lexical_selection, browse_selection, boolean_selection] {
        let response = SearchService::query(
            &coordinator,
            Request::new(QueryRequest {
                k: 5000,
                selection: Some(selection),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(!response.hits.is_empty());
        let identities = response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.identity.clone()))
            .collect();
        resolve(
            &mut nodes,
            response
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect(),
            Some(&identities),
        )
        .await;
    }
    let mut lexical = Vec::new();
    let mut facets = Vec::new();
    for probe in LEXICAL_PROBES {
        let response = SearchService::bm25_search(
            &coordinator,
            Request::new(Bm25SearchRequest {
                text: probe.into(),
                k: 5_000,
                facet_fields: vec!["grp".into()],
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let hits = response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect();
        let identities = response
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.identity.clone()))
            .collect();
        lexical.push((
            probe.to_string(),
            resolve(&mut nodes, hits, Some(&identities)).await,
        ));
        let mut counts: Vec<(String, u64)> = response
            .facets
            .iter()
            .flat_map(|f| f.counts.iter().map(|c| (c.value.clone(), c.count)))
            .collect();
        counts.sort();
        facets.push((probe.to_string(), counts));
    }
    let mut dense = Vec::new();
    let mut hybrid = Vec::new();
    for query in queries {
        let response = SearchService::search(
            &coordinator,
            Request::new(SearchRequest {
                k: 20,
                vector: query.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let hits = response
            .hits
            .iter()
            .map(|h| (h.vector_id, h.score.to_bits()))
            .collect();
        let identities = response
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.identity.clone()))
            .collect();
        let classic = resolve(&mut nodes, hits, Some(&identities)).await;
        let streamed = SearchService::search(
            &coordinator.clone().with_stream_search(true),
            Request::new(SearchRequest {
                k: 20,
                vector: query.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let hits = streamed
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.score.to_bits()))
            .collect();
        let identities = streamed
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.identity.clone()))
            .collect();
        assert_eq!(resolve(&mut nodes, hits, Some(&identities)).await, classic);
        dense.push(classic);
        let response = SearchService::hybrid_search(
            &coordinator,
            Request::new(HybridSearchRequest {
                text: "common revised".into(),
                vector: query.clone(),
                k: 20,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let hits = response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.fused_score.to_bits()))
            .collect();
        hybrid.push(resolve(&mut nodes, hits, None).await);
    }
    // Sorted browse and parents on the first node only (the routes are
    // per shard).
    let browse = nodes[0]
        .browse_shard(BrowseShardRequest {
            k: 100_000,
            first_page: true,
            sort: vec![BrowseSort {
                map: None,
                column: "num".into(),
                descending: false,
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let texts = texts_of(&mut nodes[0], &browse.doc_ids).await;
    let browse_by_num: Vec<String> = browse
        .doc_ids
        .iter()
        .map(|id| texts[id].0.clone())
        .collect();
    let parents = nodes[0]
        .resolve_parents(ResolveParentsRequest {
            doc_ids: browse.doc_ids.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut parents: Vec<(String, u64)> = parents
        .parents
        .iter()
        .map(|p| (texts[&p.doc_id].0.clone(), p.parent_id))
        .collect();
    parents.sort();
    Reads {
        lexical,
        facets,
        dense,
        hybrid,
        browse_by_num,
        parents,
    }
}

/// A fresh shard over exactly `rows`, for the reference reads.
pub(crate) async fn reference_shard(
    dir: &Path,
    name: &str,
    analysis: &str,
    shift: &[f32],
    scale: &[f32],
    rows: &[Row],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (addr, handle) = start_empty_node(config(Some(dir.join(name)), analysis)).await;
    calibrate(&addr, shift, scale).await;
    let mut client = client(&addr).await;
    for row in rows {
        append(&mut client, row).await;
    }
    client.flush(FlushRequest {}).await.unwrap();
    (addr, handle)
}
