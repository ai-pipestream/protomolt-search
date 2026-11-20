//! Relayed facet dictionary unions and hierarchical coordinator diagnostics.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dictionaries_through_a_relay_are_the_union_of_the_children() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[0], &l[1]]).await;
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let mut children = Vec::new();
    for addr in l.iter().take(2) {
        children.push(NodeServiceClient::connect(addr.clone()).await.unwrap());
    }
    let expand = |cap: u32| pipestream_search::pb::ExpandTermPrefixRequest {
        field: "body".into(),
        prefix: "o".into(),
        cap,

        ..Default::default()
    };
    let mut union = std::collections::BTreeSet::new();
    for child in children.iter_mut() {
        let direct = child
            .expand_term_prefix(expand(64))
            .await
            .unwrap()
            .into_inner();
        assert!(direct.known);
        union.extend(direct.terms);
    }
    assert!(
        union.len() >= 2,
        "the prefix expands to several terms: {union:?}"
    );
    let through = relay
        .expand_term_prefix(expand(64))
        .await
        .unwrap()
        .into_inner();
    assert!(through.known);
    assert_eq!(through.count, union.len() as u64);
    assert_eq!(through.terms, union.iter().cloned().collect::<Vec<_>>());
    // Past the cap on the union: the count, no terms.
    let past = relay
        .expand_term_prefix(expand(union.len() as u32 - 1))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(past.count, union.len() as u64);
    assert!(past.terms.is_empty());

    let suggest = |max_scan: u64| pipestream_search::pb::SuggestTermsRequest {
        field: "body".into(),
        prefix: "o".into(),
        max_scan,

        ..Default::default()
    };
    let mut dfs: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for child in children.iter_mut() {
        let direct = child.suggest_terms(suggest(64)).await.unwrap().into_inner();
        for entry in direct.entries {
            *dfs.entry(entry.term).or_default() += entry.df;
        }
    }
    let through = relay.suggest_terms(suggest(64)).await.unwrap().into_inner();
    assert!(through.known);
    assert_eq!(through.count, dfs.len() as u64);
    let got: Vec<(String, u64)> = through
        .entries
        .iter()
        .map(|e| (e.term.clone(), e.df))
        .collect();
    let want: Vec<(String, u64)> = dfs.into_iter().collect();
    assert_eq!(got, want, "entries in byte order with the df summed");
    let err = relay
        .expand_term_prefix(pipestream_search::pb::ExpandTermPrefixRequest {
            field: "nope".into(),
            prefix: "o".into(),
            cap: 8,

            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!err.known, "a field no child indexes stays unknown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diagnostics_through_the_root_merge_the_relay_children() {
    use pipestream_search::pb::diagnostics_service_client::DiagnosticsServiceClient;
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[0], &l[1]]).await;
    let mut direct_rows = 0;
    let mut direct_segments = 0;
    for addr in l.iter().take(2) {
        let layout = DiagnosticsServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .get_shard_diagnostics(pipestream_search::pb::ShardDiagnosticsRequest { shard: None })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(layout.shards.len(), 1);
        direct_rows += layout.shards[0].rows;
        direct_segments += layout.shards[0].segments.len();
    }
    assert!(direct_rows > 0, "the leaves report their rows");
    let relayed = DiagnosticsServiceClient::connect(relay_addr.clone())
        .await
        .unwrap()
        .get_shard_diagnostics(pipestream_search::pb::ShardDiagnosticsRequest { shard: None })
        .await
        .expect("the relay serves the diagnostics service")
        .into_inner();
    assert_eq!(relayed.shards.len(), 1, "one shard: the relay's");
    let merged = &relayed.shards[0];
    assert!(
        merged.layout.starts_with("relay over 2 children"),
        "{}",
        merged.layout
    );
    assert_eq!(merged.rows, direct_rows);
    assert_eq!(merged.segments.len(), direct_segments);
    // Through the root: the relay's shard is the merged view, the leaf
    // beside it is itself.
    let root = CoordinatorServiceImpl::new(vec![relay_addr, l[2].clone()]);
    let layouts = root.shard_diagnostics(None).await;
    assert_eq!(layouts.len(), 2);
    assert_eq!(layouts[0].shard, 0);
    assert!(layouts[0].layout.starts_with("relay over 2 children"));
    assert_eq!(layouts[0].rows, direct_rows);
    assert_eq!(layouts[1].shard, 1);
    assert!(layouts[1].rows > 0 && !layouts[1].layout.contains("relay"));
}
