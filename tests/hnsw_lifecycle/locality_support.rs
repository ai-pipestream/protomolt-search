//! Query evaluation and snapshot comparison helpers for HNSW locality reordering.

use super::*;
use pipestream_search::pb::{
    filter_query, CollapseSpec, CompositeSearchStrategy, FilterQuery, SearchRequest,
    SelectionOperator,
};

#[derive(Clone)]
pub(crate) struct RichRow {
    pub(crate) text: String,
    pub(crate) court: &'static str,
    pub(crate) year: i64,
    pub(crate) pages: f64,
    pub(crate) parent_id: u64,
    pub(crate) vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreCompactSnapshot {
    pub(crate) unfiltered: Vec<Vec<(String, u32)>>,
    pub(crate) facet: Vec<Vec<(String, u32)>>,
    pub(crate) number_range: Vec<Vec<(String, u32)>>,
    pub(crate) col_collapse: Vec<Vec<(String, u32, usize)>>,
    pub(crate) parent_collapse: Vec<Vec<(u64, String, u32)>>,
}

async fn fetch_texts(cli: &mut NodeServiceClient<Channel>, ids: Vec<u64>) -> BTreeMap<u64, String> {
    cli.get_documents(GetDocumentsRequest { doc_ids: ids })
        .await
        .unwrap()
        .into_inner()
        .documents
        .into_iter()
        .map(|d| (d.doc_id, d.text))
        .collect()
}

fn cel_filtered_ann(qv: &[f32], cel: &str) -> QueryRequest {
    QueryRequest {
        k: K as u32,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![
                    speed_dense_leaf(qv),
                    SelectionQuery {
                        node: Some(selection_query::Node::Filter(FilterQuery {
                            id: "flt".into(),
                            predicate: Some(filter_query::Predicate::Cel(cel.into())),
                        })),
                    },
                ],
                scoring: None,
            })),
        }),
        ..Default::default()
    }
}

async fn eval_unfiltered(
    coord: &CoordinatorServiceImpl,
    cli: &mut NodeServiceClient<Channel>,
    by_text: &BTreeMap<&str, &RichRow>,
    qv: &[f32],
    stage: &str,
) -> Vec<(String, u32)> {
    let req = QueryRequest {
        k: K as u32,
        selection: Some(speed_dense_leaf(qv)),
        ..Default::default()
    };
    let resp = coord.query(Request::new(req)).await.unwrap().into_inner();
    let tmap = fetch_texts(cli, resp.hits.iter().map(|h| h.doc_id).collect()).await;
    resp.hits
        .iter()
        .map(|h| {
            let txt = &tmap[&h.doc_id];
            let row = by_text
                .get(txt.as_str())
                .unwrap_or_else(|| panic!("{stage}: stale/deleted row {txt}"));
            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
            (txt.clone(), h.score.to_bits())
        })
        .collect()
}

async fn eval_filtered_pair(
    coord: &CoordinatorServiceImpl,
    cli: &mut NodeServiceClient<Channel>,
    by_text: &BTreeMap<&str, &RichRow>,
    qv: &[f32],
    stage: &str,
) -> (Vec<(String, u32)>, Vec<(String, u32)>) {
    let fac_resp = coord
        .query(Request::new(cel_filtered_ann(qv, "court == 'ca9'")))
        .await
        .unwrap()
        .into_inner();
    let tmap = fetch_texts(cli, fac_resp.hits.iter().map(|h| h.doc_id).collect()).await;
    let fac_vec = fac_resp
        .hits
        .iter()
        .map(|h| {
            let txt = &tmap[&h.doc_id];
            let row = by_text[txt.as_str()];
            assert_eq!(row.court, "ca9", "{stage}: wrong court on {txt}");
            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
            (txt.clone(), h.score.to_bits())
        })
        .collect();

    let range_cel = "year >= 2005 && year <= 2018 && pages >= 20.0 && pages <= 80.0";
    let num_resp = coord
        .query(Request::new(cel_filtered_ann(qv, range_cel)))
        .await
        .unwrap()
        .into_inner();
    let tmap = fetch_texts(cli, num_resp.hits.iter().map(|h| h.doc_id).collect()).await;
    let num_vec = num_resp
        .hits
        .iter()
        .map(|h| {
            let txt = &tmap[&h.doc_id];
            let row = by_text[txt.as_str()];
            assert!(
                (2005..=2018).contains(&row.year) && (20.0..=80.0).contains(&row.pages),
                "{stage}: out-of-range row {txt}"
            );
            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
            (txt.clone(), h.score.to_bits())
        })
        .collect();
    (fac_vec, num_vec)
}

async fn eval_col_collapse(
    coord: &CoordinatorServiceImpl,
    cli: &mut NodeServiceClient<Channel>,
    by_text: &BTreeMap<&str, &RichRow>,
    qv: &[f32],
    stage: &str,
) -> Vec<(String, u32, usize)> {
    let col_req = QueryRequest {
        k: 4,
        selection_k: 40,
        selection: Some(speed_dense_leaf(qv)),
        collapse: Some(CollapseSpec {
            column: "court".into(),
            inner_hits: 2,
            map: None,
        }),
        ..Default::default()
    };
    let col_resp = coord
        .query(Request::new(col_req))
        .await
        .unwrap()
        .into_inner();
    let tmap = fetch_texts(cli, col_resp.hits.iter().map(|h| h.doc_id).collect()).await;
    assert_eq!(col_resp.groups.len(), col_resp.hits.len());
    let mut seen_courts = std::collections::BTreeSet::new();
    let mut col_vec = Vec::new();
    for (h, g) in col_resp.hits.iter().zip(&col_resp.groups) {
        let txt = &tmap[&h.doc_id];
        let row = by_text[txt.as_str()];
        assert!(
            seen_courts.insert(row.court),
            "{stage}: duplicate collapsed court {}",
            row.court
        );
        assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
        col_vec.push((txt.clone(), h.score.to_bits(), g.hits.len()));
    }
    col_vec
}

async fn eval_parent_collapse(
    coord: &CoordinatorServiceImpl,
    cli: &mut NodeServiceClient<Channel>,
    by_text: &BTreeMap<&str, &RichRow>,
    qv: &[f32],
    stage: &str,
) -> Vec<(u64, String, u32)> {
    let par_req = SearchRequest {
        request_id: format!("{stage}-pcollapse"),
        k: K as u32,
        vector: qv.to_vec(),
        collapse_parents: true,
        ..Default::default()
    };
    let par_resp = coord
        .search(Request::new(par_req))
        .await
        .unwrap()
        .into_inner();
    let tmap = fetch_texts(cli, par_resp.hits.iter().map(|h| h.vector_id).collect()).await;
    let mut seen_parents = std::collections::BTreeSet::new();
    let mut par_vec = Vec::new();
    for h in &par_resp.hits {
        let txt = &tmap[&h.vector_id];
        let row = by_text[txt.as_str()];
        assert_eq!(
            h.parent_id, row.parent_id,
            "{stage}: lineage parent_id mismatch on {txt}"
        );
        assert!(
            seen_parents.insert(h.parent_id),
            "{stage}: duplicate parent_id {}",
            h.parent_id
        );
        assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
        par_vec.push((h.parent_id, txt.clone(), h.score.to_bits()));
    }
    par_vec
}

pub(crate) async fn eval_locality_queries(
    addr: &str,
    live_map: &BTreeMap<u64, RichRow>,
    probes: &[Vec<f32>],
    stage: &str,
) -> PreCompactSnapshot {
    let coord = coordinator(addr);
    let mut cli = client(addr).await;
    let by_text: BTreeMap<&str, &RichRow> =
        live_map.values().map(|r| (r.text.as_str(), r)).collect();
    let mut snap = PreCompactSnapshot {
        unfiltered: Vec::new(),
        facet: Vec::new(),
        number_range: Vec::new(),
        col_collapse: Vec::new(),
        parent_collapse: Vec::new(),
    };
    for qv in probes {
        snap.unfiltered
            .push(eval_unfiltered(&coord, &mut cli, &by_text, qv, stage).await);
        let (fac, num) = eval_filtered_pair(&coord, &mut cli, &by_text, qv, stage).await;
        snap.facet.push(fac);
        snap.number_range.push(num);
        snap.col_collapse
            .push(eval_col_collapse(&coord, &mut cli, &by_text, qv, stage).await);
        snap.parent_collapse
            .push(eval_parent_collapse(&coord, &mut cli, &by_text, qv, stage).await);
    }
    snap
}
