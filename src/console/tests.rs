use super::*;

#[test]
fn url_decoding_handles_percent_and_plus() {
    assert_eq!(url_decode("a+b%20c%2Fd"), "a b c/d");
    assert_eq!(url_decode("%zz"), "%zz");
    assert_eq!(url_decode("trail%2"), "trail%2");
}

#[test]
fn query_parsing_keeps_order_and_empty_values() {
    let q = parse_query("interval_ms=250&flag&x=%7B%22a%22%3A1%7D");
    assert_eq!(q[0], ("interval_ms".into(), "250".into()));
    assert_eq!(q[1], ("flag".into(), String::new()));
    assert_eq!(q[2].1, "{\"a\":1}");
}

#[test]
fn the_descriptor_set_exposes_both_services() {
    let methods = exposed_methods();
    assert!(methods
        .iter()
        .any(|(s, m, streaming)| s == "SearchService" && m == "Query" && !streaming));
    assert!(methods
        .iter()
        .any(|(s, m, streaming)| s == "SearchService" && m == "QueryStream" && *streaming));
    assert!(methods
        .iter()
        .any(|(s, m, streaming)| s == "DiagnosticsService" && m == "StreamMetrics" && *streaming));
    assert!(!methods
        .iter()
        .any(|(s, m, _)| s == "SearchService" && m == "RoutedIngestMapped"));
    assert!(methods
        .iter()
        .any(|(s, m, streaming)| s == "SearchService" && m == "PlanPlacement" && !streaming));
    // ClusterControl: the balance dry run and only that.
    assert!(methods
        .iter()
        .any(|(s, m, streaming)| s == "ClusterControl" && m == "PlanBalance" && !streaming));
    assert_eq!(
        methods
            .iter()
            .filter(|(s, _, _)| s == "ClusterControl")
            .count(),
        1
    );
    assert!(resolve_method("ClusterControl", "PlanBalance").is_ok());
    assert!(resolve_method("ClusterControl", "GetClusterPlan").is_err());
    assert!(resolve_method("ClusterControl", "ReconcileCluster").is_err());
    assert!(resolve_method("NodeService", "Health").is_err());
    assert!(resolve_method("SearchService", "Nope").is_err());
}

#[test]
fn json_round_trips_through_the_descriptor() {
    let method = resolve_method("SearchService", "Query").unwrap();
    let json = br#"{"k": 3, "explain": true, "selection": {"search": {"id": "q", "lexical": {"text": "coffee"}}}}"#;
    let bytes = encode_request(&method, json).unwrap();
    let typed = <crate::pb::QueryRequest as prost::Message>::decode(bytes.as_ref()).unwrap();
    assert_eq!(typed.k, 3);
    assert!(typed.explain);
    let response = crate::pb::QueryResponse {
        request_id: "r".into(),
        hits: vec![crate::pb::QueryHit {
            doc_id: 7,
            score: 1.5,
            rank: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    let encoded = Bytes::from(prost::Message::encode_to_vec(&response));
    let out = decode_response(&method, encoded).unwrap();
    let value: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(value["requestId"], "r");
    assert_eq!(value["hits"][0]["docId"], "7");
    assert_eq!(value["hits"][0]["rank"], 1);
    assert!(encode_request(&method, b"{\"k\": \"three\"}").is_err());
}

#[test]
fn scalar_query_parameters_become_top_level_fields() {
    let request = HttpRequest {
        method: "GET".into(),
        path: "/api/stream/DiagnosticsService/StreamMetrics".into(),
        query: parse_query("interval_ms=250&target=node0&verbose=true"),
        body: Vec::new(),
        keep_alive: true,
        headers: Headers::default(),
    };
    let json: Value = serde_json::from_slice(&streaming_request_json(&request)).unwrap();
    assert_eq!(json["interval_ms"], 250);
    assert_eq!(json["verbose"], true);
    assert!(json.get("target").is_none());
}
