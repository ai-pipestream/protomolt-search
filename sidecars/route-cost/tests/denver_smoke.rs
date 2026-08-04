//! End-to-end smoke test over the self-contained downtown Denver
//! fixture (fixtures/downtown-denver/ATTRIBUTION.md): the binary
//! loads a real graph, routes a real (anchors x points) matrix, emits
//! one record per pair, reports per-pair failures as explicit error
//! records, and exits with the code the failure model promises.

use std::io::Write;
use std::process::Command;

/// Coordinates ON the fixture graph (vertex positions from
/// vertices-compass.csv.gz), plus one deliberately outside it (NREL's
/// campus in Golden, well beyond the downtown box) to exercise the
/// per-pair error path without inventing failure.
const V0: (f64, f64) = (39.7400593, -104.9848634);
const V198: (f64, f64) = (39.7598918, -104.987088);
const V398: (f64, f64) = (39.7533464, -104.995548);
const OUTSIDE: (f64, f64) = (39.798311884359094, -104.86796368632217);

fn fixture(path: &str) -> String {
    format!(
        "{}/fixtures/downtown-denver/{path}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn write_inputs(dir: &std::path::Path, points: &[(&str, (f64, f64))]) -> (String, String) {
    let anchors_path = dir.join("anchors.json");
    let mut f = std::fs::File::create(&anchors_path).unwrap();
    write!(
        f,
        r#"[{{"name":"union","lat":{},"lon":{}}},{{"name":"capitol","lat":{},"lon":{}}}]"#,
        V398.0, V398.1, V0.0, V0.1
    )
    .unwrap();
    let points_path = dir.join("points.jsonl");
    let mut f = std::fs::File::create(&points_path).unwrap();
    for (id, (lat, lon)) in points {
        writeln!(f, r#"{{"id":"{id}","lat":{lat},"lon":{lon}}}"#).unwrap();
    }
    (
        anchors_path.to_str().unwrap().to_string(),
        points_path.to_str().unwrap().to_string(),
    )
}

fn run_route_cost(anchors: &str, points: &str, out: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_route-cost"))
        .args([
            "--config",
            &fixture("travel-time.toml"),
            "--anchors",
            anchors,
            "--points",
            points,
            "--out",
            out,
        ])
        .output()
        .expect("binary runs")
}

fn read_records(out: &str) -> Vec<serde_json::Value> {
    std::fs::read_to_string(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn matrix_with_an_unroutable_point_reports_it_loudly() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("denver_mixed");
    std::fs::create_dir_all(&dir).unwrap();
    let (anchors, points) = write_inputs(
        &dir,
        &[("doc-a", V198), ("doc-b", V0), ("doc-outside", OUTSIDE)],
    );
    let out = dir.join("costs.jsonl").to_str().unwrap().to_string();
    let result = run_route_cost(&anchors, &points, &out);
    assert_eq!(
        result.status.code(),
        Some(2),
        "some pairs failed, exit must say so; stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    let records = read_records(&out);
    assert_eq!(
        records.len(),
        6,
        "3 points x 2 anchors, one record per pair"
    );
    // Every (id, anchor) pair appears exactly once; nothing is dropped
    // and nothing is doubled.
    let mut pairs: Vec<(String, String)> = records
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap().to_string(),
                r["anchor"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    pairs.sort();
    let mut expected: Vec<(String, String)> = ["doc-a", "doc-b", "doc-outside"]
        .iter()
        .flat_map(|id| {
            ["union", "capitol"]
                .iter()
                .map(|a| (id.to_string(), a.to_string()))
        })
        .collect();
    expected.sort();
    assert_eq!(pairs, expected);

    for r in &records {
        let id = r["id"].as_str().unwrap();
        if id == "doc-outside" {
            assert!(
                r.get("error").is_some(),
                "the out-of-graph point must carry an error record, not vanish: {r}"
            );
            assert!(r.get("cost").is_none());
        } else if r["id"] == "doc-b" && r["anchor"] == "capitol" {
            // doc-b sits AT the capitol anchor: the empty route's cost
            // is the empty sum, exactly zero, marked as such.
            assert_eq!(r["cost"], 0.0, "{r}");
            assert_eq!(r["empty_route"], true, "{r}");
        } else {
            let cost = r["cost"]
                .as_f64()
                .unwrap_or_else(|| panic!("finite cost: {r}"));
            assert!(
                cost.is_finite() && cost > 0.0,
                "travel minutes between distinct places are positive: {r}"
            );
            assert_eq!(
                r["unit"], "minutes",
                "the config's time_unit rides along: {r}"
            );
        }
    }
    // A real route between distinct downtown vertices takes real time.
    let a_union = records
        .iter()
        .find(|r| r["id"] == "doc-a" && r["anchor"] == "union")
        .unwrap();
    assert!(a_union["cost"].as_f64().unwrap() > 0.0);
}

#[test]
fn all_routable_matrix_exits_zero() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("denver_clean");
    std::fs::create_dir_all(&dir).unwrap();
    let (anchors, points) = write_inputs(&dir, &[("doc-a", V198), ("doc-b", V0)]);
    let out = dir.join("costs.jsonl").to_str().unwrap().to_string();
    let result = run_route_cost(&anchors, &points, &out);
    assert_eq!(
        result.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let records = read_records(&out);
    assert_eq!(records.len(), 4);
    assert!(records.iter().all(|r| r.get("error").is_none()));
}

#[test]
fn malformed_inputs_refuse_before_routing() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("denver_refusals");
    std::fs::create_dir_all(&dir).unwrap();
    let out = dir.join("costs.jsonl").to_str().unwrap().to_string();

    // A repeated anchor name would collide as a map-numeric key.
    let anchors_path = dir.join("dup_anchors.json");
    std::fs::write(
        &anchors_path,
        r#"[{"name":"union","lat":39.75,"lon":-104.99},{"name":"union","lat":39.74,"lon":-104.98}]"#,
    )
    .unwrap();
    let points_path = dir.join("points.jsonl");
    std::fs::write(
        &points_path,
        "{\"id\":\"doc-a\",\"lat\":39.75,\"lon\":-104.99}\n",
    )
    .unwrap();
    let result = run_route_cost(
        anchors_path.to_str().unwrap(),
        points_path.to_str().unwrap(),
        &out,
    );
    assert_eq!(result.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("union") && stderr.contains("repeats"),
        "{stderr}"
    );

    // An impossible latitude is a producer bug, refused by name.
    let anchors_path = dir.join("bad_lat.json");
    std::fs::write(
        &anchors_path,
        r#"[{"name":"union","lat":139.75,"lon":-104.99}]"#,
    )
    .unwrap();
    let result = run_route_cost(
        anchors_path.to_str().unwrap(),
        points_path.to_str().unwrap(),
        &out,
    );
    assert_eq!(result.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("139.75"), "{stderr}");
}
