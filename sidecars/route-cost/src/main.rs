//! route-cost: the batch travel-cost enrichment sidecar
//! (`docs/plans/routing-enrichment.md`). Computes a cost matrix over
//! (anchors x points) through an embedded routee-compass application
//! and emits one JSONL record per pair, shaped for map-numeric ingest
//! (`travel_min["<anchor>"] = <cost>` via `MapNumericEntry`).
//!
//! The boundary this binary exists to hold: routing is an ENRICHMENT
//! SIDECAR. The search engine never links routee-compass; precomputed
//! anchor costs land in ordinary map-numeric columns, where range
//! facets, score chains, and (future) CEL filters work unchanged. This
//! crate is deliberately not a workspace member of turbovec-search.
//!
//! Failure model, per the loud-failures rule: malformed inputs refuse
//! up front by name, before any routing runs. PER-PAIR routing
//! failures (a point outside the graph, no path) are expected in real
//! corpora and are emitted as explicit `error` records, never silently
//! skipped; the exit code says whether any occurred (0 = all routed,
//! 2 = some pairs failed, 1 = refused before routing).

use std::io::{BufRead, Write};
use std::path::Path;

use routee_compass::app::compass::CompassApp;
use serde_json::{json, Value};

/// One fixed origin the cost matrix is computed against. Anchors are
/// the operator-designed side of the matrix (courthouses, circuit
/// seats); their names become the map-numeric KEYS at ingest.
#[derive(serde::Deserialize)]
struct Anchor {
    name: String,
    lat: f64,
    lon: f64,
}

/// One destination point, usually a document's geocoded place. The id
/// is echoed on every output record so the consumer can join costs
/// back to documents.
#[derive(serde::Deserialize)]
struct Point {
    id: String,
    lat: f64,
    lon: f64,
}

struct Args {
    config: String,
    anchors: String,
    points: String,
    out: String,
    /// RFC 6901 JSON pointer to the cost inside one result. The
    /// config decides what is computed and under which summary name,
    /// so the pointer is explicit configuration, never a guess.
    cost_pointer: String,
}

const USAGE: &str = "route-cost: batch (anchors x points) travel-cost matrix over routee-compass

usage: route-cost --config <compass.toml> --anchors <anchors.json> \\
                  --points <points.jsonl> --out <costs.jsonl> \\
                  [--cost-pointer </route/traversal_summary/trip_time/value>]

  --config        routee-compass configuration TOML (graph, traversal
                  models, [cost.weights]; parallelism is config-owned)
  --anchors       JSON array of {\"name\", \"lat\", \"lon\"}
  --points        JSONL, one {\"id\", \"lat\", \"lon\"} per line
  --out           JSONL output, one record per (point, anchor):
                  {\"id\", \"anchor\", \"cost\", \"unit\"} on success,
                  {\"id\", \"anchor\", \"error\"} on a per-pair failure
  --cost-pointer  where the cost lives in a routee-compass result
                  (default /route/traversal_summary/trip_time/value)

Queries run FROM the anchor TO the point (one-way streets make
direction real); swap your inputs to ask the other direction.
Exit codes: 0 all pairs routed, 2 some pairs failed (error records in
the output say which), 1 refused before routing.";

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut config = None;
    let mut anchors = None;
    let mut points = None;
    let mut out = None;
    let mut cost_pointer = String::from("/route/traversal_summary/trip_time/value");
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].as_str();
        let value = argv
            .get(i + 1)
            .ok_or_else(|| format!("{flag} needs a value\n\n{USAGE}"))?;
        match flag {
            "--config" => config = Some(value.clone()),
            "--anchors" => anchors = Some(value.clone()),
            "--points" => points = Some(value.clone()),
            "--out" => out = Some(value.clone()),
            "--cost-pointer" => cost_pointer = value.clone(),
            other => return Err(format!("unknown flag {other:?}\n\n{USAGE}")),
        }
        i += 2;
    }
    Ok(Args {
        config: config.ok_or_else(|| format!("--config is required\n\n{USAGE}"))?,
        anchors: anchors.ok_or_else(|| format!("--anchors is required\n\n{USAGE}"))?,
        points: points.ok_or_else(|| format!("--points is required\n\n{USAGE}"))?,
        out: out.ok_or_else(|| format!("--out is required\n\n{USAGE}"))?,
        cost_pointer,
    })
}

/// Refuse a coordinate that cannot be a place on Earth. The same
/// bounds the engine's geo columns enforce at ingest; a NaN or an
/// out-of-range value here is a producer bug, not a routing failure.
fn validate_coord(what: &str, name: &str, lat: f64, lon: f64) -> Result<(), String> {
    if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
        return Err(format!("{what} {name:?}: lat {lat} is not in [-90, 90]"));
    }
    if !lon.is_finite() || !(-180.0..=180.0).contains(&lon) {
        return Err(format!("{what} {name:?}: lon {lon} is not in [-180, 180]"));
    }
    Ok(())
}

fn load_anchors(path: &str) -> Result<Vec<Anchor>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("anchors {path}: {e}"))?;
    let anchors: Vec<Anchor> =
        serde_json::from_reader(file).map_err(|e| format!("anchors {path}: {e}"))?;
    if anchors.is_empty() {
        return Err(format!("anchors {path}: empty list; nothing to compute"));
    }
    for (i, a) in anchors.iter().enumerate() {
        if a.name.is_empty() {
            return Err(format!("anchors {path}: entry {i} has an empty name"));
        }
        if anchors[..i].iter().any(|p| p.name == a.name) {
            return Err(format!(
                "anchors {path}: name {:?} repeats; anchor names become map keys \
                 and a key holds one value",
                a.name
            ));
        }
        validate_coord("anchor", &a.name, a.lat, a.lon)?;
    }
    Ok(anchors)
}

fn load_points(path: &str) -> Result<Vec<Point>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("points {path}: {e}"))?;
    let mut points = Vec::new();
    for (ln, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|e| format!("points {path} line {}: {e}", ln + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let p: Point = serde_json::from_str(&line)
            .map_err(|e| format!("points {path} line {}: {e}", ln + 1))?;
        if p.id.is_empty() {
            return Err(format!("points {path} line {}: empty id", ln + 1));
        }
        validate_coord("point", &p.id, p.lat, p.lon)?;
        points.push(p);
    }
    if points.is_empty() {
        return Err(format!("points {path}: no points; nothing to compute"));
    }
    Ok(points)
}

fn main() {
    let code = match run() {
        Ok(pair_failures) => {
            if pair_failures == 0 {
                0
            } else {
                2
            }
        }
        Err(e) => {
            eprintln!("route-cost: {e}");
            1
        }
    };
    std::process::exit(code);
}

fn run() -> Result<u64, String> {
    let args = parse_args()?;
    let anchors = load_anchors(&args.anchors)?;
    let points = load_points(&args.points)?;

    let app = CompassApp::try_from(Path::new(&args.config))
        .map_err(|e| format!("config {}: {e}", args.config))?;

    // One query per (point, anchor), tagged with the pair's indices.
    // Parallel batches may reorder results, so the tag rides the query
    // and comes back in the result's "request" echo; positional
    // matching would be a silent wrong-join waiting to happen.
    let mut queries = Vec::with_capacity(points.len() * anchors.len());
    for (pi, p) in points.iter().enumerate() {
        for (ai, a) in anchors.iter().enumerate() {
            queries.push(json!({
                "origin_x": a.lon,
                "origin_y": a.lat,
                "destination_x": p.lon,
                "destination_y": p.lat,
                "route_cost_tag": { "point": pi, "anchor": ai },
            }));
        }
    }
    let n_queries = queries.len() as u64;
    let results = app
        .run(queries, None)
        .map_err(|e| format!("routing batch: {e}"))?;
    if results.len() as u64 != n_queries {
        return Err(format!(
            "routing batch returned {} results for {n_queries} queries; refusing to \
             emit a partial matrix",
            results.len()
        ));
    }

    let out_file =
        std::fs::File::create(&args.out).map_err(|e| format!("out {}: {e}", args.out))?;
    let mut w = std::io::BufWriter::new(out_file);
    let mut failed: u64 = 0;
    let mut seen = vec![false; points.len() * anchors.len()];
    for r in &results {
        let tag = r
            .pointer("/request/route_cost_tag")
            .ok_or_else(|| format!("a result carries no request echo with our tag: {r}"))?;
        let pi = tag["point"]
            .as_u64()
            .ok_or_else(|| format!("tag without point index: {tag}"))? as usize;
        let ai = tag["anchor"]
            .as_u64()
            .ok_or_else(|| format!("tag without anchor index: {tag}"))? as usize;
        if pi >= points.len() || ai >= anchors.len() {
            return Err(format!("tag indices out of range: {tag}"));
        }
        if seen[pi * anchors.len() + ai] {
            return Err(format!(
                "pair (point {pi}, anchor {ai}) answered twice; refusing an ambiguous matrix"
            ));
        }
        seen[pi * anchors.len() + ai] = true;
        let id = &points[pi].id;
        let anchor = &anchors[ai].name;
        let record = if let Some(err) = r.get("error") {
            failed += 1;
            json!({ "id": id, "anchor": anchor, "error": err })
        } else if r.pointer("/route/path").and_then(Value::as_array).is_some_and(Vec::is_empty) {
            // Origin and destination map-matched to the same spot: the
            // route is the empty path, whose cost is the empty sum,
            // exactly zero. Not a fabrication and not a failure; the
            // marker keeps it distinguishable downstream. (The summary
            // block is empty for an empty route, so the pointer would
            // find nothing; there is no unit to echo.)
            json!({ "id": id, "anchor": anchor, "cost": 0.0, "unit": Value::Null,
                    "empty_route": true })
        } else {
            match r.pointer(&args.cost_pointer).and_then(Value::as_f64) {
                Some(cost) if cost.is_finite() => {
                    // The unit sibling, when the summary shape has one,
                    // makes records self-describing; its absence is not
                    // an error (the pointer may target something
                    // unitless), but a missing or non-finite COST is.
                    let unit = args
                        .cost_pointer
                        .strip_suffix("/value")
                        .and_then(|base| r.pointer(&format!("{base}/unit")))
                        .cloned()
                        .unwrap_or(Value::Null);
                    json!({ "id": id, "anchor": anchor, "cost": cost, "unit": unit })
                }
                _ => {
                    // A routed, non-empty result without a finite cost
                    // at the pointer is a config/pointer mismatch, not
                    // a data condition: refuse the batch by name
                    // rather than emitting a matrix of error records
                    // for a typo.
                    return Err(format!(
                        "result for (point {id:?}, anchor {anchor:?}) has no finite \
                         number at --cost-pointer {:?}; the pointer must match what \
                         the config's summary emits. The result's traversal_summary: {}",
                        args.cost_pointer,
                        r.pointer("/route/traversal_summary").unwrap_or(&Value::Null)
                    ))
                }
            }
        };
        serde_json::to_writer(&mut w, &record).map_err(|e| format!("out {}: {e}", args.out))?;
        w.write_all(b"\n").map_err(|e| format!("out {}: {e}", args.out))?;
    }
    if let Some(missing) = seen.iter().position(|s| !s) {
        return Err(format!(
            "pair (point {}, anchor {}) never answered; refusing a partial matrix",
            missing / anchors.len(),
            missing % anchors.len()
        ));
    }
    w.flush().map_err(|e| format!("out {}: {e}", args.out))?;
    eprintln!(
        "route-cost: {} pairs routed, {} failed (error records in {})",
        n_queries - failed,
        failed,
        args.out
    );
    Ok(failed)
}
