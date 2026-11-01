//! `ann_bench gate`: merge result files from every runner and judge the
//! candidate against the best baseline in every cell (#53 pass rule).
//!
//! - Every engine is compared at its best Pareto point: the highest QPS of
//!   any build and breadth whose mean recall reaches the target.
//! - Filtered cells gate: candidate QPS >= best baseline QPS.
//! - Unfiltered cells are a bound: candidate QPS >= (1 - margin) x best.
//!   The margin is `--unfiltered-margin` (0.20 is a placeholder).
//! - The exact enumerated scan is a baseline unfiltered (it is the flat
//!   scan) and a reported row filtered; the crossover selectivity where the
//!   candidate overtakes it is T1 for #55.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;

use serde_json::{json, Value};

use crate::util::{arg, list_str, read_json, write_json, Error};

const TARGETS: [f64; 2] = [0.95, 0.99];
const MODES: [&str; 2] = ["qps_one_core", "qps_all_cores"];
const ENUMERATE: &str = "enumerate";

#[derive(Clone, Debug)]
struct Best {
    qps: f64,
    recall: f64,
    ef: u64,
    params: Value,
    p50: f64,
    p99: f64,
    predicate_ms: f64,
}

/// group (fixture | shape) -> engine -> (filter, k) -> per (target, mode) best.
type Table = BTreeMap<String, BTreeMap<String, BTreeMap<(String, u64), Vec<Option<Best>>>>>;

fn slot(target: usize, mode: usize) -> usize {
    target * MODES.len() + mode
}

pub fn command() -> Result<(), Error> {
    let inputs = list_str("results", "");
    if inputs.is_empty() {
        return Err("--results=a.json,b.json is required".into());
    }
    let candidate = arg("candidate", "leann-graph");
    let margin: f64 = arg("unfiltered-margin", "0.20").parse()?;
    let out_md = PathBuf::from(arg("out", "gate.md"));
    let out_json = out_md.with_extension("json");

    let mut table: Table = BTreeMap::new();
    let mut builds: BTreeMap<String, BTreeMap<String, Vec<Value>>> = BTreeMap::new();
    let mut engines_meta: BTreeMap<String, Value> = BTreeMap::new();
    let mut hosts = BTreeSet::new();
    let mut selectivity: BTreeMap<String, f64> = BTreeMap::new();
    for input in &inputs {
        let report = read_json(&PathBuf::from(input))?;
        let group = format!(
            "{} | {}",
            report["fixture"]["name"].as_str().unwrap_or("?"),
            report["shape"].as_str().unwrap_or("single")
        );
        hosts.insert(format!(
            "{} ({}, {}, {} threads)",
            report["host"]["name"].as_str().unwrap_or("?"),
            report["host"]["cpu"].as_str().unwrap_or("?"),
            report["host"]["arch"].as_str().unwrap_or("?"),
            report["host"]["threads"]
        ));
        for f in report["fixture"]["filters"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if let (Some(name), Some(pct)) = (f["name"].as_str(), f["actual_mean_percent"].as_f64())
            {
                selectivity.insert(name.to_string(), pct);
            }
        }
        for engine in report["engines"].as_array().into_iter().flatten() {
            let name = engine["engine"].as_str().unwrap_or("?").to_string();
            engines_meta.insert(
                name.clone(),
                json!({
                    "library": engine["library"], "version": engine["version"],
                    "license": engine["license"], "filter": engine["filter"],
                }),
            );
            let per_engine = table
                .entry(group.clone())
                .or_default()
                .entry(name.clone())
                .or_default();
            for build in engine["builds"].as_array().into_iter().flatten() {
                builds
                    .entry(group.clone())
                    .or_default()
                    .entry(name.clone())
                    .or_default()
                    .push(json!({
                        "params": build["params"], "build_s": build["build_s"],
                        "retained_bytes_per_row": build["retained_bytes_per_row"],
                        "disk_bytes_per_row": build["disk_bytes_per_row"], "error": build["error"],
                    }));
                for cell in build["cells"].as_array().into_iter().flatten() {
                    let key = (
                        cell["filter"].as_str().unwrap_or("?").to_string(),
                        cell["k"].as_u64().unwrap_or(0),
                    );
                    let bests = per_engine
                        .entry(key)
                        .or_insert_with(|| vec![None; TARGETS.len() * MODES.len()]);
                    for point in cell["points"].as_array().into_iter().flatten() {
                        let recall = point["recall_mean"].as_f64().unwrap_or(0.0);
                        for (t, target) in TARGETS.iter().enumerate() {
                            if recall + 1e-9 < *target {
                                continue;
                            }
                            for (m, mode) in MODES.iter().enumerate() {
                                let qps = point[*mode].as_f64().unwrap_or(0.0);
                                let entry = &mut bests[slot(t, m)];
                                if entry.as_ref().is_none_or(|b| qps > b.qps) {
                                    *entry = Some(Best {
                                        qps,
                                        recall,
                                        ef: point["ef"].as_u64().unwrap_or(0),
                                        params: build["params"].clone(),
                                        p50: point["p50_ms"].as_f64().unwrap_or(0.0),
                                        p99: point["p99_ms"].as_f64().unwrap_or(0.0),
                                        predicate_ms: point["predicate_ms_mean"]
                                            .as_f64()
                                            .unwrap_or(0.0),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut md = String::new();
    let mut verdicts = Vec::new();
    let mut overall_pass = true;
    let mut gated_cells = 0usize;
    writeln!(md, "# ANN gate\n")?;
    writeln!(
        md,
        "Candidate: **{candidate}**. Filtered cells gate (candidate QPS at least the best baseline's). \
         Unfiltered cells are a bound: within {:.0}% of the best baseline (`--unfiltered-margin={margin}`, \
         a placeholder until Kristian sets it). Every engine is taken at its best point that reaches the \
         recall target (mean recall over the queries, against exact FP32 truth).\n",
        margin * 100.0
    )?;
    writeln!(
        md,
        "Hosts: {}\n",
        hosts.into_iter().collect::<Vec<_>>().join("; ")
    )?;
    let calibration = arg("calibration", "");
    let calibration_ok = if calibration.is_empty() {
        writeln!(md, "Calibration: **not supplied** (`--calibration=calibration.json`); comparisons do not count until it passes.\n")?;
        false
    } else {
        let c = read_json(&PathBuf::from(&calibration))?;
        let verdict = c["verdict"].as_str().unwrap_or("?").to_string();
        writeln!(
            md,
            "Calibration (hnsw_rs, fashion-mnist, M=24 efc=400 ef=24 k=10): measured recall {:.4} against the \
             published {} (tolerance {}): **{verdict}**. Comparisons count only once this passes.\n",
            c["recall_hnsw_rs_definition"].as_f64().unwrap_or(0.0),
            c["published_recall"],
            c["tolerance"]
        )?;
        verdict == "PASS"
    };
    writeln!(md, "## Engines\n")?;
    writeln!(md, "| Engine | Library | Version | License | Filter |")?;
    writeln!(md, "| --- | --- | --- | --- | --- |")?;
    for (name, meta) in &engines_meta {
        writeln!(
            md,
            "| {name} | {} | {} | {} | {} |",
            meta["library"].as_str().unwrap_or(""),
            meta["version"].as_str().unwrap_or(""),
            meta["license"].as_str().unwrap_or(""),
            meta["filter"].as_str().unwrap_or("")
        )?;
    }

    for (group, engines) in &table {
        writeln!(md, "\n## {group}\n")?;
        let Some(cand) = engines.get(&candidate) else {
            writeln!(
                md,
                "No results for the candidate `{candidate}` in this group.\n"
            )?;
            continue;
        };
        let mut cells: Vec<(String, u64)> =
            engines.values().flat_map(|e| e.keys().cloned()).collect();
        cells.sort_by(|a, b| {
            let rank = |f: &str| if f == "none" { 0 } else { 1 };
            rank(&a.0)
                .cmp(&rank(&b.0))
                .then(a.0.cmp(&b.0))
                .then(a.1.cmp(&b.1))
        });
        cells.dedup();
        writeln!(
            md,
            "| Filter | allows | k | recall | cores | {candidate} QPS | best baseline | baseline QPS | ratio | enumerate QPS | verdict |"
        )?;
        writeln!(
            md,
            "| --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |"
        )?;
        for (filter, k) in &cells {
            let unfiltered = filter == "none";
            for (t, target) in TARGETS.iter().enumerate() {
                for (m, mode) in MODES.iter().enumerate() {
                    let c = cand
                        .get(&(filter.clone(), *k))
                        .and_then(|b| b[slot(t, m)].clone());
                    let mut best: Option<(String, Best)> = None;
                    for (name, cellmap) in engines {
                        if *name == candidate || (!unfiltered && name == ENUMERATE) {
                            continue;
                        }
                        if let Some(b) = cellmap
                            .get(&(filter.clone(), *k))
                            .and_then(|b| b[slot(t, m)].clone())
                        {
                            if best.as_ref().is_none_or(|(_, x)| b.qps > x.qps) {
                                best = Some((name.clone(), b));
                            }
                        }
                    }
                    let enumerate = engines
                        .get(ENUMERATE)
                        .and_then(|e| e.get(&(filter.clone(), *k)))
                        .and_then(|b| b[slot(t, m)].clone());
                    let (verdict, ratio) = match (&c, &best) {
                        (None, _) => ("FAIL (target not reached)".to_string(), None),
                        (Some(_), None) => {
                            ("PASS (no baseline reached the target)".to_string(), None)
                        }
                        (Some(c), Some((_, b))) => {
                            let ratio = c.qps / b.qps;
                            let pass = if unfiltered {
                                ratio >= 1.0 - margin
                            } else {
                                ratio >= 1.0
                            };
                            (if pass { "PASS" } else { "FAIL" }.to_string(), Some(ratio))
                        }
                    };
                    let pass = verdict.starts_with("PASS");
                    overall_pass &= pass;
                    gated_cells += 1;
                    let allows = if unfiltered {
                        "100%".to_string()
                    } else {
                        selectivity
                            .get(filter)
                            .map_or("?".to_string(), |p| format!("{p:.1}%"))
                    };
                    writeln!(
                        md,
                        "| {filter} | {allows} | {k} | {target} | {} | {} | {} | {} | {} | {} | {verdict} |",
                        if m == 0 { "1" } else { "all" },
                        c.as_ref().map_or("-".into(), |c| format!("{:.0}", c.qps)),
                        best.as_ref().map_or("-".into(), |(n, _)| n.clone()),
                        best.as_ref().map_or("-".into(), |(_, b)| format!("{:.0}", b.qps)),
                        ratio.map_or("-".into(), |r| format!("{r:.2}")),
                        enumerate.as_ref().map_or("-".into(), |e| format!("{:.0}", e.qps)),
                    )?;
                    verdicts.push(json!({
                        "group": group, "filter": filter, "k": k, "recall_target": target, "mode": mode,
                        "candidate": c.as_ref().map(best_json), "best_baseline": best.as_ref().map(|(n, b)| json!({"engine": n, "point": best_json(b)})),
                        "enumerate": enumerate.as_ref().map(best_json), "ratio": ratio, "verdict": verdict,
                    }));
                }
            }
        }

        // Crossover against the exact enumerated scan (T1 for #55).
        writeln!(md, "\n### Crossover with the exact enumerated scan\n")?;
        writeln!(
            md,
            "Ratio is candidate QPS / enumerate QPS at each measured selectivity (one core); above 1 the graph wins. \
             T1 is interpolated in log selectivity where the ratio crosses 1.\n"
        )?;
        writeln!(md, "| Set | k | recall | ratio by allowed % | T1 |")?;
        writeln!(md, "| --- | ---: | ---: | --- | --- |")?;
        for set in ["uncorrelated", "near", "far"] {
            for k in cells.iter().map(|c| c.1).collect::<BTreeSet<_>>() {
                for (t, target) in TARGETS.iter().enumerate() {
                    let mut series = Vec::new();
                    for pct in [1usize, 10, 50] {
                        let f = format!("{set}-{pct}");
                        let get = |e: &str| {
                            engines
                                .get(e)
                                .and_then(|m| m.get(&(f.clone(), k)))
                                .and_then(|b| b[slot(t, 0)].clone())
                        };
                        if let (Some(c), Some(e)) = (get(&candidate), get(ENUMERATE)) {
                            let actual = selectivity.get(&f).copied().unwrap_or(pct as f64);
                            series.push((actual, c.qps / e.qps));
                        }
                    }
                    if series.is_empty() {
                        continue;
                    }
                    let t1 = crossover(&series);
                    writeln!(
                        md,
                        "| {set} | {k} | {target} | {} | {t1} |",
                        series
                            .iter()
                            .map(|(p, r)| format!("{p:.1}%: {r:.2}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
            }
        }

        writeln!(
            md,
            "\n### Build cost (best unfiltered k=10 recall 0.95 one-core point)\n"
        )?;
        writeln!(
            md,
            "| Engine | params | ef | build s | retained bytes/row | disk bytes/row |"
        )?;
        writeln!(md, "| --- | --- | ---: | ---: | ---: | ---: |")?;
        for (name, cellmap) in engines {
            let Some(b) = cellmap
                .get(&("none".to_string(), 10))
                .and_then(|b| b[slot(0, 0)].clone())
            else {
                continue;
            };
            let build = builds
                .get(group)
                .and_then(|m| m.get(name))
                .and_then(|v| v.iter().find(|x| x["params"] == b.params));
            let num = |v: Option<&Value>| {
                v.and_then(Value::as_f64)
                    .map_or("-".to_string(), |x| format!("{x:.1}"))
            };
            writeln!(
                md,
                "| {name} | {} | {} | {} | {} | {} |",
                b.params,
                b.ef,
                num(build.map(|x| &x["build_s"])),
                num(build.map(|x| &x["retained_bytes_per_row"])),
                num(build.map(|x| &x["disk_bytes_per_row"])),
            )?;
        }
    }

    writeln!(md, "\n## Not yet measurable\n")?;
    writeln!(
        md,
        "These #53 cells need provider features that land with #54/#55; the harness reserves them and reports \
         them here until they run:\n\n\
         - Collaborative floor (K = 100 and 1,000, floor on/off, segmented and two shards) and the Lucene \
           collaborative-floor row (apache/lucene#16958 sandbox build).\n\
         - Collapse cells (`collapse_parents`, D = 10 and 100, parent table on/off) on the court fixture.\n\
         - Seeded-entry cells (own top / local skeleton descent / coordinator-seeded entry).\n\
         - Tombstone cells (10% and 30% deleted rows).\n"
    )?;
    writeln!(
        md,
        "\n**Overall: {}** ({gated_cells} judged cells{}).",
        if overall_pass { "PASS" } else { "FAIL" },
        if calibration_ok {
            ""
        } else {
            "; calibration has not passed, so this verdict does not count yet"
        }
    )?;

    std::fs::write(&out_md, &md)?;
    write_json(
        &out_json,
        &json!({
            "format": "protomolt-ann-gate-v1",
            "candidate": candidate,
            "unfiltered_margin": margin,
            "inputs": inputs,
            "overall": if overall_pass { "PASS" } else { "FAIL" },
            "calibration_passed": calibration_ok,
            "verdicts": verdicts,
        }),
    )?;
    eprintln!(
        "gate: wrote {} and {}",
        out_md.display(),
        out_json.display()
    );
    Ok(())
}

fn best_json(b: &Best) -> Value {
    json!({
        "qps": b.qps, "recall": b.recall, "ef": b.ef, "params": b.params,
        "p50_ms": b.p50, "p99_ms": b.p99, "predicate_ms": b.predicate_ms,
    })
}

/// Selectivity (percent) where the ratio crosses 1, interpolated in log
/// selectivity between measured points.
fn crossover(series: &[(f64, f64)]) -> String {
    let mut s = series.to_vec();
    s.sort_by(|a, b| a.0.total_cmp(&b.0));
    if s.iter().all(|(_, r)| *r >= 1.0) {
        return format!(
            "graph wins at every measured selectivity (>= {:.1}%)",
            s[0].0
        );
    }
    if s.iter().all(|(_, r)| *r < 1.0) {
        return format!(
            "enumerate wins at every measured selectivity (<= {:.1}%)",
            s[s.len() - 1].0
        );
    }
    for w in s.windows(2) {
        let ((p0, r0), (p1, r1)) = (w[0], w[1]);
        if (r0 < 1.0) != (r1 < 1.0) {
            let (l0, l1) = (p0.ln(), p1.ln());
            let f = (1.0 - r0) / (r1 - r0);
            return format!("{:.1}%", (l0 + f * (l1 - l0)).exp());
        }
    }
    "not monotone".to_string()
}
