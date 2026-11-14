//! `ann_bench gate`: merge result files from every runner and judge the
//! candidate against the best required baseline in every expected cell
//! (#53 pass rule, #74 evidence rules).
//!
//! The expected cells come from a checked-in matrix file (`gates/*.json`):
//! fixture, shape, filter, k, recall target, cores, and the baselines each
//! cell requires. A block with `"shape": "sharded"` also lists shard
//! counts, placements and floor settings, and names one group per
//! combination (`sharded:<shards>:<placement>:floor-<off|on>`), the shape a
//! sharded result file records. A gate verdict is evidence only when it is
//! complete:
//!
//! - Every engine is compared at its best Pareto point: the highest QPS of
//!   any build and breadth whose mean recall reaches the target.
//! - Filtered cells gate: candidate QPS >= best required baseline QPS.
//! - Unfiltered cells are a bound: candidate QPS >= (1 - margin) x best. The
//!   margin is the matrix's `unfiltered_margin`; while the matrix marks it
//!   provisional the best overall result is `PROVISIONAL PASS`.
//! - A cell whose candidate or a required baseline was not measured is
//!   `INCOMPLETE`, never skipped. A cell where no required baseline reaches
//!   the target is `NO-BASELINE`, which is incomplete unless the matrix marks
//!   it expected.
//! - Result files that disagree on the host or on a fixture's digest are
//!   refused. Missing provenance (kernel, commit, run time, digest) and a
//!   missing or failed calibration make the overall result `INCOMPLETE`.
//! - The exact enumerated scan is a reported column in filtered cells; the
//!   crossover selectivity where the candidate overtakes it is T1 for #55.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;

use serde_json::{json, Value};

use crate::util::{arg, list_str, read_json, write_json, Error};

const TARGETS: [f64; 2] = [0.95, 0.99];
const MODES: [&str; 2] = ["qps_one_core", "qps_all_cores"];
const CORES: [&str; 2] = ["1", "all"];
const ENUMERATE: &str = "enumerate";
const MATRIX_FORMAT: &str = "protomolt-ann-gate-matrix-v1";

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

/// engine -> (filter, k) -> per (target, mode) best. A cell is present only
/// when the engine measured at least one point for it.
type Engines = BTreeMap<String, BTreeMap<(String, u64), Vec<Option<Best>>>>;
/// group (fixture | shape) -> engines.
type Table = BTreeMap<String, Engines>;

fn slot(target: usize, mode: usize) -> usize {
    target * MODES.len() + mode
}

/// One expected cell of the matrix.
#[derive(Clone, Debug)]
struct Expected {
    group: String,
    filter: String,
    k: u64,
    target: usize,
    mode: usize,
    baselines: Vec<String>,
    no_baseline_expected: bool,
}

struct Matrix {
    name: String,
    candidate: String,
    margin: f64,
    margin_provisional: bool,
    cells: Vec<Expected>,
}

fn strings(value: &Value, what: &str) -> Result<Vec<String>, Error> {
    let list = value
        .as_array()
        .ok_or_else(|| format!("matrix: {what} must be a list"))?;
    if list.is_empty() {
        return Err(format!("matrix: {what} is empty").into());
    }
    list.iter()
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("matrix: {what} must hold strings").into())
        })
        .collect()
}

fn parse_matrix(matrix: &Value) -> Result<Matrix, Error> {
    if matrix["format"] != MATRIX_FORMAT {
        return Err(format!("matrix: format must be {MATRIX_FORMAT:?}").into());
    }
    let text = |key: &str| {
        matrix[key]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("matrix: {key} is required"))
    };
    let margin = matrix["unfiltered_margin"]
        .as_f64()
        .filter(|m| (0.0..1.0).contains(m))
        .ok_or("matrix: unfiltered_margin must be a number in [0, 1)")?;
    let margin_provisional = matrix["unfiltered_margin_provisional"]
        .as_bool()
        .ok_or("matrix: unfiltered_margin_provisional must be true or false")?;
    let mut cells = Vec::new();
    let mut seen = BTreeSet::new();
    let blocks = matrix["cells"]
        .as_array()
        .filter(|b| !b.is_empty())
        .ok_or("matrix: cells must be a non-empty list")?;
    for block in blocks {
        let fixture = block["fixture"]
            .as_str()
            .ok_or("matrix: a cell block needs a fixture")?;
        let shape = block["shape"]
            .as_str()
            .ok_or("matrix: a cell block needs a shape")?;
        for group in block_groups(fixture, shape, block)? {
            parse_block(block, group, &mut cells, &mut seen)?;
        }
    }
    let candidate = text("candidate")?;
    if cells.iter().any(|c| c.baselines.contains(&candidate)) {
        return Err(format!("matrix: the candidate {candidate:?} is listed as a baseline").into());
    }
    Ok(Matrix {
        name: text("name")?,
        candidate,
        margin,
        margin_provisional,
        cells,
    })
}

/// The shape name a sharded result file records, and the group a sharded
/// matrix block expands to: `sharded:<shards>:<placement>:floor-<off|on>`.
pub fn sharded_shape_name(shards: usize, placement: &str, floor: bool) -> String {
    format!(
        "sharded:{shards}:{placement}:floor-{}",
        if floor { "on" } else { "off" }
    )
}

const SHARD_KEYS: [&str; 3] = ["shards", "placements", "floor"];
const PLACEMENTS: [&str; 2] = ["hash", "skewed"];

/// The groups (fixture | shape) a block names. A `"shape": "sharded"` block
/// names every combination of its `shards`, `placements` and `floor` lists;
/// any other shape names itself and must not carry those keys.
fn block_groups(fixture: &str, shape: &str, block: &Value) -> Result<Vec<String>, Error> {
    if shape != "sharded" {
        if shape.starts_with("sharded") {
            return Err(format!(
                "matrix: shape {shape:?}: name sharded cells with \"shape\": \"sharded\" and its shards, placements and floor lists"
            )
            .into());
        }
        if let Some(key) = SHARD_KEYS.iter().find(|k| !block[**k].is_null()) {
            return Err(
                format!("matrix: {key} belongs to a sharded block, not shape {shape:?}").into(),
            );
        }
        return Ok(vec![format!("{fixture} | {shape}")]);
    }
    let shards = block["shards"]
        .as_array()
        .filter(|s| !s.is_empty())
        .ok_or("matrix: a sharded block needs a non-empty shards list")?
        .iter()
        .map(|s| {
            s.as_u64()
                .filter(|n| *n >= 2)
                .map(|n| n as usize)
                .ok_or_else(|| Error::from("matrix: shards must hold integers of at least 2"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let placements = strings(&block["placements"], "placements")?;
    if let Some(bad) = placements
        .iter()
        .find(|p| !PLACEMENTS.contains(&p.as_str()))
    {
        return Err(format!("matrix: placement {bad:?} must be one of {PLACEMENTS:?}").into());
    }
    let floors = strings(&block["floor"], "floor")?
        .iter()
        .map(|f| match f.as_str() {
            "off" => Ok(false),
            "on" => Ok(true),
            other => Err(Error::from(format!(
                "matrix: floor {other:?} must be \"off\" or \"on\""
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut groups = Vec::new();
    for &n in &shards {
        for placement in &placements {
            for &floor in &floors {
                groups.push(format!(
                    "{fixture} | {}",
                    sharded_shape_name(n, placement, floor)
                ));
            }
        }
    }
    Ok(groups)
}

/// The expected cells of one block for one group.
fn parse_block(
    block: &Value,
    group: String,
    cells: &mut Vec<Expected>,
    seen: &mut BTreeSet<(String, String, u64, usize, usize)>,
) -> Result<(), Error> {
    let baselines = strings(&block["baselines"], "baselines")?;
    let no_baseline_expected = match &block["no_baseline_expected"] {
        Value::Null => false,
        Value::Bool(b) => *b,
        _ => return Err("matrix: no_baseline_expected must be true or false".into()),
    };
    let ks = block["k"]
        .as_array()
        .filter(|k| !k.is_empty())
        .ok_or("matrix: k must be a non-empty list")?;
    let recalls = block["recall"]
        .as_array()
        .filter(|r| !r.is_empty())
        .ok_or("matrix: recall must be a non-empty list")?;
    for filter in strings(&block["filters"], "filters")? {
        for k in ks {
            let k = k.as_u64().ok_or("matrix: k must hold integers")?;
            for recall in recalls {
                let recall = recall.as_f64().ok_or("matrix: recall must hold numbers")?;
                let target = TARGETS
                    .iter()
                    .position(|t| (t - recall).abs() < 1e-9)
                    .ok_or_else(|| {
                        format!("matrix: recall {recall} is not a measured target {TARGETS:?}")
                    })?;
                for cores in strings(&block["cores"], "cores")? {
                    let mode = CORES.iter().position(|c| *c == cores).ok_or_else(|| {
                        format!("matrix: cores {cores:?} must be one of {CORES:?}")
                    })?;
                    if !seen.insert((group.clone(), filter.clone(), k, target, mode)) {
                        return Err(format!(
                                "matrix: cell {group} / {filter} / k={k} / recall {recall} / cores {cores} is listed twice"
                            )
                            .into());
                    }
                    cells.push(Expected {
                        group: group.clone(),
                        filter: filter.clone(),
                        k,
                        target,
                        mode,
                        baselines: baselines.clone(),
                        no_baseline_expected,
                    });
                }
            }
        }
    }
    Ok(())
}

fn normal_arch(arch: &str) -> &str {
    match arch {
        "amd64" => "x86_64",
        "arm64" => "aarch64",
        other => other,
    }
}

/// What a result file says about where and when it ran.
struct Provenance {
    file: String,
    runner: String,
    host: String,
    cpu: String,
    arch: String,
    threads: String,
    kernel: String,
    commit: String,
    started_at: String,
    fixture: String,
    digest: String,
}

fn provenance(file: &str, report: &Value) -> Provenance {
    let text = |v: &Value| match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => String::new(),
    };
    Provenance {
        file: file.to_string(),
        runner: report["harness"]["runner"]
            .as_str()
            .unwrap_or("ann_bench run")
            .to_string(),
        host: text(&report["host"]["name"]),
        cpu: text(&report["host"]["cpu"]),
        arch: normal_arch(&text(&report["host"]["arch"])).to_string(),
        threads: text(&report["host"]["threads"]),
        kernel: text(&report["host"]["kernel"]),
        commit: text(&report["harness"]["commit"]),
        started_at: text(&report["harness"]["started_at"]),
        fixture: text(&report["fixture"]["name"]),
        digest: text(&report["fixture"]["base_sha256"]),
    }
}

/// Refuse result files that cannot be compared (another host, another
/// fixture under the same name) and list what each file fails to record.
fn check_provenance(files: &[Provenance]) -> Result<Vec<String>, Error> {
    let mut missing = Vec::new();
    for p in files {
        let absent: Vec<&str> = [
            ("host name", &p.host),
            ("cpu", &p.cpu),
            ("arch", &p.arch),
            ("threads", &p.threads),
            ("kernel", &p.kernel),
            ("commit", &p.commit),
            ("run time", &p.started_at),
            ("fixture digest", &p.digest),
        ]
        .into_iter()
        .filter(|(_, v)| v.is_empty())
        .map(|(name, _)| name)
        .collect();
        if !absent.is_empty() {
            missing.push(format!("{} does not record: {}", p.file, absent.join(", ")));
        }
    }
    let differs = |a: &str, b: &str| !a.is_empty() && !b.is_empty() && a != b;
    for (i, a) in files.iter().enumerate() {
        for b in &files[i + 1..] {
            for (what, x, y) in [
                ("host name", &a.host, &b.host),
                ("cpu", &a.cpu, &b.cpu),
                ("arch", &a.arch, &b.arch),
                ("threads", &a.threads, &b.threads),
                ("kernel", &a.kernel, &b.kernel),
            ] {
                if differs(x, y) {
                    return Err(format!(
                        "refusing to combine results from different hosts: {} has {what} {x:?}, {} has {y:?}",
                        a.file, b.file
                    )
                    .into());
                }
            }
            if a.fixture == b.fixture && differs(&a.digest, &b.digest) {
                return Err(format!(
                    "refusing to combine results from different fixtures named {:?}: {} has base_sha256 {}, {} has {}",
                    a.fixture, a.file, a.digest, b.file, b.digest
                )
                .into());
            }
        }
    }
    Ok(missing)
}

/// The verdict of one expected cell.
#[derive(Clone, Debug, PartialEq)]
enum Verdict {
    Pass,
    Fail(String),
    Incomplete(String),
    NoBaseline { expected: bool },
}

impl Verdict {
    fn label(&self) -> String {
        match self {
            Verdict::Pass => "PASS".into(),
            Verdict::Fail(why) if why.is_empty() => "FAIL".into(),
            Verdict::Fail(why) => format!("FAIL ({why})"),
            Verdict::Incomplete(why) => format!("INCOMPLETE ({why})"),
            Verdict::NoBaseline { expected: true } => "NO-BASELINE (expected)".into(),
            Verdict::NoBaseline { expected: false } => "NO-BASELINE".into(),
        }
    }
    fn failed(&self) -> bool {
        matches!(self, Verdict::Fail(_))
    }
    fn incomplete(&self) -> bool {
        matches!(
            self,
            Verdict::Incomplete(_) | Verdict::NoBaseline { expected: false }
        )
    }
}

struct Judged {
    verdict: Verdict,
    candidate: Option<Best>,
    best: Option<(String, Best)>,
    ratio: Option<f64>,
}

fn judge_cell(cell: &Expected, candidate: &str, margin: f64, engines: Option<&Engines>) -> Judged {
    let key = (cell.filter.clone(), cell.k);
    let at = slot(cell.target, cell.mode);
    let measured = |engine: &str| {
        engines
            .and_then(|e| e.get(engine))
            .and_then(|m| m.get(&key))
    };
    let mut judged = Judged {
        verdict: Verdict::Pass,
        candidate: None,
        best: None,
        ratio: None,
    };
    let Some(cand) = measured(candidate) else {
        judged.verdict = Verdict::Incomplete("candidate not measured".into());
        return judged;
    };
    let mut missing = Vec::new();
    for name in &cell.baselines {
        match measured(name) {
            None => missing.push(name.as_str()),
            Some(bests) => {
                if let Some(b) = &bests[at] {
                    if judged.best.as_ref().is_none_or(|(_, x)| b.qps > x.qps) {
                        judged.best = Some((name.clone(), b.clone()));
                    }
                }
            }
        }
    }
    let Some(c) = cand[at].clone() else {
        judged.verdict = Verdict::Fail("target not reached".into());
        return judged;
    };
    judged.candidate = Some(c.clone());
    // A missing baseline can only raise the bar, so a ratio that already
    // fails is a failure; one that passes is not yet a pass.
    if let Some((_, b)) = &judged.best {
        let ratio = c.qps / b.qps;
        judged.ratio = Some(ratio);
        let bar = if cell.filter == "none" {
            1.0 - margin
        } else {
            1.0
        };
        if ratio < bar {
            judged.verdict = Verdict::Fail(String::new());
            return judged;
        }
    }
    judged.verdict = if !missing.is_empty() {
        Verdict::Incomplete(format!("not measured: {}", missing.join(", ")))
    } else if judged.best.is_none() {
        Verdict::NoBaseline {
            expected: cell.no_baseline_expected,
        }
    } else {
        Verdict::Pass
    };
    judged
}

/// Build records per group and engine.
type Builds = BTreeMap<String, BTreeMap<String, Vec<Value>>>;
/// Everything the result files hold: best points, build records, engine
/// descriptions, and each filter's measured selectivity per group.
type Loaded = (
    Table,
    Builds,
    BTreeMap<String, Value>,
    BTreeMap<(String, String), f64>,
);

fn load(reports: &[(String, Value)]) -> Loaded {
    let mut table: Table = BTreeMap::new();
    let mut builds: Builds = BTreeMap::new();
    let mut engines_meta: BTreeMap<String, Value> = BTreeMap::new();
    let mut selectivity: BTreeMap<(String, String), f64> = BTreeMap::new();
    for (_, report) in reports {
        let group = format!(
            "{} | {}",
            report["fixture"]["name"].as_str().unwrap_or("?"),
            report["shape"].as_str().unwrap_or("single")
        );
        for f in report["fixture"]["filters"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if let (Some(name), Some(pct)) = (f["name"].as_str(), f["actual_mean_percent"].as_f64())
            {
                selectivity.insert((group.clone(), name.to_string()), pct);
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
                    let points = cell["points"].as_array().map_or(&[][..], Vec::as_slice);
                    if points.is_empty() {
                        continue;
                    }
                    let key = (
                        cell["filter"].as_str().unwrap_or("?").to_string(),
                        cell["k"].as_u64().unwrap_or(0),
                    );
                    let bests = per_engine
                        .entry(key)
                        .or_insert_with(|| vec![None; TARGETS.len() * MODES.len()]);
                    for point in points {
                        if point["below_qps_floor"].as_bool().unwrap_or(false) {
                            continue;
                        }
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
    (table, builds, engines_meta, selectivity)
}

/// The gate's tables and its machine-readable verdict.
struct Outcome {
    overall: &'static str,
    md: String,
    json: Value,
}

pub fn command() -> Result<(), Error> {
    let inputs = list_str("results", "");
    if inputs.is_empty() {
        return Err("--results=a.json,b.json is required".into());
    }
    let matrix_path = arg("matrix", "");
    if matrix_path.is_empty() {
        return Err("--matrix=gates/<gate>.json is required: the gate judges only an explicit matrix of expected cells".into());
    }
    let matrix = read_json(&PathBuf::from(&matrix_path))?;
    let out_md = PathBuf::from(arg("out", "gate.md"));
    let out_json = out_md.with_extension("json");
    let mut reports = Vec::new();
    for input in &inputs {
        reports.push((input.clone(), read_json(&PathBuf::from(input))?));
    }
    let calibration = arg("calibration", "");
    let calibration = if calibration.is_empty() {
        None
    } else {
        Some(read_json(&PathBuf::from(&calibration))?)
    };
    let outcome = judge(&matrix, &reports, calibration.as_ref())?;
    std::fs::write(&out_md, &outcome.md)?;
    write_json(&out_json, &outcome.json)?;
    eprintln!(
        "gate: {}; wrote {} and {}",
        outcome.overall,
        out_md.display(),
        out_json.display()
    );
    Ok(())
}

fn judge(
    matrix: &Value,
    reports: &[(String, Value)],
    calibration: Option<&Value>,
) -> Result<Outcome, Error> {
    let matrix = parse_matrix(matrix)?;
    let candidate = matrix.candidate.clone();
    let margin = matrix.margin;
    let files: Vec<Provenance> = reports.iter().map(|(f, r)| provenance(f, r)).collect();
    let mut reasons = check_provenance(&files)?;
    let (table, builds, engines_meta, selectivity) = load(reports);

    let mut md = String::new();
    writeln!(md, "# ANN gate: {}\n", matrix.name)?;
    writeln!(
        md,
        "Candidate: **{candidate}**. Filtered cells gate (candidate QPS at least the best required baseline's). \
         Unfiltered cells are a bound: within {:.0}% of the best required baseline{}. Every engine is taken at \
         its best point that reaches the recall target (mean recall over the queries, against exact FP32 truth). \
         Only the matrix's expected cells are judged, and each must be measured for the candidate and for \
         every baseline the matrix requires.\n",
        margin * 100.0,
        if matrix.margin_provisional {
            " (**PROVISIONAL**: the margin is not decided, #59)"
        } else {
            ""
        }
    )?;

    writeln!(md, "## Provenance\n")?;
    writeln!(
        md,
        "| File | Runner | Host | CPU | Arch | Threads | Kernel | Commit | Started (unix s) | Fixture | base_sha256 |"
    )?;
    writeln!(
        md,
        "| --- | --- | --- | --- | --- | ---: | --- | --- | ---: | --- | --- |"
    )?;
    let dash = |s: &str| {
        if s.is_empty() {
            "**missing**".to_string()
        } else {
            s.to_string()
        }
    };
    for p in &files {
        writeln!(
            md,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            p.file,
            p.runner,
            dash(&p.host),
            dash(&p.cpu),
            dash(&p.arch),
            dash(&p.threads),
            dash(&p.kernel),
            dash(p.commit.get(..12).unwrap_or(&p.commit)),
            dash(&p.started_at),
            dash(&p.fixture),
            dash(p.digest.get(..12).unwrap_or(&p.digest)),
        )?;
    }
    let commits: BTreeSet<&str> = files
        .iter()
        .map(|p| p.commit.as_str())
        .filter(|c| !c.is_empty())
        .collect();
    if commits.len() > 1 {
        writeln!(
            md,
            "\nThe result files come from {} different commits; baselines measured at an earlier commit are \
             compared as recorded.",
            commits.len()
        )?;
    }

    let calibration_ok = match calibration {
        None => {
            writeln!(
                md,
                "\nCalibration: **not supplied** (`--calibration=calibration.json`).\n"
            )?;
            reasons.push("calibration was not supplied".into());
            false
        }
        Some(c) => {
            let verdict = c["verdict"].as_str().unwrap_or("?").to_string();
            writeln!(
                md,
                "\nCalibration (hnsw_rs, fashion-mnist, M=24 efc=400 ef=24 k=10): measured recall {:.4} against the \
                 published {} (tolerance {}): **{verdict}**.\n",
                c["recall_hnsw_rs_definition"].as_f64().unwrap_or(0.0),
                c["published_recall"],
                c["tolerance"]
            )?;
            let ok = verdict == "PASS";
            if !ok {
                reasons.push(format!("calibration has not passed ({verdict})"));
            }
            ok
        }
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

    let mut verdicts = Vec::new();
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let groups: Vec<String> = {
        let mut seen = BTreeSet::new();
        matrix
            .cells
            .iter()
            .filter(|c| seen.insert(c.group.clone()))
            .map(|c| c.group.clone())
            .collect()
    };
    for group in &groups {
        writeln!(md, "\n## {group}\n")?;
        let engines = table.get(group);
        if engines.is_none() {
            writeln!(md, "No result file covers this fixture and shape.\n")?;
        }
        writeln!(
            md,
            "| Filter | allows | k | recall | cores | {candidate} QPS | best baseline | baseline QPS | ratio | enumerate QPS | verdict |"
        )?;
        writeln!(
            md,
            "| --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |"
        )?;
        for cell in matrix.cells.iter().filter(|c| &c.group == group) {
            let judged = judge_cell(cell, &candidate, margin, engines);
            let enumerate = engines
                .and_then(|e| e.get(ENUMERATE))
                .and_then(|e| e.get(&(cell.filter.clone(), cell.k)))
                .and_then(|b| b[slot(cell.target, cell.mode)].clone());
            let bucket = match &judged.verdict {
                Verdict::Pass | Verdict::NoBaseline { expected: true } => "pass",
                Verdict::Fail(_) => "fail",
                Verdict::Incomplete(_) => "incomplete",
                Verdict::NoBaseline { expected: false } => "no_baseline",
            };
            *counts.entry(bucket).or_default() += 1;
            let allows = if cell.filter == "none" {
                "100%".to_string()
            } else {
                selectivity
                    .get(&(group.clone(), cell.filter.clone()))
                    .map_or("?".to_string(), |p| format!("{p:.1}%"))
            };
            writeln!(
                md,
                "| {} | {allows} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                cell.filter,
                cell.k,
                TARGETS[cell.target],
                CORES[cell.mode],
                judged
                    .candidate
                    .as_ref()
                    .map_or("-".into(), |c| format!("{:.0}", c.qps)),
                judged.best.as_ref().map_or("-".into(), |(n, _)| n.clone()),
                judged
                    .best
                    .as_ref()
                    .map_or("-".into(), |(_, b)| format!("{:.0}", b.qps)),
                judged.ratio.map_or("-".into(), |r| format!("{r:.2}")),
                enumerate
                    .as_ref()
                    .map_or("-".into(), |e| format!("{:.0}", e.qps)),
                judged.verdict.label(),
            )?;
            verdicts.push(json!({
                "group": group, "filter": cell.filter, "k": cell.k,
                "recall_target": TARGETS[cell.target], "mode": MODES[cell.mode],
                "required_baselines": cell.baselines,
                "candidate": judged.candidate.as_ref().map(best_json),
                "best_baseline": judged.best.as_ref().map(|(n, b)| json!({"engine": n, "point": best_json(b)})),
                "enumerate": enumerate.as_ref().map(best_json),
                "ratio": judged.ratio, "verdict": judged.verdict.label(),
            }));
            debug_assert!(!(judged.verdict.failed() && judged.verdict.incomplete()));
        }
        let Some(engines) = engines else { continue };
        let cells: BTreeSet<u64> = engines
            .values()
            .flat_map(|e| e.keys().map(|c| c.1))
            .collect();

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
            for k in &cells {
                for (t, target) in TARGETS.iter().enumerate() {
                    let mut series = Vec::new();
                    for pct in [1usize, 10, 50] {
                        let f = format!("{set}-{pct}");
                        let get = |e: &str| {
                            engines
                                .get(e)
                                .and_then(|m| m.get(&(f.clone(), *k)))
                                .and_then(|b| b[slot(t, 0)].clone())
                        };
                        if let (Some(c), Some(e)) = (get(&candidate), get(ENUMERATE)) {
                            let actual = selectivity
                                .get(&(group.clone(), f.clone()))
                                .copied()
                                .unwrap_or(pct as f64);
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

        // The smallest unfiltered k the group measured: 10 for the single
        // and segmented shapes, 100 for the sharded shape.
        let build_k = engines
            .values()
            .flat_map(|e| e.keys())
            .filter(|(filter, _)| filter == "none")
            .map(|(_, k)| *k)
            .min()
            .unwrap_or(10);
        writeln!(
            md,
            "\n### Build cost (best unfiltered k={build_k} recall 0.95 one-core point)\n"
        )?;
        writeln!(
            md,
            "| Engine | params | ef | build s | retained bytes/row | disk bytes/row |"
        )?;
        writeln!(md, "| --- | --- | ---: | ---: | ---: | ---: |")?;
        for (name, cellmap) in engines {
            let Some(b) = cellmap
                .get(&("none".to_string(), build_k))
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
        "These #53 cells need provider features and harness cells that land with #72, #73 and #76:\n\n\
         - Floor-on cells. `gates/floor.json` lists the sharded cells at k = 100, 1,000 and 10,000 with the \
           floor off and on; the floor-on cells stay incomplete until the shared floor exists (#72) and the \
           Lucene runner drives the shared-floor code of apache/lucene PR #16357 (for apache/lucene \
           issue #16958).\n\
         - Collapse cells (`collapse_parents`, D = 10 and 100, parent table on/off) on the court fixture.\n\
         - Seeded-entry cells (own top / local skeleton descent / coordinator-seeded entry).\n\
         - Tombstone cells (10% and 30% deleted rows).\n"
    )?;

    let count = |bucket: &str| counts.get(bucket).copied().unwrap_or(0);
    if count("incomplete") > 0 {
        reasons.push(format!(
            "{} expected cells are not fully measured",
            count("incomplete")
        ));
    }
    if count("no_baseline") > 0 {
        reasons.push(format!(
            "{} expected cells have no baseline at the recall target",
            count("no_baseline")
        ));
    }
    let overall = if count("fail") > 0 {
        "FAIL"
    } else if !reasons.is_empty() {
        "INCOMPLETE"
    } else if matrix.margin_provisional {
        "PROVISIONAL PASS"
    } else {
        "PASS"
    };
    writeln!(
        md,
        "\n**Overall: {overall}** ({} expected cells: {} pass, {} fail, {} incomplete, {} without a baseline).",
        matrix.cells.len(),
        count("pass"),
        count("fail"),
        count("incomplete"),
        count("no_baseline"),
    )?;
    if overall == "FAIL" && !reasons.is_empty() {
        writeln!(md, "\nThe evidence is also incomplete:")?;
    } else if overall == "INCOMPLETE" {
        writeln!(
            md,
            "\nThis is not a verdict on the candidate. The evidence is incomplete:"
        )?;
    } else if overall == "PROVISIONAL PASS" {
        writeln!(
            md,
            "\nEvery expected cell passes, but the unfiltered margin is provisional until #59 is decided."
        )?;
    }
    if overall == "FAIL" || overall == "INCOMPLETE" {
        writeln!(md)?;
        for reason in &reasons {
            writeln!(md, "- {reason}")?;
        }
    }

    let json = json!({
        "format": "protomolt-ann-gate-v2",
        "matrix": matrix.name,
        "candidate": candidate,
        "unfiltered_margin": margin,
        "unfiltered_margin_provisional": matrix.margin_provisional,
        "inputs": reports.iter().map(|(f, _)| f.clone()).collect::<Vec<_>>(),
        "overall": overall,
        "incomplete_reasons": reasons,
        "calibration_passed": calibration_ok,
        "cells": {
            "expected": matrix.cells.len(), "pass": count("pass"), "fail": count("fail"),
            "incomplete": count("incomplete"), "no_baseline": count("no_baseline"),
        },
        "verdicts": verdicts,
    });
    Ok(Outcome { overall, md, json })
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

#[cfg(test)]
mod tests {
    use super::*;

    const BASELINES: [&str; 2] = ["faiss-hnsw", "usearch"];

    fn matrix(provisional: bool, no_baseline_expected: bool) -> Value {
        json!({
            "format": MATRIX_FORMAT, "name": "test", "candidate": "hnsw",
            "unfiltered_margin": 0.2, "unfiltered_margin_provisional": provisional,
            "cells": [{
                "fixture": "fx", "shape": "single", "filters": ["none", "far-10"], "k": [10],
                "recall": [0.95], "cores": ["1"], "baselines": BASELINES,
                "no_baseline_expected": no_baseline_expected,
            }],
        })
    }

    /// (filter, recall, QPS) of one measured cell at k = 10.
    type Cells = [(&'static str, f64, f64)];

    /// One result file: engines as (name, cells).
    fn report(host: &str, digest: &str, engines: &[(&str, &Cells)]) -> Value {
        report_at(host, digest, "single", 10, engines)
    }

    /// One result file of a shape, every cell at one k.
    fn report_at(
        host: &str,
        digest: &str,
        shape: &str,
        k: u64,
        engines: &[(&str, &Cells)],
    ) -> Value {
        json!({
            "fixture": { "name": "fx", "base_sha256": digest, "filters": [] },
            "shape": shape,
            "host": { "name": host, "cpu": "cpu", "arch": "x86_64", "threads": 32, "kernel": "6.1" },
            "harness": { "commit": "abc", "started_at": 1 },
            "engines": engines.iter().map(|(name, cells)| json!({
                "engine": name,
                "builds": [{ "params": {}, "cells": cells.iter().map(|(filter, recall, qps)| json!({
                    "filter": filter, "k": k,
                    "points": [{ "recall_mean": recall, "qps_one_core": qps, "qps_all_cores": qps, "ef": 64 }],
                })).collect::<Vec<_>>() }],
            })).collect::<Vec<_>>(),
        })
    }

    /// A sharded matrix: one block for floor off, one for floor on, as
    /// gates/floor.json lists them.
    fn floor_matrix() -> Value {
        json!({
            "format": MATRIX_FORMAT, "name": "floor", "candidate": "hnsw",
            "unfiltered_margin": 0.2, "unfiltered_margin_provisional": false,
            "cells": [
                {
                    "fixture": "fx", "shape": "sharded", "shards": [4], "placements": ["hash", "skewed"],
                    "floor": ["off"], "filters": ["none"], "k": [1000], "recall": [0.95], "cores": ["1"],
                    "baselines": ["lucene-hnsw", "lucene-hnsw-16357"],
                },
                {
                    "fixture": "fx", "shape": "sharded", "shards": [4], "placements": ["hash", "skewed"],
                    "floor": ["on"], "filters": ["none"], "k": [1000], "recall": [0.95], "cores": ["1"],
                    "baselines": ["lucene-hnsw-16357"],
                },
            ],
        })
    }

    /// Floor-off results for both placements at k = 1,000: ours and both
    /// Lucene builds.
    fn floor_off_reports() -> Vec<(String, Value)> {
        let mut out = Vec::new();
        for placement in ["hash", "skewed"] {
            let shape = sharded_shape_name(4, placement, false);
            out.push((
                format!("ours-{placement}.json"),
                report_at("krick", "d1", &shape, 1000, &[("hnsw", FAST)]),
            ));
            out.push((
                format!("lucene-{placement}.json"),
                report_at(
                    "krick",
                    "d1",
                    &shape,
                    1000,
                    &[("lucene-hnsw", BOTH), ("lucene-hnsw-16357", BOTH)],
                ),
            ));
        }
        out
    }

    #[test]
    fn a_sharded_block_names_one_group_per_shard_count_placement_and_floor() {
        let m = parse_matrix(&floor_matrix()).unwrap();
        let groups: BTreeSet<&str> = m.cells.iter().map(|c| c.group.as_str()).collect();
        assert_eq!(
            groups,
            BTreeSet::from([
                "fx | sharded:4:hash:floor-off",
                "fx | sharded:4:hash:floor-on",
                "fx | sharded:4:skewed:floor-off",
                "fx | sharded:4:skewed:floor-on",
            ])
        );
        assert!(m.cells.iter().all(|c| c.k == 1000));
    }

    #[test]
    fn floor_on_cells_without_a_floor_measurement_are_incomplete() {
        let pass = calibration("PASS");
        let out = judge(&floor_matrix(), &floor_off_reports(), Some(&pass)).unwrap();
        assert_eq!(out.overall, "INCOMPLETE");
        assert_eq!(out.json["cells"]["pass"], 2);
        assert_eq!(out.json["cells"]["incomplete"], 2);
        for v in out.json["verdicts"].as_array().unwrap() {
            let group = v["group"].as_str().unwrap();
            let verdict = v["verdict"].as_str().unwrap();
            if group.ends_with("floor-on") {
                assert_eq!(verdict, "INCOMPLETE (candidate not measured)", "{group}");
            } else {
                assert_eq!(verdict, "PASS", "{group}");
            }
        }
        // A floor-off result at another k does not fill the k = 1,000 cell.
        let mut reports = floor_off_reports();
        reports[0].1 = report_at(
            "krick",
            "d1",
            &sharded_shape_name(4, "hash", false),
            100,
            &[("hnsw", FAST)],
        );
        let out = judge(&floor_matrix(), &reports, Some(&pass)).unwrap();
        assert_eq!(out.json["cells"]["incomplete"], 3);
        // The #16357 build missing from a floor-off cell is incomplete too.
        let mut reports = floor_off_reports();
        reports[1].1 = report_at(
            "krick",
            "d1",
            &sharded_shape_name(4, "hash", false),
            1000,
            &[("lucene-hnsw", BOTH)],
        );
        let out = judge(&floor_matrix(), &reports, Some(&pass)).unwrap();
        assert!(out
            .md
            .contains("INCOMPLETE (not measured: lucene-hnsw-16357)"));
    }

    #[test]
    fn the_checked_in_matrices_parse_and_floor_lists_every_sharded_cell() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("gates");
        for name in ["core-unfiltered", "filtered", "replacement", "floor"] {
            let m = read_json(&dir.join(format!("{name}.json"))).unwrap();
            parse_matrix(&m).unwrap_or_else(|e| panic!("gates/{name}.json: {e}"));
        }
        // 2 fixtures x 2 shard counts x 2 placements x floor off and on x
        // k 100, 1,000, 10,000 x 2 recall targets x 2 core modes.
        let floor = read_json(&dir.join("floor.json")).unwrap();
        let m = parse_matrix(&floor).unwrap();
        assert_eq!(m.cells.len(), 192);
        let on = m
            .cells
            .iter()
            .filter(|c| c.group.ends_with("floor-on"))
            .count();
        assert_eq!(on, 96);
        assert!(m.cells.iter().all(|c| [100, 1000, 10000].contains(&c.k)));
    }

    #[test]
    fn a_malformed_sharded_block_is_refused() {
        for (pointer, value) in [
            ("/cells/0/shards", json!([1])),
            ("/cells/0/shards", json!([])),
            ("/cells/0/placements", json!(["random"])),
            ("/cells/0/floor", json!(["maybe"])),
            ("/cells/0/floor", Value::Null),
            ("/cells/0/shape", json!("sharded:4:hash:floor-off")),
            ("/cells/0/shape", json!("single")),
        ] {
            let mut m = floor_matrix();
            *m.pointer_mut(pointer).unwrap() = value;
            assert!(parse_matrix(&m).is_err(), "{pointer}");
        }
        // The same cell listed by two blocks is refused.
        let mut m = floor_matrix();
        m["cells"][1]["floor"] = json!(["off"]);
        assert!(parse_matrix(&m).is_err());
    }

    fn calibration(verdict: &str) -> Value {
        json!({ "verdict": verdict, "recall_hnsw_rs_definition": 0.977, "published_recall": 0.977, "tolerance": 0.01 })
    }

    const BOTH: &Cells = &[("none", 0.96, 1000.0), ("far-10", 0.96, 100.0)];
    const FAST: &Cells = &[("none", 0.96, 1100.0), ("far-10", 0.96, 120.0)];

    fn complete() -> Vec<(String, Value)> {
        vec![
            ("ours.json".into(), report("krick", "d1", &[("hnsw", FAST)])),
            (
                "base.json".into(),
                report("krick", "d1", &[("faiss-hnsw", BOTH), ("usearch", BOTH)]),
            ),
        ]
    }

    fn overall(m: &Value, reports: &[(String, Value)], c: Option<&Value>) -> &'static str {
        judge(m, reports, c).unwrap().overall
    }

    #[test]
    fn complete_evidence_passes_and_a_provisional_margin_says_so() {
        let pass = calibration("PASS");
        assert_eq!(
            overall(&matrix(false, false), &complete(), Some(&pass)),
            "PASS"
        );
        assert_eq!(
            overall(&matrix(true, false), &complete(), Some(&pass)),
            "PROVISIONAL PASS"
        );
    }

    #[test]
    fn a_missing_candidate_cell_or_baseline_is_incomplete() {
        let pass = calibration("PASS");
        // The candidate never ran the filtered cell.
        let mut reports = complete();
        reports[0].1 = report("krick", "d1", &[("hnsw", &FAST[..1])]);
        let out = judge(&matrix(false, false), &reports, Some(&pass)).unwrap();
        assert_eq!(out.overall, "INCOMPLETE");
        assert!(out.md.contains("INCOMPLETE (candidate not measured)"));
        // A required baseline is absent from every file.
        let mut reports = complete();
        reports[1].1 = report("krick", "d1", &[("usearch", BOTH)]);
        let out = judge(&matrix(false, false), &reports, Some(&pass)).unwrap();
        assert_eq!(out.overall, "INCOMPLETE");
        assert!(out.md.contains("INCOMPLETE (not measured: faiss-hnsw)"));
        // No result file covers the group at all.
        assert_eq!(
            overall(&matrix(false, false), &[], Some(&pass)),
            "INCOMPLETE"
        );
    }

    #[test]
    fn a_losing_ratio_fails_even_with_a_baseline_missing() {
        let pass = calibration("PASS");
        let slow: &Cells = &[("none", 0.96, 700.0), ("far-10", 0.96, 99.0)];
        let reports = vec![
            ("ours.json".into(), report("krick", "d1", &[("hnsw", slow)])),
            (
                "base.json".into(),
                report("krick", "d1", &[("usearch", BOTH)]),
            ),
        ];
        let out = judge(&matrix(false, false), &reports, Some(&pass)).unwrap();
        assert_eq!(out.overall, "FAIL");
        assert_eq!(out.json["cells"]["fail"], 2);
        // Unfiltered: 0.85 of the best baseline is inside the 20% margin.
        let near: &Cells = &[("none", 0.96, 850.0), ("far-10", 0.96, 100.0)];
        let mut reports = complete();
        reports[0].1 = report("krick", "d1", &[("hnsw", near)]);
        assert_eq!(
            overall(&matrix(false, false), &reports, Some(&pass)),
            "PASS"
        );
    }

    #[test]
    fn a_candidate_below_the_recall_target_fails() {
        let pass = calibration("PASS");
        let low: &Cells = &[("none", 0.90, 9000.0), ("far-10", 0.96, 120.0)];
        let mut reports = complete();
        reports[0].1 = report("krick", "d1", &[("hnsw", low)]);
        let out = judge(&matrix(false, false), &reports, Some(&pass)).unwrap();
        assert_eq!(out.overall, "FAIL");
        assert!(out.md.contains("FAIL (target not reached)"));
    }

    #[test]
    fn no_baseline_at_the_target_is_not_a_pass_unless_expected() {
        let pass = calibration("PASS");
        let low: &Cells = &[("none", 0.96, 1000.0), ("far-10", 0.50, 100.0)];
        let mut reports = complete();
        reports[1].1 = report("krick", "d1", &[("faiss-hnsw", low), ("usearch", low)]);
        let out = judge(&matrix(false, false), &reports, Some(&pass)).unwrap();
        assert_eq!(out.overall, "INCOMPLETE");
        assert_eq!(out.json["cells"]["no_baseline"], 1);
        let out = judge(&matrix(false, true), &reports, Some(&pass)).unwrap();
        assert_eq!(out.overall, "PASS");
        assert!(out.md.contains("NO-BASELINE (expected)"));
    }

    #[test]
    fn calibration_is_part_of_the_verdict() {
        assert_eq!(
            overall(&matrix(false, false), &complete(), None),
            "INCOMPLETE"
        );
        let fail = calibration("FAIL: 1.4 recall points above the published figure");
        let out = judge(&matrix(false, false), &complete(), Some(&fail)).unwrap();
        assert_eq!(out.overall, "INCOMPLETE");
        assert_eq!(out.json["calibration_passed"], false);
    }

    #[test]
    fn mixed_hosts_and_mixed_fixtures_are_refused() {
        let pass = calibration("PASS");
        let mut reports = complete();
        reports[1].1 = report("cloud", "d1", &[("faiss-hnsw", BOTH), ("usearch", BOTH)]);
        let err = judge(&matrix(false, false), &reports, Some(&pass))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("different hosts"), "{err}");
        let mut reports = complete();
        reports[1].1 = report("krick", "d2", &[("faiss-hnsw", BOTH), ("usearch", BOTH)]);
        let err = judge(&matrix(false, false), &reports, Some(&pass))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("different fixtures"), "{err}");
        // The JVM's spelling of the architecture is the same host.
        let mut reports = complete();
        reports[1].1["host"]["arch"] = json!("amd64");
        assert_eq!(
            overall(&matrix(false, false), &reports, Some(&pass)),
            "PASS"
        );
    }

    #[test]
    fn missing_provenance_is_incomplete() {
        let pass = calibration("PASS");
        for pointer in [
            "/host/kernel",
            "/harness/commit",
            "/harness/started_at",
            "/fixture/base_sha256",
        ] {
            let mut reports = complete();
            *reports[0].1.pointer_mut(pointer).unwrap() = Value::Null;
            let out = judge(&matrix(false, false), &reports, Some(&pass)).unwrap();
            assert_eq!(out.overall, "INCOMPLETE", "{pointer}");
            assert!(out.md.contains("ours.json does not record"), "{pointer}");
        }
    }

    #[test]
    fn a_malformed_matrix_is_refused() {
        let pass = calibration("PASS");
        for (pointer, value) in [
            ("/format", json!("other")),
            ("/unfiltered_margin_provisional", Value::Null),
            ("/unfiltered_margin", json!(1.5)),
            ("/cells/0/recall", json!([0.9])),
            ("/cells/0/cores", json!(["2"])),
            ("/cells/0/baselines", json!([])),
            ("/cells/0/baselines", json!(["hnsw"])),
            ("/cells/0/filters", json!(["none", "none"])),
        ] {
            let mut m = matrix(false, false);
            *m.pointer_mut(pointer).unwrap() = value;
            assert!(judge(&m, &complete(), Some(&pass)).is_err(), "{pointer}");
        }
    }
}
