//! Reconcile a re-placement child against its sources, document for
//! document (`src/reconcile.rs`, `docs/derived-columns.md`).
//!
//! Usage:
//!   reconcile --logs=<log dir or generation>[,...] --child=<out>/shard-<i>.tv \
//!     --placement-tree=<file> --child-index=<i> [--derived-columns=<file>] \
//!     [--derive=a,b] [--equal=<column>:<other>]... [--civil-year=<column>:<timestamp>]... \
//!     [--threads=<n>]
//!
//! Exits 0 when the report is clean, 1 otherwise, 2 on a refusal.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pipestream_search::reconcile::{reconcile, ColumnCheck, ReconcileOptions};
use pipestream_search::reshard;

fn run() -> Result<bool, String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut logs: Vec<PathBuf> = Vec::new();
    let mut child: Option<PathBuf> = None;
    let mut tree: Option<PathBuf> = None;
    let mut child_index: Option<usize> = None;
    let mut declaration_path: Option<PathBuf> = None;
    let mut derive: Vec<String> = Vec::new();
    let mut checks: Vec<ColumnCheck> = Vec::new();
    let mut threads = 0usize;
    for arg in &args {
        let (name, value) = arg
            .strip_prefix("--")
            .and_then(|rest| rest.split_once('='))
            .ok_or_else(|| format!("unrecognized argument {arg:?}; every flag is --name=value"))?;
        match name {
            "logs" | "log" => {
                for one in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    logs.push(reshard::resolve_gen(Path::new(one))?);
                }
            }
            "child" => child = Some(PathBuf::from(value)),
            "placement-tree" => tree = Some(PathBuf::from(value)),
            "child-index" => {
                child_index = Some(
                    value
                        .parse()
                        .map_err(|e| format!("--child-index={value}: {e}"))?,
                )
            }
            "derived-columns" => declaration_path = Some(PathBuf::from(value)),
            "derive" => {
                derive.extend(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(String::from),
                );
            }
            "equal" | "civil-year" => checks.push(ColumnCheck::parse(name, value)?),
            "threads" => {
                threads = value
                    .parse()
                    .map_err(|e| format!("--threads={value}: {e}"))?
            }
            other => return Err(format!("unrecognized flag --{other}")),
        }
    }
    if logs.is_empty() {
        return Err("--logs names the sources".to_string());
    }
    let child = child.ok_or_else(|| "--child names the child index path".to_string())?;
    let tree = tree.ok_or_else(|| "--placement-tree names the tree".to_string())?;
    let child_index = child_index.ok_or_else(|| "--child-index names the child".to_string())?;
    let tree = pipestream_search::config::load_placement_tree(&tree)?;
    let declaration = match declaration_path {
        Some(path) => {
            let spec = pipestream_search::derived::load_declaration(&path)?;
            Some(Arc::new(pipestream_search::derived::Declaration::compile(
                &spec,
            )?))
        }
        None => None,
    };
    let started = std::time::Instant::now();
    let report = reconcile(&ReconcileOptions {
        sources: logs,
        child,
        tree,
        child_index,
        declaration,
        derive,
        checks,
        threads,
    })?;
    print!("{}", report.render());
    println!("{:.1} s", started.elapsed().as_secs_f64());
    Ok(report.is_clean())
}

fn main() {
    match run() {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(e) => {
            eprintln!("reconcile: {e}");
            std::process::exit(2);
        }
    }
}
