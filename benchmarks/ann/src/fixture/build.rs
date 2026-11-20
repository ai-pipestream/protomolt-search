//! Generate filter definitions, exact neighbors and the fixture manifest.
use super::{
    bucket, challenge, court, exact_top, hash_placement, kmeans, placement_name, skewed_placement,
    synthetic, Column, ColumnIndex, Filter, Rng, Rows, FORMAT, PLACEMENTS, SELECTIVITIES, SETS,
};
use crate::util::{arg, dot, list_usize, par_map, write_f32, write_json, write_u32, Error};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

struct Options {
    out: PathBuf,
    name: String,
    cells: usize,
    k_truth: usize,
    k_truth_none: usize,
    shard_counts: Vec<usize>,
}

fn parse_options() -> Result<(Rows, Options), Error> {
    let source = arg("source", "challenge");
    let queries: usize = arg("queries", "1000").parse()?;
    let out = PathBuf::from(arg("out", ""));
    if out.as_os_str().is_empty() {
        return Err("--out=DIR is required".into());
    }
    let rows = match source.as_str() {
        "challenge" => challenge(queries),
        "court" => court(
            Path::new(&arg("input", "/work/court-corpus/embeddings-full.bin")),
            arg("vectors", "1000000").parse()?,
            queries,
        )?,
        s if s.starts_with("synthetic") => {
            let spec = s.strip_prefix("synthetic:").unwrap_or("50000:64");
            let (r, d) = spec.split_once(':').ok_or("--source=synthetic:ROWS:DIM")?;
            synthetic(r.parse()?, d.parse()?, queries)
        }
        other => return Err(format!("unknown --source {other:?}").into()),
    };
    let n = rows.rows();
    let default_cells = if n >= 500_000 { 400 } else { 200 };
    let cells: usize = arg("cells", &default_cells.to_string()).parse()?;
    let k_truth: usize = arg("k-truth", "100").parse()?;
    let k_truth_none: usize = arg("k-truth-none", "10000").parse()?;
    if k_truth == 0 || k_truth_none < k_truth {
        return Err(format!(
            "--k-truth={k_truth} must be at least 1 and --k-truth-none={k_truth_none} at least --k-truth"
        )
        .into());
    }
    if k_truth_none > n {
        return Err(format!("--k-truth-none={k_truth_none} exceeds the fixture's {n} rows").into());
    }
    let shard_counts = list_usize("shard-counts", "4,16")?;
    if let Some(bad) = shard_counts
        .iter()
        .find(|s| **s < 2 || **s > u16::MAX as usize)
    {
        return Err(format!("--shard-counts: {bad} is not a shard count of at least 2").into());
    }
    let name = arg("name", &format!("{}-{}x{}", rows.source, n, rows.dim));
    Ok((
        rows,
        Options {
            out,
            name,
            cells,
            k_truth,
            k_truth_none,
            shard_counts,
        },
    ))
}

/// Build and write a fixture directory.
pub fn build_command() -> Result<(), Error> {
    let (rows, options) = parse_options()?;
    let threads = crate::util::host_threads();
    let n = rows.rows();
    let cells = options.cells;
    eprintln!(
        "fixture: {} ({n} rows, {} queries)",
        rows.provenance,
        rows.query_count()
    );

    let started = std::time::Instant::now();
    let (centroids, cell) = kmeans(&rows, cells, 12, threads);
    eprintln!(
        "fixture: k-means {cells} cells in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let bucket: Vec<u8> = (0..n as u64).map(bucket).collect();
    let index = ColumnIndex::new(&bucket, &cell, cells);

    // Unfiltered truth first: correlated filters are defined from it.
    let qn = rows.query_count();
    let unfiltered: Vec<Vec<(u32, f32)>> = par_map(qn, threads, |q| {
        exact_top(&rows, rows.query(q), options.k_truth_none.max(100), None)
    });

    let filters = build_filters(&rows, &centroids, &cell, &index, &unfiltered);
    let meta = write_truth(&options, &rows, &index, &unfiltered, &filters, threads)?;
    let placements = write_rows(&options, &rows, &bucket, &cell, &centroids)?;
    write_manifest(&options, &rows, meta, placements)
}

fn build_filters(
    rows: &Rows,
    centroids: &[f32],
    cell: &[u16],
    index: &ColumnIndex,
    unfiltered: &[Vec<(u32, f32)>],
) -> Vec<Filter> {
    let n = rows.rows();
    let qn = rows.query_count();
    let cells = index.cell.len();
    let mut filters = Vec::new();
    for set in SETS {
        for pct in SELECTIVITIES {
            let want = (n * pct).div_ceil(100);
            let (column, per_query, shared) = match set {
                "uncorrelated" => (
                    Column::Bucket,
                    vec![(0..pct as u16).collect::<Vec<_>>()],
                    true,
                ),
                "near" => {
                    let lists = (0..qn)
                        .map(|q| {
                            let query = rows.query(q);
                            let mut votes = vec![0usize; cells];
                            for (r, _) in unfiltered[q].iter().take(10) {
                                votes[cell[*r as usize] as usize] += 1;
                            }
                            let home = (0..cells)
                                .max_by_key(|c| (votes[*c], usize::MAX - c))
                                .unwrap();
                            let mut order: Vec<(usize, f32)> = (0..cells)
                                .filter(|c| *c != home)
                                .map(|c| {
                                    (c, dot(query, &centroids[c * rows.dim..(c + 1) * rows.dim]))
                                })
                                .collect();
                            order.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                            let mut chosen = vec![home as u16];
                            let mut have = index.cell[home].len();
                            for (c, _) in order {
                                if have >= want {
                                    break;
                                }
                                chosen.push(c as u16);
                                have += index.cell[c].len();
                            }
                            chosen.sort_unstable();
                            chosen
                        })
                        .collect();
                    (Column::Cell, lists, false)
                }
                "far" => {
                    let lists = (0..qn)
                        .map(|q| far_cells(q, pct, want, cell, index, &unfiltered[q]))
                        .collect();
                    (Column::Cell, lists, false)
                }
                _ => unreachable!(),
            };
            filters.push(Filter {
                name: format!("{set}-{pct}"),
                set: set.to_string(),
                target_percent: pct,
                column,
                per_query,
                shared,
            });
        }
    }

    filters
}

fn far_cells(
    q: usize,
    pct: usize,
    want: usize,
    cell: &[u16],
    index: &ColumnIndex,
    hits: &[(u32, f32)],
) -> Vec<u16> {
    let cells = index.cell.len();
    let mut banned = vec![false; cells];
    for (r, _) in hits.iter().take(100) {
        banned[cell[*r as usize] as usize] = true;
    }
    let mut order: Vec<usize> = (0..cells).filter(|c| !banned[*c]).collect();
    let mut rng = Rng(0xFA12_0000 ^ ((q as u64) << 8) ^ pct as u64);
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i + 1));
    }
    let mut chosen = Vec::new();
    let mut have = 0;
    for c in order {
        if have >= want {
            break;
        }
        chosen.push(c as u16);
        have += index.cell[c].len();
    }
    chosen.sort_unstable();
    chosen
}

fn write_truth(
    options: &Options,
    rows: &Rows,
    index: &ColumnIndex,
    unfiltered: &[Vec<(u32, f32)>],
    filters: &[Filter],
    threads: usize,
) -> Result<Vec<Value>, Error> {
    let out = &options.out;
    let (qn, n) = (rows.query_count(), rows.rows());
    let (k_truth, k_truth_none) = (options.k_truth, options.k_truth_none);
    std::fs::create_dir_all(out.join("filters"))?;
    std::fs::create_dir_all(out.join("truth"))?;
    let mut truth_none = Vec::with_capacity(qn * k_truth_none);
    for hits in unfiltered {
        truth_none.extend(hits.iter().take(k_truth_none).map(|(r, _)| *r));
    }
    write_u32(&out.join("truth/none.u32"), &truth_none)?;

    let mut filter_meta = Vec::new();
    for f in filters {
        let started = std::time::Instant::now();
        let per: Vec<(Vec<u32>, f64)> = par_map(qn, threads, |q| {
            let bits = index.mask(f.column, f.values(q));
            let actual = bits.count() as f64 / n as f64;
            let top = exact_top(rows, rows.query(q), k_truth, Some(&bits));
            let mut ids: Vec<u32> = top.iter().map(|(r, _)| *r).collect();
            // A filter smaller than k_truth pads with u32::MAX (never matches).
            ids.resize(k_truth, u32::MAX);
            (ids, actual)
        });
        let mean_actual = per.iter().map(|p| p.1).sum::<f64>() / qn as f64 * 100.0;
        let truth: Vec<u32> = per.into_iter().flat_map(|p| p.0).collect();
        write_u32(&out.join(format!("truth/{}.u32", f.name)), &truth)?;
        let body = if f.shared {
            json!({ "column": f.column.name(), "shared": f.per_query[0] })
        } else {
            json!({ "column": f.column.name(), "per_query": f.per_query })
        };
        write_json(&out.join(format!("filters/{}.json", f.name)), &body)?;
        filter_meta.push(json!({
            "name": f.name, "set": f.set, "target_percent": f.target_percent,
            "actual_mean_percent": mean_actual, "column": f.column.name(),
        }));
        eprintln!(
            "fixture: {} truth in {:.1}s (allows {mean_actual:.2}% on average)",
            f.name,
            started.elapsed().as_secs_f64()
        );
    }

    Ok(filter_meta)
}

fn write_rows(
    options: &Options,
    rows: &Rows,
    bucket: &[u8],
    cell: &[u16],
    centroids: &[f32],
) -> Result<Vec<Value>, Error> {
    let out = &options.out;
    write_f32(&out.join("base.f32"), &rows.base)?;
    write_f32(&out.join("queries.f32"), &rows.queries)?;
    let parent_bytes: Vec<u8> = rows.parents.iter().flat_map(|p| p.to_le_bytes()).collect();
    std::fs::write(out.join("parents.u64"), parent_bytes)?;
    std::fs::write(out.join("bucket.u8"), bucket)?;
    let cell_bytes: Vec<u8> = cell.iter().flat_map(|c| c.to_le_bytes()).collect();
    std::fs::write(out.join("cell.u16"), cell_bytes)?;
    std::fs::create_dir_all(out.join("placements"))?;
    let mut placements = Vec::new();
    for &shards in &options.shard_counts {
        for placement in PLACEMENTS {
            let shard = match placement {
                "hash" => hash_placement(rows.rows(), shards),
                _ => skewed_placement(cell, centroids, rows.dim, shards)?,
            };
            let name = placement_name(placement, shards);
            let mut rows_per_shard = vec![0usize; shards];
            for s in &shard {
                rows_per_shard[*s as usize] += 1;
            }
            let bytes: Vec<u8> = shard.iter().flat_map(|s| s.to_le_bytes()).collect();
            std::fs::write(out.join(format!("placements/{name}.u16")), bytes)?;
            placements.push(json!({
                "name": name, "placement": placement, "shards": shards,
                "rows_per_shard": rows_per_shard,
            }));
        }
    }
    Ok(placements)
}

fn write_manifest(
    options: &Options,
    rows: &Rows,
    filter_meta: Vec<Value>,
    placements: Vec<Value>,
) -> Result<(), Error> {
    let out = &options.out;
    let base_bytes: Vec<u8> = rows.base.iter().flat_map(|v| v.to_le_bytes()).collect();
    let manifest = json!({
        "format": FORMAT,
        "name": options.name,
        "source": rows.source,
        "provenance": rows.provenance,
        "rows": rows.rows(),
        "dim": rows.dim,
        "queries": rows.query_count(),
        "metric": "inner product over unit vectors (equivalently L2)",
        "cells": options.cells,
        "k_truth": options.k_truth,
        "k_truth_none": options.k_truth_none,
        "base_sha256": turbo_sha256::sha256(&base_bytes).to_hex(),
        "filters": filter_meta,
        "masks": "uncorrelated: bucket in 0..p (bucket = splitmix64(row) mod 100). \
                  near: the k-means cell holding most of the query's unfiltered top-10, then cells by \
                  centroid similarity to the query until p% of rows. far: cells holding none of the \
                  query's unfiltered top-100, in a seeded random order, until p% of rows.",
        "placements": placements,
        "placement_rules": "hash: splitmix64(row) mod shards. skewed: whole k-means cells in the \
                  order of a walk from cell 0 to each centroid's most similar unvisited centroid, a \
                  shard closed once it holds its share of the rows not yet placed.",
    });
    write_json(&out.join("manifest.json"), &manifest)?;
    eprintln!("fixture: wrote {}", out.display());
    Ok(())
}
