//! `ann_bench calibrate`: reproduce hnsw_rs's published recall before any
//! comparison counts (#53 "Fairness").
//!
//! The published point (hnsw_rs 0.3.5 README and
//! `examples/ann-mnist-784-euclidean.rs`): fashion-mnist-784-euclidean,
//! max_nb_connection = 24, ef_construction = 400, nb_layer =
//! min(16, ln(n)), extend_candidates off, parallel insertion, k = 10,
//! ef_search = 24: recall 0.977. hnsw_rs counts a returned neighbor as a
//! hit when its distance is at most the true 10th distance; that is the
//! recall compared here (the id-overlap recall is reported beside it).
//! Published queries per second are another machine's and are not a
//! target.
//!
//! The ann-benchmarks file is Fashion-MNIST's 60,000 training and 10,000
//! test images as f32 pixels; it is rebuilt here from the original IDX
//! files (`fetch-fashion-mnist.sh`) and the truth is computed exactly.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde_json::json;

use crate::util::{arg, host, host_threads, l2sq, par_map, write_json, Error, TopK};

const PUBLISHED_RECALL: f64 = 0.977;
const TOLERANCE: f64 = 0.01;

fn read_idx_images(path: &Path) -> Result<(usize, usize, Vec<f32>), Error> {
    let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(file).read_to_end(&mut bytes)?;
    if bytes.len() < 16 || u32::from_be_bytes(bytes[0..4].try_into()?) != 0x0000_0803 {
        return Err(format!("{}: not an IDX3 image file", path.display()).into());
    }
    let n = u32::from_be_bytes(bytes[4..8].try_into()?) as usize;
    let rows = u32::from_be_bytes(bytes[8..12].try_into()?) as usize;
    let cols = u32::from_be_bytes(bytes[12..16].try_into()?) as usize;
    let dim = rows * cols;
    if bytes.len() != 16 + n * dim {
        return Err(format!("{}: truncated", path.display()).into());
    }
    Ok((n, dim, bytes[16..].iter().map(|b| f32::from(*b)).collect()))
}

pub fn command() -> Result<(), Error> {
    use hnsw_rs::anndists::dist::DistL2;
    use hnsw_rs::hnsw::Hnsw;

    let dir = PathBuf::from(arg("dir", "fashion-mnist"));
    let out = PathBuf::from(arg("out", "calibration.json"));
    let threads = host_threads();
    let (n, dim, train) = read_idx_images(&dir.join("train-images-idx3-ubyte.gz"))?;
    let (qn_all, qdim, test) = read_idx_images(&dir.join("t10k-images-idx3-ubyte.gz"))?;
    if dim != qdim {
        return Err("train and test dimensions differ".into());
    }
    let qn: usize = arg("queries", &qn_all.to_string())
        .parse::<usize>()?
        .min(qn_all);
    let knbn = 10;
    eprintln!("calibrate: fashion-mnist {n} x {dim}, {qn} queries");

    let started = Instant::now();
    let truth: Vec<Vec<(u32, f32)>> = par_map(qn, threads, |q| {
        let query = &test[q * dim..(q + 1) * dim];
        let mut top = TopK::new(knbn);
        for r in 0..n {
            // TopK keeps the largest; negate the distance.
            let d = -l2sq(query, &train[r * dim..(r + 1) * dim]);
            if d > top.threshold() {
                top.push(r as u32, d);
            }
        }
        top.into_sorted()
    });
    eprintln!(
        "calibrate: exact truth in {:.1}s",
        started.elapsed().as_secs_f64()
    );

    let (m, efc, ef) = (24usize, 400usize, 24usize);
    let nb_layer = 16.min((n as f32).ln().trunc() as usize);
    let started = Instant::now();
    let mut hnsw = Hnsw::<f32, DistL2>::new(m, n, nb_layer, efc, DistL2 {});
    let serial = arg("insert", "parallel") == "serial";
    let extend = arg("extend", "0") == "1";
    hnsw.set_extend_candidates(extend);
    let data: Vec<(&[f32], usize)> = (0..n)
        .map(|r| (&train[r * dim..(r + 1) * dim], r))
        .collect();
    if serial {
        for d in data {
            hnsw.insert_slice(d);
        }
    } else {
        hnsw.parallel_insert_slice(&data);
    }
    hnsw.set_searching_mode(true);
    let build_s = started.elapsed().as_secs_f64();
    eprintln!("calibrate: hnsw_rs built in {build_s:.1}s");

    let queries: Vec<Vec<f32>> = (0..qn)
        .map(|q| test[q * dim..(q + 1) * dim].to_vec())
        .collect();
    let started = Instant::now();
    let answers = hnsw.parallel_search(&queries, knbn, ef);
    let search_s = started.elapsed().as_secs_f64();

    let mut hits_by_distance = 0usize;
    let mut hits_by_id = 0usize;
    for (q, answer) in answers.iter().enumerate() {
        let max_dist = (-truth[q][knbn - 1].1).sqrt();
        hits_by_distance += answer
            .iter()
            .filter(|nb| nb.distance <= max_dist * (1.0 + 1e-6))
            .count();
        let want: std::collections::HashSet<u32> = truth[q].iter().map(|(r, _)| *r).collect();
        hits_by_id += answer
            .iter()
            .filter(|nb| want.contains(&(nb.d_id as u32)))
            .count();
    }
    let recall = hits_by_distance as f64 / (qn * knbn) as f64;
    let recall_ids = hits_by_id as f64 / (qn * knbn) as f64;
    let pass = (recall - PUBLISHED_RECALL).abs() <= TOLERANCE;
    let verdict = if pass {
        "PASS".to_string()
    } else if recall > PUBLISHED_RECALL {
        format!(
            "FAIL: {:.1} recall points above the published figure",
            (recall - PUBLISHED_RECALL) * 100.0
        )
    } else {
        format!(
            "FAIL: {:.1} recall points below the published figure",
            (PUBLISHED_RECALL - recall) * 100.0
        )
    };
    let report = json!({
        "format": "protomolt-ann-calibration-v1",
        "engine": "hnsw_rs",
        "version": "0.3.5",
        "dataset": "fashion-mnist-784-euclidean (rebuilt from the Fashion-MNIST IDX files; exact L2 truth)",
        "rows": n, "dim": dim, "queries": qn,
        "params": { "max_nb_connection": m, "ef_construction": efc, "nb_layer": nb_layer, "extend_candidates": extend, "insertion": if serial { "serial" } else { "parallel" }, "k": knbn, "ef_search": ef },
        "published_recall": PUBLISHED_RECALL,
        "tolerance": TOLERANCE,
        "recall_hnsw_rs_definition": recall,
        "recall_id_overlap": recall_ids,
        "build_s": build_s,
        "qps_all_cores_parallel_search": qn as f64 / search_s,
        "host": host(),
        "verdict": verdict,
    });
    write_json(&out, &report)?;
    eprintln!("calibrate: recall {recall:.4} (published {PUBLISHED_RECALL}, id overlap {recall_ids:.4}) -> {verdict}");
    Ok(())
}
