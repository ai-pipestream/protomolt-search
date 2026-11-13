//! Graph ANN benchmark harness and gate (#53).
//!
//! ```text
//! ann_bench fixture   --source=challenge|court|synthetic:ROWS:DIM --out=DIR [--queries=1000]
//! ann_bench run       --fixture=DIR --out=results.json [--engines=...] [--segment-rows=N]
//! ann_bench gate      --results=a.json,b.json --matrix=gates/replacement.json --out=gate.md
//! ann_bench calibrate --dir=fashion-mnist --out=calibration.json
//! ```
//!
//! See README.md for the fixture format, the engines and the pass rule.

mod calibrate;
mod engines;
mod fixture;
mod gate;
mod merge;
mod patience;
mod run;
mod tail;
mod util;

fn main() {
    let command = std::env::args().nth(1).unwrap_or_default();
    let result = match command.as_str() {
        "fixture" => fixture::build_command(),
        "run" => run::command(),
        "gate" => gate::command(),
        "calibrate" => calibrate::command(),
        "tail" => tail::command(),
        "patience" => patience::command(),
        "merge" => merge::command(),
        _ => Err(
            "usage: ann_bench fixture|run|gate|calibrate|tail|patience|merge [--key=value ...] (see README.md)"
                .into(),
        ),
    };
    if let Err(e) = result {
        eprintln!("ann_bench {command}: {e}");
        std::process::exit(1);
    }
}
