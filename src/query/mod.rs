//! The public `Query` adapter.
//!
//! This module is deliberately an ADAPTER and nothing more: it
//! validates the public shape, maps a supported request onto the
//! ordinary routes that already exist (`Search`, `Bm25Search`,
//! `HybridSearch` with its fusion modes and `BoostRescore`), executes
//! that route through the same handler every direct caller uses, and
//! translates the response into per-signal provenance. The one piece
//! of scoring that lives ABOVE the routes is the composite scorer
//! (`src/ltr.rs`), which reorders the already-selected candidate pool
//! and so never touches a pruning certificate. It never forks route
//! scoring logic, and it refuses every shape outside the mapping table
//! BY NAME — compatibility never authorizes a heuristic substitute (no
//! `AND` reinterpreted as a union, no filter applied to one leg, no
//! silent strategy swap).
//!
//! The refusal messages are load-bearing surface: each one names the
//! unsupported construct and, where one exists, the supported way to
//! ask for the same thing.
//!
//! The submodules follow a request through the adapter: `entry` checks
//! the request-level rules and picks a route; `selection`,
//! `composite_shape` and `ids` validate the selection tree and its
//! names; `prepare`, `prepare_dense` and `prepare_depth` fix every
//! policy and depth before a shard is asked, into one `run`; `browse`,
//! `lexical`, `dense` and `composite` run one route each; and `pool`
//! reorders, pages and answers the candidate pool, through `boosts`,
//! `rerank`, `collapse`, `cursor` and `response`. The `boolean_*`
//! modules serve the recursive `BooleanQuery` root.

mod boolean_compile;
mod boolean_execute;
mod boolean_request;
mod boosts;
mod browse;
mod collapse;
mod composite;
mod composite_shape;
mod cursor;
mod dense;
mod entry;
mod ids;
mod lexical;
mod pool;
mod prepare;
mod prepare_dense;
mod prepare_depth;
mod rerank;
mod response;
mod run;
mod selection;

pub(crate) use entry::execute;

use tonic::Status;

fn refuse(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}
