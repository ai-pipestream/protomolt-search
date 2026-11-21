//! Run `OptimizeDensePolicy` against a live coordinator: the coordinator
//! measures its fleet's dense techniques on its own hardware and returns the
//! version-2 policy, which this tool writes to `--out`; `--install` also has
//! the coordinator write it to its `--dense-execution-policy` path and serve
//! it. Supply a representative held-out sample (at least 64 queries is
//! recommended, as each cell qualifies on worst-query recall).
//!
//! ```text
//! dense_optimize --coord=http://127.0.0.1:59291 \
//!     --queries=/corpus/held-out.f32 --dim=256 --sample=64 \
//!     --k=10,100,1000 --target=950000 --max-runtime-ms=600000 \
//!     --policy-id=court-2026-11 --embedding-model=minilm-static-256 \
//!     --out=/etc/protomolt/dense-policy.binpb --install
//! ```
//!
//! `--queries` is a raw little-endian f32 rows file of `--dim` columns; the
//! first `--sample` rows are sent.

use pipestream_search::dense_policy::DenseExecutionPolicy;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::OptimizeDensePolicyRequest;
use pipestream_search::security::ToolClient;
use pipestream_search::MAX_MESSAGE_BYTES;

type Error = Box<dyn std::error::Error + Send + Sync>;

fn arg(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

fn required(key: &str) -> Result<String, Error> {
    arg(key).ok_or_else(|| format!("--{key}=<value> is required").into())
}

fn number<T: std::str::FromStr>(key: &str, default: Option<T>) -> Result<T, Error>
where
    T::Err: std::fmt::Display,
{
    match (arg(key), default) {
        (Some(text), _) => text.parse().map_err(|e| format!("--{key}: {e}").into()),
        (None, Some(default)) => Ok(default),
        (None, None) => Err(format!("--{key}=<number> is required").into()),
    }
}

/// The first `sample` rows of a raw little-endian f32 file of `dim` columns.
fn read_queries(path: &str, dim: usize, sample: usize) -> Result<Vec<f32>, Error> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {path}: {e}"))?;
    if dim == 0 || !bytes.len().is_multiple_of(dim * 4) {
        return Err(format!("{path} is not whole rows of {dim} f32 columns").into());
    }
    let rows = (bytes.len() / (dim * 4)).min(sample);
    Ok(bytes[..rows * dim * 4]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
        .collect())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let coord = required("coord")?;
    let dim: usize = number("dim", None)?;
    let sample: usize = number("sample", Some(64))?;
    let ks = match arg("k") {
        Some(list) => list
            .split(',')
            .map(|k| k.trim().parse::<u32>().map_err(|e| format!("--k: {e}")))
            .collect::<Result<Vec<_>, _>>()?,
        None => Vec::new(),
    };
    let request = OptimizeDensePolicyRequest {
        collection: arg("collection").unwrap_or_default(),
        policy_id: required("policy-id")?,
        embedding_model: required("embedding-model")?,
        target_recall_ppm: number("target", Some(950_000))?,
        k: ks,
        query_vectors: read_queries(&required("queries")?, dim, sample)?,
        max_runtime_ms: number("max-runtime-ms", None)?,
        install: std::env::args().any(|a| a == "--install"),
    };
    let out = required("out")?;
    let security = ToolClient::from_env_args()?;
    let mut client =
        SearchServiceClient::with_interceptor(security.connect(&coord).await?, security.bearer())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let response = client.optimize_dense_policy(request).await?.into_inner();
    let document = response
        .policy
        .ok_or("the coordinator returned no policy")?;
    let policy = DenseExecutionPolicy::from_document(document)?;
    policy.save(std::path::Path::new(&out))?;
    println!(
        "optimize: {} cells in {} ms{}; policy {} ({} points, fingerprint {}){}",
        response.cells_measured,
        response.elapsed_ms,
        if response.truncated {
            " (runtime bound reached)"
        } else {
            ""
        },
        out,
        policy.points().len(),
        policy.fingerprint(),
        if response.installed {
            "; installed"
        } else {
            ""
        }
    );
    Ok(())
}
