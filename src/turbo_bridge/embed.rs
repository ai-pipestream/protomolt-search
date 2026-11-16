//! The batch embedding seam: one call shape for every model bundle.
//!
//! A remote `turbo-kserve` model and an in-process bundle answer the same
//! question — rows of text in, one vector per row out — so both sit behind
//! [`BatchEmbedder`] and return an [`OwnedMatrix`]. Static (`Model2Vec`)
//! and transformer bundles are not distinguished here: libturbo loads both
//! through the same session contract, so `LocalBatchEmbedder` opens
//! either. A row whose text pools to nothing, or whose norm is not finite,
//! comes back as a zero row listed in [`OwnedMatrix::empty_rows`] rather
//! than failing the batch.

use std::future::Future;

use tonic::Status;

use super::tensor::OwnedMatrix;
use crate::analyzer::RemoteCallOptions;

/// Which side of a retrieval pair the texts are, for bundles that prompt
/// queries and documents differently.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EmbedRole {
    /// Search queries.
    #[default]
    Query,
    /// Indexed passages.
    Document,
    /// No prompt: the bundle embeds the text as given.
    None,
}

impl EmbedRole {
    /// The `role` parameter `turbo-kserve` reads, if any.
    pub fn wire_name(self) -> Option<&'static str> {
        match self {
            Self::Query => Some("query"),
            Self::Document => Some("document"),
            Self::None => None,
        }
    }

    #[cfg(feature = "local-core")]
    fn prompt(self) -> turbo_client::EmbedPromptRole {
        match self {
            Self::Query => turbo_client::EmbedPromptRole::Query,
            Self::Document => turbo_client::EmbedPromptRole::Document,
            Self::None => turbo_client::EmbedPromptRole::None,
        }
    }
}

/// Per-batch embedding options. `call` reaches only remote endpoints.
#[derive(Debug, Clone, Default)]
pub struct EmbedOptions {
    /// Prompt role for every text in the batch.
    pub role: EmbedRole,
    /// Scheduling, auth, TLS, deadline, and handshake for a remote call.
    pub call: RemoteCallOptions,
}

/// Embeds a batch of texts into one row each, in input order.
pub trait BatchEmbedder: Send + Sync {
    /// Embed `texts`; the result has exactly `texts.len()` rows.
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [&'a str],
        options: &'a EmbedOptions,
    ) -> impl Future<Output = Result<OwnedMatrix, Status>> + Send + 'a;
}

/// A `turbo-kserve` endpoint, addressed exactly as
/// [`crate::analyzer::embed_text_with_options`] addresses it.
#[derive(Debug, Clone)]
pub struct RemoteBatchEmbedder {
    addr: String,
}

impl RemoteBatchEmbedder {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

impl BatchEmbedder for RemoteBatchEmbedder {
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [&'a str],
        options: &'a EmbedOptions,
    ) -> impl Future<Output = Result<OwnedMatrix, Status>> + Send + 'a {
        crate::analyzer::embed_texts_with_options(&self.addr, texts, options)
    }
}

#[cfg(feature = "net")]
/// Shape and normalize a flat `[rows, dim]` response: each row is scaled
/// to unit length once, and a row with a zero or non-finite norm becomes a
/// zero row listed in `empty_rows`.
pub(crate) fn unit_rows(flat: Vec<f32>, rows: usize, dim: usize) -> Result<OwnedMatrix, Status> {
    if flat.is_empty() || dim == 0 {
        return Err(Status::failed_precondition(
            "turbo-kserve returned no embeddings",
        ));
    }
    if flat.len() != rows.saturating_mul(dim) {
        return Err(Status::failed_precondition(format!(
            "turbo-kserve returned {} values for {rows} rows of dimension {dim}",
            flat.len()
        )));
    }
    let mut flat = flat;
    for row in flat.chunks_exact_mut(dim) {
        let norm = row
            .iter()
            .map(|&v| f64::from(v) * f64::from(v))
            .sum::<f64>()
            .sqrt();
        if !norm.is_finite() {
            row.fill(0.0);
        }
    }
    let mut matrix = OwnedMatrix::from_flat(flat, dim)?;
    matrix.l2_normalize_rows();
    Ok(matrix)
}

/// An in-process libturbo embedding bundle, static or transformer.
#[cfg(feature = "local-core")]
#[derive(Clone)]
pub struct LocalBatchEmbedder {
    engine: std::sync::Arc<turbo_client::LocalEmbedEngine>,
}

#[cfg(feature = "local-core")]
impl LocalBatchEmbedder {
    /// Open the bundle at `dir`, verifying `handshake` when given.
    pub fn open(
        dir: &std::path::Path,
        handshake: Option<&turbo_client::IndexGenerationHandshake>,
    ) -> Result<Self, Status> {
        let engine = turbo_client::LocalEmbedEngine::open(
            dir,
            &turbo_client::LocalEncoderOptions::default(),
            None,
            handshake,
        )
        .map_err(|e| Status::failed_precondition(format!("embedding bundle: {e}")))?;
        Ok(Self {
            engine: std::sync::Arc::new(engine),
        })
    }

    /// The bundle's full output width.
    pub fn dim(&self) -> usize {
        self.engine.scoring_spec().dim as usize
    }

    /// Embed on the calling thread. A text that pools to nothing becomes a
    /// zero row listed in `empty_rows`; the bundle's own refusals surface
    /// as `internal`.
    pub fn embed_blocking(&self, texts: &[&str], role: EmbedRole) -> Result<OwnedMatrix, Status> {
        if texts.is_empty() {
            return Err(Status::invalid_argument("embed batch is empty"));
        }
        let vectors = self
            .engine
            .embed_texts_multires(
                texts,
                role.prompt(),
                &turbo_client::MultiResOptions::default(),
            )
            .map_err(|e| Status::internal(format!("local embedding failed: {e}")))?;
        let tier = vectors.full;
        if tier.rows as usize != texts.len() {
            return Err(Status::internal(format!(
                "local embedding returned {} rows for {} texts",
                tier.rows,
                texts.len()
            )));
        }
        let matrix = OwnedMatrix::from_flat(tier.data, tier.dim as usize)?;
        let empty = matrix
            .view()
            .iter_rows()
            .enumerate()
            .filter(|(_, row)| row.iter().all(|v| *v == 0.0))
            .map(|(index, _)| index as u32)
            .collect();
        Ok(matrix.with_empty_rows(empty)?)
    }
}

#[cfg(feature = "local-core")]
impl BatchEmbedder for LocalBatchEmbedder {
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [&'a str],
        options: &'a EmbedOptions,
    ) -> impl Future<Output = Result<OwnedMatrix, Status>> + Send + 'a {
        let owned: Vec<String> = texts.iter().map(|&t| t.to_owned()).collect();
        let role = options.role;
        let this = self.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                let texts: Vec<&str> = owned.iter().map(String::as_str).collect();
                this.embed_blocking(&texts, role)
            })
            .await
            .map_err(|e| Status::internal(format!("local embedding join error: {e}")))?
        }
    }
}

#[cfg(all(test, feature = "net"))]
#[path = "embed_tests.rs"]
mod tests;
