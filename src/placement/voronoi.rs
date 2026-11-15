//! Spherical k-means Voronoi routing: centroid calibration on
//! `turbo-kserve` and the router over a decoded `TCENT001` bundle.

/// Parameters for spherical k-means Voronoi routing centroid calibration on
/// `turbo-kserve`.
#[cfg(feature = "net")]
#[derive(Clone, Debug)]
pub struct VoronoiTrainingSpec {
    /// Embedding model identifier (`model_info.model_id`).
    pub model_id: String,
    /// Embedding model revision (`model_info.revision`).
    pub model_revision: String,
    /// Embedding manifest SHA-256 (`model_info.manifest_sha256`).
    pub model_sha256: String,
    /// Vector dimension (`> 0`).
    pub dim: u32,
    /// Target number of spherical k-means Voronoi partitions (`> 0`).
    pub num_centroids: u32,
    /// Maximum Lloyd iterations (`0` uses server default).
    pub max_iters: u32,
    /// Deterministic PRNG seed for k-means++ initialization.
    pub seed: u64,
    /// Tag set / routing table name stored in the `TCENT001` bundle.
    pub tag_set_name: String,
    /// Softmax temperature (`> 0.0`, default `1.0`).
    pub temperature: f64,
}

/// Streams sample document embeddings to `turbo-kserve`'s
/// `TurboVocabularyService::TrainVoronoiCentroids`,
/// calibrating spherical k-means Voronoi routing centroids (`TCENT001`) under
/// `Bulk` priority.
#[cfg(feature = "net")]
pub async fn train_voronoi_centroids_on_kserve(
    client: &turbo_client::RemoteClient,
    spec: &VoronoiTrainingSpec,
    flat_samples: &[f32],
    finish_deadline_us: u64,
) -> Result<turbo_client::proto::vocabulary::VoronoiCentroidArtifactDescriptor, tonic::Status> {
    use turbo_client::proto::vocabulary::{
        train_voronoi_centroids_request::Frame, TrainVoronoiCentroidsHeader,
        TrainVoronoiCentroidsRequest, VoronoiSampleBatch,
    };

    let dim = spec.dim as usize;
    if dim == 0 || !flat_samples.len().is_multiple_of(dim) {
        return Err(tonic::Status::invalid_argument(format!(
            "Voronoi sample buffer length {} is not a positive multiple of dim {}",
            flat_samples.len(),
            spec.dim
        )));
    }
    let timeout = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
    let rows_per_batch = (4096 / dim).max(1);
    let floats_per_batch = rows_per_batch * dim;

    let mut frames = Vec::with_capacity(1 + flat_samples.len().div_ceil(floats_per_batch));
    frames.push(TrainVoronoiCentroidsRequest {
        frame: Some(Frame::Header(TrainVoronoiCentroidsHeader {
            model_id: spec.model_id.clone(),
            model_revision: spec.model_revision.clone(),
            model_sha256: spec.model_sha256.clone(),
            dim: spec.dim,
            num_centroids: spec.num_centroids,
            max_iters: spec.max_iters,
            seed: spec.seed,
            tag_set_name: spec.tag_set_name.clone(),
            temperature: spec.temperature,
        })),
    });
    for chunk in flat_samples.chunks(floats_per_batch) {
        frames.push(TrainVoronoiCentroidsRequest {
            frame: Some(Frame::Batch(VoronoiSampleBatch {
                values: chunk.to_vec(),
            })),
        });
    }

    let mut vocab = client
        .vocabulary_client()
        .map_err(|e| crate::analyzer::remote_client_err_to_status(client, e))?;
    let req = client
        .prepare_request(
            tokio_stream::iter(frames),
            Some(turbo_client::RemotePriority::Bulk),
            timeout,
        )
        .map_err(|e| crate::analyzer::remote_client_err_to_status(client, e))?;
    vocab
        .train_voronoi_centroids(req)
        .await
        .map(|r| r.into_inner())
        .map_err(|s| crate::analyzer::inspect_remote_status(&client.config().endpoint, s))
}

/// Spherical k-means Voronoi shard router decoded from a `TCENT001` centroid
/// bundle.
///
/// Consumes centroids calibrated by [`train_voronoi_centroids_on_kserve`] (or
/// loaded from disk) to assign document vectors to Voronoi partitions at ingest
/// and rank the top-`nprobe` partitions at query fan-out.
#[derive(Clone, Debug, PartialEq)]
pub struct VoronoiRouter {
    artifact_id: String,
    model_id: String,
    model_revision: String,
    model_sha256: String,
    tag_set_name: String,
    dim: usize,
    temperature: f64,
    labels: Vec<String>,
    centroids: Vec<f32>,
}

impl VoronoiRouter {
    /// Decodes a `TCENT001` binary centroid bundle into a live [`VoronoiRouter`].
    pub fn from_tcent_bytes(bytes: &[u8], artifact_id: impl Into<String>) -> Result<Self, String> {
        const MAGIC: &[u8; 8] = b"TCENT001";
        let mut reader = TcentReader { bytes, pos: 0 };
        if reader.slice(8)? != MAGIC {
            return Err("invalid Voronoi centroid bundle header (expected TCENT001)".to_string());
        }
        let dim = reader.u32()? as usize;
        let num_centroids = reader.u32()? as usize;
        let norm_code = reader.slice(1)?[0];
        if norm_code > 1 {
            return Err(format!(
                "unrecognized TCENT001 normalization code {norm_code}"
            ));
        }
        let temp_bytes = reader.slice(8)?;
        let temperature = f64::from_le_bytes(temp_bytes.try_into().unwrap());
        let model_id = reader.string()?;
        let model_revision = reader.string()?;
        let model_sha256 = reader.string()?;
        let tag_set_name = reader.string()?;
        if dim == 0 || num_centroids == 0 {
            return Err("TCENT001 bundle must have positive dim and num_centroids".to_string());
        }
        let mut labels = Vec::with_capacity(num_centroids);
        for _ in 0..num_centroids {
            labels.push(reader.string()?);
        }
        let total_floats = num_centroids
            .checked_mul(dim)
            .ok_or_else(|| "TCENT001 centroid dimensions overflow".to_string())?;
        let mut centroids = Vec::with_capacity(total_floats);
        for _ in 0..total_floats {
            let b = reader.slice(4)?;
            let v = f32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            if !v.is_finite() {
                return Err("TCENT001 bundle contains non-finite centroid coordinate".to_string());
            }
            centroids.push(v);
        }
        if reader.pos != bytes.len() {
            return Err(format!(
                "TCENT001 bundle has {} trailing byte(s)",
                bytes.len() - reader.pos
            ));
        }
        Ok(Self {
            artifact_id: artifact_id.into(),
            model_id,
            model_revision,
            model_sha256,
            tag_set_name,
            dim,
            temperature: if temperature.is_finite() && temperature > 0.0 {
                temperature
            } else {
                1.0
            },
            labels,
            centroids,
        })
    }

    /// Calibrates spherical k-means Voronoi centroids on `turbo-kserve` via
    /// [`train_voronoi_centroids_on_kserve`] and downloads the resulting
    /// `TCENT001` artifact into a ready-to-route [`VoronoiRouter`].
    #[cfg(feature = "net")]
    pub async fn train_and_load_from_kserve(
        client: &turbo_client::RemoteClient,
        spec: &VoronoiTrainingSpec,
        flat_samples: &[f32],
        finish_deadline_us: u64,
    ) -> Result<Self, tonic::Status> {
        use turbo_client::proto::vocabulary::{
            vocabulary_artifact_chunk::Frame, DownloadVocabularyRequest,
        };

        let desc =
            train_voronoi_centroids_on_kserve(client, spec, flat_samples, finish_deadline_us)
                .await?;
        let timeout = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
        let mut vocab = client
            .vocabulary_client()
            .map_err(|e| tonic::Status::from_error(Box::new(e)))?;
        let dl_req = client
            .prepare_request(
                DownloadVocabularyRequest {
                    artifact_id: desc.artifact_id.clone(),
                },
                Some(turbo_client::RemotePriority::Bulk),
                timeout,
            )
            .map_err(|e| tonic::Status::from_error(Box::new(e)))?;
        let mut stream = vocab.download_vocabulary(dl_req).await?.into_inner();
        let mut bytes = Vec::with_capacity(desc.byte_size as usize);
        while let Some(chunk) = stream.message().await? {
            if let Some(Frame::Data(data)) = chunk.frame {
                bytes.extend_from_slice(&data);
            }
        }
        if bytes.len() as u64 != desc.byte_size {
            return Err(tonic::Status::data_loss(format!(
                "downloaded Voronoi artifact {} length {} != descriptor byte_size {}",
                desc.artifact_id,
                bytes.len(),
                desc.byte_size
            )));
        }
        Self::from_tcent_bytes(&bytes, desc.artifact_id).map_err(tonic::Status::internal)
    }

    /// Artifact identifier in `turbo-kserve`'s vocabulary store.
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    /// Vector dimension expected by this router.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Number of Voronoi partitions (`centroids`).
    pub fn num_partitions(&self) -> usize {
        self.labels.len()
    }

    /// Partition labels ordered by partition index `0..num_partitions()`.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// Model `(id, revision, sha256)` recorded in the `TCENT001` header.
    pub fn model_identity(&self) -> (&str, &str, &str) {
        (&self.model_id, &self.model_revision, &self.model_sha256)
    }

    /// Tag set name recorded in the `TCENT001` header.
    pub fn tag_set_name(&self) -> &str {
        &self.tag_set_name
    }

    /// Assigns `vector` to its primary Voronoi partition (`0..num_partitions()`)
    /// by maximum inner product (`f64` accumulation, deterministic lower-index
    /// tie-break).
    pub fn route_vector(&self, vector: &[f32]) -> Result<usize, String> {
        let ranked = self.route_top_probes(vector, 1)?;
        Ok(ranked[0].0)
    }

    /// Ranks the top-`nprobe` Voronoi partitions `(partition_index, probability)`
    /// for multi-probe query fan-out using temperature-scaled softmax over `f64`
    /// centroid dot products.
    pub fn route_top_probes(
        &self,
        vector: &[f32],
        nprobe: usize,
    ) -> Result<Vec<(usize, f64)>, String> {
        if vector.len() != self.dim {
            return Err(format!(
                "VoronoiRouter expected vector of dim {}, got {}",
                self.dim,
                vector.len()
            ));
        }
        let k = self.labels.len();
        let take = nprobe.clamp(1, k);
        let mut logits = Vec::with_capacity(k);
        let mut max_logit = f64::NEG_INFINITY;
        for c_idx in 0..k {
            let centroid = &self.centroids[c_idx * self.dim..(c_idx + 1) * self.dim];
            let dot: f64 = vector
                .iter()
                .zip(centroid.iter())
                .map(|(&x, &c)| f64::from(x) * f64::from(c))
                .sum();
            let logit = dot / self.temperature;
            if logit > max_logit {
                max_logit = logit;
            }
            logits.push((c_idx, logit));
        }
        let mut sum_exp = 0.0_f64;
        let mut probs: Vec<(usize, f64)> = logits
            .into_iter()
            .map(|(idx, logit)| {
                let w = (logit - max_logit).exp();
                sum_exp += w;
                (idx, w)
            })
            .collect();
        if sum_exp > 0.0 && sum_exp.is_finite() {
            for (_, w) in &mut probs {
                *w /= sum_exp;
            }
        }
        probs.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        probs.truncate(take);
        Ok(probs)
    }
}

/// A cursor over a `TCENT001` payload.
struct TcentReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> TcentReader<'a> {
    fn slice(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| "TCENT001 offset overflow".to_string())?;
        if end > self.bytes.len() {
            return Err("unexpected EOF in TCENT001 payload".to_string());
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32, String> {
        let b = self.slice(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn string(&mut self) -> Result<String, String> {
        let len = self.u32()? as usize;
        let b = self.slice(len)?;
        std::str::from_utf8(b)
            .map(str::to_owned)
            .map_err(|e| format!("invalid UTF-8 in TCENT001 string: {e}"))
    }
}
