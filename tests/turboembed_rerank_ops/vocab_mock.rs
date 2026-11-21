//! A loopback `TurboVocabularyService` recording the security and priority
//! metadata of every call it serves.

use super::*;

#[derive(Default)]
pub(crate) struct RecordedCallMeta {
    pub(crate) priorities: Vec<String>,
    pub(crate) has_timeout: Vec<bool>,
    pub(crate) compiled_idf_bytes: Vec<u8>,
}

pub(crate) struct LoopbackVocabServer {
    pub(crate) expected_bearer: String,
    pub(crate) recorded: Arc<Mutex<RecordedCallMeta>>,
}

impl LoopbackVocabServer {
    fn verify_and_record<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let want = format!("Bearer {}", self.expected_bearer);
        if auth != want {
            return Err(Status::unauthenticated("missing or invalid bearer token"));
        }
        let prio = req
            .metadata()
            .get("x-turbo-priority")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let has_timeout = req.metadata().get("grpc-timeout").is_some();
        let mut guard = self.recorded.lock().unwrap();
        guard.priorities.push(prio);
        guard.has_timeout.push(has_timeout);
        Ok(())
    }
}

#[tonic::async_trait]
impl TurboVocabularyService for LoopbackVocabServer {
    type DownloadVocabularyStream = ReceiverStream<Result<VocabularyArtifactChunk, Status>>;

    async fn list_dictionary_formats(
        &self,
        _req: Request<ListDictionaryFormatsRequest>,
    ) -> Result<Response<ListDictionaryFormatsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn list_dictionaries(
        &self,
        _req: Request<ListDictionariesRequest>,
    ) -> Result<Response<ListDictionariesResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn list_vocabularies(
        &self,
        _req: Request<ListVocabulariesRequest>,
    ) -> Result<Response<ListVocabulariesResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn delete_vocabulary_artifact(
        &self,
        _req: Request<DeleteVocabularyArtifactRequest>,
    ) -> Result<Response<DeleteVocabularyArtifactResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn import_dictionary(
        &self,
        _req: Request<Streaming<ImportDictionaryRequest>>,
    ) -> Result<Response<DictionaryArtifactDescriptor>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn learn_vocabulary(
        &self,
        _req: Request<Streaming<LearnVocabularyRequest>>,
    ) -> Result<Response<VocabularyArtifactDescriptor>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn download_vocabulary(
        &self,
        _req: Request<DownloadVocabularyRequest>,
    ) -> Result<Response<Self::DownloadVocabularyStream>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn list_suggest_lexicons(
        &self,
        _req: Request<ListSuggestLexiconsRequest>,
    ) -> Result<Response<ListSuggestLexiconsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn build_suggest_lexicon(
        &self,
        _req: Request<Streaming<BuildSuggestLexiconRequest>>,
    ) -> Result<Response<SuggestLexiconArtifactDescriptor>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn suggest_prefix(
        &self,
        _req: Request<SuggestPrefixRequest>,
    ) -> Result<Response<SuggestPrefixResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn lookup_stem_surfaces(
        &self,
        _req: Request<LookupStemSurfacesRequest>,
    ) -> Result<Response<LookupStemSurfacesResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn spellcheck_query(
        &self,
        _req: Request<SpellcheckQueryRequest>,
    ) -> Result<Response<SpellcheckQueryResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn import_corpus_statistics(
        &self,
        req: Request<Streaming<ImportCorpusStatisticsRequest>>,
    ) -> Result<Response<CorpusStatisticsArtifactDescriptor>, Status> {
        self.verify_and_record(&req)?;
        let mut stream = req.into_inner();
        let mut language = String::new();
        let mut name = String::new();
        let mut doc_count = 0u64;
        let mut entries: Vec<(String, u64)> = Vec::new();
        while let Some(frame) = stream.next().await {
            match frame?.frame {
                Some(import_corpus_statistics_request::Frame::Header(h)) => {
                    language = h.language;
                    name = h.name;
                    doc_count = h.doc_count;
                }
                Some(import_corpus_statistics_request::Frame::Batch(b)) => {
                    for e in b.entries {
                        entries.push((e.term, e.doc_freq));
                    }
                }
                None => {}
            }
        }
        let idf = VocabularyCorpusIdf::from_entries(doc_count, entries)
            .map_err(Status::invalid_argument)?;
        let bytes = idf.to_bytes();
        let sha256 = turbo_sha256::sha256(&bytes).to_hex();
        self.recorded.lock().unwrap().compiled_idf_bytes = bytes.clone();
        Ok(Response::new(CorpusStatisticsArtifactDescriptor {
            artifact_id: format!("tcidf-{}", &sha256[..12]),
            language,
            name,
            doc_count: idf.doc_count(),
            term_count: idf.len() as u64,
            byte_size: bytes.len() as u64,
            sha256,
        }))
    }

    async fn list_corpus_statistics(
        &self,
        _req: Request<ListCorpusStatisticsRequest>,
    ) -> Result<Response<ListCorpusStatisticsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn train_voronoi_centroids(
        &self,
        req: Request<Streaming<TrainVoronoiCentroidsRequest>>,
    ) -> Result<Response<VoronoiCentroidArtifactDescriptor>, Status> {
        self.verify_and_record(&req)?;
        let mut stream = req.into_inner();
        let mut header = None;
        let mut samples: Vec<f32> = Vec::new();
        while let Some(frame) = stream.next().await {
            match frame?.frame {
                Some(train_voronoi_centroids_request::Frame::Header(h)) => {
                    header = Some(h);
                }
                Some(train_voronoi_centroids_request::Frame::Batch(b)) => {
                    samples.extend(b.values);
                }
                None => {}
            }
        }
        let h = header.ok_or_else(|| Status::invalid_argument("missing Voronoi header"))?;
        let dim = h.dim as usize;
        let k = h.num_centroids as usize;
        if dim == 0 || k == 0 || !samples.len().is_multiple_of(dim) {
            return Err(Status::invalid_argument("invalid Voronoi dimensions"));
        }
        let n_samples = samples.len() / dim;

        // Deterministic spherical k-means++ / Lloyd assignment producing `TCENT001` payload.
        let mut centroids: Vec<Vec<f32>> = (0..k)
            .map(|c| {
                let src = &samples[(c * (n_samples / k.max(1))) * dim..][..dim];
                src.to_vec()
            })
            .collect();
        let mut counts = vec![0u64; k];
        let mut cosine_sum = 0.0_f64;
        for _iter in 0..h.max_iters.clamp(1, 8) {
            counts.fill(0);
            cosine_sum = 0.0;
            let mut sums = vec![vec![0.0_f32; dim]; k];
            for row in samples.chunks_exact(dim) {
                let mut best_c = 0usize;
                let mut best_dot = f32::NEG_INFINITY;
                for (c, cent) in centroids.iter().enumerate() {
                    let dot: f32 = row.iter().zip(cent).map(|(a, b)| a * b).sum();
                    if dot > best_dot {
                        best_dot = dot;
                        best_c = c;
                    }
                }
                counts[best_c] += 1;
                cosine_sum += f64::from(best_dot);
                for (dst, &v) in sums[best_c].iter_mut().zip(row) {
                    *dst += v;
                }
            }
            for c in 0..k {
                let sq: f32 = sums[c].iter().map(|v| v * v).sum();
                if sq > 0.0 {
                    let inv = 1.0 / sq.sqrt();
                    for (dst, v) in centroids[c].iter_mut().zip(&sums[c]) {
                        *dst = v * inv;
                    }
                }
            }
        }
        let mut artifact = Vec::from(&b"TCENT001"[..]);
        artifact.extend_from_slice(&h.dim.to_le_bytes());
        artifact.extend_from_slice(&h.num_centroids.to_le_bytes());
        for cent in &centroids {
            for &v in cent {
                artifact.extend_from_slice(&v.to_le_bytes());
            }
        }
        let sha256 = turbo_sha256::sha256(&artifact).to_hex();
        Ok(Response::new(VoronoiCentroidArtifactDescriptor {
            artifact_id: format!("tcent-{}", &sha256[..12]),
            model_id: h.model_id,
            model_revision: h.model_revision,
            model_sha256: h.model_sha256,
            dim: h.dim,
            num_centroids: h.num_centroids,
            sample_count: n_samples as u64,
            mean_cosine: cosine_sum / (n_samples.max(1) as f64),
            byte_size: artifact.len() as u64,
            sha256,
            cluster_counts: counts,
        }))
    }

    async fn list_voronoi_centroids(
        &self,
        _req: Request<ListVoronoiCentroidsRequest>,
    ) -> Result<Response<ListVoronoiCentroidsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }
}
