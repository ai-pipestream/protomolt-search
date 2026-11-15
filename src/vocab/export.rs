//! Export of corpus document frequencies to `turbo-kserve`.

/// Streams collection-wide `(term, doc_freq)` statistics to `turbo-kserve`'s
/// `TurboVocabularyService::ImportCorpusStatistics`,
/// compiling a deterministic `TCIDF001` corpus IDF artifact under `Bulk` priority.
pub async fn export_corpus_statistics_to_kserve(
    client: &turbo_client::RemoteClient,
    language: &str,
    artifact_name: &str,
    doc_count: u64,
    term_doc_freqs: &[(String, u64)],
    finish_deadline_us: u64,
) -> Result<turbo_client::proto::vocabulary::CorpusStatisticsArtifactDescriptor, tonic::Status> {
    use turbo_client::proto::vocabulary::{
        import_corpus_statistics_request::Frame, CorpusStatisticsHeader, CorpusTermFrequencyBatch,
        CorpusTermFrequencyEntry, ImportCorpusStatisticsRequest,
    };

    let timeout = crate::turbo_bridge::remaining_deadline_duration(finish_deadline_us)?;
    let mut frames = Vec::with_capacity(1 + term_doc_freqs.len().div_ceil(1024));
    frames.push(ImportCorpusStatisticsRequest {
        frame: Some(Frame::Header(CorpusStatisticsHeader {
            language: language.to_string(),
            doc_count,
            name: artifact_name.to_string(),
        })),
    });
    for chunk in term_doc_freqs.chunks(1024) {
        frames.push(ImportCorpusStatisticsRequest {
            frame: Some(Frame::Batch(CorpusTermFrequencyBatch {
                entries: chunk
                    .iter()
                    .map(|(term, df)| CorpusTermFrequencyEntry {
                        term: term.clone(),
                        doc_freq: *df,
                    })
                    .collect(),
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
        .import_corpus_statistics(req)
        .await
        .map(|r| r.into_inner())
        .map_err(|s| crate::analyzer::inspect_remote_status(&client.config().endpoint, s))
}
