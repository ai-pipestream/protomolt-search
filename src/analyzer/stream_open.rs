//! Opening an [`AnalyzeStream`] against each backend.

#[cfg(feature = "net")]
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
#[cfg(feature = "net")]
use turbo_client::proto::result::{
    analyze_stream_request, AnalyzeOrder, AnalyzeStreamHeader, AnalyzeStreamRequest,
    CapabilitiesRequest,
};

use super::native::{native_analysis_with_spec, native_dual_with_spec};
#[cfg(feature = "net")]
use super::pool::remote_client_for;
#[cfg(feature = "net")]
use super::remote::{analysis_client, RemoteLink};
use super::spec::validate_dual_cased_spec;
use super::spec_convert::{native_spec, turbo_field_config};
#[cfg(not(feature = "net"))]
use super::status::sidecar_needs_net;
#[cfg(feature = "net")]
use super::status::{client_err_to_status, inspect_remote_client_err, inspect_remote_status};
#[cfg(feature = "net")]
use super::stream::RemoteStreamState;
use super::stream::{
    AnalyzeRequests, AnalyzeResponses, AnalyzeStream, AnalyzeSubmit, InProcessItem,
    InProcessRequest, InProcessResponse, SUBMIT_BUFFER,
};
use super::turbo::{shared_turbo_nlp, turbo_analysis_pass};
#[cfg(feature = "net")]
use super::wire;
use super::{AnalysisBackend, RemoteCallOptions, SessionLayers};
use crate::pb::AnalysisSpec;
use crate::vocab::VocabularyListener;

/// Refuses layers the server at `link` cannot serve: entity or geography
/// columns without an NER model, and a cased field without the dual term
/// identity.
#[cfg(feature = "net")]
async fn preflight_capabilities(link: RemoteLink<'_>, layers: SessionLayers) -> Result<(), Status> {
    let addr = link.addr;
    let cap_req = link
        .remote
        .prepare_request(
            CapabilitiesRequest {},
            Some(turbo_client::RemotePriority::Interactive),
            link.timeout,
        )
        .map_err(client_err_to_status)?;
    let capabilities = link
        .client
        .capabilities(cap_req)
        .await
        .map_err(|s| inspect_remote_status(addr, s))?
        .into_inner();
    let ner_available = capabilities.profiles.iter().any(|p| {
        p.layers
            .iter()
            .any(|l| l == "turbo:entities" || l == "turbo:geo")
    });
    if (layers.geography || layers.entities) && !ner_available {
        return Err(Status::failed_precondition(
            "entity or geography columns were requested but this analysis server has no NER model configured (Capabilities.ner_available = false); configure an NER model or disable those columns",
        ));
    }
    let dual_available = !capabilities
        .profiles
        .iter()
        .any(|p| p.name == wire::NO_DUAL_CASED_PROFILE);
    if layers.dual_cased && !dual_available {
        return Err(Status::failed_precondition(format!(
            "a cased field was requested but the analysis server at {addr} does not serve the \
             dual term identity (dual_term_identity_available = false); \
             upgrade turbo-kserve or drop cased_field"
        )));
    }
    Ok(())
}

impl AnalyzeStream {
    /// Opens an [`AnalyzeStream`] session with explicit [`RemoteCallOptions`]
    /// (defaulting to `RemotePriority::Bulk` for ingest/replay streams).
    pub async fn open_with_options(
        addr: &str,
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<VocabularyListener>>,
        layers: SessionLayers,
        options: &RemoteCallOptions,
    ) -> Result<Self, Status> {
        match AnalysisBackend::parse(addr)? {
            AnalysisBackend::Native => return Self::open_native(spec, vocab, layers),
            AnalysisBackend::Turbo => return Self::open_turbo(spec, vocab, layers),
            AnalysisBackend::Sidecar(_) => {}
        }
        #[cfg(not(feature = "net"))]
        {
            let _ = (spec, vocab, layers, options);
            Err(sidecar_needs_net(addr))
        }
        #[cfg(feature = "net")]
        {
            Self::open_remote(addr, spec, vocab, layers, options).await
        }
    }

    #[cfg(feature = "net")]
    async fn open_remote(
        addr: &str,
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<VocabularyListener>>,
        layers: SessionLayers,
        options: &RemoteCallOptions,
    ) -> Result<Self, Status> {
        if layers.dual_cased {
            validate_dual_cased_spec(spec)?;
        }
        let remote = remote_client_for(addr, options)?;
        let mut client = analysis_client(&remote)?;
        if layers.geography || layers.entities || layers.dual_cased {
            let link = RemoteLink {
                addr,
                remote: &remote,
                client: &mut client,
                timeout: options.timeout,
            };
            preflight_capabilities(link, layers).await?;
        }

        let session = wire::WireSessionOptions {
            spec: spec.cloned(),
            layers,
        };
        let binding = session.to_binding();
        let (tx, feed) = tokio::sync::mpsc::channel(SUBMIT_BUFFER);
        tx.try_send(AnalyzeStreamRequest {
            kind: Some(analyze_stream_request::Kind::Header(AnalyzeStreamHeader {
                order: AnalyzeOrder::Completion as i32,
                max_inflight: 0,
                batch_linger_ms: 0,
            })),
        })
        .expect("fresh channel has capacity");

        let stream_req = remote
            .prepare_request(
                ReceiverStream::new(feed),
                Some(options.priority()),
                options.timeout,
            )
            .map_err(|e| inspect_remote_client_err(addr, e))?;
        let proj_client = client.clone();
        let responses = client
            .analyze_stream(stream_req)
            .await
            .map_err(|s| inspect_remote_status(addr, s))?
            .into_inner();
        Ok(Self {
            submit: Some(AnalyzeSubmit {
                requests: AnalyzeRequests::Remote { tx, binding },
            }),
            responses: AnalyzeResponses::Remote(Box::new(RemoteStreamState {
                addr: addr.to_string(),
                remote,
                client: proj_client,
                timeout: options.timeout,
                responses,
            })),
            vocab,
            spec: spec.cloned(),
            layers,
        })
    }

    fn open_native(
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
    ) -> Result<Self, Status> {
        let sidecar_only = SessionLayers {
            sentences: false,
            dual_cased: false,
            ..layers
        };
        if sidecar_only != SessionLayers::default() {
            let mut requested = Vec::new();
            if layers.quality {
                requested.push("quality");
            }
            if layers.geography {
                requested.push("geography");
            }
            if layers.entities {
                requested.push("entities");
            }
            return Err(Status::failed_precondition(format!(
                "native lexical analysis does not provide {} layers; configure --analysis-addr=turbo or turbo-kserve for this ingest",
                requested.join(", ")
            )));
        }
        if layers.dual_cased {
            validate_dual_cased_spec(spec)?;
        }
        let dual_cased = layers.dual_cased;
        let parsed_spec = std::sync::Arc::new(native_spec(spec)?);
        let (requests, mut feed) = tokio::sync::mpsc::channel::<InProcessRequest>(SUBMIT_BUFFER);
        let (emit, responses) = tokio::sync::mpsc::channel::<InProcessResponse>(SUBMIT_BUFFER);
        tokio::spawn(async move {
            while let Some(request) = feed.recv().await {
                let sequence = request.sequence;
                let spec_clone = parsed_spec.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let native = if dual_cased {
                        native_dual_with_spec(&request.text, &spec_clone)?
                    } else {
                        native_analysis_with_spec(&request.text, &spec_clone)?
                    };
                    Ok(InProcessItem {
                        doc: native.doc,
                        surface_tokens: native.tokens,
                    })
                })
                .await
                .unwrap_or_else(|error| {
                    Err(Status::internal(format!(
                        "native analysis worker failed: {error}"
                    )))
                });
                if emit
                    .send(InProcessResponse { sequence, result })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Self {
            submit: Some(AnalyzeSubmit {
                requests: AnalyzeRequests::InProcess(requests),
            }),
            responses: AnalyzeResponses::InProcess(responses),
            vocab,
            spec: spec.cloned(),
            layers,
        })
    }

    fn open_turbo(
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
    ) -> Result<Self, Status> {
        if layers.dual_cased {
            validate_dual_cased_spec(spec)?;
        }
        let _ = turbo_field_config(spec)?;
        if layers.quality || layers.geography || layers.entities {
            let _ = shared_turbo_nlp()?;
        }
        let owned_spec = spec.cloned();
        let (requests, mut feed) = tokio::sync::mpsc::channel::<InProcessRequest>(SUBMIT_BUFFER);
        let (emit, responses) = tokio::sync::mpsc::channel::<InProcessResponse>(SUBMIT_BUFFER);
        tokio::spawn(async move {
            while let Some(request) = feed.recv().await {
                let sequence = request.sequence;
                let spec_clone = owned_spec.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let out = turbo_analysis_pass(&request.text, spec_clone.as_ref(), layers)?;
                    Ok(InProcessItem {
                        doc: out.doc,
                        surface_tokens: out.surface_tokens,
                    })
                })
                .await
                .unwrap_or_else(|error| {
                    Err(Status::internal(format!(
                        "turbo in-process analysis worker failed: {error}"
                    )))
                });
                if emit
                    .send(InProcessResponse { sequence, result })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Self {
            submit: Some(AnalyzeSubmit {
                requests: AnalyzeRequests::InProcess(requests),
            }),
            responses: AnalyzeResponses::InProcess(responses),
            vocab,
            spec: spec.cloned(),
            layers,
        })
    }
}
