//! Per-RPC metadata deadlines without changing the pooled channel's identity.

use tonic::Request;
use turbo_client::{
    proto::{grpc_inference_service_client::GrpcInferenceServiceClient, ModelMetadataRequest},
    BundleScoringFingerprint, BundleScoringSpec, IndexGenerationHandshake, RemoteClient,
    RemotePriority,
};

use crate::turbo_bridge::embed_deadline::EmbedDeadline;

pub(super) fn request(
    remote: &RemoteClient,
    model: &str,
    deadline: EmbedDeadline,
) -> turbo_client::Result<Request<ModelMetadataRequest>> {
    remote.prepare_request(
        ModelMetadataRequest {
            name: model.to_owned(),
            version: String::new(),
        },
        Some(RemotePriority::Interactive),
        deadline.remaining()?,
    )
}

pub(super) async fn verify(
    remote: &RemoteClient,
    model: &str,
    expected: &IndexGenerationHandshake,
    deadline: EmbedDeadline,
) -> turbo_client::Result<(BundleScoringSpec, BundleScoringFingerprint)> {
    let channel = remote.pool().channel(remote.config())?;
    let mut client =
        GrpcInferenceServiceClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);
    let metadata = client
        .model_metadata(request(remote, model, deadline)?)
        .await?
        .into_inner();
    let property = |key: &str| metadata.properties.get(key).cloned().unwrap_or_default();
    let spec = BundleScoringSpec {
        model_id: property("model_info.model_id"),
        revision: property("model_info.revision"),
        manifest_sha256: property("model_info.manifest_sha256"),
        task: property("model_info.task"),
        dim: property("model_info.dim").parse().unwrap_or(0),
        pooling: property("model_info.pooling"),
        normalize: property("model_info.normalize"),
    };
    let fingerprint = spec.fingerprint();
    expected.verify_bundle(&fingerprint)?;
    Ok((spec, fingerprint))
}
