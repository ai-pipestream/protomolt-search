//! Query stream identity tests.

use super::*;
use crate::pb::{DocumentIdentity, QueryStreamIdentityState as State, QueryStreamPhase as Phase};

#[test]
fn fingerprint_is_reproducible_from_the_protobuf_revision() {
    use prost::Message;
    for score in [0.0, -0.0, f32::MIN_POSITIVE, f32::MAX] {
        let revision = query_stream_revision(
            1,
            Phase::Dense,
            vec![(9, score, None)],
            String::new(),
            State::Resolved,
        );
        let decoded =
            crate::pb::QueryStreamRevision::decode(revision.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            decoded.content_fingerprint,
            query_stream_content_fingerprint(Phase::Dense, &decoded.hits, State::Resolved),
            "score bits {}",
            score.to_bits()
        );
    }
}

#[test]
fn fingerprints_bind_disclosed_identity_presence_and_score_bits() {
    let identity = DocumentIdentity {
        document_key: vec![0, 255],
        version: u64::MAX,
        chunk_ordinal: Some(0),
    };
    let make = |identity, score, state| {
        query_stream_revision(
            1,
            Phase::Dense,
            vec![(9, score, identity)],
            String::new(),
            state,
        )
    };
    let base = make(Some(identity.clone()), 0.0, State::Resolved);
    let mut changed_key = identity.clone();
    changed_key.document_key.push(0);
    let mut changed_version = identity.clone();
    changed_version.version -= 1;
    let mut absent_ordinal = identity.clone();
    absent_ordinal.chunk_ordinal = None;
    for revision in [
        make(Some(changed_key.clone()), 0.0, State::Resolved),
        make(Some(changed_version), 0.0, State::Resolved),
        make(Some(absent_ordinal), 0.0, State::Resolved),
        make(None, 0.0, State::Resolved),
        make(Some(identity.clone()), f32::MIN_POSITIVE, State::Resolved),
    ] {
        assert_ne!(base.content_fingerprint, revision.content_fingerprint);
    }
    assert_eq!(
        base.content_fingerprint,
        make(Some(identity.clone()), -0.0, State::Resolved).content_fingerprint
    );
    let hidden = make(Some(identity.clone()), 0.0, State::Withheld);
    assert!(hidden.hits[0].identity.is_none());
    assert_eq!(
        hidden.content_fingerprint,
        make(Some(changed_key), 0.0, State::Withheld).content_fingerprint
    );
    assert_ne!(
        hidden.content_fingerprint,
        make(None, 0.0, State::Resolved).content_fingerprint
    );
    assert_ne!(
        hidden.content_fingerprint,
        make(None, 0.0, State::Unspecified).content_fingerprint
    );
    assert_eq!(
        base.content_fingerprint,
        query_stream_revision(
            99,
            Phase::Dense,
            vec![(9, 0.0, Some(identity))],
            "later-score-certificate".into(),
            State::Resolved
        )
        .content_fingerprint
    );
}
