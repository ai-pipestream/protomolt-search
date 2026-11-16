use super::*;
use crate::pb::{AccessAction, AccessDecision, QueryCursorRoute};
use tonic::Code;

fn context() -> QueryCursorContext {
    QueryCursorContext {
        collection: "docs".into(),
        topology_generation: 9,
        access: Some(AccessDecision {
            field_permissions: None,
            document_visibility: None,
            policy_revision: 1,
            principal: "alice".into(),
            workspace: "work".into(),
            collection: "docs".into(),
            action: AccessAction::Search as i32,
        }),
        routes: vec![QueryCursorRoute {
            address: "node".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}
fn issued(signer: Arc<CursorSigner>) -> String {
    let mut binding = CursorBinding::prepare(
        signer,
        &mut QueryRequest {
            k: 2,
            ..Default::default()
        },
        context(),
    )
    .unwrap();
    let mut response = QueryResponse {
        next_cursor: "tvq1:2:00000000:7".into(),
        ..Default::default()
    };
    binding.bind_read_versions(&[]).unwrap();
    binding.finish(&mut response).unwrap();
    response.next_cursor
}

#[test]
fn read_versions_are_mandatory_ordered_and_cannot_be_rebound() {
    use crate::stats_identity::StatsClaim;
    let signer = Arc::new(CursorSigner::from_key([8; 32]));
    let query = || QueryRequest {
        k: 2,
        ..Default::default()
    };
    let claims = vec![
        StatsClaim::required(3, &[7; 32]).unwrap(),
        StatsClaim::required(4, &[9; 32]).unwrap(),
    ];
    let mut binding = CursorBinding::prepare(signer.clone(), &mut query(), context()).unwrap();
    let mut response = QueryResponse {
        next_cursor: "tvq1:2:00000000:7".into(),
        ..Default::default()
    };
    assert_eq!(
        binding.finish(&mut response).unwrap_err().code(),
        Code::Internal
    );
    binding.bind_read_versions(&claims).unwrap();
    binding.finish(&mut response).unwrap();
    let token = signer.open(&response.next_cursor).unwrap();
    assert_eq!(token.format_version, 2);
    assert_eq!(token.read_versions_sha256.len(), 32);
    assert!(!token
        .encode_to_vec()
        .windows(32)
        .any(|window| window == [7; 32] || window == [9; 32]));
    let mut reversed = claims.clone();
    reversed.reverse();
    assert!(binding.bind_read_versions(&reversed).is_err());
    for versions in [
        claims.clone(),
        reversed,
        vec![claims[0]],
        vec![StatsClaim::required(5, &[7; 32]).unwrap(), claims[1]],
        vec![StatsClaim::required(3, &[6; 32]).unwrap(), claims[1]],
        vec![StatsClaim::default(); 2],
    ] {
        let mut request = query();
        request.cursor = response.next_cursor.clone();
        // Static context still validates without contacting a shard.
        let mut resumed = CursorBinding::prepare(signer.clone(), &mut request, context()).unwrap();
        assert_eq!(
            resumed.bind_read_versions(&versions).is_ok(),
            versions == claims
        );
    }
}

#[test]
fn boundary_is_integrity_protected_and_metadata_is_not_a_credential() {
    let signer = Arc::new(CursorSigner::from_key([7; 32]));
    let token = issued(signer.clone());
    let mut request = QueryRequest {
        k: 2,
        cursor: token.clone(),
        request_id: "new trace".into(),
        profile: true,
        required_topology_generation: 9,
        ..Default::default()
    };
    CursorBinding::prepare(signer.clone(), &mut request, context()).unwrap();
    assert_eq!(request.cursor, "tvq1:2:00000000:7");
    for position in [PREFIX.len(), token.len() - 1] {
        let mut corrupt = token.clone().into_bytes();
        corrupt[position] = if corrupt[position] == b'0' {
            b'1'
        } else {
            b'0'
        };
        assert_eq!(
            signer
                .open(std::str::from_utf8(&corrupt).unwrap())
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }
    // Possessing the token never supplies the server's decision.
    let mut anonymous = context();
    anonymous.access = None;
    assert!(CursorBinding::prepare(
        signer.clone(),
        &mut QueryRequest {
            k: 2,
            cursor: token.clone(),
            ..Default::default()
        },
        anonymous
    )
    .is_err());
    let other = CursorSigner::from_key([8; 32]);
    assert_eq!(
        other.open(&token).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert!(!format!("{signer:?}").contains('7'));
}

#[test]
fn every_authority_and_route_component_is_bound() {
    let signer = Arc::new(CursorSigner::from_key([7; 32]));
    let token = issued(signer.clone());
    for change in 0..11 {
        let mut ctx = context();
        match change {
            0 => ctx.collection = "other".into(),
            1 => ctx.topology_generation += 1,
            2 => ctx.access.as_mut().unwrap().principal = "bob".into(),
            3 => ctx.access.as_mut().unwrap().workspace = "other".into(),
            4 => ctx.access.as_mut().unwrap().policy_revision += 1,
            5 => ctx.access.as_mut().unwrap().action = AccessAction::Admin as i32,
            6 => ctx.access.as_mut().unwrap().collection = "other".into(),
            7 => ctx.routes[0].address = "different node".into(),
            8 => ctx.routes[0].replica = Some("replica".into()),
            9 => {
                ctx.routes[0].hash_start = Some(0);
                ctx.routes[0].hash_end = Some(u64::MAX);
            }
            10 => ctx.routes[0].placement = Some(-1),
            _ => unreachable!(),
        }
        let result = CursorBinding::prepare(
            signer.clone(),
            &mut QueryRequest {
                k: 2,
                cursor: token.clone(),
                ..Default::default()
            },
            ctx,
        );
        assert!(
            matches!(result, Err(error) if error.code() == Code::FailedPrecondition),
            "context change {change}"
        );
    }
}

#[test]
fn decoder_refuses_old_malformed_oversized_and_noncanonical_tokens() {
    let signer = CursorSigner::from_key([7; 32]);
    for token in [
        "tvq1:1:00000000:0",
        "pqc1:",
        "pqc1:ff:00",
        "pqc1:gg:00000000",
    ] {
        assert_eq!(
            signer.open(token).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(
        signer
            .open(&"x".repeat(MAX_PAYLOAD * 2 + 100))
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let envelope = QueryCursorEnvelope {
        format_version: 2,
        read_versions_sha256: vec![2; 32],
        context_sha256: vec![1; 32],
        boundary: "b".into(),
    };
    for change in 0..5 {
        let mut envelope = envelope.clone();
        match change {
            0 => envelope.format_version = 1,
            1 => envelope.context_sha256.pop().map(|_| ()).unwrap(),
            2 => envelope.boundary.clear(),
            4 => {
                envelope.read_versions_sha256.pop();
            }
            _ => {}
        }
        let mut bytes = envelope.encode_to_vec();
        if change == 3 {
            bytes.extend([0x78, 1]);
        } // Unknown field, valid MAC.
        let token = format!(
            "{PREFIX}{}:{}",
            encode_hex(&bytes),
            crate::sha256::to_hex(&signer.tag(&bytes).unwrap())
        );
        assert_eq!(
            signer.open(&token).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    let oversized = QueryCursorEnvelope {
        boundary: "x".repeat(MAX_PAYLOAD),
        ..envelope
    };
    assert_eq!(
        signer.seal(oversized).unwrap_err().code(),
        Code::ResourceExhausted
    );
}

#[test]
fn entropy_failure_never_falls_back_to_a_predictable_key() {
    let signer = CursorSigner {
        key: OnceLock::from(Err("entropy test failure".into())),
    };
    assert_eq!(signer.tag(b"test").unwrap_err().code(), Code::Internal);
}
