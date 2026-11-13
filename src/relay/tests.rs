use super::*;

fn tuple(epochs: &[u64]) -> TokenTuple {
    TokenTuple {
        collection: String::new(),
        control_revision: 0,
        topology_generation: 0,
        children: vec!["a".into(), "b".into()],
        epochs: epochs
            .iter()
            .map(|&epoch| {
                if epoch == 0 {
                    StatsClaim::default()
                } else {
                    StatsClaim::required(epoch, &[1; 32]).unwrap()
                }
            })
            .collect(),
    }
}

/// A fixed map over two named children, for the receipt checks
/// that need a relay and no network.
struct FixedMap(MapSnapshot);

impl MapSource for FixedMap {
    fn current(&self) -> MapSnapshot {
        self.0.clone()
    }

    fn changes(&self) -> watch::Receiver<u64> {
        let (tx, rx) = watch::channel(self.0.control_revision);
        std::mem::forget(tx);
        rx
    }
}

fn fixed_relay() -> RelayService {
    let children = vec!["a".to_string(), "b".to_string()];
    let routes = children
        .iter()
        .map(|addr| TopologyRoute {
            addr: addr.clone(),
            replica: None,
            hash_range: None,
            placement: None,
        })
        .collect();
    let map = MapSnapshot {
        control_revision: 3,
        topology_generation: 3,
        map: Arc::new(RelayMap {
            routes,
            placement: None,
        }),
    };
    RelayService::with_map(
        Arc::new(CoordinatorServiceImpl::new(children)),
        Arc::new(FixedMap(map)),
    )
}

fn child_receipt(epoch: u64, fingerprint: &[u8]) -> crate::pb::VectorReadReceipt {
    crate::pb::VectorReadReceipt {
        vector_binding: None,
        stats_epoch: epoch,
        stats_incarnation: vec![7; 32],
        visibility_fingerprint: fingerprint.to_vec(),
        visibility_columns_known: Vec::new(),
    }
}

/// The receipt every composed route folds its children's read views
/// through: a child echoing another visibility fingerprint, or a
/// version other than the claim the parent's token stands for,
/// refuses by name; agreeing children yield one relay token.
#[test]
fn a_read_receipt_refuses_a_child_off_the_scope_or_off_the_claim() {
    let relay = fixed_relay();
    let (pinned, _) = relay.pin();
    let children = relay.children();
    let scope = crate::visibility::VisibilityScope::new(None).unwrap();
    let fingerprint = scope.fingerprint().to_vec();
    let good = [
        child_receipt(4, &fingerprint),
        child_receipt(9, &fingerprint),
    ];
    let receipt = relay
        .read_receipt(&pinned, children.clone(), None, None, &good)
        .expect("agreeing children");
    assert_ne!(receipt.stats_epoch, 0);
    assert_eq!(receipt.stats_incarnation.len(), 32);
    assert_eq!(receipt.visibility_fingerprint, fingerprint);
    let claims = relay.translate_epoch(receipt.stats_epoch).unwrap();
    assert_eq!(
        claims.iter().map(|c| c.epoch).collect::<Vec<_>>(),
        vec![4, 9],
        "the token stands for each child's version"
    );
    // Under the token's claims the same children pass; a child at
    // another version is a changed read.
    relay
        .read_receipt(&pinned, children.clone(), None, Some(&claims), &good)
        .expect("the claimed versions");
    let moved = [
        child_receipt(4, &fingerprint),
        child_receipt(10, &fingerprint),
    ];
    let err = relay
        .read_receipt(&pinned, children.clone(), None, Some(&claims), &moved)
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("read version changed"),
        "{}",
        err.message()
    );
    // A child echoing another fingerprint answered another scope.
    let mut other = fingerprint.clone();
    other.push(1);
    let off_scope = [child_receipt(4, &fingerprint), child_receipt(9, &other)];
    let err = relay
        .read_receipt(&pinned, children.clone(), None, None, &off_scope)
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "{}",
        err.message()
    );
    // A child that did not answer leaves the receipt unfounded.
    let err = relay
        .read_receipt(&pinned, children, None, None, &good[..1])
        .unwrap_err();
    assert!(err.message().contains("every child"), "{}", err.message());
}

#[test]
fn tokens_repeat_for_one_tuple_and_move_with_an_epoch() {
    let mut registry = TokenRegistry::new(7);
    let t1 = registry.allocate(tuple(&[1, 1])).unwrap();
    assert_ne!(t1, 0);
    assert_eq!(t1 >> 32, 7, "the incarnation is the high half");
    assert_eq!(
        registry.allocate(tuple(&[1, 1])).unwrap(),
        t1,
        "same tuple, same token"
    );
    let t2 = registry.allocate(tuple(&[2, 1])).unwrap();
    assert_ne!(t2, t1);
    assert_eq!(registry.lookup(t1).unwrap().epochs, tuple(&[1, 1]).epochs);
    assert_eq!(registry.lookup(t2).unwrap().epochs, tuple(&[2, 1]).epochs);
    assert!(registry.lookup(t1 + 1000).is_none());
}

#[test]
fn token_exhaustion_refuses_instead_of_reusing_an_old_token() {
    let mut registry = TokenRegistry::new(1);
    let issued = registry.allocate(tuple(&[1, 1])).unwrap();
    registry.counter = u32::MAX;
    assert_eq!(registry.allocate(tuple(&[1, 1])).unwrap(), issued);
    assert_eq!(
        registry.allocate(tuple(&[2, 1])).unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        registry.lookup(issued).unwrap().epochs,
        tuple(&[1, 1]).epochs
    );
}

#[test]
fn retention_is_bounded() {
    let mut registry = TokenRegistry::new(1);
    let first = registry.allocate(tuple(&[0, 0])).unwrap();
    for i in 1..=(RETAINED_TOKENS as u64) {
        registry.allocate(tuple(&[i, 0])).unwrap();
    }
    assert!(
        registry.lookup(first).is_none(),
        "the oldest token is forgotten"
    );
    assert_eq!(registry.entries.len(), RETAINED_TOKENS);
}

#[test]
fn grpc_timeouts_parse() {
    let mut m = tonic::metadata::MetadataMap::new();
    assert_eq!(grpc_timeout(&m), None);
    m.insert("grpc-timeout", "1500m".parse().unwrap());
    assert_eq!(grpc_timeout(&m), Some(Duration::from_millis(1500)));
    m.insert("grpc-timeout", "2S".parse().unwrap());
    assert_eq!(grpc_timeout(&m), Some(Duration::from_secs(2)));
    m.insert("grpc-timeout", "3H".parse().unwrap());
    assert_eq!(grpc_timeout(&m), Some(Duration::from_secs(10800)));
    m.insert("grpc-timeout", "bogus".parse().unwrap());
    assert_eq!(grpc_timeout(&m), None);
}

fn share(doc_count: u64, dfs: &[u32], known: bool, positions: bool) -> TermStatsResponse {
    TermStatsResponse {
        version_only: false,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
        doc_count,
        total_doc_length: doc_count * 10,
        doc_frequencies: dfs.to_vec(),
        field_stats: vec![crate::pb::FieldStats {
            total_doc_length: doc_count,
            doc_frequencies: dfs.to_vec(),
            known,
            positions,
            sentences: false,
        }],
        stats_epoch: 3,
        stats_incarnation: vec![1; 32],
    }
}

fn request() -> TermStatsRequest {
    TermStatsRequest {
        version_only: false,
        visibility: None,
        terms: vec!["court".into(), "opinion".into()],
        fields: vec![crate::pb::FieldTerms {
            field: "title".into(),
            terms: vec!["court".into(), "opinion".into()],
        }],
    }
}

#[test]
fn version_probes_refuse_legacy_mixed_or_nonempty_statistics() {
    let request = TermStatsRequest {
        version_only: true,
        ..Default::default()
    };
    let probe = TermStatsResponse {
        version_only: true,
        stats_epoch: 3,
        stats_incarnation: vec![1; 32],
        ..Default::default()
    };
    let merged = merge_term_stats(&request, &[probe.clone(), probe.clone()]).unwrap();
    assert!(merged.version_only);
    assert_eq!(merged.doc_count, 0);
    for case in 0..5 {
        let mut malformed = probe.clone();
        match case {
            0 => malformed.version_only = false,
            1 => malformed.doc_count = 1,
            2 => malformed.total_doc_length = 1,
            3 => malformed.doc_frequencies.push(0),
            _ => malformed.field_stats.push(Default::default()),
        }
        assert_eq!(
            merge_term_stats(&request, &[probe.clone(), malformed])
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert_eq!(
        merge_term_stats(&TermStatsRequest::default(), std::slice::from_ref(&probe))
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    for fields in [false, true] {
        let mut malformed = request.clone();
        if fields {
            malformed.fields.push(Default::default());
        } else {
            malformed.terms.push("term".into());
        }
        assert_eq!(
            merge_term_stats(&malformed, std::slice::from_ref(&probe))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn term_stats_sum_and_carry_the_shared_flags() {
    let merged = merge_term_stats(
        &request(),
        &[
            share(10, &[3, 0], true, true),
            share(5, &[1, 2], true, true),
        ],
    )
    .unwrap();
    assert_eq!(merged.doc_count, 15);
    assert_eq!(merged.total_doc_length, 150);
    assert_eq!(merged.doc_frequencies, vec![4, 2]);
    assert_eq!(merged.field_stats[0].doc_frequencies, vec![4, 2]);
    assert_eq!(merged.field_stats[0].total_doc_length, 15);
    assert!(merged.field_stats[0].known && merged.field_stats[0].positions);
    assert_eq!(merged.stats_epoch, 0, "the caller allocates the token");
}

#[test]
fn a_u32_document_frequency_past_the_contract_refuses_by_name() {
    let err = merge_term_stats(
        &request(),
        &[
            share(1, &[3_000_000_000, 0], true, true),
            share(1, &[2_000_000_000, 0], true, true),
        ],
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("\"court\""), "{}", err.message());
    assert!(err.message().contains("u32"), "{}", err.message());
    assert!(err.message().contains("child 1"), "{}", err.message());
}

#[test]
fn mixed_field_capabilities_refuse_by_name() {
    let err = merge_term_stats(
        &request(),
        &[
            share(1, &[1, 0], true, true),
            share(1, &[1, 0], true, false),
        ],
    )
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("\"title\""), "{}", err.message());
    assert!(err.message().contains("positions"), "{}", err.message());
    let err = merge_term_stats(
        &request(),
        &[
            share(1, &[1, 0], false, false),
            share(1, &[1, 0], true, false),
        ],
    )
    .unwrap_err();
    assert!(err.message().contains("known"), "{}", err.message());
    assert!(merge_term_stats(&request(), &[]).is_err());
}

fn report(slot_offset: u64, num_vectors: u64, document_slots: u64) -> HealthResponse {
    HealthResponse {
        slot_offset,
        num_vectors,
        bm25_docs: num_vectors,
        document_slots,
        dim: 4,
        scoring_fingerprint: "fp".into(),
        vector_backend: "tv".into(),
        exact_vectors_available: true,
        exact_vectors_mmap: true,
        wal_clocked: true,
        wal_generation: 9,
        ..Default::default()
    }
}

#[test]
fn health_merges_contiguous_children_and_refuses_gaps() {
    let children = vec!["a".to_string(), "b".to_string()];
    let merged = merge_health("", &children, &[report(100, 50, 50), report(0, 100, 100)]).unwrap();
    assert_eq!(merged.slot_offset, 0);
    assert_eq!(merged.num_vectors, 150);
    assert_eq!(merged.document_slots, 150);
    assert_eq!(merged.scoring_fingerprint, "fp");
    assert!(
        !merged.wal_clocked && merged.wal_generation == 0,
        "a relay has no WAL"
    );

    let err = merge_health("", &children, &[report(0, 100, 100), report(105, 5, 5)]).unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("gap"), "{}", err.message());
    assert!(err.message().contains("0..100"), "{}", err.message());

    let err = merge_health("", &children, &[report(0, 100, 100), report(90, 5, 5)]).unwrap_err();
    assert!(err.message().contains("overlaps"), "{}", err.message());

    let mut other = report(100, 1, 1);
    other.scoring_fingerprint = "other".into();
    let err = merge_health("", &children, &[report(0, 100, 100), other]).unwrap_err();
    assert!(
        err.message().contains("scoring_fingerprint"),
        "{}",
        err.message()
    );
}
