use super::*;

#[test]
fn a_store_whose_control_rows_disagree_with_the_headers_refuses_to_open() {
    let authority = identity(7);
    type Tamper = fn(&redb::WriteTransaction);
    let tamper: [(&str, Tamper); 4] = [
        ("format", |tx| {
            let mut meta = tx.open_table(META).unwrap();
            let mut header: ControlStoreHeader =
                contract::decode(meta.get(CONTROL_HEADER).unwrap().unwrap().value()).unwrap();
            header.format_version = 2;
            meta.insert(CONTROL_HEADER, header.encode_to_vec().as_slice())
                .unwrap();
        }),
        ("count", |tx| {
            let mut meta = tx.open_table(META).unwrap();
            let mut header: ControlStoreHeader =
                contract::decode(meta.get(CONTROL_HEADER).unwrap().unwrap().value()).unwrap();
            header.node_count += 1;
            meta.insert(CONTROL_HEADER, header.encode_to_vec().as_slice())
                .unwrap();
        }),
        ("orphan fact", |tx| {
            let mut nodes = tx.open_table(NODES).unwrap();
            let k = ControlNodeKey {
                key: Some(key("music")),
                node_id: "ghost".into(),
            };
            nodes
                .insert(k.encode_to_vec().as_slice(), [].as_slice())
                .unwrap();
        }),
        ("extra table", |tx| {
            tx.open_table(redb::TableDefinition::<&[u8], &[u8]>::new("stray"))
                .unwrap();
        }),
    ];
    for (name, apply) in tamper {
        let dir = Directory::new(&format!("tamper-{name}"));
        let store = store(&dir, 7, &limits(64 << 10, 64));
        install(&store, &authority, state(2), 1);
        {
            let tx = store.inner.database().begin_write().unwrap();
            apply(&tx);
            tx.commit().unwrap();
        }
        drop(store);
        let error = SourceAuthorityStore::open(&dir.authority(), &authority)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::DataLoss, "{name}: {error}");
    }
}

fn recovery_exact_value<T: Message + Default>(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    mutate: impl FnOnce(&mut T),
) {
    let mut table = tx.open_table(definition).unwrap();
    let (key, bytes) = {
        let entry = table.iter().unwrap().next().unwrap().unwrap();
        (entry.0.value().to_vec(), entry.1.value().to_vec())
    };
    let mut value: T = contract::decode(&bytes).unwrap();
    mutate(&mut value);
    let changed = value.encode_to_vec();
    assert_eq!(
        changed.len(),
        bytes.len(),
        "substitution must preserve payload accounting"
    );
    table.insert(key.as_slice(), changed.as_slice()).unwrap();
}
fn recovery_exact_key<T: Message + Default>(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    mutate: impl FnOnce(&mut T),
) {
    let mut table = tx.open_table(definition).unwrap();
    let (key, value) = {
        let entry = table.iter().unwrap().next().unwrap().unwrap();
        (entry.0.value().to_vec(), entry.1.value().to_vec())
    };
    let mut decoded: T = contract::decode(&key).unwrap();
    mutate(&mut decoded);
    let changed = decoded.encode_to_vec();
    assert_eq!(changed.len(), key.len());
    table.remove(key.as_slice()).unwrap();
    assert!(table
        .insert(changed.as_slice(), value.as_slice())
        .unwrap()
        .is_none());
}
fn recovery_exact_tamper(name: &str, mutate: impl FnOnce(&redb::WriteTransaction)) {
    let dir = Directory::new(name);
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    install(&store, &authority, state(2), 1);
    let before = table_bytes(&store);
    {
        let tx = store.inner.database().begin_write().unwrap();
        mutate(&tx);
        tx.commit().unwrap();
    }
    // The original accepted commands, receipts, headers and declared accounting
    // are untouched. Equal-size tests also keep every table's byte total.
    let after = table_bytes(&store);
    for name in [META.name(), INSTALL_OPERATIONS.name()] {
        assert_eq!(
            before.iter().find(|(n, _)| n == name),
            after.iter().find(|(n, _)| n == name)
        );
    }
    drop(store);
    let error = SourceAuthorityStore::open(&dir.authority(), &authority)
        .err()
        .unwrap_or_else(|| panic!("recovery accepted substituted {name}"));
    assert_eq!(error.code(), Code::DataLoss, "{name}: {error}");
}

#[test]
fn recovery_exact_topology_current_address_substitution() {
    recovery_exact_tamper("current-topology", |tx| {
        let mut table = tx.open_table(TOPOLOGIES).unwrap();
        let key = ControlTopologyKey {
            key: Some(key("books")),
            generation: 3,
        }
        .encode_to_vec();
        let old = table.get(key.as_slice()).unwrap().unwrap().value().to_vec();
        let mut row: ControlTopology = contract::decode(&old).unwrap();
        row.routes[0].addr.replace_range(0..1, "x");
        let changed = row.encode_to_vec();
        assert_eq!(changed.len(), old.len());
        table.insert(key.as_slice(), changed.as_slice()).unwrap();
    });
}
#[test]
fn recovery_exact_topology_history_address_substitution() {
    recovery_exact_tamper("history-topology", |tx| {
        recovery_exact_value::<ControlTopology>(tx, TOPOLOGIES, |row| {
            row.routes[0].addr.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_node_value_substitution() {
    recovery_exact_tamper("node-value", |tx| {
        recovery_exact_value::<ControlNode>(tx, NODES, |row| {
            row.node.as_mut().unwrap().addr.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_node_key_substitution() {
    recovery_exact_tamper("node-key", |tx| {
        recovery_exact_key::<ControlNodeKey>(tx, NODES, |row| row.node_id.replace_range(0..1, "x"))
    });
}
#[test]
fn recovery_exact_replica_value_substitution() {
    recovery_exact_tamper("replica-value", |tx| {
        recovery_exact_value::<ControlReplica>(tx, REPLICAS, |row| {
            row.replica
                .as_mut()
                .unwrap()
                .analysis_fingerprint
                .replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_replica_key_substitution() {
    recovery_exact_tamper("replica-key", |tx| {
        recovery_exact_key::<ControlReplicaKey>(tx, REPLICAS, |row| {
            row.replica_key.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_action_value_substitution() {
    recovery_exact_tamper("action-value", |tx| {
        recovery_exact_value::<ControlAction>(tx, ACTIONS, |row| {
            row.action.as_mut().unwrap().reason.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_action_key_substitution() {
    recovery_exact_tamper("action-key", |tx| {
        recovery_exact_key::<ControlActionKey>(tx, ACTIONS, |row| row.action_id -= 1)
    });
}
#[test]
fn recovery_exact_completed_action_key_substitution() {
    recovery_exact_tamper("completed-key", |tx| {
        recovery_exact_key::<ControlActionKey>(tx, COMPLETED, |row| row.action_id += 1)
    });
}
#[test]
fn recovery_exact_configuration_provider_substitution() {
    recovery_exact_tamper("provider-config", |tx| {
        recovery_exact_value::<ControlCollectionState>(tx, CONTROL, |row| {
            if let Some(Provider::Geometry(geometry)) =
                row.configuration.as_mut().unwrap().provider.as_mut()
            {
                geometry.config_payload[0] ^= 1;
            } else {
                panic!("expected provider geometry");
            }
        })
    });
}
#[test]
fn recovery_exact_configuration_policy_substitution() {
    recovery_exact_tamper("planner-config", |tx| {
        recovery_exact_value::<ControlCollectionState>(tx, CONTROL, |row| {
            row.configuration
                .as_mut()
                .unwrap()
                .policy
                .as_mut()
                .unwrap()
                .reconcile
                .as_mut()
                .unwrap()
                .lease_ms -= 1
        })
    });
}
#[test]
fn recovery_exact_allocator_substitution() {
    recovery_exact_tamper("allocator", |tx| {
        recovery_exact_value::<ControlCollectionState>(tx, CONTROL, |row| row.next_token -= 1)
    });
}
#[test]
fn recovery_exact_missing_or_extra_fact_refuses() {
    for extra in [false, true] {
        recovery_exact_tamper(if extra { "extra" } else { "missing" }, |tx| {
            let mut table = tx.open_table(COMPLETED).unwrap();
            let key = ControlActionKey {
                key: Some(key("books")),
                action_id: if extra { 10 } else { 3 },
            }
            .encode_to_vec();
            if extra {
                table
                    .insert(
                        key.as_slice(),
                        ControlCompletedAction { format_version: 1 }
                            .encode_to_vec()
                            .as_slice(),
                    )
                    .unwrap();
            } else {
                table.remove(key.as_slice()).unwrap();
            }
        });
    }
}

#[test]
fn recovery_exact_valid_install_survives_later_superseding_capacity_and_policy_updates() {
    let dir = Directory::new("later-control-updates");
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    install(&store, &authority, state(2), 1);
    let initial = store.control_snapshot("alice", &key("books")).unwrap();
    let mut configuration = CapacityConfiguration {
        format_version: 1,
        cohort_length_ms: 60_000,
        cohort_phase_unix_ms: 0,
        max_total_records: 64,
        max_total_bytes: 1 << 20,
        max_registered_nodes: 8,
        policy: Some(CapacityTierPolicy {
            format_version: 1,
            tiers: vec![CapacityTierSpec {
                name: "first".into(),
                residency: CapacityResidency::Server as i32,
                min_replicas: 1,
                scans_per_byte_nanos_lo: 0,
                scans_per_byte_nanos_hi: 1_000_000_000,
                max_seconds_since_scan: 3600,
            }],
        }),
    };
    for (revision, id) in [(2, "first-config"), (3, "latest-config")] {
        configuration.policy.as_mut().unwrap().tiers[0].name = id.into();
        let accepted = store
            .configure_capacity(
                "alice",
                &CapacityConfigureCommand {
                    format_version: 1,
                    authority: Some(authority.clone()),
                    key: Some(key("books")),
                    command_id: id.as_bytes().to_vec(),
                    expected_control_revision: revision,
                    expected_policy_revision: 1,
                    configuration: Some(configuration.clone()),
                },
            )
            .unwrap();
        assert_eq!(accepted.code, 0, "{}", accepted.message);
    }
    let policy = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: b"later-grants".to_vec(),
        expected_control_revision: 4,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![grant("alice", "books")],
        })),
    };
    assert_eq!(store.execute("alice", &policy).unwrap().code, 0);
    let bytes = table_bytes(&store);
    let expected = store.control_snapshot("alice", &key("books")).unwrap();
    assert_eq!(expected.state, initial.state);
    assert_eq!(expected.control_revision, 5);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(
        reopened.control_snapshot("alice", &key("books")).unwrap(),
        expected
    );
    assert_eq!(
        reopened
            .capacity_state("alice", &key("books"))
            .unwrap()
            .configuration,
        Some(configuration)
    );
    assert_eq!(
        table_bytes(&reopened),
        bytes,
        "recovery must not repair or rewrite accepted state"
    );
}
