use std::path::PathBuf;

use super::{args, args_raw};
use crate::config::{parse, parse_raft_local, Config};

/// The flags of a complete raft member on a coordinator.
const MEMBER: [&str; 8] = [
    "--role=coordinator",
    "--nodes=127.0.0.1:1",
    "--raft-dir=/tmp/member-1",
    "--raft-node-id=1",
    "--raft-group-id=0102030405060708090a0b0c0d0e0f10",
    "--raft-authority-incarnation=ffffffffffffffffffffffffffffffff",
    "--raft-listen=127.0.0.1:19500",
    "--raft-peers=/tmp/peers.toml",
];

fn flag_name(pair: &str) -> &str {
    pair.split('=').next().unwrap_or(pair)
}

/// A member with `extra` replacing the base option of the same flag.
fn member(extra: &[&str]) -> Result<Config, String> {
    let overridden: Vec<&str> = extra.iter().map(|p| flag_name(p)).collect();
    let mut pairs: Vec<&str> = MEMBER
        .into_iter()
        .filter(|p| !overridden.contains(&flag_name(p)))
        .collect();
    pairs.extend_from_slice(extra);
    parse(&args(&pairs))
}

fn member_error(extra: &[&str]) -> String {
    member(extra).unwrap_err()
}

#[test]
fn raft_member_options_parse_together() {
    let cfg = member(&["--raft-advertise=krick-1:19500", "--raft-lease-ms=400"]).unwrap();
    let raft = cfg.raft.unwrap();
    assert_eq!(raft.node_id, 1);
    assert_eq!(raft.group_id, (1..=16).collect::<Vec<u8>>());
    assert_eq!(raft.authority_incarnation, vec![255; 16]);
    assert_eq!(raft.listen.port(), 19500);
    assert_eq!(raft.advertise.as_deref(), Some("krick-1:19500"));
    assert_eq!(raft.peers, PathBuf::from("/tmp/peers.toml"));
    assert_eq!(raft.lease_ms, 400);
    assert_eq!(raft.election_min_ms, 1_000);
    assert!(raft.map.is_none());
    let plain = parse(&args(&["--role=node", "--index=/tmp/x.tv"])).unwrap();
    assert!(plain.raft.is_none());
}

#[test]
fn raft_member_without_its_directory_or_a_required_option_refuses() {
    let error = parse(&args(&[
        "--role=node",
        "--index=/tmp/x.tv",
        "--raft-node-id=1",
    ]))
    .unwrap_err();
    assert!(error.contains("needs --raft-dir"), "{error}");
    for (missing, expect) in [
        ("--raft-node-id=", "needs --raft-node-id"),
        ("--raft-group-id=", "needs --raft-group-id"),
        ("--raft-peers=", "needs --raft-peers"),
    ] {
        let pairs: Vec<&str> = MEMBER
            .into_iter()
            .filter(|p| !p.starts_with(missing))
            .collect();
        let error = parse(&args(&pairs)).unwrap_err();
        assert!(error.contains(expect), "{missing}: {error}");
    }
}

#[test]
fn raft_member_malformed_values_refuse_by_name() {
    for (extra, expect) in [
        ("--raft-group-id=0102", "32 hex digits"),
        (
            "--raft-group-id=zz02030405060708090a0b0c0d0e0f10",
            "non-hex",
        ),
        (
            "--raft-authority-incarnation=00000000000000000000000000000000",
            "nonzero",
        ),
        ("--raft-node-id=0", "nonzero"),
        ("--raft-lease-ms=soon", "--raft-lease-ms"),
    ] {
        let error = member_error(&[extra]);
        assert!(error.contains(expect), "{extra}: {error}");
    }
}

#[test]
fn raft_map_options_go_together_and_need_a_relay() {
    let map = [
        "--raft-map-principal=relay",
        "--raft-map-workspace=workspace-a",
        "--raft-map-collection=books",
    ];
    let error = member_error(&map[..1]);
    assert!(error.contains("go together"), "{error}");
    let error = member_error(&map);
    assert!(error.contains("needs --relay"), "{error}");
    let mut relayed = vec!["--relay"];
    relayed.extend_from_slice(&map);
    let map = member(&relayed).unwrap().raft.unwrap().map.unwrap();
    assert_eq!(
        (
            map.principal.as_str(),
            map.workspace.as_str(),
            map.collection.as_str()
        ),
        ("relay", "workspace-a", "books")
    );
}

#[test]
fn raft_local_parse_serves_the_directory_commands_without_shards() {
    // The local commands create a directory but serve nothing: raft
    // flags alone are a member, with no shard or collection flags.
    let local = parse_raft_local(&args_raw(&MEMBER[2..])).unwrap();
    let member = local
        .member
        .as_ref()
        .expect("raft flags alone are a member");
    assert_eq!(member.node_id, 1);
    assert!(local.tls.is_none());
    assert!(local.client_tls.is_none());
    // No raft flags is no member, not an error.
    let local = parse_raft_local(&args_raw(&[])).unwrap();
    assert!(local.member.is_none());
    // A partial member is refused by name.
    let error = parse_raft_local(&args_raw(&["--raft-node-id=1"]))
        .err()
        .expect("a member without its directory refuses");
    assert!(error.contains("needs --raft-dir"), "{error}");
}

#[test]
#[cfg(all(feature = "raft", feature = "tls"))]
fn raft_managed_catalog_options_parse_repeatable() {
    let raft = member(&[
        "--raft-managed-catalog=books=/tmp/books.redb",
        "--raft-managed-catalog=papers=/tmp/papers.redb",
        "--raft-managed-principal=alice",
    ])
    .unwrap()
    .raft
    .unwrap();
    assert_eq!(raft.managed_catalogs.len(), 2);
    assert_eq!(raft.managed_catalogs[0].collection, "books");
    assert_eq!(
        raft.managed_catalogs[0].path,
        PathBuf::from("/tmp/books.redb")
    );
    assert_eq!(raft.managed_catalogs[1].collection, "papers");
    assert_eq!(raft.managed_principal.as_deref(), Some("alice"));
    // The service limits default to one MiB per write, sixteen MiB
    // admitted and sixty-four operations; each is an option.
    let limits = |r: &crate::config::RaftMemberConfig| {
        (
            r.managed_max_request_bytes,
            r.managed_max_pending_bytes,
            r.managed_max_in_flight,
        )
    };
    assert_eq!(limits(&raft), (1024 * 1024, 16 * 1024 * 1024, 64));
    let raft = member(&[
        "--raft-managed-catalog=books=/tmp/books.redb",
        "--raft-managed-principal=alice",
        "--raft-managed-max-request-bytes=4096",
        "--raft-managed-max-pending-bytes=65536",
        "--raft-managed-max-in-flight=1",
    ])
    .unwrap()
    .raft
    .unwrap();
    assert_eq!(limits(&raft), (4096, 65536, 1));
}

#[test]
#[cfg(all(feature = "raft", feature = "tls"))]
fn raft_managed_catalog_options_refuse_by_name() {
    let error = member_error(&["--raft-managed-max-in-flight=many"]);
    assert!(error.contains("--raft-managed-max-in-flight"), "{error}");
    // A catalog without its recovery actor refuses.
    let error = member_error(&["--raft-managed-catalog=books=/tmp/books.redb"]);
    assert!(error.contains("--raft-managed-principal"), "{error}");
    // A malformed entry refuses by name.
    let error = member_error(&[
        "--raft-managed-catalog=books",
        "--raft-managed-principal=alice",
    ]);
    assert!(error.contains("--raft-managed-catalog"), "{error}");
    // Managed options on a non-member refuse by name.
    for extra in [
        &[
            "--raft-managed-catalog=books=/tmp/books.redb",
            "--raft-managed-principal=alice",
        ][..],
        &["--raft-managed-principal=alice"][..],
    ] {
        let mut pairs = vec!["--role=node", "--index=/tmp/x.tv"];
        pairs.extend_from_slice(extra);
        let error = parse(&args(&pairs)).unwrap_err();
        assert!(error.contains("needs --raft-dir"), "{error}");
    }
}

#[test]
#[cfg(not(all(feature = "raft", feature = "tls")))]
fn raft_managed_catalog_options_refuse_without_raft_support() {
    let error = member_error(&[
        "--raft-managed-catalog=books=/tmp/books.redb",
        "--raft-managed-principal=alice",
    ]);
    assert!(error.contains("has no Raft support"), "{error}");
}
