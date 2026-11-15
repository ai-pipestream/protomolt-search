//! Raft member, map, and managed-catalog options.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::file::{read_file_config, FileConfig};
use super::option::Options;
use super::tls::parse_tls;

/// The resource a relay routes on from a Raft-hosted authority's
/// committed map (`--raft-map-principal`, `--raft-map-workspace`,
/// `--raft-map-collection`; docs/raft-hosting.md).
#[derive(Debug, Clone)]
pub struct RaftMapConfig {
    pub principal: String,
    pub workspace: String,
    pub collection: String,
}

/// One access-controlled source catalog this member serves owner writes
/// for (`--raft-managed-catalog=<collection>=<path>`, repeatable): a file
/// bound and activated for the named collection of the member's
/// authority, recovered at start before the service listens.
#[derive(Debug, Clone)]
pub struct RaftManagedCatalog {
    pub collection: String,
    pub path: PathBuf,
}

/// A Raft member of the source authority group (`--raft-*`,
/// docs/raft-hosting.md "Operator configuration"). Serving opens existing
/// state; `raft-prepare` creates it.
#[derive(Debug, Clone)]
pub struct RaftMemberConfig {
    /// The member's directory: store, log and snapshots.
    pub dir: PathBuf,
    pub node_id: u64,
    /// The group identity the store must carry (16 bytes each, given as
    /// 32 hex digits).
    pub group_id: Vec<u8>,
    pub authority_incarnation: Vec<u8>,
    pub listen: SocketAddr,
    /// The address peers dial, when it is not the bound one.
    pub advertise: Option<String>,
    /// The peers file (`[[peers]]` of `node_id` and `certificate`).
    pub peers: PathBuf,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    pub lease_ms: u64,
    pub skew_ms: u64,
    pub max_snapshot_bytes: u64,
    pub map: Option<RaftMapConfig>,
    /// Managed catalogs served through the hosted write service, with the
    /// actor holding the Admin that recovers them at start
    /// (`--raft-managed-principal`).
    pub managed_catalogs: Vec<RaftManagedCatalog>,
    pub managed_principal: Option<String>,
    /// The hosted write service's limits (`--raft-managed-max-request-bytes`,
    /// `--raft-managed-max-pending-bytes`, `--raft-managed-max-in-flight`):
    /// one write's bound, the admitted bytes in flight, the operations in
    /// flight. Validated where the service is built, by name.
    pub managed_max_request_bytes: u64,
    pub managed_max_pending_bytes: u64,
    pub managed_max_in_flight: u64,
}

/// The member and TLS material for the local directory commands
/// (`raft-bootstrap`): the raft flags and the listener and client TLS,
/// without the serving roles' shard and collection requirements, which
/// a command that creates a directory but serves nothing does not have.
pub struct RaftLocal {
    pub member: Option<RaftMemberConfig>,
    pub tls: Option<crate::security::ServerTls>,
    pub client_tls: Option<crate::security::ClientTls>,
}

pub fn parse_raft_local(args: &[String]) -> Result<RaftLocal, String> {
    let o = Options::new(args);
    let file = read_file_config(o)?;
    let relay = o.bare("relay");
    let member = parse_raft(o, &file, relay)?;
    let (tls, client_tls) = parse_tls(o, &file)?;
    Ok(RaftLocal {
        member,
        tls,
        client_tls,
    })
}

/// 32 hex digits to 16 bytes, refusing anything else by name.
fn hex16(what: &str, value: &str) -> Result<Vec<u8>, String> {
    let digits = value.as_bytes();
    if digits.len() != 32 {
        return Err(format!(
            "{what} must be 32 hex digits, got {} characters",
            digits.len()
        ));
    }
    let nibble = |d: u8| -> Result<u8, String> {
        match d {
            b'0'..=b'9' => Ok(d - b'0'),
            b'a'..=b'f' => Ok(d - b'a' + 10),
            b'A'..=b'F' => Ok(d - b'A' + 10),
            _ => Err(format!("{what} holds a non-hex character {:?}", d as char)),
        }
    };
    let mut bytes = Vec::with_capacity(16);
    for pair in digits.chunks(2) {
        bytes.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    if bytes.iter().all(|b| *b == 0) {
        return Err(format!("{what} must be nonzero"));
    }
    Ok(bytes)
}

/// The `--raft-*` options: none of them makes a member; `--raft-dir`
/// does, and then node id, group, incarnation, listen and peers are
/// required. Timing defaults are the host's (`HostConfig::default`) and
/// are validated when the host starts.
pub(super) fn parse_raft(
    o: Options<'_>,
    file: &FileConfig,
    relay: bool,
) -> Result<Option<RaftMemberConfig>, String> {
    let Some(dir) = o.text("raft-dir", file.raft_dir.as_deref()) else {
        refuse_options_without_dir(o, file)?;
        return Ok(None);
    };
    let number = |key: &str, file_value: Option<u64>, default: u64| -> Result<u64, String> {
        Ok(o.value::<u64>(key, file_value)?.unwrap_or(default))
    };
    let identity = member_identity(o, file)?;
    let managed_catalogs = managed_catalogs(o, file)?;
    let managed_principal = o.text(
        "raft-managed-principal",
        file.raft_managed_principal.as_deref(),
    );
    if !managed_catalogs.is_empty() && managed_principal.is_none() {
        return Err("--raft-managed-catalog needs --raft-managed-principal, the actor holding the Admin that recovers the catalogs at start".to_string());
    }
    #[cfg(not(all(feature = "raft", feature = "tls")))]
    if !managed_catalogs.is_empty() || managed_principal.is_some() {
        return Err(
            "this build has no Raft support (features `raft` and `tls` are needed)".to_string(),
        );
    }
    Ok(Some(RaftMemberConfig {
        dir: PathBuf::from(dir),
        node_id: identity.node_id,
        group_id: identity.group_id,
        authority_incarnation: identity.authority_incarnation,
        listen: identity.listen,
        advertise: identity.advertise,
        peers: identity.peers,
        heartbeat_ms: number("raft-heartbeat-ms", file.raft_heartbeat_ms, 250)?,
        election_min_ms: number("raft-election-min-ms", file.raft_election_min_ms, 1_000)?,
        election_max_ms: number("raft-election-max-ms", file.raft_election_max_ms, 2_000)?,
        lease_ms: number("raft-lease-ms", file.raft_lease_ms, 500)?,
        skew_ms: number("raft-skew-ms", file.raft_skew_ms, 250)?,
        max_snapshot_bytes: number(
            "raft-max-snapshot-bytes",
            file.raft_max_snapshot_bytes,
            4 << 30,
        )?,
        map: raft_map(o, file, relay)?,
        managed_catalogs,
        managed_principal,
        managed_max_request_bytes: number(
            "raft-managed-max-request-bytes",
            file.raft_managed_max_request_bytes,
            1024 * 1024,
        )?,
        managed_max_pending_bytes: number(
            "raft-managed-max-pending-bytes",
            file.raft_managed_max_pending_bytes,
            16 * 1024 * 1024,
        )?,
        managed_max_in_flight: number(
            "raft-managed-max-in-flight",
            file.raft_managed_max_in_flight,
            64,
        )?,
    }))
}

/// Without `--raft-dir` no member option may be set: a member is its directory.
fn refuse_options_without_dir(o: Options<'_>, file: &FileConfig) -> Result<(), String> {
    let text = |key: &str, file_value: Option<&str>| o.text(key, file_value);
    let managed_catalog_present = !o.arguments("raft-managed-catalog").is_empty()
        || text("raft-managed-catalog", None).is_some()
        || file
            .raft_managed_catalog
            .as_ref()
            .is_some_and(|v| !v.is_empty());
    let managed_principal_present = text(
        "raft-managed-principal",
        file.raft_managed_principal.as_deref(),
    )
    .is_some();
    for (key, present) in [
        ("raft-node-id", text("raft-node-id", None).is_some()),
        (
            "raft-peers",
            text("raft-peers", file.raft_peers.as_deref()).is_some(),
        ),
        (
            "raft-listen",
            text("raft-listen", file.raft_listen.as_deref()).is_some(),
        ),
        ("raft-managed-catalog", managed_catalog_present),
        ("raft-managed-principal", managed_principal_present),
    ] {
        if present {
            return Err(format!(
                "--{key} needs --raft-dir; a member is its directory"
            ));
        }
    }
    Ok(())
}

/// What names a member and where it is reached.
struct MemberIdentity {
    node_id: u64,
    group_id: Vec<u8>,
    authority_incarnation: Vec<u8>,
    listen: SocketAddr,
    advertise: Option<String>,
    peers: PathBuf,
}

/// Node id, group, incarnation, listen address and peers are required once
/// `--raft-dir` makes a member.
fn member_identity(o: Options<'_>, file: &FileConfig) -> Result<MemberIdentity, String> {
    let required = |key: &str, file_value: Option<&str>| {
        o.text(key, file_value)
            .ok_or_else(|| format!("a raft member needs --{key}"))
    };
    let node_id = o
        .value::<u64>("raft-node-id", file.raft_node_id)?
        .ok_or_else(|| "a raft member needs --raft-node-id".to_string())?;
    if node_id == 0 {
        return Err("--raft-node-id must be nonzero".to_string());
    }
    let advertise = o.text("raft-advertise", file.raft_advertise.as_deref());
    if advertise
        .as_deref()
        .is_some_and(|a| a.is_empty() || a.len() > 1024)
    {
        return Err("--raft-advertise must be 1..1024 bytes".to_string());
    }
    Ok(MemberIdentity {
        node_id,
        group_id: hex16(
            "--raft-group-id",
            &required("raft-group-id", file.raft_group_id.as_deref())?,
        )?,
        authority_incarnation: hex16(
            "--raft-authority-incarnation",
            &required(
                "raft-authority-incarnation",
                file.raft_authority_incarnation.as_deref(),
            )?,
        )?,
        listen: o
            .parsed::<SocketAddr>("raft-listen", file.raft_listen.as_deref())?
            .ok_or_else(|| "a raft member needs --raft-listen".to_string())?,
        advertise,
        peers: PathBuf::from(required("raft-peers", file.raft_peers.as_deref())?),
    })
}

/// The relay's route on the committed map: all three `--raft-map-*`
/// options, or none.
fn raft_map(
    o: Options<'_>,
    file: &FileConfig,
    relay: bool,
) -> Result<Option<RaftMapConfig>, String> {
    let text = |key: &str, file_value: Option<&str>| o.text(key, file_value);
    let principal = text("raft-map-principal", file.raft_map_principal.as_deref());
    let workspace = text("raft-map-workspace", file.raft_map_workspace.as_deref());
    let collection = text("raft-map-collection", file.raft_map_collection.as_deref());
    let map =
        match (principal, workspace, collection) {
            (None, None, None) => None,
            (Some(principal), Some(workspace), Some(collection)) => {
                if !relay {
                    return Err(
                        "--raft-map-* routes a relay on the committed map and needs --relay"
                            .to_string(),
                    );
                }
                Some(RaftMapConfig {
                    principal,
                    workspace,
                    collection,
                })
            }
            _ => return Err(
                "--raft-map-principal, --raft-map-workspace and --raft-map-collection go together"
                    .to_string(),
            ),
        };
    Ok(map)
}

/// Managed catalogs: repeatable on the command line, comma-separated in
/// the environment, a list in the file, in that order of precedence.
fn managed_catalogs(o: Options<'_>, file: &FileConfig) -> Result<Vec<RaftManagedCatalog>, String> {
    let text = |key: &str, file_value: Option<&str>| o.text(key, file_value);
    let managed_entries = {
        let cli = o.arguments("raft-managed-catalog");
        if !cli.is_empty() {
            cli
        } else if let Some(env) = text("raft-managed-catalog", None) {
            env.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        } else {
            file.raft_managed_catalog.clone().unwrap_or_default()
        }
    };
    let mut managed_catalogs = Vec::with_capacity(managed_entries.len());
    for entry in &managed_entries {
        let (collection, path) = entry.split_once('=').ok_or_else(|| {
            "--raft-managed-catalog takes <collection>=<path>, repeatable".to_string()
        })?;
        if collection.trim().is_empty() || path.trim().is_empty() {
            return Err("--raft-managed-catalog takes <collection>=<path>, repeatable".to_string());
        }
        managed_catalogs.push(RaftManagedCatalog {
            collection: collection.trim().to_string(),
            path: PathBuf::from(path.trim()),
        });
    }
    Ok(managed_catalogs)
}
