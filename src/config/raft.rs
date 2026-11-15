//! Raft member, map, and managed-catalog options.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::file::{read_file_config, FileConfig};
use super::option::{invalid, split_list, Options};
use super::tls::parse_tls;

/// The resource a relay routes on from a Raft-hosted authority's
/// committed map (`--raft-map-principal`, `--raft-map-workspace`,
/// `--raft-map-collection`).
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

/// A Raft member of the source authority group (`--raft-*`). Serving opens
/// existing state; `raft-prepare` creates it.
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

/// The member and TLS material for the local directory commands.
pub fn parse_raft_local(args: &[String]) -> Result<RaftLocal, String> {
    let o = Options::new(args);
    let file = read_file_config(o)?;
    let member = parse_raft(o, &file, o.bare("relay"))?;
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

/// A numeric raft option; surrounding whitespace is ignored.
fn number(o: Options, flag: &str, file: Option<u64>, default: u64) -> Result<u64, String> {
    match o.given(flag) {
        Some(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|e| invalid(flag, &raw, e)),
        None => Ok(file.unwrap_or(default)),
    }
}

/// A raft option every member must carry.
fn required(o: Options, flag: &str, file: Option<&str>) -> Result<String, String> {
    o.text(flag, file)
        .ok_or_else(|| format!("a raft member needs --{flag}"))
}

/// Managed catalog entries: repeatable on the command line, comma-separated
/// in the environment, a list in the file.
fn managed_entries(o: Options, file: &FileConfig) -> Vec<String> {
    let cli = o.arguments("raft-managed-catalog");
    if !cli.is_empty() {
        return cli;
    }
    match o.given("raft-managed-catalog") {
        Some(env) => split_list(&env),
        None => file.raft_managed_catalog.clone().unwrap_or_default(),
    }
}

fn managed_catalogs(o: Options, file: &FileConfig) -> Result<Vec<RaftManagedCatalog>, String> {
    let usage = "--raft-managed-catalog takes <collection>=<path>, repeatable";
    managed_entries(o, file)
        .iter()
        .map(|entry| {
            let (collection, path) = entry.split_once('=').ok_or_else(|| usage.to_string())?;
            if collection.trim().is_empty() || path.trim().is_empty() {
                return Err(usage.to_string());
            }
            Ok(RaftManagedCatalog {
                collection: collection.trim().to_string(),
                path: PathBuf::from(path.trim()),
            })
        })
        .collect()
}

/// Without `--raft-dir` there is no member, so a member option is refused.
fn refuse_without_dir(o: Options, file: &FileConfig) -> Result<(), String> {
    let present = [
        ("raft-node-id", o.given("raft-node-id").is_some()),
        (
            "raft-peers",
            o.text("raft-peers", file.raft_peers.as_deref()).is_some(),
        ),
        (
            "raft-listen",
            o.text("raft-listen", file.raft_listen.as_deref()).is_some(),
        ),
        ("raft-managed-catalog", !managed_entries(o, file).is_empty()),
        (
            "raft-managed-principal",
            o.text(
                "raft-managed-principal",
                file.raft_managed_principal.as_deref(),
            )
            .is_some(),
        ),
    ];
    match present.iter().find(|(_, given)| *given) {
        Some((flag, _)) => Err(format!(
            "--{flag} needs --raft-dir; a member is its directory"
        )),
        None => Ok(()),
    }
}

/// The relay's routing resource on the committed map: all three or none.
fn map_config(o: Options, file: &FileConfig, relay: bool) -> Result<Option<RaftMapConfig>, String> {
    let principal = o.text("raft-map-principal", file.raft_map_principal.as_deref());
    let workspace = o.text("raft-map-workspace", file.raft_map_workspace.as_deref());
    let collection = o.text("raft-map-collection", file.raft_map_collection.as_deref());
    match (principal, workspace, collection) {
        (None, None, None) => Ok(None),
        (Some(_), Some(_), Some(_)) if !relay => {
            Err("--raft-map-* routes a relay on the committed map and needs --relay".to_string())
        }
        (Some(principal), Some(workspace), Some(collection)) => Ok(Some(RaftMapConfig {
            principal,
            workspace,
            collection,
        })),
        _ => Err(
            "--raft-map-principal, --raft-map-workspace and --raft-map-collection go together"
                .to_string(),
        ),
    }
}

/// The member's identity: its id, its group, and the authority incarnation.
fn identity(o: Options, file: &FileConfig) -> Result<(u64, Vec<u8>, Vec<u8>), String> {
    let file_id = file.raft_node_id.map(|v| v.to_string());
    let raw = required(o, "raft-node-id", file_id.as_deref())?;
    let node_id = raw
        .trim()
        .parse::<u64>()
        .map_err(|e| invalid("raft-node-id", &raw, e))?;
    if node_id == 0 {
        return Err("--raft-node-id must be nonzero".to_string());
    }
    let group = required(o, "raft-group-id", file.raft_group_id.as_deref())?;
    let incarnation = required(
        o,
        "raft-authority-incarnation",
        file.raft_authority_incarnation.as_deref(),
    )?;
    Ok((
        node_id,
        hex16("--raft-group-id", &group)?,
        hex16("--raft-authority-incarnation", &incarnation)?,
    ))
}

/// The managed write service: its catalogs, the recovering actor, and its limits.
struct Managed {
    catalogs: Vec<RaftManagedCatalog>,
    principal: Option<String>,
    max_request_bytes: u64,
    max_pending_bytes: u64,
    max_in_flight: u64,
}

fn managed(o: Options, file: &FileConfig) -> Result<Managed, String> {
    let catalogs = managed_catalogs(o, file)?;
    let principal = o.text(
        "raft-managed-principal",
        file.raft_managed_principal.as_deref(),
    );
    if !catalogs.is_empty() && principal.is_none() {
        return Err("--raft-managed-catalog needs --raft-managed-principal, the actor holding                     the Admin that recovers the catalogs at start"
            .to_string());
    }
    #[cfg(not(all(feature = "raft", feature = "tls")))]
    if !catalogs.is_empty() || principal.is_some() {
        return Err(
            "this build has no Raft support (features `raft` and `tls` are needed)".to_string(),
        );
    }
    Ok(Managed {
        catalogs,
        principal,
        max_request_bytes: number(
            o,
            "raft-managed-max-request-bytes",
            file.raft_managed_max_request_bytes,
            1024 * 1024,
        )?,
        max_pending_bytes: number(
            o,
            "raft-managed-max-pending-bytes",
            file.raft_managed_max_pending_bytes,
            16 * 1024 * 1024,
        )?,
        max_in_flight: number(
            o,
            "raft-managed-max-in-flight",
            file.raft_managed_max_in_flight,
            64,
        )?,
    })
}

/// The `--raft-*` options: none of them makes a member; `--raft-dir`
/// does, and then node id, group, incarnation, listen and peers are
/// required. Timing defaults are the host's (`HostConfig::default`) and
/// are validated when the host starts.
pub(super) fn parse_raft(
    o: Options,
    file: &FileConfig,
    relay: bool,
) -> Result<Option<RaftMemberConfig>, String> {
    let Some(dir) = o.text("raft-dir", file.raft_dir.as_deref()) else {
        refuse_without_dir(o, file)?;
        return Ok(None);
    };
    let (node_id, group_id, authority_incarnation) = identity(o, file)?;
    let listen_raw = required(o, "raft-listen", file.raft_listen.as_deref())?;
    let listen = listen_raw
        .parse::<SocketAddr>()
        .map_err(|e| invalid("raft-listen", &listen_raw, e))?;
    let advertise = o.text("raft-advertise", file.raft_advertise.as_deref());
    if advertise
        .as_deref()
        .is_some_and(|a| a.is_empty() || a.len() > 1024)
    {
        return Err("--raft-advertise must be 1..1024 bytes".to_string());
    }
    let peers = PathBuf::from(required(o, "raft-peers", file.raft_peers.as_deref())?);
    let map = map_config(o, file, relay)?;
    let managed = managed(o, file)?;
    Ok(Some(RaftMemberConfig {
        dir: PathBuf::from(dir),
        node_id,
        group_id,
        authority_incarnation,
        listen,
        advertise,
        peers,
        heartbeat_ms: number(o, "raft-heartbeat-ms", file.raft_heartbeat_ms, 250)?,
        election_min_ms: number(o, "raft-election-min-ms", file.raft_election_min_ms, 1_000)?,
        election_max_ms: number(o, "raft-election-max-ms", file.raft_election_max_ms, 2_000)?,
        lease_ms: number(o, "raft-lease-ms", file.raft_lease_ms, 500)?,
        skew_ms: number(o, "raft-skew-ms", file.raft_skew_ms, 250)?,
        max_snapshot_bytes: number(
            o,
            "raft-max-snapshot-bytes",
            file.raft_max_snapshot_bytes,
            4 << 30,
        )?,
        map,
        managed_catalogs: managed.catalogs,
        managed_principal: managed.principal,
        managed_max_request_bytes: managed.max_request_bytes,
        managed_max_pending_bytes: managed.max_pending_bytes,
        managed_max_in_flight: managed.max_in_flight,
    }))
}
