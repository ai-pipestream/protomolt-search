//! The identities a capacity report carries and the observation itself.

use crate::capacity_tiers::{put_str, put_u32, put_u64};

/// §2: a declared hash partition inside an explicit resource.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionIdentity {
    pub workspace: String,
    pub collection: String,
    pub derived_fingerprint: [u8; 32],
    pub column: String,
    pub bucket: u64,
}

impl PartitionIdentity {
    pub(in crate::capacity_tiers) fn label(&self) -> String {
        format!("{}={}", self.column, self.bucket)
    }

    pub(in crate::capacity_tiers) fn put_canonical(&self, out: &mut Vec<u8>) {
        put_str(out, &self.workspace);
        put_str(out, &self.collection);
        out.extend_from_slice(&self.derived_fingerprint);
        put_str(out, &self.column);
        put_u64(out, self.bucket);
    }
}

/// §2/§3: a node process, pinned to one run of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReporterIdentity {
    pub node_id: String,
    pub process_incarnation: [u8; 16],
}

/// §3: a shard's bytes as one committed installation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRef {
    pub shard: String,
    pub source_generation: u64,
    pub storage_incarnation: [u8; 16],
    pub ownership_epoch: u64,
}

/// §3: one node's measured statement about one partition in one shard,
/// attributed to one placement leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionObservation {
    pub partition: PartitionIdentity,
    pub reporter: ReporterIdentity,
    pub shard: ShardRef,
    pub topology_generation: u64,
    pub leaf: String,
    pub rows: u64,
    pub resident_bytes: u64,
    pub scans_observed: u64,
    pub scan_bytes: u64,
    pub queue_wait_p50_us: u64,
    pub queue_wait_p99_us: u64,
    pub samples: u32,
    pub last_scanned_unix_ms: u64,
    pub window_start_unix_ms: u64,
    pub window_end_unix_ms: u64,
}

/// §3: the stored observation key. Field declaration order is the sort
/// order, matching the canonical tuple in the design document.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObsKey {
    pub workspace: String,
    pub collection: String,
    pub derived_fingerprint: [u8; 32],
    pub column: String,
    pub bucket: u64,
    pub topology_generation: u64,
    pub leaf: String,
    pub shard: String,
    pub source_generation: u64,
    pub ownership_epoch: u64,
    pub node_id: String,
    pub process_incarnation: [u8; 16],
    pub storage_incarnation: [u8; 16],
}

impl PartitionObservation {
    pub fn key(&self) -> ObsKey {
        ObsKey {
            workspace: self.partition.workspace.clone(),
            collection: self.partition.collection.clone(),
            derived_fingerprint: self.partition.derived_fingerprint,
            column: self.partition.column.clone(),
            bucket: self.partition.bucket,
            topology_generation: self.topology_generation,
            leaf: self.leaf.clone(),
            shard: self.shard.shard.clone(),
            source_generation: self.shard.source_generation,
            ownership_epoch: self.shard.ownership_epoch,
            node_id: self.reporter.node_id.clone(),
            process_incarnation: self.reporter.process_incarnation,
            storage_incarnation: self.shard.storage_incarnation,
        }
    }

    /// The canonical bytes of §3/§5, fields in specification order.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.partition.put_canonical(&mut out);
        put_u64(&mut out, self.topology_generation);
        put_str(&mut out, &self.leaf);
        put_str(&mut out, &self.shard.shard);
        put_u64(&mut out, self.shard.source_generation);
        put_u64(&mut out, self.shard.ownership_epoch);
        put_str(&mut out, &self.reporter.node_id);
        out.extend_from_slice(&self.reporter.process_incarnation);
        out.extend_from_slice(&self.shard.storage_incarnation);
        put_u64(&mut out, self.rows);
        put_u64(&mut out, self.resident_bytes);
        put_u64(&mut out, self.scans_observed);
        put_u64(&mut out, self.scan_bytes);
        put_u64(&mut out, self.queue_wait_p50_us);
        put_u64(&mut out, self.queue_wait_p99_us);
        put_u64(&mut out, self.last_scanned_unix_ms);
        put_u64(&mut out, self.window_start_unix_ms);
        put_u64(&mut out, self.window_end_unix_ms);
        put_u32(&mut out, self.samples);
        out
    }

    /// The measured fields after the key and window: what a conflicting
    /// retry would change (§3, E6).
    pub(super) fn payload_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u64(&mut out, self.rows);
        put_u64(&mut out, self.resident_bytes);
        put_u64(&mut out, self.scans_observed);
        put_u64(&mut out, self.scan_bytes);
        put_u64(&mut out, self.queue_wait_p50_us);
        put_u64(&mut out, self.queue_wait_p99_us);
        put_u64(&mut out, self.last_scanned_unix_ms);
        put_u32(&mut out, self.samples);
        out
    }

    /// The retained-byte charge: the store keeps both the cloned key and
    /// the observation, so every string is charged twice, by allocation
    /// capacity rather than length — a short string with a large capacity
    /// must not defeat the bound.
    pub(in crate::capacity_tiers) fn heap_bytes(&self) -> usize {
        let twice = self.partition.workspace.capacity()
            + self.partition.collection.capacity()
            + self.partition.column.capacity()
            + self.leaf.capacity()
            + self.shard.shard.capacity()
            + self.reporter.node_id.capacity();
        336 + 2 * twice
    }
}
