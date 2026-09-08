//! Durable retry ownership and explicit attribution of legacy actorless records.
use super::*;
use crate::pb::storage::{ActorOperationKey, SourceActorAssignment, SourceActorNamespace};
use redb::TableHandle;

pub(super) const OPERATIONS: checkpoint::BinaryTable = TableDefinition::new("actor_operations");

pub(super) fn namespace(legacy_operations: u64) -> SourceActorNamespace {
    SourceActorNamespace {
        format_version: 1,
        legacy_operations,
        assigned_operations: 0,
    }
}

fn valid_principal(principal: &str) -> bool {
    !principal.is_empty() && principal.len() <= 16384
}

pub(super) struct OperationKey {
    pub bytes: Vec<u8>,
    scoped: bool,
}
impl OperationKey {
    pub fn new(principal: Option<&str>, operation_id: &[u8]) -> Result<Self, Status> {
        if operation_id.is_empty() || operation_id.len() > 1024 {
            return Err(Status::invalid_argument(
                "operation_id must contain 1 to 1024 bytes",
            ));
        }
        let bytes = match principal {
            Some(principal) => {
                if !valid_principal(principal) {
                    return Err(Status::invalid_argument(
                        "retry principal must contain 1 to 16384 UTF-8 bytes",
                    ));
                }
                ActorOperationKey {
                    format_version: 1,
                    principal: principal.into(),
                    operation_id: operation_id.to_vec(),
                }
                .encode_to_vec()
            }
            None => operation_id.to_vec(),
        };
        Ok(Self {
            bytes,
            scoped: principal.is_some(),
        })
    }
    pub fn table(&self) -> checkpoint::BinaryTable {
        if self.scoped {
            OPERATIONS
        } else {
            super::OPERATIONS
        }
    }
    pub fn check_header(&self, header: &DocumentCatalogHeader) -> Result<(), Status> {
        if self.scoped != header.resource_binding.is_some() {
            return Err(Status::failed_precondition("controlled acceptance requires an authenticated actor; unbound acceptance has no actor namespace"));
        }
        if self.scoped
            && header
                .actor_namespace
                .as_ref()
                .is_none_or(|n| n.assigned_operations != n.legacy_operations)
        {
            return Err(Status::failed_precondition("legacy actor attribution is incomplete; assign all legacy operations before controlled acceptance or retry"));
        }
        Ok(())
    }
}

pub(super) fn validate_key(bytes: &[u8]) -> Result<(), Status> {
    if bytes.len() > 17424 {
        return Err(Status::data_loss(
            "actor operation key exceeds its size bound",
        ));
    }
    let key: ActorOperationKey = decode(bytes)?;
    if key.format_version != 1
        || !valid_principal(&key.principal)
        || key.operation_id.is_empty()
        || key.operation_id.len() > 1024
        || key.encode_to_vec() != bytes
    {
        return Err(Status::data_loss(
            "actor operation key is invalid or noncanonical",
        ));
    }
    Ok(())
}

pub(super) fn validate_namespace(header: &DocumentCatalogHeader) -> Result<(), Status> {
    match (&header.actor_namespace, header.format_version) {
        (Some(n), ACCESS_CONTROLLED_FORMAT)
            if n.format_version == 1
                && n.assigned_operations <= n.legacy_operations
                && n.legacy_operations <= header.accepted_sequence
                && (n.assigned_operations == n.legacy_operations
                    || header.accepted_sequence == n.legacy_operations) =>
        {
            Ok(())
        }
        (None, version) if version != ACCESS_CONTROLLED_FORMAT => Ok(()),
        _ => Err(Status::data_loss(
            "catalog format and actor namespace migration metadata disagree",
        )),
    }
}

fn counts(header: &DocumentCatalogHeader, raw: u64, actor: Option<u64>) -> Result<(), Status> {
    validate_namespace(header)?;
    let valid = match &header.actor_namespace {
        Some(n) => {
            actor.is_some_and(|actor| raw.checked_add(actor) == Some(header.accepted_sequence))
                && raw == n.legacy_operations - n.assigned_operations
        }
        None => actor.is_none() && raw == header.accepted_sequence,
    };
    if !valid {
        return Err(Status::data_loss(
            "operation table counts differ from accepted history or actor attribution watermark",
        ));
    }
    Ok(())
}

macro_rules! count_validator {
    ($name:ident, $transaction:ty) => {
        pub(super) fn $name(
            tx: &$transaction,
            header: &DocumentCatalogHeader,
        ) -> Result<(), Status> {
            let has_actor = tx
                .list_tables()
                .map_err(storage)?
                .any(|table| table.name() == OPERATIONS.name());
            let actor = if has_actor {
                Some(
                    tx.open_table(OPERATIONS)
                        .map_err(storage)?
                        .len()
                        .map_err(storage)?,
                )
            } else {
                None
            };
            counts(
                header,
                tx.open_table(super::OPERATIONS)
                    .map_err(storage)?
                    .len()
                    .map_err(storage)?,
                actor,
            )
        }
    };
}
count_validator!(validate_read_counts, redb::ReadTransaction);
count_validator!(validate_write_counts, redb::WriteTransaction);

// Bound one migration transaction even if a damaged record has a huge payload.
// Normal receipts contain at most a 16 KiB document key; unknown fields survive
// within this explicit record budget instead of being stripped on attribution.
fn copy_operation(bytes: &[u8]) -> Result<Vec<u8>, Status> {
    if bytes.len() > 64 << 10 {
        return Err(Status::resource_exhausted(
            "actor attribution operation record exceeds 64 KiB",
        ));
    }
    Ok(bytes.to_vec())
}

fn attributed_receipt(
    tx: &redb::WriteTransaction,
    header: &DocumentCatalogHeader,
    bytes: &[u8],
    sha: &[u8; 32],
    legacy_through: u64,
) -> Result<DocumentWriteReceipt, Status> {
    let operation: DocumentOperation = decode(bytes)?;
    if operation.receipt.as_ref().is_none_or(|r| r.replayed) {
        return Err(Status::data_loss(
            "legacy attribution requires an immutable stored receipt",
        ));
    }
    let receipt = retry::receipt(header, bytes, sha)?;
    if !receipt.accepted
        || !receipt.durable
        || receipt.searchable
        || receipt.document_key.is_empty()
        || receipt.document_key.len() > 16384
        || receipt.version == 0
        || receipt.accepted_sequence == 0
        || receipt.accepted_sequence > legacy_through
    {
        return Err(Status::data_loss("legacy attribution receipt is not a durable accepted operation within the migration watermark"));
    }
    let key = DocumentVersionKey {
        document_key: receipt.document_key.clone(),
        version: receipt.version,
    }
    .encode_to_vec();
    let versions = tx.open_table(VERSIONS).map_err(storage)?;
    let stored = versions
        .get(key.as_slice())
        .map_err(storage)?
        .ok_or_else(|| Status::data_loss("legacy attribution receipt has no accepted version"))?;
    let version: DocumentVersion = decode(stored.value())?;
    if version.document_key != receipt.document_key
        || version.version != receipt.version
        || version.accepted_sequence != receipt.accepted_sequence
    {
        return Err(Status::data_loss(
            "legacy attribution receipt differs from accepted history",
        ));
    }
    let changes = tx.open_table(CHANGES).map_err(storage)?;
    if changes
        .get(receipt.accepted_sequence)
        .map_err(storage)?
        .is_none_or(|v| v.value() != key)
    {
        return Err(Status::data_loss(
            "legacy attribution receipt has no matching sequence link",
        ));
    }
    Ok(receipt)
}

impl DocumentCatalog {
    pub(super) fn upgrade_empty_actor_namespace(&self) -> Result<(), Status> {
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let mut header = seal::header_from(self, &tx)?;
        validate_write_counts(&tx, &header)?;
        if header.format_version != LEGACY_ACCESS_CONTROLLED_FORMAT || header.accepted_sequence != 0
        {
            return Err(Status::failed_precondition(
                "empty actor namespace upgrade requires an empty format-7 catalog",
            ));
        }
        header.format_version = ACCESS_CONTROLLED_FORMAT;
        header.actor_namespace = Some(namespace(0));
        tx.open_table(OPERATIONS).map_err(storage)?;
        tx.open_table(META)
            .map_err(storage)?
            .insert("header", header.encode_to_vec().as_slice())
            .map_err(storage)?;
        tx.commit().map_err(storage)
    }

    pub(super) fn assign_legacy_actor(
        &self,
        request: &SourceActorAssignment,
    ) -> Result<(), Status> {
        if request.format_version != 1
            || !valid_history_id(&request.history_id)
            || request.request_sha256.len() != 32
        {
            return Err(Status::invalid_argument("actor assignment requires format 1, a history identity and a 32-byte request digest"));
        }
        let key = OperationKey::new(Some(&request.principal), &request.operation_id)?;
        let sha: [u8; 32] = request
            .request_sha256
            .as_slice()
            .try_into()
            .expect("validated digest");
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        let mut header = seal::header_from(self, &tx)?;
        if header.resource_binding.is_none() || request.history_id != header.history_id {
            return Err(Status::failed_precondition(
                "actor assignment requires this bound catalog history",
            ));
        }
        validate_write_counts(&tx, &header)?;
        if header.format_version == LEGACY_ACCESS_CONTROLLED_FORMAT {
            header.format_version = ACCESS_CONTROLLED_FORMAT;
            header.actor_namespace = Some(namespace(header.accepted_sequence));
        }
        let migration = header
            .actor_namespace
            .as_ref()
            .expect("validated controlled format");
        let legacy_through = migration.legacy_operations;
        let raw = tx
            .open_table(super::OPERATIONS)
            .map_err(storage)?
            .get(request.operation_id.as_slice())
            .map_err(storage)?
            .map(|v| copy_operation(v.value()))
            .transpose()?;
        let prior = tx
            .open_table(OPERATIONS)
            .map_err(storage)?
            .get(key.bytes.as_slice())
            .map_err(storage)?
            .map(|v| copy_operation(v.value()))
            .transpose()?;
        if let Some(prior) = prior {
            let operation: DocumentOperation = decode(&prior)?;
            if operation
                .receipt
                .as_ref()
                .is_some_and(|r| r.accepted_sequence > legacy_through)
            {
                return Err(Status::failed_precondition("actor operation was accepted after legacy migration; it cannot be attributed again"));
            }
            attributed_receipt(&tx, &header, &prior, &sha, legacy_through)?;
            if raw.is_some() {
                return Err(Status::failed_precondition(
                    "actor key is not an attributed legacy operation",
                ));
            }
            return Ok(());
        }
        let raw = raw.ok_or_else(|| {
            Status::not_found("legacy operation is missing or attributed to another actor")
        })?;
        attributed_receipt(&tx, &header, &raw, &sha, legacy_through)?;
        tx.open_table(OPERATIONS)
            .map_err(storage)?
            .insert(key.bytes.as_slice(), raw.as_slice())
            .map_err(storage)?;
        tx.open_table(super::OPERATIONS)
            .map_err(storage)?
            .remove(request.operation_id.as_slice())
            .map_err(storage)?;
        let migration = header
            .actor_namespace
            .as_mut()
            .expect("validated namespace");
        migration.assigned_operations = migration
            .assigned_operations
            .checked_add(1)
            .ok_or_else(|| Status::data_loss("actor attribution count overflow"))?;
        validate_write_counts(&tx, &header)?;
        tx.open_table(META)
            .map_err(storage)?
            .insert("header", header.encode_to_vec().as_slice())
            .map_err(storage)?;
        tx.commit().map_err(storage)
    }
}

#[cfg(test)]
mod key_golden_tests {
    use super::*;
    use prost::Message;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn ascii_principal_and_binary_operation_have_stable_wire_key() {
        // 08 01: format_version=1
        // 12 05 6163746f72: principal="actor" (5 UTF-8 bytes)
        // 1a 02 00ff: operation_id=[NUL, 0xff]
        let literal = "080112056163746f721a0200ff";
        let encoded = OperationKey::new(Some("actor"), &[0x00, 0xff]).unwrap();
        assert_eq!(hex(&encoded.bytes), literal);
        let decoded = ActorOperationKey::decode(encoded.bytes.as_slice()).unwrap();
        assert_eq!(decoded.format_version, 1);
        assert_eq!(decoded.principal, "actor");
        assert_eq!(decoded.operation_id, [0x00, 0xff]);
    }

    #[test]
    fn unicode_principal_length_is_its_utf8_byte_length() {
        // "é猫" is c3a9 e78cab: 5 UTF-8 bytes, so field 2 length is 05.
        // Full wire: format=1, principal="é猫", operation_id="x".
        let literal = "08011205c3a9e78cab1a0178";
        let encoded = OperationKey::new(Some("é猫"), b"x").unwrap();
        assert_eq!(hex(&encoded.bytes), literal);
        let decoded = ActorOperationKey::decode(encoded.bytes.as_slice()).unwrap();
        assert_eq!(decoded.format_version, 1);
        assert_eq!(decoded.principal.as_bytes(), "é猫".as_bytes());
        assert_eq!(decoded.operation_id, b"x");
    }

    #[test]
    fn protobuf_framing_separates_concatenation_ambiguous_pairs() {
        // ("ab", "c") => 0801 1202 6162 1a01 63
        // ("a", "bc") => 0801 1201 61   1a02 6263
        let first = OperationKey::new(Some("ab"), b"c").unwrap();
        let second = OperationKey::new(Some("a"), b"bc").unwrap();
        assert_eq!(hex(&first.bytes), "0801120261621a0163");
        assert_eq!(hex(&second.bytes), "08011201611a026263");
        assert_ne!(first.bytes, second.bytes);
    }
}
