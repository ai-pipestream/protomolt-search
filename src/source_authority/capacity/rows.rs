//! The retained reporter and observation rows of one resource, and the
//! observation store they rebuild.
use super::*;

/// The retained capacity rows of one resource.
pub(in crate::source_authority) struct Rows {
    pub(in crate::source_authority) reporters: Vec<(String, [u8; 16])>,
    pub(in crate::source_authority) observations: Vec<(Vec<u8>, CapacityObservation)>,
}

pub(in crate::source_authority) fn load_rows(
    reporters: &impl ReadableTable<&'static [u8], &'static [u8]>,
    observations: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &LogicalSourceOwner,
) -> Result<Rows, Status> {
    let prefix = resource_prefix(key);
    let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
    let mut reporter_rows = Vec::new();
    for entry in reporters
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (k, v) = entry.map_err(storage)?;
        let reporter_key: CapacityReporterKey = contract::decode(k.value())?;
        let reporter: CapacityReporter = contract::decode(v.value())?;
        if reporter.format_version != 1 {
            return Err(corrupt("unsupported capacity reporter format"));
        }
        let incarnation: [u8; 16] = reporter
            .process_incarnation
            .as_slice()
            .try_into()
            .map_err(|_| corrupt("stored reporter incarnation is not 16 bytes"))?;
        reporter_rows.push((reporter_key.node_id, incarnation));
    }
    let mut observation_rows = Vec::new();
    for entry in observations
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (k, v) = entry.map_err(storage)?;
        let value: CapacityObservation = contract::decode(v.value())?;
        if observation_key(key, &value) != k.value() {
            return Err(corrupt("stored observation key differs from its value"));
        }
        observation_rows.push((k.value().to_vec(), value));
    }
    Ok(Rows {
        reporters: reporter_rows,
        observations: observation_rows,
    })
}

/// Rebuild Kimi's store from the committed rows: every retained row must
/// land again under the same rules, or the rows disagree with the committed
/// view and the open refuses.
pub(in crate::source_authority) fn rebuild(
    configuration: &CapacityConfiguration,
    view: tiers::CommittedView,
    rows: &Rows,
) -> Result<tiers::ObservationStore, Status> {
    let mut store = tiers::ObservationStore::new(
        configuration.cohort_length_ms,
        configuration.cohort_phase_unix_ms,
        configuration.max_total_records as usize,
        configuration.max_total_bytes as usize,
        configuration.max_registered_nodes as usize,
        view,
    )
    .map_err(corrupt)?;
    for (node_id, incarnation) in &rows.reporters {
        store
            .register_incarnation(node_id, *incarnation)
            .map_err(|error| corrupt(format!("stored reporter: {error}")))?;
    }
    for (_, value) in &rows.observations {
        let observation = observation_from_proto(value)
            .map_err(|error| corrupt(format!("stored observation: {}", error.message())))?;
        match store.ingest(observation) {
            Ok(tiers::IngestOutcome::Landed) => {}
            Ok(other) => {
                return Err(corrupt(format!(
                    "stored observation did not land on rebuild: {other:?}"
                )))
            }
            Err(error) => {
                return Err(corrupt(format!(
                    "stored observation no longer validates: {error}"
                )))
            }
        }
    }
    Ok(store)
}
