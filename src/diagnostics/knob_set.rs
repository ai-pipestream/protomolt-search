//! Listing the knobs and setting a live one, every refusal by name.

use super::*;

impl Knobs {
    fn live_entries(&self) -> Vec<RuntimeKnob> {
        fn entry(spec: &KnobSpec, value: String, startup: String) -> RuntimeKnob {
            RuntimeKnob {
                name: spec.name.to_string(),
                scope: spec.scope as i32,
                kind: spec.kind as i32,
                value,
                startup_value: startup,
                mutable: true,
                description: spec.description.to_string(),
            }
        }
        match &self.live {
            Live::Node { startup, .. } => vec![
                entry(
                    &FLOOR_SHARING,
                    self.share_floors().to_string(),
                    startup.share_floors.to_string(),
                ),
                entry(
                    &SEGMENT_PRUNING,
                    self.segment_pruning().to_string(),
                    startup.segment_pruning.to_string(),
                ),
                entry(
                    &FLOOR_DELTA,
                    self.floor_delta().to_string(),
                    startup.floor_delta.to_string(),
                ),
                entry(
                    &FLOOR_WARMUP_CHUNKS,
                    self.floor_warmup_chunks().to_string(),
                    startup.floor_warmup_chunks.to_string(),
                ),
                entry(
                    &FLOOR_MIN_INTERVAL_MS,
                    self.floor_min_interval_ms().to_string(),
                    startup.floor_min_interval_ms.to_string(),
                ),
            ],
            Live::Coordinator { startup, .. } => vec![
                entry(&MAX_K, self.max_k().to_string(), startup.max_k.to_string()),
                entry(
                    &HEDGE_DELAY_MS,
                    self.hedge_delay()
                        .map_or(0, |d| d.as_millis() as u64)
                        .to_string(),
                    startup.hedge_delay_ms.to_string(),
                ),
                entry(
                    &SHARD_PRUNING,
                    self.shard_pruning().to_string(),
                    startup.shard_pruning.to_string(),
                ),
                entry(
                    &SIGNAL_BATCH,
                    self.signal_batch().to_string(),
                    startup.signal_batch.to_string(),
                ),
            ],
        }
    }

    /// Every knob this process has, live ones first.
    pub fn list(&self) -> RuntimeKnobs {
        let mut knobs = self.live_entries();
        knobs.extend(
            self.fixed
                .read()
                .expect("knob list lock poisoned")
                .iter()
                .cloned(),
        );
        RuntimeKnobs {
            knobs,
            process: self.process.clone(),
        }
    }

    fn known_names(&self) -> Vec<String> {
        self.list().knobs.into_iter().map(|k| k.name).collect()
    }

    /// Set one live knob. Unknown names, immutable knobs, and values that
    /// do not parse for the knob's kind are rejected by name.
    pub fn set(&self, name: &str, value: &str) -> Result<(), Status> {
        let value = value.trim();
        match &self.live {
            Live::Node {
                share_floors,
                segment_pruning,
                floor_delta,
                floor_warmup_chunks,
                floor_min_interval_ms,
                ..
            } => match name {
                "floor_sharing" => store_bool(share_floors, name, value),
                "segment_pruning" => store_bool(segment_pruning, name, value),
                "floor_delta" => {
                    floor_delta.store(parse_floor_delta(name, value)?.to_bits(), Ordering::Relaxed);
                    Ok(())
                }
                "floor_warmup_chunks" => {
                    floor_warmup_chunks.store(parse_u32(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                "floor_min_interval_ms" => {
                    floor_min_interval_ms.store(parse_u64(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                other => self.reject(other),
            },
            Live::Coordinator {
                max_k,
                hedge_delay_ms,
                shard_pruning,
                signal_batch,
                ..
            } => match name {
                "max_k" => store_nonzero(
                    max_k,
                    name,
                    value,
                    "knob \"max_k\": 0 is not a cap; give the largest k to admit",
                ),
                "hedge_delay_ms" => {
                    hedge_delay_ms.store(parse_u64(name, value)?, Ordering::Relaxed);
                    Ok(())
                }
                "shard_pruning" => store_bool(shard_pruning, name, value),
                "signal_batch" => store_nonzero(
                    signal_batch,
                    name,
                    value,
                    "knob \"signal_batch\": 0 is not a batch; give the ids per call",
                ),
                other => self.reject(other),
            },
        }
    }

    fn reject(&self, name: &str) -> Result<(), Status> {
        if self
            .fixed
            .read()
            .expect("knob list lock poisoned")
            .iter()
            .any(|k| k.name == name)
        {
            return Err(Status::failed_precondition(format!(
                "knob {name:?} is read at startup only; restart the process to change it"
            )));
        }
        Err(Status::invalid_argument(format!(
            "knob {name:?} is not one this process has; known: {}",
            self.known_names().join(", ")
        )))
    }
}

fn parse_bool(name: &str, value: &str) -> Result<bool, Status> {
    match value {
        "true" | "on" | "1" => Ok(true),
        "false" | "off" | "0" => Ok(false),
        other => Err(Status::invalid_argument(format!(
            "knob {name:?}: {other:?} is not a boolean (true/false)"
        ))),
    }
}

fn parse_u32(name: &str, value: &str) -> Result<u32, Status> {
    value.parse::<u32>().map_err(|e| {
        Status::invalid_argument(format!("knob {name:?}: {value:?} is not a u32 ({e})"))
    })
}

fn parse_u64(name: &str, value: &str) -> Result<u64, Status> {
    value.parse::<u64>().map_err(|e| {
        Status::invalid_argument(format!("knob {name:?}: {value:?} is not a u64 ({e})"))
    })
}

/// A floor delta: a finite, non-negative float.
fn parse_floor_delta(name: &str, value: &str) -> Result<f32, Status> {
    let parsed = value.parse::<f32>().map_err(|e| {
        Status::invalid_argument(format!("knob {name:?}: {value:?} is not a float ({e})"))
    })?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err(Status::invalid_argument(format!(
            "knob {name:?}: {parsed} must be finite and not negative"
        )));
    }
    Ok(parsed)
}

fn store_bool(cell: &AtomicBool, name: &str, value: &str) -> Result<(), Status> {
    cell.store(parse_bool(name, value)?, Ordering::Relaxed);
    Ok(())
}

/// A u32 knob that refuses 0 with `zero`.
fn store_nonzero(
    cell: &AtomicU32,
    name: &str,
    value: &str,
    zero: &'static str,
) -> Result<(), Status> {
    let parsed = parse_u32(name, value)?;
    if parsed == 0 {
        return Err(Status::invalid_argument(zero));
    }
    cell.store(parsed, Ordering::Relaxed);
    Ok(())
}
