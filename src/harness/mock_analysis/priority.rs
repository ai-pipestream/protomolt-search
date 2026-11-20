//! A one-slot accelerator shared by two admission lanes, with deterministic
//! queue-depth and displacement evidence for priority tests.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, Notify};

/// The admission lane a call names in `x-turbo-priority`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Interactive,
    Bulk,
}

impl Lane {
    /// The lane `metadata` asks for; a call that names none is bulk.
    pub fn of(metadata: &tonic::metadata::MetadataMap) -> Self {
        match metadata
            .get("x-turbo-priority")
            .and_then(|v| v.to_str().ok())
        {
            Some("interactive") => Lane::Interactive,
            _ => Lane::Bulk,
        }
    }
}

/// What the lanes admitted, including displacement at interactive grants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LaneStats {
    pub interactive_units: u64,
    pub bulk_units: u64,
    pub max_interactive_wait: Duration,
    pub max_bulk_wait: Duration,
    /// Most bulk units admitted between an interactive arrival and its grant.
    pub max_units_before_interactive: u64,
    /// Bulk waiters at interactive arrivals that those grants passed.
    pub overtaken_bulk_waiters: u64,
}

#[derive(Default)]
struct Slot {
    busy: bool,
    interactive: VecDeque<oneshot::Sender<()>>,
    bulk: VecDeque<oneshot::Sender<()>>,
    stats: LaneStats,
}

/// The slot, its queues, and queue-depth notifications.
pub struct PriorityLanes {
    unit: Duration,
    slot: Mutex<Slot>,
    changed: Notify,
    #[cfg(test)]
    gate: Option<(Arc<tokio::sync::Barrier>, std::sync::atomic::AtomicBool)>,
}

/// Hands the slot on when a running unit ends, or is dropped mid-unit.
struct Held<'a>(&'a PriorityLanes);

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// A queued unit's claim on the slot and its arrival measurements.
struct Ticket<'a> {
    lanes: &'a PriorityLanes,
    granted: oneshot::Receiver<()>,
    ran: bool,
    bulk_at_arrival: u64,
    bulk_waiters: usize,
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        if !self.ran && self.granted.try_recv().is_ok() {
            self.lanes.release();
        }
    }
}

impl PriorityLanes {
    /// Lanes whose every unit holds the slot for `unit`.
    pub fn new(unit: Duration) -> Arc<Self> {
        Arc::new(Self {
            unit,
            slot: Mutex::new(Slot::default()),
            changed: Notify::new(),
            #[cfg(test)]
            gate: None,
        })
    }

    #[cfg(test)]
    fn with_gate() -> (Arc<Self>, Arc<tokio::sync::Barrier>) {
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let lanes = Arc::new(Self {
            unit: Duration::ZERO,
            slot: Mutex::new(Slot::default()),
            changed: Notify::new(),
            gate: Some((release.clone(), std::sync::atomic::AtomicBool::new(false))),
        });
        (lanes, release)
    }

    /// Run one unit of `lane`'s work: wait for the slot, hold it for one
    /// unit, hand it on.
    pub async fn run(&self, lane: Lane) {
        let queued = Instant::now();
        let ticket = {
            let mut slot = self.slot.lock().unwrap();
            if slot.busy {
                let (tx, rx) = oneshot::channel();
                let (bulk_at_arrival, bulk_waiters) = match lane {
                    Lane::Interactive => (slot.stats.bulk_units, slot.bulk.len()),
                    Lane::Bulk => (0, 0),
                };
                match lane {
                    Lane::Interactive => slot.interactive.push_back(tx),
                    Lane::Bulk => slot.bulk.push_back(tx),
                }
                self.changed.notify_waiters();
                Some(Ticket {
                    lanes: self,
                    granted: rx,
                    ran: false,
                    bulk_at_arrival,
                    bulk_waiters,
                })
            } else {
                slot.busy = true;
                None
            }
        };
        if let Some(mut ticket) = ticket {
            let _ = (&mut ticket.granted).await;
            ticket.ran = true;
            self.record(lane, queued.elapsed(), Some(&ticket));
        } else {
            self.record(lane, queued.elapsed(), None);
        }
        let held = Held(self);
        #[cfg(test)]
        if let Some((gate, entered)) = &self.gate {
            if !entered.swap(true, std::sync::atomic::Ordering::AcqRel) {
                gate.wait().await;
            }
        }
        tokio::time::sleep(self.unit).await;
        drop(held);
    }

    /// Wait until at least `bulk` and `interactive` waiters are queued.
    pub async fn wait_for_waiters(&self, bulk: usize, interactive: usize) {
        loop {
            let changed = self.changed.notified();
            let ready = {
                let slot = self.slot.lock().unwrap();
                slot.bulk.len() >= bulk && slot.interactive.len() >= interactive
            };
            if ready {
                return;
            }
            changed.await;
        }
    }

    #[cfg(test)]
    async fn wait_for_bulk_units(&self, units: u64) {
        loop {
            let changed = self.changed.notified();
            if self.slot.lock().unwrap().stats.bulk_units >= units {
                return;
            }
            changed.await;
        }
    }

    fn record(&self, lane: Lane, waited: Duration, ticket: Option<&Ticket<'_>>) {
        let mut slot = self.slot.lock().unwrap();
        let stats = &mut slot.stats;
        match lane {
            Lane::Interactive => {
                stats.interactive_units += 1;
                stats.max_interactive_wait = stats.max_interactive_wait.max(waited);
                if let Some(ticket) = ticket {
                    let units = stats.bulk_units.saturating_sub(ticket.bulk_at_arrival);
                    stats.max_units_before_interactive =
                        stats.max_units_before_interactive.max(units);
                    stats.overtaken_bulk_waiters +=
                        ticket.bulk_waiters.saturating_sub(units as usize) as u64;
                }
            }
            Lane::Bulk => {
                stats.bulk_units += 1;
                stats.max_bulk_wait = stats.max_bulk_wait.max(waited);
            }
        }
        self.changed.notify_waiters();
    }

    /// Hand the slot to the oldest live interactive waiter, else the
    /// oldest live bulk one, else free it.
    fn release(&self) {
        let mut slot = self.slot.lock().unwrap();
        loop {
            let next = match slot.interactive.pop_front() {
                Some(waiter) => Some(waiter),
                None => slot.bulk.pop_front(),
            };
            let Some(waiter) = next else {
                slot.busy = false;
                self.changed.notify_waiters();
                return;
            };
            if waiter.send(()).is_ok() {
                self.changed.notify_waiters();
                return;
            }
        }
    }

    pub fn stats(&self) -> LaneStats {
        self.slot.lock().unwrap().stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_freed_slot_goes_to_interactive_before_queued_bulk() {
        let (lanes, release) = PriorityLanes::with_gate();
        let order = Arc::new(Mutex::new(Vec::new()));
        let run = |lane, tag: &'static str| {
            let (lanes, order) = (Arc::clone(&lanes), Arc::clone(&order));
            tokio::spawn(async move {
                lanes.run(lane).await;
                order.lock().unwrap().push(tag);
            })
        };
        let running = run(Lane::Bulk, "bulk-running");
        lanes.wait_for_bulk_units(1).await;
        let queued = run(Lane::Bulk, "bulk-queued");
        let later = run(Lane::Bulk, "bulk-queued-later");
        let interactive = run(Lane::Interactive, "interactive");
        lanes.wait_for_waiters(2, 1).await;
        release.wait().await;
        for task in [running, queued, later, interactive] {
            task.await.unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            [
                "bulk-running",
                "interactive",
                "bulk-queued",
                "bulk-queued-later"
            ]
        );
        let stats = lanes.stats();
        assert_eq!((stats.interactive_units, stats.bulk_units), (1, 3));
        assert_eq!(stats.max_units_before_interactive, 0);
        assert_eq!(stats.overtaken_bulk_waiters, 2);
    }

    #[test]
    fn a_call_naming_no_lane_is_bulk() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        assert_eq!(Lane::of(&metadata), Lane::Bulk);
        metadata.insert("x-turbo-priority", "interactive".parse().unwrap());
        assert_eq!(Lane::of(&metadata), Lane::Interactive);
        metadata.insert("x-turbo-priority", "bulk".parse().unwrap());
        assert_eq!(Lane::of(&metadata), Lane::Bulk);
    }
}
