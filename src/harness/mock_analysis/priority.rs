//! A one-slot accelerator shared by two admission lanes, the scheduling a
//! priority-enforcing analysis server applies: every unit of work holds
//! the slot for a fixed time, and a freed slot goes to the oldest
//! interactive waiter before any bulk one. A bulk unit already running
//! is never preempted, so an interactive call waits at most one unit.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

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

/// What the lanes admitted: units per lane, and the longest any unit of
/// each lane waited for the slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LaneStats {
    pub interactive_units: u64,
    pub bulk_units: u64,
    pub max_interactive_wait: Duration,
    pub max_bulk_wait: Duration,
}

#[derive(Default)]
struct Slot {
    busy: bool,
    interactive: VecDeque<oneshot::Sender<()>>,
    bulk: VecDeque<oneshot::Sender<()>>,
    stats: LaneStats,
}

/// The slot and its two queues.
pub struct PriorityLanes {
    unit: Duration,
    slot: Mutex<Slot>,
}

/// Hands the slot on when a running unit ends, or is dropped mid-unit.
struct Held<'a>(&'a PriorityLanes);

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// A queued unit's claim on the slot. Dropped after the slot was handed
/// to it but before it ran, it hands the slot on.
struct Ticket<'a> {
    lanes: &'a PriorityLanes,
    granted: oneshot::Receiver<()>,
    ran: bool,
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
        })
    }

    /// Run one unit of `lane`'s work: wait for the slot, hold it for one
    /// unit, hand it on.
    pub async fn run(&self, lane: Lane) {
        let queued = Instant::now();
        let ticket = {
            let mut slot = self.slot.lock().unwrap();
            if slot.busy {
                let (tx, rx) = oneshot::channel();
                match lane {
                    Lane::Interactive => slot.interactive.push_back(tx),
                    Lane::Bulk => slot.bulk.push_back(tx),
                }
                Some(Ticket {
                    lanes: self,
                    granted: rx,
                    ran: false,
                })
            } else {
                slot.busy = true;
                None
            }
        };
        if let Some(mut ticket) = ticket {
            // A live waiter is only ever sent the slot, never dropped unsent.
            let _ = (&mut ticket.granted).await;
            ticket.ran = true;
        }
        let held = Held(self);
        self.record(lane, queued.elapsed());
        tokio::time::sleep(self.unit).await;
        drop(held);
    }

    fn record(&self, lane: Lane, waited: Duration) {
        let mut slot = self.slot.lock().unwrap();
        let stats = &mut slot.stats;
        match lane {
            Lane::Interactive => {
                stats.interactive_units += 1;
                stats.max_interactive_wait = stats.max_interactive_wait.max(waited);
            }
            Lane::Bulk => {
                stats.bulk_units += 1;
                stats.max_bulk_wait = stats.max_bulk_wait.max(waited);
            }
        }
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
                return;
            };
            if waiter.send(()).is_ok() {
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
        let lanes = PriorityLanes::new(Duration::from_millis(20));
        let order = Arc::new(Mutex::new(Vec::new()));
        let run = |lane, tag: &'static str, after: u64| {
            let (lanes, order) = (Arc::clone(&lanes), Arc::clone(&order));
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(after)).await;
                lanes.run(lane).await;
                order.lock().unwrap().push(tag);
            })
        };
        let tasks = [
            run(Lane::Bulk, "bulk-running", 0),
            run(Lane::Bulk, "bulk-queued", 2),
            run(Lane::Bulk, "bulk-queued-later", 4),
            run(Lane::Interactive, "interactive", 6),
        ];
        for task in tasks {
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
        assert!(stats.max_interactive_wait < stats.max_bulk_wait);
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
