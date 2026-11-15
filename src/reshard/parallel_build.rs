//! Building planned images on several threads and placing them in plan
//! order, the shared engine of a segmented re-placement split and a
//! compaction from segments.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crate::analyzer::SessionLayers;
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;

use super::Analyzer;

/// An analyzer for a transplant build, which takes every row's fields
/// from the spill: being called is a defect, refused with `message`.
pub(super) fn refusing_analyzer(message: &'static str) -> Box<Analyzer<'static>> {
    Box::new(
        move |_: &[(&str, Option<&AnalysisSpec>, SessionLayers)]| -> Result<Vec<AnalyzedDoc>, String> {
            Err(message.to_string())
        },
    )
}

/// How many builds run at once, and how many finished results may wait
/// for the ordered placement.
#[derive(Debug, Clone, Copy)]
pub(super) struct ParallelBuild {
    pub(super) threads: usize,
    /// The backlog of finished results waiting on the ordered placement.
    /// A worker waits for space BEFORE claiming an item, never while
    /// holding a finished one (placement takes them in plan order, so a
    /// worker holding the wanted item while it waits for space would
    /// deadlock). Workers may all pass the check at once, so the backlog
    /// can reach the queue plus the thread count; the builds in flight
    /// stay the threads'.
    pub(super) queue: usize,
    /// What one item is called in a stop message ("bucket", "cut").
    pub(super) item: &'static str,
}

impl ParallelBuild {
    /// `threads` workers and a backlog of `queue` (the thread count when
    /// `None`), both bounded by the number of items.
    pub(super) fn new(
        threads: usize,
        queue: Option<usize>,
        items: usize,
        item: &'static str,
    ) -> Self {
        let most = items.max(1);
        Self {
            threads: threads.min(most),
            queue: queue.unwrap_or(threads).min(most),
            item,
        }
    }

    /// Build every item of `plan` with `build` on the workers and hand
    /// each result to `place` in plan order. The first failure, of a
    /// build or a placement, stops every worker and is returned.
    pub(super) fn run<T, R>(
        &self,
        plan: &[T],
        build: impl Fn(&T) -> Result<R, String> + Sync,
        mut place: impl FnMut(&T, R) -> Result<(), String>,
    ) -> Result<(), String>
    where
        T: Sync,
        R: Send,
    {
        let shared = Shared::<R>::default();
        std::thread::scope(|scope| {
            for _ in 0..self.threads {
                scope.spawn(|| self.work(&shared, plan, &build));
            }
            for (i, item) in plan.iter().enumerate() {
                let result = self.take(&shared, i);
                shared.ready.notify_all();
                if let Err(error) = result.and_then(|built| place(item, built)) {
                    shared.stop.store(true, Ordering::SeqCst);
                    return Err(error);
                }
            }
            Ok(())
        })
    }

    /// One worker: claim the next item when the backlog has room, build
    /// it, and file the result by plan position.
    fn work<T, R>(
        &self,
        shared: &Shared<R>,
        plan: &[T],
        build: &(impl Fn(&T) -> Result<R, String> + Sync),
    ) {
        while !shared.stopped() {
            {
                let mut guard = shared.done.lock().expect("build results");
                while guard.len() >= self.queue && !shared.stopped() {
                    guard = shared
                        .ready
                        .wait_timeout(guard, Duration::from_millis(200))
                        .expect("build results")
                        .0;
                }
            }
            if shared.stopped() {
                break;
            }
            let i = shared.next.fetch_add(1, Ordering::SeqCst);
            let Some(item) = plan.get(i) else {
                break;
            };
            let result = build(item);
            let failed = result.is_err();
            shared.done.lock().expect("build results").insert(i, result);
            shared.ready.notify_all();
            if failed {
                shared.stop.store(true, Ordering::SeqCst);
                break;
            }
        }
    }

    /// The result at plan position `i`, waiting for it; once the build
    /// has stopped, the first failure filed instead.
    fn take<R>(&self, shared: &Shared<R>, i: usize) -> Result<R, String> {
        let mut guard = shared.done.lock().expect("build results");
        loop {
            if let Some(result) = guard.remove(&i) {
                return result;
            }
            if shared.stopped() {
                return Err(guard
                    .values()
                    .find_map(|r| r.as_ref().err().cloned())
                    .unwrap_or_else(|| format!("a {} build stopped without a result", self.item)));
            }
            guard = shared
                .ready
                .wait_timeout(guard, Duration::from_millis(200))
                .expect("build results")
                .0;
        }
    }
}

/// What the workers and the placing thread share.
struct Shared<R> {
    next: AtomicUsize,
    stop: AtomicBool,
    done: Mutex<BTreeMap<usize, Result<R, String>>>,
    ready: Condvar,
}

impl<R> Default for Shared<R> {
    fn default() -> Self {
        Self {
            next: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            done: Mutex::new(BTreeMap::new()),
            ready: Condvar::new(),
        }
    }
}

impl<R> Shared<R> {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }
}
