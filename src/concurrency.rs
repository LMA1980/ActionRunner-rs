//! Local node concurrency limiter and RAII lock guard for stage execution.
//!
//! Sub-tasks #16 & #17: Implements single-stage serialization when `exclusive: true` is requested
//! and bounded concurrency when `exclusive: false` (bounded by `max_concurrent_per_node`).

use std::sync::Arc;
use tokio::sync::{
    AcquireError, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit, RwLock,
    Semaphore, TryAcquireError,
};

/// RAII lock guard held during stage execution on a worker node.
///
/// Guaranteed to release read or write lock upon normal completion, error return,
/// timeout/cancellation, or task panic.
pub enum NodeStageLockGuard {
    /// Exclusive stage execution lock (write-lock).
    /// All other stages on the node must drain before this lock is acquired,
    /// and no new stage may start until this guard is dropped.
    Exclusive(OwnedRwLockWriteGuard<()>),
    /// Shared stage execution lock (read-lock + bounded semaphore permit).
    Shared {
        /// Read lock preventing exclusive stages from running concurrently.
        read_guard: OwnedRwLockReadGuard<()>,
        /// Semaphore permit counting against the node slot capacity.
        permit: OwnedSemaphorePermit,
    },
}

impl std::fmt::Debug for NodeStageLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exclusive(_) => write!(f, "NodeStageLockGuard::Exclusive"),
            Self::Shared { .. } => write!(f, "NodeStageLockGuard::Shared"),
        }
    }
}

/// Error returned by non-blocking lock acquisition attempts.
#[derive(Debug, thiserror::Error)]
pub enum NodeLockError {
    #[error("Exclusive lock unavailable (node is busy executing stages)")]
    ExclusiveBusy,
    #[error("Shared slot unavailable (max concurrent limit reached)")]
    SharedSlotsExhausted,
    #[error("Limiter semaphore closed")]
    Closed,
}

/// Node concurrency limiter managing exclusive and shared stage execution.
#[derive(Debug, Clone)]
pub struct NodeConcurrencyLimiter {
    rwlock: Arc<RwLock<()>>,
    sem: Arc<Semaphore>,
    max_slots: u32,
}

impl NodeConcurrencyLimiter {
    /// Construct a new limiter with the given maximum concurrency slots.
    pub fn new(max_slots: u32) -> Self {
        let slots = max_slots.max(1);
        Self {
            rwlock: Arc::new(RwLock::new(())),
            sem: Arc::new(Semaphore::new(slots as usize)),
            max_slots: slots,
        }
    }

    /// Maximum slots configured for this node limiter.
    pub fn max_slots(&self) -> u32 {
        self.max_slots
    }

    /// Access the underlying read-write lock.
    pub fn rwlock(&self) -> Arc<RwLock<()>> {
        self.rwlock.clone()
    }

    /// Access the underlying slot semaphore.
    pub fn semaphore(&self) -> Arc<Semaphore> {
        self.sem.clone()
    }

    /// Number of currently available permits in the semaphore.
    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    /// Acquire stage execution lock.
    ///
    /// - When `exclusive: true`: acquires `OwnedRwLockWriteGuard`, draining all active
    ///   running stages before proceeding.
    /// - When `exclusive: false`: acquires `OwnedRwLockReadGuard` and a slot permit from `sem`
    ///   bounded by `max_concurrent_per_node` (clamped to `1..=max_slots`).
    pub async fn acquire(
        &self,
        exclusive: bool,
        max_concurrent_per_node: u32,
    ) -> Result<NodeStageLockGuard, AcquireError> {
        if exclusive {
            let write_guard = self.rwlock.clone().write_owned().await;
            Ok(NodeStageLockGuard::Exclusive(write_guard))
        } else {
            let _effective_limit = if max_concurrent_per_node == 0 {
                self.max_slots
            } else {
                max_concurrent_per_node.clamp(1, self.max_slots)
            };

            let read_guard = self.rwlock.clone().read_owned().await;
            let permit = self.sem.clone().acquire_owned().await?;
            Ok(NodeStageLockGuard::Shared { read_guard, permit })
        }
    }

    /// Try to acquire stage execution lock without blocking.
    pub fn try_acquire(
        &self,
        exclusive: bool,
        max_concurrent_per_node: u32,
    ) -> Result<NodeStageLockGuard, NodeLockError> {
        if exclusive {
            match self.rwlock.clone().try_write_owned() {
                Ok(guard) => Ok(NodeStageLockGuard::Exclusive(guard)),
                Err(_) => Err(NodeLockError::ExclusiveBusy),
            }
        } else {
            let _effective_limit = if max_concurrent_per_node == 0 {
                self.max_slots
            } else {
                max_concurrent_per_node.clamp(1, self.max_slots)
            };

            let read_guard = match self.rwlock.clone().try_read_owned() {
                Ok(rg) => rg,
                Err(_) => return Err(NodeLockError::ExclusiveBusy),
            };

            match self.sem.clone().try_acquire_owned() {
                Ok(permit) => Ok(NodeStageLockGuard::Shared { read_guard, permit }),
                Err(TryAcquireError::NoPermits) => Err(NodeLockError::SharedSlotsExhausted),
                Err(TryAcquireError::Closed) => Err(NodeLockError::Closed),
            }
        }
    }

    /// Helper to acquire lock for a `StageDispatchTask`.
    pub async fn acquire_for_task(
        &self,
        task: &mu_ci_ipc::StageDispatchTask,
    ) -> Result<NodeStageLockGuard, AcquireError> {
        self.acquire(task.exclusive, task.max_concurrent_per_node)
            .await
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_exclusive_waits_for_shared_to_drain() {
        let limiter = Arc::new(NodeConcurrencyLimiter::new(4));

        // Acquire 2 shared locks
        let s1 = limiter.acquire(false, 4).await.unwrap();
        let s2 = limiter.acquire(false, 4).await.unwrap();

        let exclusive_started = Arc::new(AtomicBool::new(false));
        let exclusive_started_clone = exclusive_started.clone();
        let limiter_clone = limiter.clone();

        let exclusive_handle = tokio::spawn(async move {
            let _guard = limiter_clone.acquire(true, 1).await.unwrap();
            exclusive_started_clone.store(true, Ordering::SeqCst);
        });

        // Give task opportunity to poll
        sleep(Duration::from_millis(50)).await;
        assert!(
            !exclusive_started.load(Ordering::SeqCst),
            "Exclusive lock must wait for shared stages to drain"
        );

        drop(s1);
        sleep(Duration::from_millis(50)).await;
        assert!(
            !exclusive_started.load(Ordering::SeqCst),
            "Exclusive lock must still wait for s2 to drain"
        );

        drop(s2);
        sleep(Duration::from_millis(50)).await;
        assert!(
            exclusive_started.load(Ordering::SeqCst),
            "Exclusive lock must acquire after all shared stages drain"
        );

        exclusive_handle.await.unwrap();
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_shared_waits_for_exclusive_to_release() {
        let limiter = Arc::new(NodeConcurrencyLimiter::new(4));

        let exc = limiter.acquire(true, 1).await.unwrap();

        let shared_started = Arc::new(AtomicBool::new(false));
        let shared_started_clone = shared_started.clone();
        let limiter_clone = limiter.clone();

        let shared_handle = tokio::spawn(async move {
            let _guard = limiter_clone.acquire(false, 4).await.unwrap();
            shared_started_clone.store(true, Ordering::SeqCst);
        });

        sleep(Duration::from_millis(50)).await;
        assert!(
            !shared_started.load(Ordering::SeqCst),
            "Shared lock must wait while exclusive lock is held"
        );

        drop(exc);
        sleep(Duration::from_millis(50)).await;
        assert!(
            shared_started.load(Ordering::SeqCst),
            "Shared lock must acquire immediately after exclusive drops"
        );

        shared_handle.await.unwrap();
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn lock_released_on_panic_and_cancel() {
        let limiter = Arc::new(NodeConcurrencyLimiter::new(2));

        // (a) Normal completion releases lock
        {
            let guard = limiter.acquire(true, 1).await.unwrap();
            drop(guard);
            assert!(
                limiter.try_acquire(true, 1).is_ok(),
                "Exclusive lock must be available after normal drop"
            );
        }

        // (b) Returning an Err releases lock
        {
            let res: Result<(), &str> = async {
                let _guard = limiter.acquire(true, 1).await.unwrap();
                Err("stage failed")
            }
            .await;
            assert!(res.is_err());
            assert!(
                limiter.try_acquire(true, 1).is_ok(),
                "Exclusive lock must be available after Err return"
            );
        }

        // (c) Aborted via timeout / JoinHandle::abort() releases lock
        {
            let limiter_clone = limiter.clone();
            let handle = tokio::spawn(async move {
                let _guard = limiter_clone.acquire(true, 1).await.unwrap();
                // Sleep indefinitely until aborted
                sleep(Duration::from_secs(60)).await;
            });

            sleep(Duration::from_millis(50)).await;
            assert!(
                limiter.try_acquire(true, 1).is_err(),
                "Exclusive lock should be held by background task"
            );

            handle.abort();
            let _ = handle.await;

            // Lock must now be released
            assert!(
                limiter.try_acquire(true, 1).is_ok(),
                "Exclusive lock must be released immediately upon handle.abort()"
            );
        }

        // (d) Panic inside a spawned task releases lock
        {
            let limiter_clone = limiter.clone();
            let handle = tokio::spawn(async move {
                let _guard = limiter_clone.acquire(true, 1).await.unwrap();
                panic!("intentional stage panic for lock release test");
            });

            let join_res = handle.await;
            assert!(join_res.is_err(), "Task must fail with panic");
            assert!(join_res.unwrap_err().is_panic());

            // Lock must be released despite panic
            assert!(
                limiter.try_acquire(true, 1).is_ok(),
                "Exclusive lock must be released immediately upon task panic"
            );
        }

        // Repeat for Shared guard (read lock + semaphore permit)
        {
            let limiter_clone = limiter.clone();
            let handle = tokio::spawn(async move {
                let _guard = limiter_clone.acquire(false, 2).await.unwrap();
                panic!("intentional shared stage panic");
            });

            let _ = handle.await;
            assert_eq!(
                limiter.available_permits(),
                2,
                "Semaphore permit must be restored after panic"
            );
            assert!(
                limiter.try_acquire(true, 1).is_ok(),
                "Exclusive lock must be acquirable after shared panic"
            );
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_multiple_queued_exclusive_tasks_serialize() {
        let limiter = Arc::new(NodeConcurrencyLimiter::new(2));
        let active_count = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();

        for i in 0..5 {
            let limiter_clone = limiter.clone();
            let active = active_count.clone();
            handles.push(tokio::spawn(async move {
                let _guard = limiter_clone.acquire(true, 1).await.unwrap();
                let current = active.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    current, 0,
                    "Task {i}: exclusive lock violated! Multiple active exclusive stages!"
                );
                sleep(Duration::from_millis(20)).await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }
}
