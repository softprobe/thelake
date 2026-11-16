//! Best-effort post-commit derived read-model hooks for committed span batches.

use crate::models::Span;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type CommitWrite = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Derived Postgres read model updated best-effort after a span batch commits.
pub trait SpanCommitHook: Send + Sync {
    /// Fold `batches` synchronously before DuckLake consumes them.
    /// Return `None` when there is nothing to write.
    fn prepare(&self, batches: &[Vec<Span>]) -> Option<CommitWrite>;
}

#[derive(Clone, Default)]
pub struct PostCommitHooks {
    hooks: Arc<Vec<Arc<dyn SpanCommitHook>>>,
}

impl PostCommitHooks {
    pub fn new(hooks: Vec<Arc<dyn SpanCommitHook>>) -> Self {
        Self {
            hooks: Arc::new(hooks),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }

    pub fn prepare(&self, batches: &[Vec<Span>]) -> Vec<CommitWrite> {
        self.hooks
            .iter()
            .filter_map(|hook| hook.prepare(batches))
            .collect()
    }

    pub async fn apply_all(writes: Vec<CommitWrite>) {
        if writes.is_empty() {
            return;
        }
        futures::future::join_all(writes).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingHook {
        emit: bool,
        prepared: Arc<AtomicUsize>,
        applied: Arc<AtomicUsize>,
    }

    impl SpanCommitHook for CountingHook {
        fn prepare(&self, batches: &[Vec<Span>]) -> Option<CommitWrite> {
            self.prepared.fetch_add(batches.len(), Ordering::SeqCst);
            if !self.emit {
                return None;
            }
            let applied = self.applied.clone();
            Some(Box::pin(async move {
                applied.fetch_add(1, Ordering::SeqCst);
            }))
        }
    }

    #[tokio::test]
    async fn empty_hooks_prepare_and_apply_are_noops() {
        let hooks = PostCommitHooks::default();
        assert!(hooks.is_empty());
        let writes = hooks.prepare(&[]);
        assert!(writes.is_empty());
        PostCommitHooks::apply_all(writes).await;
    }

    #[tokio::test]
    async fn prepare_collects_some_writes_and_apply_all_runs_them() {
        let prepared = Arc::new(AtomicUsize::new(0));
        let applied = Arc::new(AtomicUsize::new(0));
        let hooks = PostCommitHooks::new(vec![
            Arc::new(CountingHook {
                emit: false,
                prepared: prepared.clone(),
                applied: applied.clone(),
            }),
            Arc::new(CountingHook {
                emit: true,
                prepared: prepared.clone(),
                applied: applied.clone(),
            }),
            Arc::new(CountingHook {
                emit: true,
                prepared: prepared.clone(),
                applied: applied.clone(),
            }),
        ]);
        assert!(!hooks.is_empty());
        let batch = vec![vec![crate::session_summary::test_span::span_at("s1", 1)]];
        let writes = hooks.prepare(&batch);
        assert_eq!(prepared.load(Ordering::SeqCst), 3);
        assert_eq!(writes.len(), 2);
        assert_eq!(applied.load(Ordering::SeqCst), 0);

        PostCommitHooks::apply_all(writes).await;
        assert_eq!(applied.load(Ordering::SeqCst), 2);
    }
}
