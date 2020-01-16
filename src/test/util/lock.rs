use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct TestLock {
    inner: RwLock<()>,
}

impl TestLock {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(()),
        }
    }

    pub async fn run_concurrently(&self) -> RwLockReadGuard<'_, ()> {
        self.inner.read().await
    }

    pub async fn run_exclusively(&self) -> RwLockWriteGuard<'_, ()> {
        self.inner.write().await
    }
}
