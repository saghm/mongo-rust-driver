use std::{future::Future, pin::Pin};

#[async_trait::async_trait]
pub trait Execute {
    fn execute(&self, fut: BoxSendFuture);

    fn block_on<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T>,
        Self: Sized;
}

pub type BoxSendFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
