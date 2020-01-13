use std::{future::Future, pin::Pin};

#[async_trait::async_trait]
pub trait Execute {
    fn execute(&self, fut: BoxSendFuture);
}

pub type BoxSendFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
