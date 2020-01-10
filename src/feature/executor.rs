use std::{future::Future, pin::Pin};

pub trait Execute {
    fn execute(&self, fut: BoxSendFuture);
}

pub type BoxSendFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
