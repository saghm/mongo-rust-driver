use std::{future::Future, pin::Pin, sync::Arc};

pub trait Executor {
    fn execute(&self, fut: BoxSendFuture);
}

pub type BoxSendFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub(crate) enum Exec {
    #[cfg(feature = "tokio-runtime")]
    Tokio,

    #[cfg(feature = "async-std-runtime")]
    AsyncStd,

    /// Contains a user-defined executor.
    Custom(Arc<dyn Executor>),
}

impl Exec {
    pub(crate) fn execute<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio => {
                tokio::task::spawn(fut);
            }
            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd => {
                async_std::task::spawn(fut);
            }
            Self::Custom(ref e) => {
                e.execute(Box::pin(fut));
            }
        }
    }
}
