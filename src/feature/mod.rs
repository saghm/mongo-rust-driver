mod executor;
mod stream;

use std::future::Future;

use derivative::Derivative;

pub(crate) use self::stream::AsyncStream;
use crate::{cmap::conn::StreamOptions, error::Result};

#[cfg(feature = "custom-runtime")]
pub use executor::Execute;

#[cfg(feature = "custom-runtime")]
pub use stream::connect::{AsyncReadWrite, Connect};

#[cfg(feature = "custom-runtime")]
pub use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(test)]
#[macro_export]
macro_rules! define_test {
    ($name:ident, $body:block) => {
        paste::item! {
            #[cfg(feature = "tokio-runtime")]
            #[tokio::test]
            async fn [<$name _tokio>]() {
                $body
            }
        }

        paste::item! {
            #[cfg(all(not(feature = "tokio-runtime"), feature = "async-std-runtime"))]
            #[async_std::test]
            async fn [<$name _async_std>]() {
                $body
            }
        }
    };
}

#[cfg(feature = "custom-runtime")]
#[derive(Clone)]
pub struct CustomAsyncRuntime {
    pub executor: std::sync::Arc<dyn Execute + Send + Sync>,
    pub stream_connector: std::sync::Arc<dyn Connect + Send + Sync>,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum AsyncRuntime {
    #[cfg(feature = "tokio-runtime")]
    Tokio,

    #[cfg(feature = "async-std-runtime")]
    AsyncStd,

    #[cfg(feature = "custom-runtime")]
    Custom(#[derivative(Debug = "ignore")] CustomAsyncRuntime),
}

impl Default for AsyncRuntime {
    fn default() -> Self {
        // If no runtime is given, use tokio if enabled.
        #[cfg(feature = "tokio-runtime")]
        {
            AsyncRuntime::Tokio
        }

        // If no runtime is given and tokio is not enabled, use async-std if enabled.
        #[cfg(all(not(feature = "tokio-runtime"), feature = "async-std-runtime"))]
        {
            AsyncRuntime::AsyncStd
        }

        // If no runtime is given and neither tokio or async-std is enabled, return a
        // ConfigurationError.
        #[cfg(all(
            not(feature = "tokio-runtime"),
            not(feature = "async-std-runtime"),
            not(feature = "custom-runtime")
        ))]
        compile_error!(
            "One of the `tokio-runtime`, `async-runtime`, or `custom-runtime` features must be \
             enabled."
        )
    }
}

impl AsyncRuntime {
    #[cfg(test)]
    pub(crate) fn block_on<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        use tokio::sync::oneshot;

        let (sender, receiver) = oneshot::channel();
        self.execute(async move {
            let _ = sender.send(fut.await);
        });

        futures::executor::block_on(async move { receiver.await.unwrap() })
    }

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

            #[cfg(feature = "custom-runtime")]
            Self::Custom(ref runtime) => {
                runtime.executor.execute(Box::pin(fut));
            }
        }
    }

    pub(crate) async fn connect_stream(&self, options: StreamOptions) -> Result<AsyncStream> {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio => AsyncStream::connect_tokio(options).await,

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd => AsyncStream::connect_async_std(options).await,

            #[cfg(feature = "custom-runtime")]
            Self::Custom(ref runtime) => {
                Ok(runtime.stream_connector.connect(options).await?.into())
            }
        }
    }
}
