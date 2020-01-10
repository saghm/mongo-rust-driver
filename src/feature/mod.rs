mod executor;
mod stream;

use std::future::Future;

use crate::{cmap::conn::StreamOptions, error::Result};

use self::stream::AsyncStream;

#[cfg(feature = "custom-runtime")]
pub use executor::Execute;

#[cfg(feature = "custom-runtime")]
pub use stream::connect::{AsyncReadWrite, Connect};

#[cfg(feature = "custom-runtime")]
pub use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(feature = "custom-runtime")]
pub struct CustomAsyncRuntime {
    pub executor: std::sync::Arc<dyn Execute + Send>,
    pub stream_connector: std::sync::Arc<dyn Connect + Send>,
}

pub(crate) enum AsyncRuntime {
    #[cfg(feature = "tokio-runtime")]
    Tokio,

    #[cfg(feature = "async-std-runtime")]
    AsyncStd,

    #[cfg(feature = "custom-runtime")]
    Custom(CustomAsyncRuntime),
}

impl AsyncRuntime {
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
