use tokio::io::{AsyncRead, AsyncWrite};

use crate::{cmap::conn::StreamOptions, error::Result};

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Sync {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Sync {}

#[async_trait::async_trait]
pub trait Connect {
    async fn connect(&self, options: StreamOptions) -> Result<Box<dyn AsyncReadWrite>>;
}
