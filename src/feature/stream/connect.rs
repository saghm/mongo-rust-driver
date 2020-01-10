use tokio::io::{AsyncRead, AsyncWrite};

use crate::{cmap::conn::StreamOptions, error::Result};

pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite {}

#[async_trait::async_trait]
pub trait Connect {
    async fn connect(&self, options: StreamOptions) -> Result<Box<dyn AsyncReadWrite>>;
}
