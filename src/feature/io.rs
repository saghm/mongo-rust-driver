use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait AsyncReadWrite {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    async fn write(&mut self, buf: &[u8]) -> Result<usize>;
}

pub(crate) enum AsyncStream {
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::net::TcpStream),

    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::net::TcpStream),

    /// Contains a user-defined stream.
    Custom(Box<dyn AsyncReadWrite>),
}

impl AsyncStream {
    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                use tokio::io::AsyncReadExt;

                let count = stream.read(buf).await?;
                Ok(count)
            }
            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::ReadExt;

                stream.read(buf).await
            }
            Self::Custom(ref mut stream) => stream.read(buf).await,
        }
    }

    pub(crate) async fn write(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                use tokio::io::AsyncWriteExt;

                let count = stream.write(buf).await?;
                Ok(count)
            }
            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::WriteExt;

                stream.write(buf).await
            }
            Self::Custom(ref mut stream) => stream.write(buf).await,
        }
    }
}
