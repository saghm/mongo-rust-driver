#[cfg(feature = "custom-runtime")]
pub(super) mod connect;

use std::{pin::Pin, time::Duration};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{cmap::conn::StreamOptions, error::Result};

#[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) enum AsyncStream {
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::net::TcpStream),

    #[cfg(feature = "tokio-runtime")]
    TokioTls(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),

    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::net::TcpStream),

    #[cfg(feature = "async-std-runtime")]
    AsyncStdTls(async_tls::client::TlsStream<async_std::net::TcpStream>),

    #[cfg(feature = "custom-runtime")]
    Custom(Pin<Box<dyn connect::AsyncReadWrite>>),
}

#[cfg(feature = "tokio-runtime")]
impl From<tokio::net::TcpStream> for AsyncStream {
    fn from(stream: tokio::net::TcpStream) -> Self {
        Self::Tokio(stream)
    }
}

#[cfg(feature = "async-std-runtime")]
impl From<async_std::net::TcpStream> for AsyncStream {
    fn from(stream: async_std::net::TcpStream) -> Self {
        Self::AsyncStd(stream)
    }
}

#[cfg(feature = "custom-runtime")]
impl From<Box<dyn connect::AsyncReadWrite>> for AsyncStream {
    fn from(stream: Box<dyn connect::AsyncReadWrite>) -> Self {
        Self::Custom(stream.into())
    }
}

impl AsyncStream {
    #[cfg(feature = "tokio-runtime")]
    pub(super) async fn connect_tokio(options: StreamOptions) -> Result<AsyncStream> {
        use std::sync::Arc;

        use tokio::net::TcpStream;
        use tokio_rustls::TlsConnector;
        use webpki::DNSNameRef;

        let std_timeout = options.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        let timeout = tokio::time::Duration::new(std_timeout.as_secs(), std_timeout.subsec_nanos());

        let stream_future = TcpStream::connect((
            options.address.hostname.as_str(),
            options.address.port.unwrap_or(27017),
        ));

        // The URI options spec requires that the default is 10 seconds, but that 0 should indicate
        // no timeout.
        let inner = if timeout.as_nanos() == 0 {
            stream_future.await?
        } else {
            tokio::time::timeout(timeout, stream_future).await??
        };

        inner.set_nodelay(true)?;

        match options.tls_options {
            Some(cfg) => {
                let name = DNSNameRef::try_from_ascii_str(&options.address.hostname)?;
                let mut tls_config = cfg.into_rustls_config()?;
                tls_config.enable_sni = true;

                let session = TlsConnector::from(Arc::new(tls_config))
                    .connect(name, inner)
                    .await?;

                Ok(Self::TokioTls(session))
            }
            None => Ok(Self::Tokio(inner)),
        }
    }

    #[cfg(feature = "async-std-runtime")]
    pub(crate) async fn connect_async_std(options: StreamOptions) -> Result<AsyncStream> {
        use std::sync::Arc;

        use async_std::net::TcpStream;
        use async_tls::TlsConnector;

        let timeout = options.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let stream_future = TcpStream::connect((
            options.address.hostname.as_str(),
            options.address.port.unwrap_or(27017),
        ));

        // The URI options spec requires that the default is 10 seconds, but that 0 should indicate
        // no timeout.
        let inner = if timeout.as_nanos() == 0 {
            stream_future.await?
        } else {
            async_std::future::timeout(timeout, stream_future).await??
        };

        inner.set_nodelay(true)?;

        match options.tls_options {
            Some(cfg) => {
                let mut tls_config = cfg.into_rustls_config()?;
                tls_config.enable_sni = true;

                let session = TlsConnector::from(Arc::new(tls_config))
                    .connect(options.address.hostname, inner)?
                    .await?;

                Ok(Self::AsyncStdTls(session))
            }
            None => Ok(Self::AsyncStd(inner)),
        }
    }

    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let count = match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                use tokio::io::AsyncReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "tokio-runtime")]
            Self::TokioTls(ref mut stream) => {
                use tokio::io::AsyncReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::ReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStdTls(ref mut stream) => {
                use async_std::io::ReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "custom-runtime")]
            Self::Custom(ref mut stream) => {
                use tokio::io::AsyncReadExt;

                stream.read(buf).await?
            }
        };

        Ok(count)
    }

    pub(crate) async fn write(&mut self, buf: &mut [u8]) -> Result<usize> {
        let count = match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                use tokio::io::AsyncWriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "tokio-runtime")]
            Self::TokioTls(ref mut stream) => {
                use tokio::io::AsyncWriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::prelude::WriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStdTls(ref mut stream) => {
                use async_std::io::prelude::WriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "custom-runtime")]
            Self::Custom(ref mut stream) => {
                use tokio::io::AsyncWriteExt;

                stream.write(buf).await?
            }
        };

        Ok(count)
    }
}

impl AsyncRead for AsyncStream {
}
