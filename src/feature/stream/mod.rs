#[cfg(feature = "custom-runtime")]
pub(super) mod connect;

use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{cmap::conn::StreamOptions, error::Result};

#[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) enum AsyncStream{
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
        Self {
            inner: AsyncStream::Tokio(stream),
        }
    }
}

#[cfg(feature = "async-std-runtime")]
impl From<async_std::net::TcpStream> for AsyncStream {
    fn from(stream: async_std::net::TcpStream) -> Self {
        Self {
            inner: AsyncStream::AsyncStd(stream),
            bytes_read: 0,
        }
    }
}

#[cfg(feature = "custom-runtime")]
impl From<Box<dyn connect::AsyncReadWrite>> for AsyncStream {
    fn from(stream: Box<dyn connect::AsyncReadWrite>) -> Self {
        Self {
            inner: AsyncStream::Custom(stream.into()),
            bytes_read: 0,
        }
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

        let inner = match options.tls_options {
            Some(cfg) => {
                let name = DNSNameRef::try_from_ascii_str(&options.address.hostname)?;
                let mut tls_config = cfg.into_rustls_config()?;
                tls_config.enable_sni = true;

                let session = TlsConnector::from(Arc::new(tls_config))
                    .connect(name, inner)
                    .await?;

                AsyncStream::TokioTls(session)
            }
            None => AsyncStream::Tokio(inner),
        };

        Ok(Self {
            inner,
        })
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

        let inner = match options.tls_options {
            Some(cfg) => {
                let mut tls_config = cfg.into_rustls_config()?;
                tls_config.enable_sni = true;

                let session = TlsConnector::from(Arc::new(tls_config))
                    .connect(options.address.hostname, inner)?
                    .await?;

                AsyncStream::AsyncStdTls(session)
            }
            None => AsyncStream::AsyncStd(inner),
        };

        Ok(Self {
            inner,
        })
    }

    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let count = match self.inner {
            #[cfg(feature = "tokio-runtime")]
            AsyncStream::Tokio(ref mut stream) => {
                use tokio::io::AsyncReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "tokio-runtime")]
            AsyncStream::TokioTls(ref mut stream) => {
                use tokio::io::AsyncReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStd(ref mut stream) => {
                use async_std::io::ReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStdTls(ref mut stream) => {
                use async_std::io::ReadExt;

                stream.read(buf).await?
            }

            #[cfg(feature = "custom-runtime")]
            AsyncStream::Custom(ref mut stream) => {
                use tokio::io::AsyncReadExt;

                stream.read(buf).await?
            }
        };

        Ok(count)
    }

    pub(crate) async fn write(&mut self, buf: &mut [u8]) -> Result<usize> {
        let count = match self.inner {
            #[cfg(feature = "tokio-runtime")]
            AsyncStream::Tokio(ref mut stream) => {
                use tokio::io::AsyncWriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "tokio-runtime")]
            AsyncStream::TokioTls(ref mut stream) => {
                use tokio::io::AsyncWriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStd(ref mut stream) => {
                use async_std::io::prelude::WriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStdTls(ref mut stream) => {
                use async_std::io::prelude::WriteExt;

                stream.write(buf).await?
            }

            #[cfg(feature = "custom-runtime")]
            AsyncStream::Custom(ref mut stream) => {
                use tokio::io::AsyncWriteExt;

                stream.write(buf).await?
            }
        };

        Ok(count)
    }
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<tokio::io::Result<usize>> {
        let result = match self.deref_mut().inner {
            #[cfg(feature = "tokio-runtime")]
            AsyncStream::Tokio(ref mut stream) => Pin::new(stream).poll_read(cx, buf),

            #[cfg(feature = "tokio-runtime")]
            AsyncStream::TokioTls(ref mut stream) => Pin::new(stream).poll_read(cx, buf),

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStd(ref mut stream) => {
                use async_std::io::Read;

                Pin::new(stream).poll_read(cx, buf)
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStdTls(ref mut stream) => {
                use async_std::io::Read;

                Pin::new(stream).poll_read(cx, buf)
            }

            #[cfg(feature = "custom-runtime")]
            AsyncStream::Custom(ref mut stream) => stream.as_mut().poll_read(cx, buf),
        };

        result
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        match self.deref_mut().inner {
            #[cfg(feature = "tokio-runtime")]
            AsyncStream::Tokio(ref mut stream) => Pin::new(stream).poll_write(cx, buf),

            #[cfg(feature = "tokio-runtime")]
            AsyncStream::TokioTls(ref mut stream) => Pin::new(stream).poll_write(cx, buf),

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStd(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_write(cx, buf)
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStdTls(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_write(cx, buf)
            }

            #[cfg(feature = "custom-runtime")]
            AsyncStream::Custom(ref mut stream) => stream.as_mut().poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut().inner {
            #[cfg(feature = "tokio-runtime")]
            AsyncStream::Tokio(ref mut stream) => Pin::new(stream).poll_flush(cx),

            #[cfg(feature = "tokio-runtime")]
            AsyncStream::TokioTls(ref mut stream) => Pin::new(stream).poll_flush(cx),

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStd(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_flush(cx)
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStdTls(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_flush(cx)
            }

            #[cfg(feature = "custom-runtime")]
            AsyncStream::Custom(ref mut stream) => stream.as_mut().poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut().inner {
            #[cfg(feature = "tokio-runtime")]
            AsyncStream::Tokio(ref mut stream) => Pin::new(stream).poll_shutdown(cx),

            #[cfg(feature = "tokio-runtime")]
            AsyncStream::TokioTls(ref mut stream) => Pin::new(stream).poll_shutdown(cx),

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStd(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_close(cx)
            }

            #[cfg(feature = "async-std-runtime")]
            AsyncStream::AsyncStdTls(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_close(cx)
            }

            #[cfg(feature = "custom-runtime")]
            AsyncStream::Custom(ref mut stream) => stream.as_mut().poll_shutdown(cx),
        }
    }
}
