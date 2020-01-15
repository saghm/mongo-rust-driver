use async_std::io::{Error as AsyncStdIoError, ErrorKind as AsyncStdIoErrorKind};
use tokio::io::{Error as TokioIoError, ErrorKind as TokioIoErrorKind};

pub(super) fn convert_error(err: AsyncStdIoError) -> TokioIoError {
    let kind = match err.kind() {
        AsyncStdIoErrorKind::NotFound => TokioIoErrorKind::NotFound,
        AsyncStdIoErrorKind::PermissionDenied => TokioIoErrorKind::PermissionDenied,
        AsyncStdIoErrorKind::ConnectionRefused => TokioIoErrorKind::ConnectionRefused,
        AsyncStdIoErrorKind::ConnectionReset => TokioIoErrorKind::ConnectionReset,
        AsyncStdIoErrorKind::ConnectionAborted => TokioIoErrorKind::ConnectionAborted,
        AsyncStdIoErrorKind::NotConnected => TokioIoErrorKind::NotConnected,
        AsyncStdIoErrorKind::AddrInUse => TokioIoErrorKind::AddrInUse,
        AsyncStdIoErrorKind::AddrNotAvailable => TokioIoErrorKind::AddrNotAvailable,
        AsyncStdIoErrorKind::BrokenPipe => TokioIoErrorKind::BrokenPipe,
        AsyncStdIoErrorKind::AlreadyExists => TokioIoErrorKind::AlreadyExists,
        AsyncStdIoErrorKind::WouldBlock => TokioIoErrorKind::WouldBlock,
        AsyncStdIoErrorKind::InvalidInput => TokioIoErrorKind::InvalidInput,
        AsyncStdIoErrorKind::InvalidData => TokioIoErrorKind::InvalidData,
        AsyncStdIoErrorKind::TimedOut => TokioIoErrorKind::TimedOut,
        AsyncStdIoErrorKind::WriteZero => TokioIoErrorKind::WriteZero,
        AsyncStdIoErrorKind::Interrupted => TokioIoErrorKind::Interrupted,
        AsyncStdIoErrorKind::Other => TokioIoErrorKind::Other,
        AsyncStdIoErrorKind::UnexpectedEof => TokioIoErrorKind::UnexpectedEof,
    };

    kind.into()
}
