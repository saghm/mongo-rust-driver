use std::{
    sync::atomic::{AtomicI32, Ordering},
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::Result;

use lazy_static::lazy_static;

/// Closure to obtain a new, unique request ID.
pub(crate) fn next_request_id() -> i32 {
    lazy_static! {
        static ref REQUEST_ID: AtomicI32 = AtomicI32::new(0);
    }

    REQUEST_ID.fetch_add(1, Ordering::SeqCst)
}

/// Serializes `string` to bytes and writes them to `writer` with a null terminator appended.
pub(super) async fn write_cstring<W: AsyncWrite + Unpin>(
    writer: &mut W,
    string: &str,
) -> Result<()> {
    // Write the string's UTF-8 bytes.
    writer.write_all(string.as_bytes()).await?;

    // Write the null terminator.
    writer.write_all(&[0]).await?;

    Ok(())
}
