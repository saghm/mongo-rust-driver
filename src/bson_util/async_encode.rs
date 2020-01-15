use bson::Document;
use tokio::io::AsyncWrite;

use crate::{error::Result};

pub(crate) async fn encode_document<W: AsyncWrite + Unpin>(writer: &mut W, document: &Document) -> Result<()> {
    todo!()
}
