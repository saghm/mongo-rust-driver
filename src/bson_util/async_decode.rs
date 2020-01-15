use bson::Document;
// use tokio::io::AsyncReadExt;

use crate::{error::Result, feature::AsyncStream};

pub(crate) async fn decode_document(reader: &mut AsyncStream) -> Result<Document> {
    todo!()
}
