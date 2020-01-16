use bson::{bson, doc, Bson};

use super::message::{Message, MessageFlags, MessageSection};
use crate::{
    cmap::conn::StreamOptions,
    test::{ASYNC_RUNTIME, CLIENT_OPTIONS, LOCK},
};

define_test! { basic, {
    if CLIENT_OPTIONS.tls_options().is_some() {
        return;
    }

    let _guard = LOCK.run_concurrently();

    let message = Message {
        response_to: 0,
        flags: MessageFlags::empty(),
        sections: vec![MessageSection::Document(
            doc! { "isMaster": 1, "$db": "admin" },
        )],
        checksum: None,
        request_id: None,
    };

    let options = StreamOptions::new(CLIENT_OPTIONS.hosts[0].clone(), None, None);

    let mut stream = ASYNC_RUNTIME.connect_stream(options).await.unwrap();
    message.write_to(&mut stream).await.unwrap();

    let reply = Message::read_from(&mut stream).await.unwrap();

    let response_doc = match reply.sections.into_iter().next().unwrap() {
        MessageSection::Document(doc) => doc,
        MessageSection::Sequence { documents, .. } => documents.into_iter().next().unwrap(),
    };

    assert_eq!(response_doc.get("ok"), Some(&Bson::FloatingPoint(1.0)));
}}
