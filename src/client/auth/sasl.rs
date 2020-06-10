use crate::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    bson_util,
    client::auth::AuthMechanism,
    error::{Error, Result},
};

pub(super) struct SaslStart {
    mechanism: AuthMechanism,
    payload: Vec<u8>,
}

impl SaslStart {
    pub(super) fn new(mechanism: AuthMechanism, payload: Vec<u8>) -> Self {
        Self { mechanism, payload }
    }

    pub(super) fn into_command(self) -> Document {
        doc! {
            "saslStart": 1,
            "mechanism": self.mechanism.as_str(),
            "payload": Binary { subtype: BinarySubtype::Generic, bytes: self.payload },
        }
    }
}

pub(super) struct SaslContinue {
    conversation_id: Bson,
    payload: Vec<u8>,
}

impl SaslContinue {
    pub(super) fn new(conversation_id: Bson, payload: Vec<u8>) -> Self {
        Self {
            conversation_id,
            payload,
        }
    }

    pub(super) fn into_command(self) -> Document {
        doc! {
            "saslContinue": 1,
            "conversationId": self.conversation_id,
            "payload": Binary { subtype: BinarySubtype::Generic, bytes: self.payload },
        }
    }
}

fn validate_command_success(auth_mechanism: &str, response: &Document) -> Result<()> {
    let ok = response
        .get("ok")
        .ok_or_else(|| Error::invalid_authentication_response(auth_mechanism))?;
    match bson_util::get_int(ok) {
        Some(1) => Ok(()),
        Some(_) => Err(Error::authentication_error(
            auth_mechanism,
            response
                .get_str("errmsg")
                .unwrap_or("Authentication failure"),
        )),
        _ => Err(Error::invalid_authentication_response(auth_mechanism)),
    }
}

pub(super) struct SaslResponse {
    pub(super) conversation_id: Bson,
    pub(super) done: bool,
    pub(super) payload: Vec<u8>,
}

impl SaslResponse {
    pub(super) fn parse(auth_mechanism: &str, mut response: Document) -> Result<Self> {
        validate_command_success(auth_mechanism, &response)?;

        let conversation_id = response
            .remove("conversationId")
            .ok_or_else(|| Error::invalid_authentication_response(auth_mechanism))?;
        let done = response
            .remove("done")
            .and_then(|b| b.as_bool())
            .ok_or_else(|| Error::invalid_authentication_response(auth_mechanism))?;
        let payload = response
            .get_binary_generic_mut("payload")
            .or_else(|_| Err(Error::invalid_authentication_response(auth_mechanism)))?
            .drain(..)
            .collect();

        Ok(SaslResponse {
            conversation_id,
            done,
            payload,
        })
    }
}
