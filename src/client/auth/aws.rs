use chrono::{offset::Utc, DateTime};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    client::auth::{
        self,
        sasl::{SaslContinue, SaslResponse, SaslStart},
        AuthMechanism,
        Credential,
    },
    cmap::{Command, Connection},
    error::{Error, Result},
    runtime::HttpClient,
};

pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    http_client: &HttpClient,
) -> Result<()> {
    let source = match credential.source.as_deref() {
        Some("$external") | None => "$external",
        Some(..) => {
            return Err(Error::authentication_error(
                "MONGODB-AWS",
                "auth source must be $external",
            ))
        }
    };

    let nonce = auth::generate_nonce_bytes();

    let client_first_payload = doc! {
        "r": Binary { subtype: BinarySubtype::Generic, bytes: nonce.clone() },
        // `110` is ASCII for the character `n`, which is required by the spec to indicate that
        // channel binding is not supported.
        "p": 110i32,
    };
    let mut client_first_payload_bytes = Vec::new();
    client_first_payload
        .to_writer(&mut client_first_payload_bytes)
        .unwrap();

    let sasl_start = SaslStart::new(AuthMechanism::MongoDbAws, client_first_payload_bytes);
    let client_first = Command::new("saslStart".into(), source.into(), sasl_start.into_command());

    let server_first_response = conn.send_command(client_first, None).await.unwrap();

    let server_first = ServerFirst::parse(server_first_response.raw_response).unwrap();
    server_first.validate(&nonce).unwrap();

    let aws_credential = AwsCredential::get(credential, http_client).await.unwrap();
    let date = Utc::now();

    let authorization_header = aws_credential
        .compute_authorization_header(date, &server_first.sts_host, &server_first.server_nonce)
        .unwrap();

    let mut client_second_payload = doc! {
        "a": authorization_header,
        "d": date.format("%Y%m%dT%H%M%SZ").to_string(),
    };

    if let Some(security_token) = aws_credential.session_token {
        client_second_payload.insert("t", security_token);
    }

    let mut client_second_payload_bytes = Vec::new();
    client_second_payload
        .to_writer(&mut client_second_payload_bytes)
        .unwrap();

    let sasl_continue = SaslContinue::new(
        server_first.conversation_id.clone(),
        client_second_payload_bytes,
    );

    let client_second = Command::new(
        "saslContinue".into(),
        source.into(),
        sasl_continue.into_command(),
    );

    let server_second_response = conn.send_command(client_second, None).await.unwrap();
    let server_second =
        SaslResponse::parse("MONGODB-AWS", server_second_response.raw_response).unwrap();

    if server_second.conversation_id != server_first.conversation_id {
        return Err(Error::invalid_authentication_response("MONGODB-AWS"));
    }

    if !server_second.done {
        return Err(Error::invalid_authentication_response("MONGODB-AWS"));
    }

    Ok(())
}

#[derive(Deserialize)]
struct AwsCredential {
    #[serde(rename = "AccessKeyId")]
    access_key: String,

    #[serde(rename = "SecretAccessKey")]
    secret_key: String,

    #[serde(rename = "Token")]
    session_token: Option<String>,
}

impl AwsCredential {
    async fn get(credential: &Credential, http_client: &HttpClient) -> Result<Self> {
        let access_key = credential
            .username
            .clone()
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok());
        let secret_key = credential
            .password
            .clone()
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok());
        let session_token = credential
            .mechanism_properties
            .as_ref()
            .and_then(|d| d.get_str("AWS_SESSION_TOKEN").ok())
            .map(|s| s.to_string())
            .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok());

        let found_access_key = access_key.is_some();
        let found_secret_key = secret_key.is_some();

        // If we have an access key and secret key, we can continue with the credentials we've
        // found.
        if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
            return Ok(Self {
                access_key,
                secret_key,
                session_token,
            });
        }

        if found_access_key || found_secret_key {
            return Err(Error::authentication_error(
                "MONGODB-AWS",
                "cannot specify only one of access key and secret key; either both or neither \
                 must be provided",
            ));
        }

        if session_token.is_some() {
            return Err(Error::authentication_error(
                "MONGODB-AWS",
                "cannot specify session token without both access key and secret key",
            ));
        }

        if let Ok(relative_uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            Self::get_from_ecs(relative_uri, http_client).await
        } else {
            Self::get_from_ec2(http_client).await
        }
    }

    async fn get_from_ecs(relative_uri: String, http_client: &HttpClient) -> Result<Self> {
        // Use the local IP address that AWS uses for ECS agents.
        let uri = format!("http://169.254.170.2/{}", relative_uri);

        http_client.get_and_deserialize_json(&uri, None).await
    }

    async fn get_from_ec2(http_client: &HttpClient) -> Result<Self> {
        let temporary_token = http_client
            .put_and_read_string(
                "http://169.254.169.254/latest/api/token",
                &[("X-aws-ec2-metadata-token-ttl-seconds", "30")],
            )
            .await?;

        let role_name_uri = "http://169.254.169.254/latest/meta-data/iam/security-credentials/";

        let role_name = http_client
            .get_and_read_string(
                role_name_uri,
                &[("X-aws-ec2-metadata-token", &temporary_token[..])],
            )
            .await?;

        let credential_uri = format!("{}/{}", role_name_uri, role_name);

        http_client
            .get_and_deserialize_json(
                &credential_uri,
                &[("X-aws-ec2-metadata-token", &temporary_token[..])],
            )
            .await
    }

    fn compute_authorization_header(
        &self,
        date: DateTime<Utc>,
        host: &str,
        server_nonce: &[u8],
    ) -> Result<String> {
        let date_str = date.format("%Y%m%dT%H%M%SZ").to_string();

        // We need to include the security token header if the user provided a token. If not, we
        // just use the empty string.
        let token = self
            .session_token
            .as_ref()
            .map(|s| format!("x-amz-security-token:{}\n", s))
            .unwrap_or_default();

        // Similarly, we need to put "x-amz-security-token" into the list of signed headers if the
        // user provided a token. If not, we just use the empty string.
        let token_signed_header = if self.session_token.is_some() {
            "x-amz-security-token;"
        } else {
            ""
        };

        // Generate the list of signed headers (either with or without the security token header).
        #[rustfmt::skip]
        let signed_headers = format!(
            "\
              content-length;\
              content-type;\
              host;\
              x-amz-date;\
              {token_signed_header}\
              x-mongodb-gs2-cb-flag;\
              x-mongodb-server-nonce\
            ",
            token_signed_header = token_signed_header,
        );

        let body = "Action=GetCallerIdentity&Version=2011-06-15";
        let hashed_body = hex::encode(Sha256::digest(body.as_bytes()));

        let nonce = base64::encode(server_nonce);

        #[rustfmt::skip]
		let request = format!(
		    "\
             POST\n\
			 /\n\n\
             content-length:43\n\
             content-type:application/x-www-form-urlencoded\n\
             host:{host}\n\
             x-amz-date:{date}\n\
			 {token}\
			 x-mongodb-gs2-cb-flag:n\n\
			 x-mongodb-server-nonce:{nonce}\n\n\
             {signed_headers}\n\
             {hashed_body}\
             ",
			host = host,
			date = date_str,
			token = token,
			nonce = nonce,
			signed_headers = signed_headers,
			hashed_body = hashed_body,
		);

        dbg!(&request);

        let hashed_request = hex::encode(Sha256::digest(request.as_bytes()));

        let small_date = date.format("%Y%m%d").to_string();

        let region = if host == "sts.amazonaws.com" {
            "us-east-1"
        } else {
            let parts: Vec<_> = host.split('.').collect();
            parts.get(1).copied().unwrap_or("us-east-1")
        };

        #[rustfmt::skip]
		let string_to_sign = format!(
			"\
             AWS4-HMAC-SHA256\n\
             {full_date}\n\
			 {small_date}/{region}/sts/aws4_request\n\
			 {hashed_request}\
            ",
			full_date = date_str,
			small_date = small_date,
			region = region,
			hashed_request = hashed_request,
		);

        dbg!(&string_to_sign);

        let first_hmac_key = format!("AWS4{}", self.secret_key);
        let k_date = hmac(first_hmac_key, &small_date)?;
        let k_region = hmac(k_date, region)?;
        let k_service = hmac(k_region, "sts")?;
        let k_signing = hmac(k_service, "aws4_request")?;

        let signature_bytes = hmac(k_signing, string_to_sign)?;
        let signature = hex::encode(signature_bytes);

        #[rustfmt::skip]
		let auth_header = format!(
	        "\
             Authorization: AWS-4-HMAC-SHA-256 \
             Credential={access_key}/{small_date}/{region}/sts/aws4_request, \
             SignedHeaders={signed_headers}, \
             Signature={signature}\
            ",
			access_key = self.access_key,
			small_date = small_date,
			region = region,
			signed_headers = signed_headers,
			signature = signature
		);

        dbg!(&auth_header);

        Ok(auth_header)
    }
}

fn hmac(key: impl AsRef<[u8]>, input: impl AsRef<[u8]>) -> Result<impl AsRef<[u8]>> {
    let mut hmac = Hmac::<Sha256>::new_varkey(key.as_ref())?;
    hmac.input(input.as_ref());

    let output = hmac.result();
    Ok(output.code())
}

struct ServerFirst {
    conversation_id: Bson,
    server_nonce: Vec<u8>,
    sts_host: String,
    done: bool,
}

impl ServerFirst {
    fn parse(response: Document) -> Result<Self> {
        let SaslResponse {
            conversation_id,
            payload,
            done,
        } = SaslResponse::parse("MONGODB-AWS", response)?;

        let mut payload_document = Document::from_reader(&mut payload.as_slice())?;

        if payload_document.len() != 2
            || !payload_document.contains_key("s")
            || !payload_document.contains_key("h")
        {
            return Err(Error::invalid_authentication_response("MONGODB-AWS"));
        }

        let server_nonce = payload_document
            .get_binary_generic_mut("s")
            .or_else(|_| Err(Error::invalid_authentication_response("MONGODB-AWS")))?
            .drain(..)
            .collect();
        let sts_host = match payload_document.remove("h") {
            Some(Bson::String(s)) => s,
            _ => return Err(Error::invalid_authentication_response("MONGODB-AWS")),
        };

        Ok(Self {
            conversation_id,
            server_nonce,
            sts_host,
            done,
        })
    }

    fn validate(&self, nonce: &[u8]) -> Result<()> {
        if self.done {
            Err(Error::authentication_error(
                "MONGODB-AWS",
                "handshake terminated early",
            ))
        } else if !self.server_nonce.starts_with(nonce) {
            Err(Error::authentication_error(
                "MONGODB-AWS",
                "mismatched nonce",
            ))
        } else if self.server_nonce.len() != 64 {
            Err(Error::authentication_error(
                "MONGODB-AWS",
                "incorrect length server nonce",
            ))
        } else if self.sts_host.is_empty() {
            Err(Error::authentication_error(
                "MONGODB-AWS",
                "sts host must be non-empty",
            ))
        } else if self.sts_host.as_bytes().len() > 255 {
            Err(Error::authentication_error(
                "MONGODB-AWS",
                "sts host cannot be more than 255 bytes",
            ))
        } else if self.sts_host.split('.').any(|s| s.is_empty()) {
            Err(Error::authentication_error(
                "MONGODB-AWS",
                "sts host cannot contain empty labels",
            ))
        } else {
            Ok(())
        }
    }
}
