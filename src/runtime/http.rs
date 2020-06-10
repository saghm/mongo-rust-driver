#[cfg(feature = "tokio-runtime")]
use reqwest::{Method, Response};
#[cfg(feature = "tokio-runtime")]
use serde::Deserialize;

#[cfg(feature = "tokio-runtime")]
use crate::error::Result;

#[derive(Clone, Debug, Default)]
pub(crate) struct HttpClient {
    #[cfg(feature = "tokio-runtime")]
    inner: reqwest::Client,
}

impl HttpClient {
    #[cfg(feature = "tokio-runtime")]
    pub(crate) async fn get_and_deserialize_json<'a, T>(
        &self,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let value = self
            .request(Method::GET, uri, headers)
            .await?
            .json()
            .await?;

        Ok(value)
    }

    #[cfg(feature = "tokio-runtime")]
    pub(crate) async fn get_and_read_string<'a>(
        &self,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> Result<String> {
        self.request_and_read_string(Method::GET, uri, headers)
            .await
    }

    #[cfg(feature = "tokio-runtime")]
    pub(crate) async fn put_and_read_string<'a>(
        &self,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> Result<String> {
        self.request_and_read_string(Method::PUT, uri, headers)
            .await
    }

    pub(crate) async fn request_and_read_string<'a>(
        &self,
        method: Method,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> Result<String> {
        let text = self.request(method, uri, headers).await?.text().await?;

        Ok(text)
    }

    #[cfg(feature = "tokio-runtime")]
    pub(crate) async fn request<'a>(
        &self,
        method: Method,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> Result<Response> {
        let response = headers
            .into_iter()
            .fold(self.inner.request(method, uri), |request, (k, v)| {
                request.header(*k, *v)
            })
            .send()
            .await?;

        Ok(response)
    }
}
