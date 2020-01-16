use std::time::Duration;

use crate::options::{StreamAddress, TlsOptions};

#[derive(Clone, Debug)]
pub struct StreamOptions {
    pub(crate) address: StreamAddress,

    pub(crate) connect_timeout: Option<Duration>,

    pub(crate) tls_options: Option<TlsOptions>,
}

impl StreamOptions {
    pub(crate) fn new(
        address: StreamAddress,
        connect_timeout: Option<Duration>,
        tls_options: Option<TlsOptions>,
    ) -> Self {
        Self {
            address,
            connect_timeout,
            tls_options,
        }
    }

    pub fn address(&self) -> &StreamAddress {
        &self.address
    }

    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    pub fn tls_options(&self) -> Option<&TlsOptions> {
        self.tls_options.as_ref()
    }
}
