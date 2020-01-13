use std::{sync::Weak, time::Duration};

use futures_timer::Delay;

use super::{description::server::ServerType, state::server::Server};
use crate::feature::AsyncRuntime;

const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

/// Starts a monitoring thread associated with a given Server. A weak reference is used to ensure
/// that the monitoring thread doesn't keep the server alive after it's been removed from the
/// topology or the client has been dropped.
pub(super) async fn monitor_server(
    runtime: AsyncRuntime,
    server: Weak<Server>,
    heartbeat_frequency: Option<Duration>,
) {
    let mut server_type = ServerType::Unknown;
    let heartbeat_frequency = heartbeat_frequency.unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

    loop {
        let server = match server.upgrade() {
            Some(server) => server,
            None => return,
        };

        server_type = match server.monitor_check(server_type).await {
            Some(server_type) => server_type,
            None => return,
        };

        Delay::new(heartbeat_frequency).await;
    }
}
