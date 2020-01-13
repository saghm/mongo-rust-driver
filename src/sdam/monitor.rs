use std::{sync::Weak, time::Duration};

use bson::doc;
use lazy_static::lazy_static;
use time::PreciseTime;

use super::{description::server::ServerType, state::server::Server};

const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

lazy_static! {
    // Unfortunately, the `time` crate has not yet updated to make the `Duration` constructors `const`, so we have to use lazy_static.
    pub(crate) static ref MIN_HEARTBEAT_FREQUENCY: time::Duration = time::Duration::milliseconds(500);
}

/// Starts a monitoring thread associated with a given Server. A weak reference is used to ensure
/// that the monitoring thread doesn't keep the server alive after it's been removed from the
/// topology or the client has been dropped.
pub(super) fn monitor_server(server: Weak<Server>, heartbeat_frequency: Option<Duration>) {
    std::thread::spawn(move || {
        let mut server_type = ServerType::Unknown;
        let heartbeat_frequency = heartbeat_frequency.unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        loop {
            let server = match server.upgrade() {
                Some(server) => server,
                None => return,
            };

            server_type = match server.monitor_check(server_type) {
                Some(server_type) => server_type,
                None => return,
            };

            let last_check = PreciseTime::now();

            let timed_out = server.wait_timeout(heartbeat_frequency);

            if !timed_out {
                let duration_since_last_check = last_check.to(PreciseTime::now());

                if duration_since_last_check < *MIN_HEARTBEAT_FREQUENCY {
                    let remaining_time = *MIN_HEARTBEAT_FREQUENCY - duration_since_last_check;

                    // Since MIN_HEARTBEAT_FREQUENCY is 500 and `duration_since_last_check` is less
                    // than it but still positive, we can be sure that the time::Duration can be
                    // successfully converted to a std::time::Duration. However, in the case of some
                    // bug causing this not to be true, rather than panicking the monitoring thread,
                    // we instead just don't sleep and proceed to checking the server a bit early.
                    if let Ok(remaining_time) = remaining_time.to_std() {
                        std::thread::sleep(remaining_time);
                    }
                }
            }
        }
    });
}
