use std::{
    convert::TryInto,
    ops::DerefMut,
    sync::{Condvar, Weak},
};

use bson::{bson, doc};
use derivative::Derivative;
use futures_timer::Delay;
use lazy_static::lazy_static;
use time::Instant;
use tokio::sync::{Mutex, RwLock};

use super::Topology;
use crate::{
    cmap::{options::ConnectionPoolOptions, Command, Connection, ConnectionPool},
    error::Result,
    is_master::IsMasterReply,
    options::{ClientOptions, StreamAddress},
    sdam::{update_topology, ServerDescription, ServerType},
};

lazy_static! {
    // Unfortunately, the `time` crate has not yet updated to make the `Duration` constructors `const`, so we have to use lazy_static.
    pub(crate) static ref MIN_HEARTBEAT_FREQUENCY: time::Duration = time::Duration::milliseconds(500);
}

/// Contains the state for a given server in the topology.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Server {
    pub(crate) address: StreamAddress,

    /// The topology that contains the server. Holding a weak reference allows monitoring threads
    /// to update the topology without keeping it alive after the Client has been dropped.
    topology: Weak<RwLock<Topology>>,

    /// The connection pool for the server.
    pool: ConnectionPool,

    condvar: Condvar,

    condvar_mutex: Mutex<()>,

    monitoring_connection: Mutex<Connection>,

    #[derivative(Debug = "ignore")]
    last_check: Mutex<Option<Instant>>,
}

impl Server {
    pub(crate) fn new(
        topology: Weak<RwLock<Topology>>,
        address: StreamAddress,
        options: &ClientOptions,
    ) -> Result<Self> {
        let monitoring_connection = Mutex::new(Connection::new(
            0,
            address.clone(),
            0,
            options.connect_timeout,
            options.tls_options(),
            options.cmap_event_handler.clone(),
        )?);

        Ok(Self {
            topology,
            pool: ConnectionPool::new(
                address.clone(),
                Some(ConnectionPoolOptions::from_client_options(options)),
            ),
            condvar: Default::default(),
            condvar_mutex: Default::default(),
            address,
            monitoring_connection,
            last_check: Mutex::new(None),
        })
    }

    /// Creates a new Server given the `address` and `options`.
    /// Checks out a connection from the server's pool.
    pub(crate) fn checkout_connection(&self) -> Result<Connection> {
        self.pool.check_out()
    }

    /// Clears the connection pool associated with the server.
    pub(crate) fn clear_connection_pool(&self) {
        self.pool.clear();
    }

    pub(crate) async fn monitor_check(&self, mut server_type: ServerType) -> Option<ServerType> {
        // If the topology has been dropped, terminate the monitoring thread.
        let topology = match self.topology.upgrade() {
            Some(topology) => topology,
            None => return None,
        };

        {
            let mut last_check_owned = self.last_check.lock().await;
            let last_check = last_check_owned.deref_mut();

            if let Some(ref mut last_check) = last_check {
                let duration_since_last_check = Instant::now() - *last_check;

                if duration_since_last_check < *MIN_HEARTBEAT_FREQUENCY {
                    let remaining_time = *MIN_HEARTBEAT_FREQUENCY - duration_since_last_check;

                    // Since MIN_HEARTBEAT_FREQUENCY is 500 and `duration_since_last_check` is less
                    // than it but still positive, we can be sure that the time::Duration can be
                    // successfully converted to a std::time::Duration. However, in the case of some
                    // bug causing this not to be true, rather than panicking the monitoring thread,
                    // we instead just don't sleep and proceed to checking the server a bit early.
                    if let Ok(remaining_time) = remaining_time.try_into() {
                        Delay::new(remaining_time).await;
                    }
                }
            }

            std::mem::replace(last_check, Some(Instant::now()));
        }

        // Send an isMaster to the server.
        let server_description = self.check_server(server_type).await;
        server_type = server_description.server_type;

        update_topology(topology, server_description);

        Some(server_type)
    }

    async fn check_server(&self, server_type: ServerType) -> ServerDescription {
        let mut conn = self.monitoring_connection.lock().await;
        let address = conn.address().clone();

        match is_master(conn.deref_mut()) {
            Ok(reply) => return ServerDescription::new(address, Some(Ok(reply))),
            Err(e) => {
                self.clear_connection_pool();

                if server_type == ServerType::Unknown {
                    return ServerDescription::new(address, Some(Err(e)));
                }
            }
        }

        ServerDescription::new(address, Some(is_master(conn.deref_mut())))
    }
}

fn is_master(conn: &mut Connection) -> Result<IsMasterReply> {
    let command = Command::new_read(
        "isMaster".into(),
        "admin".into(),
        None,
        doc! { "isMaster": 1 },
    );

    let start_time = Instant::now();
    let command_response = conn.send_command(command, None)?;
    let end_time = Instant::now();

    let command_response = command_response.body()?;

    Ok(IsMasterReply {
        command_response,
        // TODO RUST-193: Round-trip time
        round_trip_time: Some((end_time - start_time).try_into().unwrap()),
    })
}
