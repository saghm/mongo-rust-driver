use std::{
    ops::DerefMut,
    sync::{Condvar, Mutex, RwLock, Weak},
    time::Duration,
};

use bson::{bson, doc};
use time::PreciseTime;

use super::Topology;
use crate::{
    cmap::{options::ConnectionPoolOptions, Command, Connection, ConnectionPool},
    error::Result,
    is_master::IsMasterReply,
    options::{ClientOptions, StreamAddress},
    sdam::{update_topology, ServerDescription, ServerType},
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
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

    /// Waits until either the server is requested to do a topology check or until `duration` has
    /// elapsed. Returns `true` if `duration` has elapsed and `false` otherwise.
    pub(crate) fn wait_timeout(&self, duration: Duration) -> bool {
        self.condvar
            .wait_timeout(self.condvar_mutex.lock().unwrap(), duration)
            .unwrap()
            .1
            .timed_out()
    }

    pub(crate) fn monitor_check(&self, mut server_type: ServerType) -> Option<ServerType> {
        // If the topology has been dropped, terminate the monitoring thread.
        let topology = match self.topology.upgrade() {
            Some(topology) => topology,
            None => return None,
        };

        // Send an isMaster to the server.
        let server_description = self.check_server(server_type);
        server_type = server_description.server_type;

        update_topology(topology, server_description);

        Some(server_type)
    }

    fn check_server(&self, server_type: ServerType) -> ServerDescription {
        let conn = self.monitoring_connection.lock().unwrap();
        let address = conn.address().clone();

        match is_master(conn) {
            Ok(reply) => return ServerDescription::new(address, Some(Ok(reply))),
            Err(e) => {
                self.clear_connection_pool();

                if server_type == ServerType::Unknown {
                    return ServerDescription::new(address, Some(Err(e)));
                }
            }
        }

        ServerDescription::new(address, Some(is_master(conn)))
    }
}

fn is_master(conn: impl DerefMut<Target = Connection>) -> Result<IsMasterReply> {
    let command = Command::new_read(
        "isMaster".into(),
        "admin".into(),
        None,
        doc! { "isMaster": 1 },
    );

    let start_time = PreciseTime::now();
    let command_response = conn.send_command(command, None)?;
    let end_time = PreciseTime::now();

    let command_response = command_response.body()?;

    Ok(IsMasterReply {
        command_response,
        // TODO RUST-193: Round-trip time
        round_trip_time: Some(start_time.to(end_time).to_std().unwrap()),
    })
}
