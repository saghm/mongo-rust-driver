pub mod auth;
mod executor;
pub mod options;

use std::{convert::TryInto, sync::Arc, time::Duration};

use bson::{Bson, Document};
use derivative::Derivative;
use time::Instant;
use tokio::sync::RwLock;

use crate::{
    concern::{ReadConcern, WriteConcern},
    db::Database,
    error::{ErrorKind, Result},
    event::command::CommandEventHandler,
    feature::AsyncRuntime,
    operation::ListDatabases,
    options::{ClientOptions, DatabaseOptions},
    sdam::{Server, ServerType, Topology},
    selection_criteria::{ReadPreference, SelectionCriteria},
};

const DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// This is the main entry point for the API. A `Client` is used to connect to a MongoDB cluster.
/// By default, it will monitor the topology of the cluster, keeping track of any changes, such
/// as servers being added or removed
///
/// `Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// # use mongodb::{Client, error::Result};
/// #
/// # fn start_workers() -> Result<()> {
/// let client = Client::with_uri_str("mongodb://example.com")?;
///
/// for i in 0..5 {
///     let client_ref = client.clone();
///
///     std::thread::spawn(move || {
///         let collection = client_ref.database("items").collection(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
/// # // Technically we should join the threads here, but for the purpose of the example, we'll just
/// # // sleep for a bit.
/// # std::thread::sleep(std::time::Duration::from_secs(3));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    topology: Arc<RwLock<Topology>>,
    options: ClientOptions,
    runtime: AsyncRuntime,
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    ///
    /// See the documentation on
    /// [`ClientOptions::parse`](options/struct.ClientOptions.html#method.parse) for more details.
    pub async fn with_uri_str(uri: &str) -> Result<Self> {
        let options = ClientOptions::parse(uri)?;

        Client::with_options(options).await
    }

    /// Creates a new `Client` connected to the cluster specified by `options`.
    pub async fn with_options(mut options: ClientOptions) -> Result<Self> {
        let runtime = match options.async_runtime.take() {
            Some(runtime) => runtime,

            // If no runtime is given, use tokio if enabled.
            #[cfg(feature = "tokio-runtime")]
            None => AsyncRuntime::Tokio,

            // If no runtime is given and tokio is not enabled, use async-std if enabled.
            #[cfg(all(not(feature = "tokio-runtime"), feature = "async-std-runtime"))]
            None => AsyncRuntime::AsyncStd,

            // If no runtime is given and neither tokio or async-std is enabled, return a
            // ConfigurationError.
            #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
            None => {
                return Err(ErrorKind::ConfigurationError {
                    message: "tokio and async-std are not enabled, but no custom runtime was \
                              provided"
                        .into(),
                }
                .into())
            }
        };

        let inner = Arc::new(ClientInner {
            topology: Topology::new(runtime.clone(), options.clone()).await?,
            runtime,
            options,
        });

        Ok(Self { inner })
    }

    pub(crate) fn emit_command_event(&self, emit: impl FnOnce(&Arc<dyn CommandEventHandler>)) {
        if let Some(ref handler) = self.inner.options.command_event_handler {
            emit(handler);
        }
    }

    /// Gets the default selection criteria the `Client` uses for operations..
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the default read concern the `Client` uses for operations.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the default write concern the `Client` uses for operations.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// The `Database` options (e.g. read preference and write concern) will default to those of the
    /// `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database(&self, name: &str) -> Database {
        Database::new(self.clone(), name, None)
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// Operations done with this `Database` will use the options specified by `options` by default
    /// and will otherwise default to those of the `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database_with_options(&self, name: &str, options: DatabaseOptions) -> Database {
        Database::new(self.clone(), name, Some(options))
    }

    /// Gets information about each database present in the cluster the Client is connected to.
    pub async fn list_databases(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> Result<Vec<Document>> {
        let op = ListDatabases::new(filter.into(), false);
        self.execute_operation(&op, None).await
    }

    /// Gets the names of the databases present in the cluster the Client is connected to.
    pub async fn list_database_names(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> Result<Vec<String>> {
        let op = ListDatabases::new(filter.into(), true);
        match self.execute_operation(&op, None).await {
            Ok(databases) => databases
                .into_iter()
                .map(|doc| {
                    let name = doc.get("name").and_then(Bson::as_str).ok_or_else(|| {
                        ErrorKind::ResponseError {
                            message: "Expected \"name\" field in server response, but it was not \
                                      found"
                                .to_string(),
                        }
                    })?;
                    Ok(name.to_string())
                })
                .collect(),
            Err(e) => Err(e),
        }
    }

    fn topology(&self) -> Arc<RwLock<Topology>> {
        self.inner.topology.clone()
    }

    /// Select a server using the provided criteria. If none is provided, a primary read preference
    /// will be used instead.
    async fn select_server(
        &self,
        criteria: Option<&SelectionCriteria>,
    ) -> Result<(ServerType, Arc<Server>)> {
        let criteria =
            criteria.unwrap_or_else(|| &SelectionCriteria::ReadPreference(ReadPreference::Primary));
        let start_time = Instant::now();
        let timeout = self
            .inner
            .options
            .server_selection_timeout
            .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT);
        let end_time = start_time + timeout;

        while Instant::now() < end_time {
            // Because we're calling clone on the lock guard, we're actually copying the
            // Topology itself, not just making a new reference to it. The
            // `servers` field will contain references to the same instances
            // though, since each is wrapped in an `Arc`.
            let topology = self.inner.topology.read().await.clone();

            // Return error if the wire version is invalid.
            if let Some(error_msg) = topology.description.compatibility_error() {
                return Err(ErrorKind::ServerSelectionError {
                    message: error_msg.into(),
                }
                .into());
            }

            let server = topology
                .description
                .select_server(&criteria)?
                .and_then(|server_desc| {
                    topology
                        .servers
                        .get(&server_desc.address)
                        .map(|server| (server_desc.server_type, server.clone()))
                });

            if let Some(server) = server {
                return Ok(server);
            }

            // Because the servers in the copied Topology are Arc aliases of the servers in the
            // original Topology, requesting a check on the copy will in turn request a check from
            // each of the original servers, so the monitoring threads will be woken the same way
            // they would if `request_topology_check` were called on the original Topology.
            if topology
                .topology_check(
                    (end_time - Instant::now())
                        .try_into()
                        .unwrap_or(Duration::new(0, 0)),
                )
                .await
            {
                break;
            }
        }

        Err(ErrorKind::ServerSelectionError {
            message: "timed out while trying to select server".into(),
        }
        .into())
    }
}
