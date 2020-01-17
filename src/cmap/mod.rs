// #[cfg(test)]
// mod test;

mod background;
pub(crate) mod conn;
mod establish;
pub(crate) mod options;
mod wait_queue;

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use derivative::Derivative;
use futures_timer::Delay;
use tokio::sync::RwLock;

pub use self::conn::ConnectionInfo;
pub(crate) use self::conn::{Command, CommandResponse, Connection, StreamDescription};

use self::{
    establish::ConnectionEstablisher,
    options::ConnectionPoolOptions,
    wait_queue::{WaitQueue, WaitQueueHandle},
};
use crate::{
    client::auth::Credential,
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler,
        ConnectionCheckoutFailedEvent,
        ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent,
        ConnectionClosedReason,
        PoolClearedEvent,
        PoolClosedEvent,
        PoolCreatedEvent,
    },
    feature::AsyncRuntime,
    options::{StreamAddress, TlsOptions},
};

const DEFAULT_MAX_POOL_SIZE: u32 = 100;

/// A pool of connections implementing the CMAP spec.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

impl std::ops::Deref for ConnectionPool {
    type Target = ConnectionPoolInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
struct PoolState {
    closed: bool,
    connections: Vec<Connection>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolInner {
    closed: RwLock<bool>,

    /// The address the pool's connections will connect to.
    address: StreamAddress,

    /// The set of available connections in the pool. Because the CMAP spec requires that
    /// connections are checked out in a FIFO manner, connections are pushed/popped from the back
    /// of the Vec.

    /// The connect timeout passed to each underlying TcpStream when attemtping to connect to the
    /// server.
    connect_timeout: Option<Duration>,

    /// The credential to use for authenticating connections in this pool.
    credential: Option<Credential>,

    /// Contains the logic for "establishing" a connection. This includes handshaking and
    /// authenticating a connection when it's first created.
    establisher: ConnectionEstablisher,

    /// The event handler specified by the user to process CMAP events.
    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,

    /// The current generation of the pool. The generation is incremented whenever the pool is
    /// cleared. Connections belonging to a previous generation are considered stale and will be
    /// closed when checked back in or when popped off of the set of available connections.
    generation: AtomicU32,

    /// Connections that have been ready for usage in the pool for longer than `max_idle_time` will
    /// be closed either by the background thread or when popped off of the set of available
    /// connections. If `max_idle_time` is `None`, then connections will not be closed due to being
    /// idle.
    max_idle_time: Option<Duration>,

    /// The maximum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool.
    max_pool_size: u32,

    /// The minimum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool. If fewer than `min_pool_size`
    /// connections are in the pool, the background thread will create more connections and add
    /// them to the pool.
    min_pool_size: Option<u32>,

    /// The ID of the next connection created by the pool.
    next_connection_id: AtomicU32,

    runtime: AsyncRuntime,

    state: RwLock<PoolState>,

    /// If a checkout operation takes longer than `wait_queue_timeout`, the pool will return an
    /// error. If `wait_queue_timeout` is `None`, then the checkout operation will not time out.
    wait_queue_timeout: Option<Duration>,

    /// The TLS options to use for the connections. If `tls_options` is None, then TLS will not be
    /// used to connect to the server.
    tls_options: Option<TlsOptions>,

    /// The total number of connections currently in the pool. This includes connections which are
    /// currently checked out of the pool.
    total_connection_count: AtomicU32,

    /// Connections are checked out by concurrent threads on a first-come, first-server basis. This
    /// is enforced by threads entering the wait queue when they first try to check out a
    /// connection and then blocking until they are at the front of the queue.
    wait_queue: WaitQueue,
}

impl ConnectionPool {
    pub(crate) fn new(address: StreamAddress, mut options: Option<ConnectionPoolOptions>) -> Self {
        // Get the individual options from `options`.
        let connect_timeout = options.as_ref().and_then(|opts| opts.connect_timeout);
        let credential = options.as_mut().and_then(|opts| opts.credential.clone());
        let establisher = ConnectionEstablisher::new(options.as_ref());
        let event_handler = options.as_mut().and_then(|opts| opts.event_handler.take());

        // The CMAP spec indicates that a max idle time of zero means that connections should not be
        // closed due to idleness.
        let mut max_idle_time = options.as_ref().and_then(|opts| opts.max_idle_time);
        if max_idle_time == Some(Duration::from_millis(0)) {
            max_idle_time = None;
        }

        let max_pool_size = options
            .as_ref()
            .and_then(|opts| opts.max_pool_size)
            .unwrap_or(DEFAULT_MAX_POOL_SIZE);
        let min_pool_size = options.as_ref().and_then(|opts| opts.min_pool_size);
        let runtime = options
            .as_ref()
            .map(|opts| opts.runtime.clone())
            .unwrap_or_default();
        let tls_options = options.as_mut().and_then(|opts| opts.tls_options.take());
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);

        let state = PoolState {
            closed: false,
            connections: Default::default(),
        };

        let inner = ConnectionPoolInner {
            closed: RwLock::new(false),
            wait_queue: WaitQueue::new(runtime.clone(), address.clone(), wait_queue_timeout),
            address: address.clone(),
            connect_timeout,
            credential,
            establisher,
            event_handler,
            generation: AtomicU32::new(0),
            max_idle_time,
            max_pool_size,
            min_pool_size,
            next_connection_id: AtomicU32::new(1),
            runtime,
            state: RwLock::new(state),
            tls_options,
            total_connection_count: AtomicU32::new(0),
            wait_queue_timeout,
        };

        let pool = Self {
            inner: Arc::new(inner),
        };

        pool.emit_event(move |handler| {
            let event = PoolCreatedEvent { address, options };

            handler.handle_pool_created_event(event);
        });

        let pool_clone = pool.clone();

        // Start a task to periodically check for perished connections and populate up to
        // minPoolSize.
        pool.runtime.execute(async move {
            loop {
                if background::perform_checks(&pool_clone).await {
                    break;
                }

                Delay::new(Duration::from_millis(10)).await;
            }
        });

        pool
    }

    /// Emits an event from the event handler if one is present, where `emit` is a closure that uses
    /// the event handler.
    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        }
    }

    /// Checks out a connection from the pool. This method will block until this thread is at the
    /// front of the wait queue, and then will block again if no available connections are in the
    /// pool and the total number of connections is not less than the max pool size. If the method
    /// blocks for longer than `wait_queue_timeout`, a `WaitQueueTimeoutError` will be returned.
    pub(crate) async fn check_out(&self) -> Result<Connection> {
        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let start_time = Instant::now();
        let mut handle = self.wait_queue.wait_until_at_front().await?;

        let result = self
            .acquire_or_create_connection(start_time, &mut handle)
            .await;
        handle.exit_queue();

        let mut conn = match result {
            Ok(conn) => conn,
            Err(e) => {
                if let ErrorKind::WaitQueueTimeoutError { .. } = e.kind.as_ref() {
                    self.emit_event(|handler| {
                        handler.handle_connection_checkout_failed_event(
                            ConnectionCheckoutFailedEvent {
                                address: self.address.clone(),
                                reason: ConnectionCheckoutFailedReason::Timeout,
                            },
                        )
                    });
                }

                return Err(e);
            }
        };

        self.emit_event(|handler| {
            handler.handle_connection_checked_out_event(conn.checked_out_event());
        });

        conn.mark_checked_out();

        Ok(conn)
    }

    /// Waits for the thread to reach the front of the wait queue, then attempts to check out a
    /// connection.
    async fn acquire_or_create_connection(
        &self,
        start_time: Instant,
        handle: &mut WaitQueueHandle,
    ) -> Result<Connection> {
        loop {
            let mut state = self.state.write().await;

            if state.closed {
                return Err(ErrorKind::PoolClosedError {
                    address: self.address.clone(),
                }
                .into());
            }

            // Try to get the most recent available connection.
            while let Some(conn) = state.connections.pop() {
                // Close the connection if it's stale.
                if conn.is_stale(self.generation.load(Ordering::SeqCst)) {
                    self.close_connection(conn, ConnectionClosedReason::Stale);
                    continue;
                }

                // Close the connection if it's idle.
                if conn.is_idle(self.max_idle_time) {
                    self.close_connection(conn, ConnectionClosedReason::Idle);
                    continue;
                }

                // Otherwise, return the connection.
                return Ok(conn);
            }

            // Create a new connection if the pool is under max size.
            if self.total_connection_count.load(Ordering::SeqCst) < self.max_pool_size {
                return Ok(self.create_connection(true).await?);
            }

            // Check if the pool has a max timeout.
            if let Some(timeout) = self.wait_queue_timeout {
                // Check how long since the checkout process began.
                let time_waiting = Instant::now().duration_since(start_time);

                // If the timeout has been reached, return an error.
                if time_waiting >= timeout {
                    self.emit_event(|handler| {
                        let event = ConnectionCheckoutFailedEvent {
                            address: self.address.clone(),
                            reason: ConnectionCheckoutFailedReason::Timeout,
                        };

                        handler.handle_connection_checkout_failed_event(event);
                    });

                    return Err(ErrorKind::WaitQueueTimeoutError {
                        address: self.address.clone(),
                    }
                    .into());
                }

                // Wait until the either the timeout has been reached or a connection is checked
                // into the pool.
                handle
                    .wait_for_available_connection(Some(timeout - time_waiting))
                    .await?;
            } else {
                // Wait until a connection has been returned to the pool.
                handle.wait_for_available_connection(None).await?;
            }
        }
    }

    /// Checks a connection back into the pool and notifies the wait queue that a connection is
    /// ready. If the connection is stale, it will be closed instead of being added to the set of
    /// available connections. The time that the connection is checked in will be marked to
    /// facilitate detecting if the connection becomes idle.
    pub(crate) async fn check_in(&self, mut conn: Connection) {
        self.emit_event(|handler| {
            handler.handle_connection_checked_in_event(conn.checked_in_event());
        });

        let pool = self.clone();
        let wait_queue = self.wait_queue.clone();

        self.runtime.execute(async move {
            let mut state = pool.state.write().await;

            if state.closed {
                pool.close_connection(conn, ConnectionClosedReason::PoolClosed);
            } else {
                conn.mark_checked_in();

                // Close the connection if it's stale.
                if conn.is_stale(pool.generation.load(Ordering::SeqCst)) {
                    pool.close_connection(conn, ConnectionClosedReason::Stale);

                    return;
                }

                state.connections.push(conn);

                wait_queue.notify_ready();
            }
        });
    }

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) fn clear(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);

        self.emit_event(|handler| {
            let event = PoolClearedEvent {
                address: self.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        });
    }

    /// Internal helper to close a connection, emit the event for it being closed, and decrement the
    /// total connection count. Any connection being closed by the pool should be closed by using
    /// this method.
    fn close_connection(&self, conn: Connection, reason: ConnectionClosedReason) {
        self.emit_event(|handler| {
            handler.handle_connection_closed_event(conn.closed_event(reason));
        });

        self.total_connection_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// Helper method to create a connection and increment the total connection count.
    async fn create_connection(&self, checking_out: bool) -> Result<Connection> {
        self.total_connection_count.fetch_add(1, Ordering::SeqCst);

        let mut connection = Connection::new(
            self.next_connection_id.fetch_add(1, Ordering::SeqCst),
            self.address.clone(),
            self.generation.load(Ordering::SeqCst),
            self.connect_timeout,
            self.tls_options.clone(),
            self.event_handler.clone(),
            self.runtime.clone(),
        )
        .await?;

        self.emit_event(|handler| {
            handler.handle_connection_created_event(connection.created_event())
        });

        let establish_result = self
            .establisher
            .establish_connection(&mut connection, self.credential.as_ref())
            .await;

        if let Err(e) = establish_result {
            self.clear();

            if checking_out {
                self.emit_event(|handler| {
                    handler.handle_connection_checkout_failed_event(ConnectionCheckoutFailedEvent {
                        address: self.address.clone(),
                        reason: ConnectionCheckoutFailedReason::ConnectionError,
                    })
                });
            }

            return Err(e);
        }

        self.emit_event(|handler| handler.handle_connection_ready_event(connection.ready_event()));

        Ok(connection)
    }

    pub(crate) fn close(&self) {
        let pool = self.clone();

        self.runtime.execute(async move {
            let mut state = pool.state.write().await;
            state.closed = true;

            for conn in state.connections.drain(..) {
                pool.emit_event(|handler| {
                    handler.handle_connection_closed_event(
                        conn.closed_event(ConnectionClosedReason::PoolClosed),
                    );
                });
            }

            pool.emit_event(|handler| {
                handler.handle_pool_closed_event(PoolClosedEvent {
                    address: pool.address.clone(),
                });
            });
        });
    }
}
