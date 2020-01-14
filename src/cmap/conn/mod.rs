mod command;
mod stream;
mod stream_description;
mod wire;

use std::time::{Duration, Instant};

use derivative::Derivative;

use self::{wire::Message};
use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        ConnectionCheckedInEvent,
        ConnectionCheckedOutEvent,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        ConnectionCreatedEvent,
        ConnectionReadyEvent,
    },
    feature::{AsyncRuntime, AsyncStream},
    options::{StreamAddress, TlsOptions},
};

pub(crate) use command::{Command, CommandResponse};
pub use stream::StreamOptions;
pub(crate) use stream_description::StreamDescription;
pub(crate) use wire::next_request_id;

/// User-facing information about a connection to the database.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A driver-generated identifier that uniquely identifies the connection.
    pub id: u32,

    /// The address that the connection is connected to.
    pub address: StreamAddress,
}

/// A wrapper around Stream that contains all the CMAP information needed to maintain a connection.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Connection {
    pub(super) id: u32,
    pub(super) address: StreamAddress,
    pub(super) generation: u32,

    /// The cached StreamDescription from the connection's handshake.
    pub(super) stream_description: Option<StreamDescription>,

    /// Marks the time when the connection was checked into the pool and established. This is used
    /// to detect if the connection is idle.
    ready_and_available_time: Option<Instant>,

    #[derivative(Debug = "ignore")]
    stream: AsyncStream,
}

impl Connection {
    /// Constructs a new connection.
    pub(crate) async fn new(
        id: u32,
        address: StreamAddress,
        generation: u32,
        connect_timeout: Option<Duration>,
        tls_options: Option<TlsOptions>,
        runtime: AsyncRuntime,
    ) -> Result<Self> {
        let options = StreamOptions { address: address.clone(), connect_timeout, tls_options: tls_options.clone() };
        
        let conn = Self {
            id,
            generation,
            stream_description: None,
            ready_and_available_time: None,
            stream: runtime.connect_stream(options).await?,
            address,
        };

        Ok(conn)
    }

    pub(crate) fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            id: self.id,
            address: self.address.clone(),
        }
    }

    pub(crate) fn address(&self) -> &StreamAddress {
        &self.address
    }

    /// Helper to mark the time that the connection was checked into the pool for the purpose of
    /// detecting when it becomes idle.
    pub(super) fn mark_checked_in(&mut self) {
        self.ready_and_available_time = Some(Instant::now());
    }

    /// Helper to mark that the connection has been checked out of the pool. This ensures that the
    /// connection is not marked as idle based on the time that it's checked out and that it has a
    /// reference to the pool.
    pub(super) fn mark_checked_out(&mut self) {
        self.ready_and_available_time.take();
    }

    /// Checks if the connection is idle.
    pub(super) fn is_idle(&self, max_idle_time: Option<Duration>) -> bool {
        self.ready_and_available_time
            .and_then(|ready_and_available_time| {
                max_idle_time.map(|max_idle_time| {
                    Instant::now().duration_since(ready_and_available_time) >= max_idle_time
                })
            })
            .unwrap_or(false)
    }

    /// Checks if the connection is stale.
    pub(super) fn is_stale(&self, current_generation: u32) -> bool {
        self.generation != current_generation
    }

    /// Helper to create a `ConnectionCheckedOutEvent` for the connection.
    pub(super) fn checked_out_event(&self) -> ConnectionCheckedOutEvent {
        ConnectionCheckedOutEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionCheckedInEvent` for the connection.
    pub(super) fn checked_in_event(&self) -> ConnectionCheckedInEvent {
        ConnectionCheckedInEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionReadyEvent` for the connection.
    pub(super) fn ready_event(&self) -> ConnectionReadyEvent {
        ConnectionReadyEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionReadyEvent` for the connection.
    pub(super) fn created_event(&self) -> ConnectionCreatedEvent {
        ConnectionCreatedEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionReadyEvent` for the connection.
    pub(super) fn closed_event(&self, reason: ConnectionClosedReason) -> ConnectionClosedEvent {
        ConnectionClosedEvent {
            address: self.address.clone(),
            connection_id: self.id,
            reason,
        }
    }

    /// Executes a `Command` and returns a `CommandResponse` containing the result from the server.
    ///
    /// An `Ok(...)` result simply means the server received the command and that the driver
    /// driver received the response; it does not imply anything about the success of the command
    /// itself.
    pub(crate) fn send_command(
        &mut self,
        command: Command,
        request_id: impl Into<Option<i32>>,
    ) -> Result<CommandResponse> {
        let message = Message::with_command(command, request_id.into());
        message.write_to(&mut self.stream)?;

        let response_message = Message::read_from(&mut self.stream)?;
        CommandResponse::new(self.address.clone(), response_message)
    }

    /// Gets the connection's StreamDescription.
    pub(crate) fn stream_description(&self) -> Result<&StreamDescription> {
        self.stream_description.as_ref().ok_or_else(|| {
            ErrorKind::OperationError {
                message: "Stream checked out but not handshaked".to_string(),
            }
            .into()
        })
    }
}
