use super::Client;

use std::{collections::HashSet, convert::TryInto};

use bson::Document;
use lazy_static::lazy_static;
use time::Instant;

use crate::{
    cmap::Connection,
    error::Result,
    event::command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
    operation::Operation,
    sdam::{update_topology, ServerDescription},
};

lazy_static! {
    static ref REDACTED_COMMANDS: HashSet<&'static str> = {
        let mut hash_set = HashSet::new();
        hash_set.insert("authenticate");
        hash_set.insert("saslstart");
        hash_set.insert("saslcontinue");
        hash_set.insert("getnonce");
        hash_set.insert("createuser");
        hash_set.insert("updateuser");
        hash_set.insert("copydbgetnonce");
        hash_set.insert("copydbsaslstart");
        hash_set.insert("copydb");
        hash_set
    };
}

impl Client {
    /// Executes an operation and returns the connection used to do so along with the result of the
    /// operation. This will be used primarily for the opening of exhaust cursors.
    #[allow(dead_code)]
    pub(crate) async fn execute_exhaust_operation<T: Operation>(
        &self,
        op: &T,
    ) -> Result<(T::O, Connection)> {
        let mut conn = self
            .select_server(op.selection_criteria())
            .await?
            .1
            .checkout_connection()
            .await?;
        self.execute_operation_on_connection(op, &mut conn)
            .await
            .map(|r| (r, conn))
    }

    pub(crate) async fn execute_operation_owned<T: Operation>(self, op: T) -> Result<T::O> {
        self.execute_operation(&op, None).await
    }

    /// Execute the given operation, optionally specifying a connection used to do so.
    /// If no connection is provided, server selection will performed using the criteria specified
    /// on the operation, if any.
    pub(crate) async fn execute_operation<T: Operation>(
        &self,
        op: &T,
        connection: Option<&mut Connection>,
    ) -> Result<T::O> {
        // if no connection provided, select one.
        match connection {
            Some(conn) => self.execute_operation_on_connection(op, conn).await,
            None => {
                let (server_type, server) = self.select_server(op.selection_criteria()).await?;
                let mut conn = server.checkout_connection().await?;
                let result = self.execute_operation_on_connection(op, &mut conn).await;

                let wire_version = conn
                    .stream_description()
                    .map(|sd| sd.max_wire_version)
                    .ok()
                    .and_then(std::convert::identity)
                    .unwrap_or(0);

                server.checkin_connection(conn).await;

                // If we encounter certain errors, we must update the topology as per the
                // SDAM spec.
                if let Err(ref e) = result {
                    let update = || {
                        let description =
                            ServerDescription::new(server.address.clone(), Some(Err(e.clone())));
                        update_topology(self.topology(), description)
                    };

                    if e.is_non_timeout_network_error() {
                        update().await;
                    } else if e.is_recovering() || e.is_not_master() {
                        update().await;

                        // For "node is recovering" or "not master" errors, we must request a
                        // topology check.
                        server.monitor_check(server_type).await;

                        // in 4.2+, we only clear connection pool if we've received a
                        // "node is shutting down" error. Otherwise, we always clear the pool.
                        if wire_version < 8 || e.is_shutting_down() {
                            server.clear_connection_pool();
                        }
                    }
                }
                result
            }
        }
    }

    /// Executes an operation on a given connection.
    async fn execute_operation_on_connection<T: Operation>(
        &self,
        op: &T,
        connection: &mut Connection,
    ) -> Result<T::O> {
        let mut cmd = op.build(connection.stream_description()?)?;
        self.topology().read().await.update_command_with_read_pref(
            connection.address(),
            &mut cmd,
            op.selection_criteria(),
        );

        let connection_info = connection.info();
        let request_id = crate::cmap::conn::next_request_id();

        self.emit_command_event(|handler| {
            let should_redact = REDACTED_COMMANDS.contains(cmd.name.to_lowercase().as_str());

            let command_body = if should_redact {
                Document::new()
            } else {
                cmd.body.clone()
            };
            let command_started_event = CommandStartedEvent {
                command: command_body,
                db: cmd.target_db.clone(),
                command_name: cmd.name.clone(),
                request_id,
                connection: connection_info.clone(),
            };

            handler.handle_command_started_event(command_started_event);
        });

        let start_time = Instant::now();

        let response_result = connection
            .send_command(cmd.clone(), request_id)
            .await
            .and_then(|response| {
                if !op.handles_command_errors() {
                    response.validate()?;
                }
                Ok(response)
            });

        let end_time = Instant::now();
        let duration = (end_time - start_time).try_into()?;

        match response_result {
            Err(error) => {
                self.emit_command_event(|handler| {
                    let command_failed_event = CommandFailedEvent {
                        duration,
                        command_name: cmd.name,
                        failure: error.clone(),
                        request_id,
                        connection: connection_info,
                    };

                    handler.handle_command_failed_event(command_failed_event);
                });
                Err(error)
            }
            Ok(response) => {
                self.emit_command_event(|handler| {
                    let should_redact =
                        REDACTED_COMMANDS.contains(cmd.name.to_lowercase().as_str());

                    let reply = if should_redact {
                        Document::new()
                    } else {
                        response.raw_response.clone()
                    };

                    let command_succeeded_event = CommandSucceededEvent {
                        duration,
                        reply,
                        command_name: cmd.name.clone(),
                        request_id,
                        connection: connection_info,
                    };
                    handler.handle_command_succeeded_event(command_succeeded_event);
                });
                op.handle_response(response)
            }
        }
    }
}
