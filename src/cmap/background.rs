use std::sync::atomic::Ordering;

use super::ConnectionPool;
use crate::event::cmap::ConnectionClosedReason;

/// Cleans up any stale or idle connections and adds new connections if the total number is below
/// the min pool size.
pub(super) async fn perform_checks(pool: &ConnectionPool) -> bool {
    // We remove the perished connections first to ensure that the number of connections does not
    // dip under the min pool size due to the removals.
    if remove_perished_connections_from_pool(&pool).await {
        return true;
    }

    if ensure_min_connections_in_pool(&pool).await {
        return true;
    }

    false
}

/// Iterate over the connections and remove any that are stale or idle.
async fn remove_perished_connections_from_pool(pool: &ConnectionPool) -> bool {
    let mut connections = pool.inner.connections.write().await;

    if pool.inner.closed.load(Ordering::SeqCst) {
        return true;
    }

    let mut i = 0;

    while i < connections.len() {
        let reason = if connections[i].is_stale(pool.generation.load(Ordering::SeqCst)) {
            ConnectionClosedReason::Stale
        } else if connections[i].is_idle(pool.max_idle_time) {
            ConnectionClosedReason::Idle
        } else {
            i += 1;
            continue;
        };

        // We know this will always find the connection since we hold a lock, but we avoid using
        // unwrap out of principle.
        if let Some(connection) = connections.remove(i) {
            pool.close_connection(connection, reason);
        }
    }

    false
}

/// Add connections until the min pool size it met. We explicitly release the lock at the end of
/// each iteration and acquire it again during the next one to ensure that the this method doesn't
/// block other threads from acquiring connections.
async fn ensure_min_connections_in_pool(pool: &ConnectionPool) -> bool {
    if let Some(min_pool_size) = pool.min_pool_size {
        loop {
            let mut connections = pool.inner.connections.write().await;

            if pool.inner.closed.load(Ordering::SeqCst) {
                return true;
            }

            if pool.total_connection_count.load(Ordering::SeqCst) < min_pool_size {
                match pool.create_connection(false).await {
                    Ok(connection) => connections.push_back(connection),
                    e @ Err(_) => {
                        // Since we had to clear the pool, we return early from this function and
                        // put the background thread back to sleep. Next time it wakes up, the
                        // stale connections will be closed, and the thread can try to create new
                        // ones after that.
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    false
}
