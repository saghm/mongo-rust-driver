use std::{collections::VecDeque, sync::Arc, time::Duration};

use futures::future::{Either, FutureExt};
use futures_timer::Delay;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};

use crate::{
    error::{ErrorKind, Result},
    feature::AsyncRuntime,
    options::StreamAddress,
};

#[derive(Clone, Debug)]
pub(super) struct WaitQueue {
    /// The elements in the queue are conditional variables. When a thread enters the wait queue,
    /// they block on a newly-created conditional variable until either they are at the front of
    /// the queue or an optional timeout is reached.
    queue: Arc<Mutex<VecDeque<Sender<()>>>>,

    /// The timeout signifying how long a thread should wait in the queue before returning an
    /// error. This will be the `wait_queue_timeout` for a given connection pool.
    timeout: Option<Duration>,

    /// The address that the connection pool's connections will connect to. This is needed to
    /// return a WaitQueueTimeoutError when the timeout has elapsed.
    address: StreamAddress,

    runtime: AsyncRuntime,
}

impl WaitQueue {
    /// Creates a new `WaitQueue`.
    pub(super) fn new(
        runtime: AsyncRuntime,
        address: StreamAddress,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            queue: Default::default(),
            address,
            timeout,
            runtime,
        }
    }

    pub(super) async fn wait_until_at_front(&self) -> Result<WaitQueueHandle> {
        let mut queue = self.queue.lock().await;

        let (sender, receiver) = mpsc::channel(1);
        queue.push_back(sender);

        let mut handle = WaitQueueHandle {
            receiver: Some(receiver),
            queue: Some(self.queue.clone()),
            runtime: self.runtime.clone(),
            address: self.address.clone(),
        };

        if queue.len() == 1 {
            return Ok(handle);
        }

        handle.wait_for_available_connection(self.timeout).await?;
        Ok(handle)
    }

    pub(super) fn notify_ready(&self) {
        let queue = self.queue.clone();

        self.runtime.execute(async move {
            if let Some(sender) = queue.lock().await.front_mut() {
                let _ = sender.send(()).await;
            }
        });
    }
}

#[derive(Debug)]
pub(super) struct WaitQueueHandle {
    address: StreamAddress,
    receiver: Option<Receiver<()>>,
    queue: Option<Arc<Mutex<VecDeque<Sender<()>>>>>,
    runtime: AsyncRuntime,
}

impl WaitQueueHandle {
    fn null() -> Self {
        Self {
            address: StreamAddress {
                hostname: "".into(),
                port: None,
            },
            receiver: None,
            queue: None,
            runtime: AsyncRuntime::Null,
        }
    }

    pub(super) async fn wait_for_available_connection(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let receiver = match self.receiver {
            Some(ref mut receiver) => receiver,
            None => {
                return Err(ErrorKind::Invariant {
                    message: "attempted to wait on a null WaitQueueHandle".into(),
                }
                .into())
            }
        };

        if let Some(timeout) = timeout {
            match futures::future::select(receiver.recv().boxed(), Delay::new(timeout).boxed())
                .await
            {
                Either::Left(..) => Ok(()),
                Either::Right(..) => Err(ErrorKind::WaitQueueTimeoutError {
                    address: self.address.clone(),
                }
                .into()),
            }
        } else {
            receiver.recv().await;
            Ok(())
        }
    }
}

impl Drop for WaitQueueHandle {
    fn drop(&mut self) {
        if let Some(queue) = self.queue.clone() {
            let mut queue = queue.clone().lock().await;
            let handle = std::mem::replace(self, Self::null());
            let runtime = self.runtime.clone();

            runtime.execute(async move {
                queue.pop_front();

                if let Some(sender) = queue.front_mut() {
                    let _ = sender.send(()).await;
                }
            });
        }
    }
}
