//! The execution runtime's only executor/timer boundary.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::*;
#[cfg(target_arch = "wasm32")]
pub(crate) use wasm::*;

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use crate::time::Instant;

    pub(crate) use tokio::{spawn, task::JoinHandle};

    pub(crate) fn sleep_until(deadline: Instant) -> tokio::time::Sleep {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::{
        fmt,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use futures_util::future::{AbortHandle, Abortable};
    use tokio::sync::oneshot;
    use worker::{Delay, send::SendFuture, wasm_bindgen_futures::spawn_local};

    use crate::time::Instant;

    // The handle itself contains only thread-safe Rust state. JS-affine futures stay on the
    // Workers executor. Like Tokio's handle, dropping this handle detaches rather than aborts.
    pub(crate) struct JoinHandle<T> {
        result: oneshot::Receiver<Result<T, JoinError>>,
        abort: AbortHandle,
    }

    impl<T> JoinHandle<T> {
        pub(crate) fn abort(&self) {
            self.abort.abort();
        }
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.get_mut().result).poll(cx) {
                Poll::Ready(Ok(result)) => Poll::Ready(result),
                Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError::Closed)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    #[derive(Debug)]
    pub(crate) enum JoinError {
        Aborted,
        Closed,
    }

    impl fmt::Display for JoinError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Self::Aborted => "worker task was aborted",
                Self::Closed => "worker task exited without returning a result",
            })
        }
    }

    impl std::error::Error for JoinError {}

    pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let (abort, registration) = AbortHandle::new_pair();
        // Retaining a lazy future is not execution. Drive it independently, including while the
        // runtime is awaiting a lease-renewal query, and observe its result through the channel.
        spawn_local(async move {
            let result = Abortable::new(future, registration)
                .await
                .map_err(|_| JoinError::Aborted);
            let _ = sender.send(result);
        });
        JoinHandle { result, abort }
    }

    pub(crate) fn sleep_until(deadline: Instant) -> impl Future<Output = ()> + Send {
        // This SDK boundary is valid only on single-threaded Workers. It keeps run_task_once's
        // future Send without changing any backend/worker bounds or moving JS objects to threads.
        SendFuture::new(async move {
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return;
                }
                // Delay uses signed 32-bit millisecond timer handles/durations. Bound long waits,
                // and recheck the deadline after waking (including sub-millisecond/early wakes).
                let millis = remaining.as_millis().clamp(1, i32::MAX as u128) as u64;
                Delay::from(Duration::from_millis(millis)).await;
            }
        })
    }
}
