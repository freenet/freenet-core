//! Keeping a panic in a QUIC task from reaching the rest of the node (#5940).
//!
//! quinn 0.11.12 keeps each endpoint's state, and each connection's, behind
//! a `std::sync::Mutex` that it locks with `.lock().unwrap()`, and polls both
//! kinds of driver while holding it (`EndpointDriver::poll`, `endpoint.rs`
//! line 370; `ConnectionDriver::poll`, `connection.rs` line 260). A panic in
//! a driver, in quinn's own code or in the socket adapter and timers quinn
//! calls into, therefore poisons the mutex it held, and every later lock of
//! that mutex panics: in the other drivers, in every call the node makes
//! into quinn (connect, accept, stream reads and writes, close) and in the
//! `Drop` of the quinn handles (`EndpointDriver`, the last `Endpoint`,
//! `Incoming`, the last `Connection`, `SendStream`, `RecvStream`).
//!
//! Where the panic starts decides what can be done:
//!
//! - In the node's own code that quinn calls while holding a lock (the
//!   socket adapter, `super::socket`): caught there, before it unwinds into
//!   quinn, so no lock is poisoned.
//! - In quinn's code inside a CONNECTION driver: it unwinds through quinn and
//!   poisons that connection's lock. quinn's task drops the driver during the
//!   unwind, which is harmless while the node holds a handle to the
//!   connection (only the last handle's drop takes the lock). The runtime's
//!   catch ([`catch_panics`]) then stops it.
//! - In quinn's code inside the ENDPOINT driver: NOT contained. quinn's task
//!   holds the driver as a local, so the unwind drops it inside quinn, before
//!   any catch of ours runs, and `EndpointDriver::drop` unwraps the lock the
//!   panic poisoned: a panic during the unwind, and the process aborts. Only
//!   a change in quinn can fix that (as of quinn 0.11.12 and quinn's main
//!   branch); `a_panic_unwinding_through_the_endpoint_driver_still_aborts`
//!   pins it.
//!
//! Before this module, every one of these aborted the process: the endpoint
//! driver as above, and a connection driver's panic at the latest when the
//! node dropped its connection handle.
//!
//! The rules:
//!
//! 1. Every future quinn spawns (`QuicRuntime`), and every QUIC task and
//!    call into quinn the node makes, runs under [`catch_panics`],
//!    [`contain`] or [`guard`], and the socket adapter catches its own. A
//!    panic is caught, the future that panicked is LEAKED (never dropped: its
//!    drop would lock what it poisoned), and the endpoint is reported
//!    poisoned and `Down` ([`EndpointHealth::task_panicked`]).
//! 2. Once the endpoint is poisoned, nothing of the node's touches or drops a
//!    quinn object of that endpoint. New calls are refused at once
//!    ([`guard`], [`contain`]); calls in flight are abandoned, their futures
//!    leaked, and return an error ([`contain`]); every quinn handle the node
//!    holds is a [`Quinn`], which leaks it instead of dropping it. The one
//!    exception is a single close per connection
//!    (`QuicCloser::close_after_poison`), caught like everything else: it
//!    tells the peer at once when that connection's own state is intact
//!    (every connection but one whose driver panicked inside quinn), and
//!    otherwise the peer finds out by its idle timeout.
//! 3. What is leaked: the endpoint's quinn state and socket adapter, and
//!    every connection, stream and call that was open at that moment, with
//!    their buffers. Once per endpoint, bounded by what was open, never
//!    freed: the node runs with QUIC off until it restarts.
//!
//! The drivers of connections whose state is intact keep running after the
//! endpoint is poisoned, so the close above reaches the peer; they are
//! caught like any other task if they panic too.
//!
//! Not covered: the check and the poisoning are not one step. A call or a
//! drop that starts on another worker thread after a panic has poisoned a
//! lock, but before the panic is caught and the endpoint marked poisoned,
//! still reaches the poisoned lock. A call is caught ([`guard`],
//! [`contain`]); a drop is caught too ([`Quinn`]), but a quinn `Drop` that
//! panics part way leaves the rest of that object undropped. The window is
//! the unwind of one panic; the single-threaded tests never open it.
//!
//! [`EndpointHealth::task_panicked`]: super::liveness::EndpointHealth::task_panicked

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::liveness::EndpointHealth;

/// A quinn handle (an endpoint, an attempt, a connection) of one endpoint:
/// dropped normally while the endpoint is healthy, LEAKED once a QUIC task
/// of the endpoint panicked (see the module docs).
pub(crate) struct Quinn<T> {
    /// `Some` until dropped or taken.
    inner: Option<T>,
    health: Arc<EndpointHealth>,
}

impl<T> Quinn<T> {
    pub(crate) fn new(inner: T, health: Arc<EndpointHealth>) -> Self {
        Self {
            inner: Some(inner),
            health,
        }
    }

    /// The health of the endpoint this handle belongs to.
    pub(crate) fn health(&self) -> &Arc<EndpointHealth> {
        &self.health
    }

    /// The handle, or `None` once the endpoint is poisoned (the handle is
    /// then leaked).
    pub(crate) fn into_inner(mut self) -> Option<T> {
        let inner = self.inner.take();
        if self.health.is_poisoned() {
            std::mem::forget(inner);
            return None;
        }
        inner
    }
}

impl<T> std::ops::Deref for Quinn<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner
            .as_ref()
            .expect("a quinn handle is held until it is dropped or taken")
    }
}

impl<T: Clone> Clone for Quinn<T> {
    /// Cloning a quinn `Endpoint` or `Connection` takes no lock.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            health: self.health.clone(),
        }
    }
}

impl<T> Drop for Quinn<T> {
    fn drop(&mut self) {
        release(self.inner.take(), &self.health, "drop of a quinn handle");
    }
}

/// Drop `value` if the endpoint is healthy, under `catch_unwind` (a panic
/// poisons the endpoint); leak it if the endpoint is poisoned.
fn release<T>(value: Option<T>, health: &EndpointHealth, task: &'static str) {
    let Some(value) = value else { return };
    if health.is_poisoned() {
        std::mem::forget(value);
        return;
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(value))) {
        health.task_panicked(task, &*payload);
    }
}

/// Run `call` (a call into quinn) unless the endpoint is poisoned, catching
/// a panic. `None` if the endpoint was poisoned or `call` panicked (the
/// endpoint is then poisoned).
pub(crate) fn guard<R>(
    health: &EndpointHealth,
    task: &'static str,
    call: impl FnOnce() -> R,
) -> Option<R> {
    if health.is_poisoned() {
        return None;
    }
    catch(health, task, call)
}

/// Run `call`, catching a panic, whether or not the endpoint is poisoned.
/// For a call that must run to release what it holds (a quinn handle moved
/// into it would otherwise be dropped unrun).
pub(crate) fn catch<R>(
    health: &EndpointHealth,
    task: &'static str,
    call: impl FnOnce() -> R,
) -> Option<R> {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(out) => Some(out),
        Err(payload) => {
            health.task_panicked(task, &*payload);
            None
        }
    }
}

/// Run `fut` to its end, catching a panic: `None` if it panicked (it was
/// leaked and the endpoint poisoned). A future that is still pending when
/// the endpoint is poisoned keeps running (quinn's drivers: one whose
/// connection is intact still has to send the close).
pub(crate) async fn catch_panics<F: Future>(
    health: &Arc<EndpointHealth>,
    task: &'static str,
    fut: F,
) -> Option<F::Output> {
    Contained::new(health.clone(), task, Box::pin(fut))
        .run()
        .await
}

/// Run `fut` (a call into quinn, or a task that makes them), catching a
/// panic and giving up when the endpoint is poisoned. `None` if the
/// endpoint was already poisoned (`fut` never runs), became poisoned while
/// `fut` was pending, or `fut` panicked; in every case `fut` is leaked.
/// Dropping the returned future while the endpoint is healthy drops `fut`
/// normally, so a cancelled call keeps its cancellation behaviour.
pub(crate) async fn contain<F: Future>(
    health: &Arc<EndpointHealth>,
    task: &'static str,
    fut: F,
) -> Option<F::Output> {
    let mut contained = Contained::new(health.clone(), task, Box::pin(fut));
    if health.is_poisoned() {
        return None;
    }
    contained.run_until_poisoned().await.ok().flatten()
}

/// The endpoint was poisoned while a [`Contained`] future was pending.
#[derive(Debug)]
pub(crate) struct Poisoned;

/// A future polled under `catch_unwind`. When it panics it is leaked and
/// the endpoint poisoned. When it is dropped unfinished it is dropped like
/// a [`Quinn`] handle: normally while the endpoint is healthy, leaked once
/// it is poisoned.
pub(crate) struct Contained<F> {
    /// `Some` until it finished, panicked or was dropped.
    fut: Option<F>,
    health: Arc<EndpointHealth>,
    task: &'static str,
}

impl<F: Future + Unpin> Contained<F> {
    pub(crate) fn new(health: Arc<EndpointHealth>, task: &'static str, fut: F) -> Self {
        Self {
            fut: Some(fut),
            health,
            task,
        }
    }

    /// Poll once. `Ready(Some)`: finished; `Ready(None)`: it panicked, or
    /// had already finished or panicked before.
    fn poll_once(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        let Some(fut) = self.fut.as_mut() else {
            return Poll::Ready(None);
        };
        match catch_unwind(AssertUnwindSafe(|| Pin::new(fut).poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(out)) => {
                release(self.fut.take(), &self.health, self.task);
                Poll::Ready(Some(out))
            }
            Err(payload) => {
                // Never dropped: its drop would lock what the panic poisoned.
                std::mem::forget(self.fut.take());
                self.health.task_panicked(self.task, &*payload);
                Poll::Ready(None)
            }
        }
    }

    /// Run to the end. `None` if it panicked.
    pub(crate) async fn run(&mut self) -> Option<F::Output> {
        std::future::poll_fn(|cx| self.poll_once(cx)).await
    }

    /// Run to the end, or until the endpoint is poisoned (`Err`; the future
    /// is kept, and [`Self::run`] may resume it). `Ok(None)` if it panicked.
    pub(crate) async fn run_until_poisoned(&mut self) -> Result<Option<F::Output>, Poisoned> {
        let mut poisoned = std::pin::pin!(self.health.poisoned());
        // The poisoning is checked first: a future woken by it is not polled
        // again, since polling it would take a lock that may be poisoned.
        std::future::poll_fn(|cx| {
            if poisoned.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(Poisoned));
            }
            self.poll_once(cx).map(Ok)
        })
        .await
    }
}

impl<F> Drop for Contained<F> {
    fn drop(&mut self) {
        release(self.fut.take(), &self.health, self.task);
    }
}
