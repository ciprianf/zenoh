//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{
    collections::VecDeque,
    future::poll_fn,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, OnceLock,
    },
    task::Poll,
    time::{Duration, Instant},
};

use futures::{future::select_all, task::AtomicWaker};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{CongestionControl, Priority},
    network::{NetworkMessage, NetworkMessageExt},
    transport::{KeepAlive, TransportMessage},
};
use zenoh_result::{bail, zerror, ZResult};
use zenoh_sync::{event, Notifier, RecyclingObjectPool, Waiter};
use zenoh_task::TaskController;

/// Prototype: default bounded depth of each per-(link, priority) tx queue used
/// for slow-subscriber isolation. Overridable at runtime via the
/// `ZENOH_TX_QUEUE_LEN` environment variable.
const DEFAULT_TX_QUEUE_LEN: usize = 8;

/// Name of the environment variable controlling the per-(link, priority) tx
/// queue depth.
const TX_QUEUE_LEN_ENV: &str = "ZENOH_TX_QUEUE_LEN";

/// Resolve the tx queue depth from the `ZENOH_TX_QUEUE_LEN` environment
/// variable, falling back to [`DEFAULT_TX_QUEUE_LEN`] when unset or invalid.
fn tx_queue_len() -> usize {
    match std::env::var(TX_QUEUE_LEN_ENV) {
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if n > 0 => {
                println!("[zenoh-tx-queue] {TX_QUEUE_LEN_ENV}={v:?} -> using queue length {n}");
                n
            }
            _ => {
                println!(
                    "[zenoh-tx-queue] invalid {TX_QUEUE_LEN_ENV}={v:?}, falling back to default {DEFAULT_TX_QUEUE_LEN}"
                );
                DEFAULT_TX_QUEUE_LEN
            }
        },
        Err(_) => {
            println!(
                "[zenoh-tx-queue] {TX_QUEUE_LEN_ENV} not set, using default queue length {DEFAULT_TX_QUEUE_LEN}"
            );
            DEFAULT_TX_QUEUE_LEN
        }
    }
}

use super::transport::TransportUnicastUniversal;
use crate::{
    common::{
        batch::{BatchConfig, RBatch},
        pipeline::{
            PipelineConsumer, TransmissionPipeline, TransmissionPipelineConf,
            TransmissionPipelineConsumer, TransmissionPipelineProducer,
        },
        priority::TransportPriorityTx,
    },
    unicast::link::{TransportLinkUnicast, TransportLinkUnicastRx, TransportLinkUnicastTx},
};

/// Returns `true` if a message must be transmitted without ever being dropped,
/// i.e. it is allowed to exert backpressure (block) on a full queue.
///
/// Only reliable `CongestionControl::Block` traffic blocks. Everything else —
/// best-effort, `CongestionControl::Drop`, and `CongestionControl::BlockFirst`
/// — is evictable and never blocks the enqueuing thread.
#[inline]
fn may_block(msg: &impl NetworkMessageExt) -> bool {
    msg.is_reliable() && msg.congestion_control() == CongestionControl::Block
}

/// A bounded per-(link, priority) queue used as a staging buffer between the
/// shared fan-out path and the per-link transmission pipeline.
///
/// Prototype for slow-subscriber isolation: `internal_schedule` enqueues here
/// instead of pushing into the pipeline inline. A dedicated per-link drain
/// worker (see [`tx_queue_drain_loop`]) performs the potentially-blocking push
/// into the pipeline, so a congested link can no longer stall delivery to other
/// links.
///
/// Overflow policy preserves Zenoh's reliability guarantees: only messages that
/// are *not* reliable `Block` (i.e. best-effort, `Drop`, or `BlockFirst`) are
/// ever discarded. Reliable `Block` messages (e.g. `Declare` traffic) are never
/// dropped; when a priority queue is saturated exclusively with such messages,
/// the enqueuing thread blocks (backpressure) until the drain worker frees a
/// slot.
struct PriorityQueue {
    queue: Mutex<VecDeque<NetworkMessage>>,
    /// Signalled by the drain worker whenever it frees a slot, so blocked
    /// enqueuers waiting for space can make progress.
    space: Condvar,
}

struct TxQueueInner {
    queues: [PriorityQueue; Priority::NUM],
    /// Maximum depth of each per-priority queue (from `ZENOH_TX_QUEUE_LEN`).
    capacity: usize,
    disabled: AtomicBool,
}

#[derive(Clone)]
pub(super) struct TxQueue {
    inner: Arc<TxQueueInner>,
    notifier: Notifier,
    waiter: Waiter,
}

impl TxQueue {
    fn new() -> Self {
        let capacity = tx_queue_len();
        println!(
            "[zenoh-tx-queue] creating TxQueue with capacity {capacity} per (link, priority) \
             (env {TX_QUEUE_LEN_ENV})"
        );
        let (notifier, waiter) = event::new();
        Self {
            inner: Arc::new(TxQueueInner {
                queues: std::array::from_fn(|_| PriorityQueue {
                    queue: Mutex::new(VecDeque::with_capacity(capacity)),
                    space: Condvar::new(),
                }),
                capacity,
                disabled: AtomicBool::new(false),
            }),
            notifier,
            waiter,
        }
    }

    /// Enqueue a message into its priority queue.
    ///
    /// Overflow handling never drops a reliable `Block` message:
    /// - If the queue is full, the oldest evictable (non-`Block`) message is
    ///   dropped to make room.
    /// - If the queue is full and contains only reliable `Block` messages:
    ///   - a non-`Block` incoming message is dropped (returns without pushing);
    ///   - a reliable `Block` incoming message blocks until the drain worker
    ///     frees a slot (backpressure).
    pub(super) fn enqueue(&self, msg: NetworkMessage) {
        // Guard the index in case the link is non-QoS (single priority).
        let idx = (msg.priority() as usize).min(Priority::NUM - 1);
        let pq = &self.inner.queues[idx];
        let capacity = self.inner.capacity;
        let blocking = may_block(&msg);
        {
            let mut q = pq
                .queue
                .lock()
                .expect("locking `TxQueue` should not fail");
            loop {
                if q.len() < capacity {
                    break;
                }
                // Full: try to evict the oldest non-`Block` message to make room.
                if let Some(pos) = q.iter().position(|m| !may_block(m)) {
                    q.remove(pos);
                    break;
                }
                // Queue is saturated with reliable `Block` messages.
                if !blocking {
                    // Cannot make room without dropping a `Block` message:
                    // drop the incoming non-`Block` one instead.
                    return;
                }
                if self.inner.disabled.load(Ordering::Acquire) {
                    // Link is shutting down: stop blocking and discard.
                    return;
                }
                // Backpressure: wait until the drain worker frees a slot.
                q = pq
                    .space
                    .wait(q)
                    .expect("waiting on `TxQueue` should not fail");
            }
            q.push_back(msg);
        }
        let _ = self.notifier.notify();
    }

    fn pop(&self, prio: usize) -> Option<NetworkMessage> {
        let pq = &self.inner.queues[prio];
        let msg = pq
            .queue
            .lock()
            .expect("locking `TxQueue` should not fail")
            .pop_front();
        if msg.is_some() {
            // A slot was freed: wake an enqueuer blocked on backpressure.
            pq.space.notify_one();
        }
        msg
    }

    fn disable(&self) {
        self.inner.disabled.store(true, Ordering::Release);
        // Wake the drain worker and any enqueuers blocked on backpressure.
        for pq in &self.inner.queues {
            pq.space.notify_all();
        }
        let _ = self.notifier.notify();
    }
}

#[derive(Clone)]
pub(super) struct TransportLinkUnicastUniversal {
    // The underlying link
    pub(super) link: TransportLinkUnicast,
    // The transmission pipeline
    pub(super) pipeline: TransmissionPipelineProducer,
    // Bounded per-(link, priority) staging queue feeding the pipeline
    pub(super) tx_queue: TxQueue,
    // The task handling substruct
    task_controller: TaskController,
    #[cfg(feature = "stats")]
    pub(super) stats: zenoh_stats::LinkStats,
}

impl TransportLinkUnicastUniversal {
    pub(super) fn new(
        transport: &TransportUnicastUniversal,
        link: TransportLinkUnicast,
        priority_tx: &[TransportPriorityTx],
    ) -> (Self, TransmissionPipelineConsumer) {
        assert!(!priority_tx.is_empty());

        let config = TransmissionPipelineConf {
            batch: BatchConfig {
                mtu: link.config.batch.mtu,
                is_streamed: link.link.is_streamed(),
                #[cfg(feature = "transport_compression")]
                is_compression: link.config.batch.is_compression,
            },
            queue_size: transport.manager.config.queue_size,
            wait_before_drop: transport.manager.config.wait_before_drop,
            max_wait_before_drop_fragments: transport.manager.config.max_wait_before_drop_fragments,
            wait_before_close: transport.manager.config.wait_before_close,
            batching_enabled: transport.manager.config.batching,
            batching_time_limit: transport.manager.config.queue_backoff,
            queue_alloc: transport.manager.config.queue_alloc,
        };

        // The pipeline
        let (producer, consumer) =
            TransmissionPipeline::make(config, priority_tx, link.link.supports_priorities());

        // Use the complete src and dest locators including parameters
        #[cfg(feature = "stats")]
        let link_unicast = link.link();
        #[cfg(feature = "stats")]
        let stats = transport
            .stats
            .link_stats(&link_unicast.src, &link_unicast.dst);

        let result = Self {
            link,
            pipeline: producer,
            tx_queue: TxQueue::new(),
            task_controller: TaskController::default(),
            #[cfg(feature = "stats")]
            stats,
        };

        (result, consumer)
    }

    pub(super) fn start_tx(
        &mut self,
        transport: TransportUnicastUniversal,
        consumer: TransmissionPipelineConsumer,
        keep_alive: Duration,
    ) {
        // Spawn the per-link TxQueue drain worker on a dedicated thread.
        // It performs the (potentially blocking) push into the pipeline so that
        // a congested link only stalls its own worker, not the shared fan-out.
        {
            let tx_queue = self.tx_queue.clone();
            let pipeline = self.pipeline.clone();
            let drain_transport = transport.clone();
            #[cfg(feature = "stats")]
            let stats = self.stats.clone();
            std::thread::Builder::new()
                .name("zenoh-tx-queue".into())
                .spawn(move || {
                    tx_queue_drain_loop(
                        tx_queue,
                        pipeline,
                        drain_transport,
                        #[cfg(feature = "stats")]
                        stats,
                    )
                })
                .expect("spawning `zenoh-tx-queue` drain thread should not fail");
        }

        // Spawn the TX task
        let mut tx = self.link.tx();
        #[cfg(feature = "stats")]
        let stats = self.stats.clone();
        let ct = self.task_controller.get_cancellation_token();
        let task = async move {
            let res = tx_task(
                consumer,
                &mut tx,
                keep_alive,
                ct,
                #[cfg(feature = "stats")]
                stats,
            )
            .await;

            if let Err(e) = res {
                tracing::debug!("TX task failed: {}", e);
                // Spawn a task to avoid a deadlock waiting for this same task
                // to finish in the close() joining its handle
                // TODO(yuyuan): do more study to check which ZRuntime should be used or refine the
                // termination
                zenoh_runtime::ZRuntime::Net
                    .spawn(async move { transport.del_link(tx.inner.link()).await });
            }
        };
        self.task_controller
            .spawn_with_rt(zenoh_runtime::ZRuntime::TX, task);
    }

    pub(super) fn start_rx(&mut self, transport: TransportUnicastUniversal, lease: Duration) {
        let priorities = self.link.config.priorities.clone();
        let reliability = self.link.config.reliability;
        let mut rx = self.link.rx();
        let cancellation_token = self.task_controller.get_cancellation_token();
        #[cfg(feature = "stats")]
        let stats = self.stats.clone();
        let task = async move {
            // Start the consume task
            let res = cancellation_token
                .run_until_cancelled(rx_task(
                    &mut rx,
                    transport.clone(),
                    lease,
                    transport.manager.config.link_rx_buffer_size,
                    cancellation_token.clone(),
                    #[cfg(feature = "stats")]
                    stats,
                ))
                .await;

            // TODO(yuyuan): improve this callback
            if let Some(Err(e)) = res {
                // process error if task was not cancelled
                tracing::debug!("RX task failed: {}", e);

                // Spawn a task to avoid a deadlock waiting for this same task
                // to finish in the close() joining its handle
                // WARN: Must be spawned on RX

                zenoh_runtime::ZRuntime::RX.spawn(async move {
                    transport
                        .del_link(Link::new_unicast(&rx.link, priorities, reliability))
                        .await
                });

                // // WARN: This ZRuntime blocks
                // zenoh_runtime::ZRuntime::Net
                //     .spawn(async move { transport.del_link((&rx.link).into()).await });

                // // WARN: This cloud block
                // transport.del_link((&rx.link).into()).await;
            }
        };
        // WARN: If this is on ZRuntime::TX, a deadlock would occur.
        self.task_controller
            .spawn_with_rt(zenoh_runtime::ZRuntime::RX, task);
    }

    pub(super) async fn close(self) -> ZResult<()> {
        tracing::trace!("{}: closing", self.link);
        self.tx_queue.disable();
        self.task_controller.terminate_all_async().await;
        self.pipeline.disable();

        self.link.close(None).await
    }
}

/*************************************/
/*              TASKS                */
/*************************************/
/// Per-link drain worker: pops messages from the bounded per-priority
/// [`TxQueue`] in strict priority order (Control first) and pushes them into
/// the transmission pipeline. The push may block when the link is congested,
/// but it only blocks this dedicated worker thread, isolating other links.
fn tx_queue_drain_loop(
    tx_queue: TxQueue,
    pipeline: TransmissionPipelineProducer,
    transport: TransportUnicastUniversal,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) {
    loop {
        // Block until there is something to drain (or the queue is dropped).
        if tx_queue.waiter.wait().is_err() {
            break;
        }
        if tx_queue.inner.disabled.load(Ordering::Acquire) {
            break;
        }
        // Strict priority: index 0 (Control) is the highest priority.
        for prio in 0..Priority::NUM {
            while let Some(msg) = tx_queue.pop(prio) {
                match pipeline.push_network_message(msg.as_ref()) {
                    Ok(pushed) => transport.handle_push_result(
                        msg.as_ref(),
                        pushed,
                        #[cfg(feature = "stats")]
                        stats.clone(),
                    ),
                    // The transport has been closed: stop draining.
                    Err(_closed) => return,
                }
            }
        }
    }
}

async fn tx_task(
    pipeline: TransmissionPipelineConsumer,
    link: &mut TransportLinkUnicastTx,
    keep_alive: Duration,
    cancellation_token: CancellationToken,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) -> ZResult<()> {
    let keep_alive_tracker = TimeoutTracker::new(keep_alive);
    if link.inner.link.supports_priorities() {
        let (res, _, _) = select_all(pipeline.split().into_iter().map(|pipeline| {
            let mut link = link.clone();
            let cancellation_token = cancellation_token.clone();
            let keep_alive_tracker = keep_alive_tracker.clone();
            #[cfg(feature = "stats")]
            let stats = stats.clone();
            zenoh_runtime::ZRuntime::TX.spawn(async move {
                write_loop(
                    Some(pipeline.priority()),
                    pipeline,
                    &mut link,
                    keep_alive_tracker,
                    cancellation_token,
                    #[cfg(feature = "stats")]
                    stats,
                )
                .await
            })
        }))
        .await;
        res.unwrap()?;
    } else {
        write_loop(
            None,
            pipeline,
            link,
            keep_alive_tracker,
            cancellation_token,
            #[cfg(feature = "stats")]
            stats,
        )
        .await?;
    }
    Ok(())
}

async fn write_loop(
    write_priority: Option<Priority>,
    mut pipeline: impl PipelineConsumer,
    link: &mut TransportLinkUnicastTx,
    keep_alive_tracker: TimeoutTracker,
    cancellation_token: CancellationToken,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) -> ZResult<()> {
    let task = async {
        loop {
            tokio::select! {
                pull = pipeline.pull() => {
                    let Some((mut batch, priority)) = pull else {
                        // The queue has been disabled: break the tx loop, drain the queue, and exit
                        break
                    };
                    debug_assert!(write_priority.is_none() || write_priority == Some(priority));
                    link.send_batch(&mut batch, write_priority).await?;
                    // inform the latest message tracker that a message has been sent
                    keep_alive_tracker.reset();

                    #[cfg(feature = "stats")]
                    {
                        stats.inc_bytes(zenoh_stats::Tx, batch.len() as u64);
                        stats.inc_transport_message(zenoh_stats::Tx, batch.stats.t_msgs as u64);
                    }

                    // Reinsert the batch into the queue
                    pipeline.refill(batch, priority);
                },
                _ = keep_alive_tracker.wait_if(write_priority.unwrap_or(Priority::Control) == Priority::Control) => {
                    // A timeout occurred, no control/data messages have been sent during
                    // the keep_alive period, we need to send a KeepAlive message
                    let message: TransportMessage = KeepAlive.into();

                    #[allow(unused_variables)] // Used when stats feature is enabled
                    let n = link.send(&message, Some(Priority::Control)).await?;

                    #[cfg(feature = "stats")]
                    {
                        stats.inc_bytes(zenoh_stats::Tx, n as u64);
                        stats.inc_transport_message(zenoh_stats::Tx, 1);
                    }
                }
            }
        }
        ZResult::Ok(())
    };
    if let Some(result) = cancellation_token.run_until_cancelled(task).await {
        result?;
    }

    // Drain the transmission pipeline and write remaining bytes on the wire
    let mut batches = pipeline.drain();
    for (mut b, _) in batches.drain(..) {
        tokio::time::timeout(
            keep_alive_tracker.timeout(),
            link.send_batch(&mut b, write_priority),
        )
        .await
        .map_err(|_| {
            zerror!(
                "{link}: flush failed after {} ms",
                keep_alive_tracker.timeout().as_millis()
            )
        })??;

        #[cfg(feature = "stats")]
        {
            stats.inc_bytes(zenoh_stats::Tx, b.len() as u64);
            stats.inc_transport_message(zenoh_stats::Tx, b.stats.t_msgs as u64);
        }
    }

    Ok(())
}

async fn rx_task(
    link: &mut TransportLinkUnicastRx,
    transport: TransportUnicastUniversal,
    lease: Duration,
    rx_buffer_size: usize,
    cancellation_token: CancellationToken,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) -> ZResult<()> {
    // The pool of buffers
    let mtu = link.config.batch.mtu as usize;
    let mut n = rx_buffer_size / mtu;
    if n == 0 {
        tracing::debug!("RX configured buffer of {rx_buffer_size} bytes is too small for {link} that has an MTU of {mtu} bytes. Defaulting to {mtu} bytes for RX buffer.");
        n = 1;
    }
    let pool = RecyclingObjectPool::new(n, move || vec![0_u8; mtu].into_boxed_slice());

    let lease_tracker = TimeoutTracker::new(lease);
    if link.link.supports_priorities() {
        let (res, _, _) = select_all((Priority::MAX as u8..=Priority::MIN as u8).map(|prio| {
            let mut link = link.clone();
            let transport = transport.clone();
            let cancellation_token = cancellation_token.clone();
            let lease_tracker = lease_tracker.clone();
            #[cfg(feature = "stats")]
            let stats = stats.clone();
            let pool = pool.clone();
            zenoh_runtime::ZRuntime::RX.spawn(async move {
                cancellation_token
                    .run_until_cancelled(read_loop(
                        Some(Priority::try_from(prio).unwrap()),
                        &mut link,
                        transport,
                        lease_tracker,
                        #[cfg(feature = "stats")]
                        stats,
                        &pool,
                    ))
                    .await
            })
        }))
        .await;
        res.unwrap().transpose()?;
        Ok(())
    } else {
        read_loop(
            None,
            link,
            transport,
            lease_tracker,
            #[cfg(feature = "stats")]
            stats,
            &pool,
        )
        .await
    }
}

async fn read_loop<F: Fn() -> Box<[u8]>>(
    priority: Option<Priority>,
    link: &mut TransportLinkUnicastRx,
    transport: TransportUnicastUniversal,
    lease_tracker: TimeoutTracker,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
    pool: &RecyclingObjectPool<Box<[u8]>, F>,
) -> ZResult<()> {
    async fn read<F: Fn() -> Box<[u8]>>(
        link: &mut TransportLinkUnicastRx,
        priority: Option<Priority>,
        pool: &RecyclingObjectPool<Box<[u8]>, F>,
    ) -> ZResult<RBatch> {
        let batch = link
            .recv_batch(|| pool.try_take().unwrap_or_else(|| pool.alloc()), priority)
            .await?;
        Ok(batch)
    }

    let l = Link::new_unicast(
        &link.link,
        link.config.priorities.clone(),
        link.config.reliability,
    );
    loop {
        tokio::select! {
            batch = read(link, priority, pool) => {
                let batch = batch?;
                lease_tracker.reset();
                #[cfg(feature = "stats")]
                {
                    let header_bytes = if l.is_streamed { 2 } else { 0 };
                    stats.inc_bytes(zenoh_stats::Rx, header_bytes + batch.len() as u64);
                }
                transport.read_messages(batch, &l, #[cfg(feature = "stats")] &stats)?;
            }
            _ = lease_tracker.wait_if(priority.unwrap_or(Priority::Control) == Priority::Control) => {
                bail!("{link}: expired after {} milliseconds", lease_tracker.timeout().as_millis());
            }
        }
    }
}

struct TimeoutTrackerInner {
    timeout: Duration,
    waker: AtomicWaker,
    has_timed_out: AtomicBool,
    latest_reset: Mutex<Instant>,
    task: OnceLock<JoinHandle<()>>,
}

#[derive(Clone)]
struct TimeoutTracker(Arc<TimeoutTrackerInner>);

impl TimeoutTracker {
    fn new(timeout: Duration) -> TimeoutTracker {
        let now = Instant::now();
        let inner = Arc::new(TimeoutTrackerInner {
            timeout,
            waker: AtomicWaker::new(),
            has_timed_out: AtomicBool::new(false),
            latest_reset: Mutex::new(now),
            task: OnceLock::new(),
        });
        let tracker = Arc::downgrade(&inner);
        let task = tokio::spawn(async move {
            let mut latest_reset = now;
            loop {
                tokio::time::sleep_until((latest_reset + timeout).into()).await;
                let prev = latest_reset;
                let Some(tracker) = tracker.upgrade() else {
                    break;
                };
                latest_reset = *tracker.latest_reset.lock().unwrap();
                if latest_reset <= prev {
                    latest_reset = Instant::now();
                    tracker.has_timed_out.store(true, Ordering::Release);
                    tracker.waker.wake();
                }
            }
        });
        inner.task.set(task).unwrap();
        Self(inner)
    }

    fn timeout(&self) -> Duration {
        self.0.timeout
    }

    fn reset(&self) {
        *self.0.latest_reset.lock().unwrap() = Instant::now();
    }

    async fn wait_if(&self, predicate: bool) {
        poll_fn(|cx| {
            if !predicate {
                return Poll::Pending;
            }
            self.0.waker.register(cx.waker());
            if self.0.has_timed_out.load(Ordering::Acquire) {
                self.0.has_timed_out.store(false, Ordering::Release);
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await
    }
}

impl Drop for TimeoutTracker {
    fn drop(&mut self) {
        self.0.task.get().unwrap().abort();
    }
}
