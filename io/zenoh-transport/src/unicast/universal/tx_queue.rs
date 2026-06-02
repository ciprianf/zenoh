//
// Copyright (c) 2024 ZettaScale Technology
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
    sync::{Condvar, Mutex},
};

use zenoh_protocol::{core::Priority, network::NetworkMessage};

/// Per-priority capacity (in messages) of each per-link egress queue.
///
/// This bounds how many messages can be buffered per priority before the
/// drop-oldest policy starts evicting the staleest message. It is a simple
/// tunable for the prototype.
const CAPACITY_PER_PRIORITY: usize = 64;

struct Inner {
    /// One queue per priority, indexed by `priority as usize`
    /// (`0` = `Control` = highest, `NUM - 1` = `Background` = lowest).
    queues: Box<[VecDeque<NetworkMessage>]>,
    closed: bool,
}

/// A set of bounded, per-priority egress queues with a drop-oldest overflow
/// policy.
///
/// Multiple producers (the shared ingress tasks) enqueue messages without ever
/// blocking: when a priority queue is full, the oldest message is evicted to
/// make room for the new one. A single consumer (the per-link drain task)
/// dequeues messages highest-priority-first and feeds them into the
/// transmission pipeline.
///
/// Because each destination link owns its own [`LinkTxQueues`], a slow link can
/// only ever fill (and drop from) its own queues; it can no longer apply
/// back-pressure onto the ingress task and stall delivery to other links.
pub(super) struct LinkTxQueues {
    inner: Mutex<Inner>,
    not_empty: Condvar,
    capacity: usize,
}

impl LinkTxQueues {
    pub(super) fn new() -> Self {
        Self::with_capacity(CAPACITY_PER_PRIORITY)
    }

    pub(super) fn with_capacity(capacity: usize) -> Self {
        let queues = (0..Priority::NUM)
            .map(|_| VecDeque::with_capacity(capacity))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            inner: Mutex::new(Inner {
                queues,
                closed: false,
            }),
            not_empty: Condvar::new(),
            capacity,
        }
    }

    /// Enqueue `msg` at the given `priority`. Never blocks.
    ///
    /// If the target priority queue is already full, the oldest message in that
    /// queue is evicted and returned so the caller can account for the drop.
    pub(super) fn push_drop_oldest(
        &self,
        priority: Priority,
        msg: NetworkMessage,
    ) -> Option<NetworkMessage> {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            // The link is closing: the message cannot be enqueued, so report it
            // as evicted.
            return Some(msg);
        }
        let queue = &mut inner.queues[priority as usize];
        let evicted = if queue.len() >= self.capacity {
            queue.pop_front()
        } else {
            None
        };
        queue.push_back(msg);
        drop(inner);
        self.not_empty.notify_one();
        evicted
    }

    /// Dequeue the highest-priority available message, blocking until one is
    /// available or the queue is closed.
    ///
    /// Returns `None` once the queue has been closed and fully drained.
    pub(super) fn pop_blocking(&self) -> Option<NetworkMessage> {
        let mut inner = self.inner.lock().unwrap();
        loop {
            // Strict priority: index 0 (`Control`) is served before index
            // `NUM - 1` (`Background`).
            for queue in inner.queues.iter_mut() {
                if let Some(msg) = queue.pop_front() {
                    return Some(msg);
                }
            }
            if inner.closed {
                return None;
            }
            inner = self.not_empty.wait(inner).unwrap();
        }
    }

    /// Mark the queue as closed and wake the consumer so it can drain any
    /// remaining messages and then exit.
    pub(super) fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        drop(inner);
        self.not_empty.notify_all();
    }
}
