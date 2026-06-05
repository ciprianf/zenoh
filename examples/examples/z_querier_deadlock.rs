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
//! Minimal reproduction of a deadlock when a `Querier` is dropped while a query
//! is still pending and its *completion* callback re-enters the `Session`.
//!
//! The querier destructor removes the pending query while holding the session
//! lock; dropping the query runs the completion callback, which calls back into
//! the `Session` (`put`) and tries to re-acquire the same lock -> deadlock.

use std::{sync::Mutex, thread, time::Duration};

use zenoh::{handlers::CallbackDrop, query::Query, Wait};

#[tokio::main]
async fn main() {
    // Watchdog: abort if we hang.
    thread::spawn(|| {
        thread::sleep(Duration::from_secs(10));
        eprintln!("!!! DEADLOCK: did not finish in 10s !!!");
        std::process::abort();
    });

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Queryable that keeps queries pending forever (never replies), so the
    // query is still in flight when the querier is dropped.
    static HELD: Mutex<Vec<Query>> = Mutex::new(Vec::new());
    let _queryable = session
        .declare_queryable("demo/example/deadlock")
        .callback(|q| HELD.lock().unwrap().push(q))
        .background()
        .await
        .unwrap();

    let querier = session.declare_querier("demo/example/deadlock").await.unwrap();

    // The query's completion callback re-enters the Session.
    let session2 = session.clone();
    querier
        .get()
        .with(CallbackDrop {
            callback: |_reply: zenoh::query::Reply| {},
            drop: move || {
                session2.put("demo/example/log", "done").wait().unwrap();
            },
        })
        .await
        .unwrap();

    // Drop the querier while the query is pending -> deadlock.
    println!("Dropping querier...");
    drop(querier);
    println!("No deadlock.");
}
