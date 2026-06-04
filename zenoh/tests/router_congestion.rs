#![cfg(feature = "unstable")]

//! Integration test that exercises Zenoh's `BlockFirst` congestion control
//! across a *router* (rather than between two directly connected peers).
//!
//! Topology (single process, wired as a star over TCP loopback so traffic is
//! actually relayed through the router):
//!
//! ```text
//!   fast_pub_session (client) ── 3 fast topics @~700Hz, 1KiB ─┐
//!                                                             ▼
//!   slow_pub_session (client) ── slow topic @~20Hz, 3MiB ──▶ router ──┬──▶ fast_sub (instant cbs)
//!                                                                      └──▶ slow_sub (5s cbs)
//! ```
//!
//! Both subscribers subscribe to *both* the fast (wildcard) and the slow topic.
//! The slow subscriber's blocking callbacks back its router-side egress up,
//! which (with `BlockFirst`) throttles the router's shared fan-out. The intent
//! of this test is to assert that the *fast* subscriber is **isolated** from the
//! slow one: it must receive ~all the fast messages that were produced.
//!
//! This is a RED-now / GREEN-after-fix harness for local development: with the
//! current library it fails (the fast subscriber loses a large share of
//! messages); once the router egress toward the slow subscriber is properly
//! isolated it should pass.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use zenoh::{qos::CongestionControl, Wait};
use zenoh_config::{Config, WhatAmI};
use zenoh_core::ztimeout;
use zenoh_test::get_free_tcp_port;

const TIMEOUT: Duration = Duration::from_secs(60);

// Three fast topics (one per publisher thread) plus one slow topic. The
// subscribers match the fast topics through a wildcard, so each extra fast
// publisher simply piles more load onto the same congested router pipeline.
const FAST_TOPIC_A: &str = "unittest/router_bottleneck/fast/a";
const FAST_TOPIC_B: &str = "unittest/router_bottleneck/fast/b";
const FAST_TOPIC_C: &str = "unittest/router_bottleneck/fast/c";
const FAST_TOPIC_PATTERN: &str = "unittest/router_bottleneck/fast/*";
const SLOW_TOPIC: &str = "unittest/router_bottleneck/slow";

// Fast & small vs. large & slow producers, per the scenario under test.
const FAST_PAYLOAD_BYTES: usize = 1024; // 1 KiB
const SLOW_PAYLOAD_BYTES: usize = 3 * 1024 * 1024; // 3 MiB
const FAST_PUBLISH_INTERVAL: Duration = Duration::from_micros(1429); // ~700 Hz per fast thread
const SLOW_PUBLISH_INTERVAL: Duration = Duration::from_millis(50); // ~20 Hz

// The slow subscriber drains far slower than the fast topic is produced, which
// is what fills its router-side egress queue and triggers `BlockFirst`.
const SLOW_SUBSCRIBER_SLEEP: Duration = Duration::from_secs(5);

// Total measurement window.
const TEST_DURATION: Duration = Duration::from_secs(5);
const CONNECTION_SLEEP: Duration = Duration::from_millis(300);

// Fraction of produced fast messages the fast subscriber must receive. The
// buggy behavior loses roughly half of them; a correct fix should deliver ~all.
const FAST_DELIVERY_THRESHOLD: f64 = 0.90;

fn router_config(port: u16) -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Router)).unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
        .listen
        .endpoints
        .set(vec![format!("tcp/127.0.0.1:{port}").parse().unwrap()])
        .unwrap();
    config
}

fn client_config(port: u16) -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Client)).unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
        .connect
        .endpoints
        .set(vec![format!("tcp/127.0.0.1:{port}").parse().unwrap()])
        .unwrap();
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn router_congestion_fast_sub_not_starved() {
    // Widen the zenoh RX runtime BEFORE any zenoh runtime is initialized. The
    // slow subscriber's callbacks block their RX thread for 5s; with the default
    // (2) RX workers they could starve the fast subscriber's link for the wrong
    // reason and pollute the result. `ZRuntime` reads this env once, lazily, on
    // first use, so it must be set before the first session opens.
    std::env::set_var("ZENOH_RUNTIME", "(rx: (worker_threads: 16))");

    let port = get_free_tcp_port();

    // Bring the router up first so the clients have something to connect to.
    let _router = ztimeout!(zenoh::open(router_config(port))).unwrap();
    tokio::time::sleep(CONNECTION_SLEEP).await;

    let slow_sub_session = ztimeout!(zenoh::open(client_config(port))).unwrap();
    let fast_sub_session = ztimeout!(zenoh::open(client_config(port))).unwrap();
    // Two separate publisher sessions => the fast topics and the slow topic reach
    // the router over distinct TCP ingress links (distinct RX read tasks).
    let fast_pub_session = ztimeout!(zenoh::open(client_config(port))).unwrap();
    let slow_pub_session = ztimeout!(zenoh::open(client_config(port))).unwrap();

    let fast_sub_fast = Arc::new(AtomicUsize::new(0));
    let fast_sub_slow = Arc::new(AtomicUsize::new(0));
    let slow_sub_fast = Arc::new(AtomicUsize::new(0));
    let slow_sub_slow = Arc::new(AtomicUsize::new(0));

    // Slow subscriber: very slow callbacks on *both* topics so its router-side
    // egress queues back up and force `BlockFirst` on the shared fan-out.
    let _slow_sub_fast = {
        let counter = slow_sub_fast.clone();
        ztimeout!(slow_sub_session
            .declare_subscriber(FAST_TOPIC_PATTERN)
            .callback(move |_sample| {
                thread::sleep(SLOW_SUBSCRIBER_SLEEP);
                counter.fetch_add(1, Ordering::Relaxed);
            }))
        .unwrap()
    };
    let _slow_sub_slow = {
        let counter = slow_sub_slow.clone();
        ztimeout!(slow_sub_session
            .declare_subscriber(SLOW_TOPIC)
            .callback(move |_sample| {
                thread::sleep(SLOW_SUBSCRIBER_SLEEP);
                counter.fetch_add(1, Ordering::Relaxed);
            }))
        .unwrap()
    };

    // Fast subscriber: instant, no-op callbacks on both topics.
    let _fast_sub_fast = {
        let counter = fast_sub_fast.clone();
        ztimeout!(fast_sub_session
            .declare_subscriber(FAST_TOPIC_PATTERN)
            .callback(move |_sample| {
                counter.fetch_add(1, Ordering::Relaxed);
            }))
        .unwrap()
    };
    let _fast_sub_slow = {
        let counter = fast_sub_slow.clone();
        ztimeout!(fast_sub_session
            .declare_subscriber(SLOW_TOPIC)
            .callback(move |_sample| {
                counter.fetch_add(1, Ordering::Relaxed);
            }))
        .unwrap()
    };

    // Let the star topology form (declarations propagate) before measuring.
    tokio::time::sleep(CONNECTION_SLEEP).await;

    let stop = Arc::new(AtomicBool::new(false));
    let produced_fast = Arc::new(AtomicUsize::new(0));
    let produced_slow = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Three fast publisher threads, one per fast topic. They reuse the
    // production `BlockFirst` congestion control so a policy change is caught.
    for topic in [FAST_TOPIC_A, FAST_TOPIC_B, FAST_TOPIC_C] {
        let session = fast_pub_session.clone();
        let stop = stop.clone();
        let produced = produced_fast.clone();
        handles.push(thread::spawn(move || {
            let publisher = session
                .declare_publisher(topic)
                .congestion_control(CongestionControl::BlockFirst)
                .wait()
                .unwrap();
            let payload = vec![b'f'; FAST_PAYLOAD_BYTES];
            while !stop.load(Ordering::Relaxed) {
                publisher.put(payload.clone()).wait().unwrap();
                produced.fetch_add(1, Ordering::Relaxed);
                thread::sleep(FAST_PUBLISH_INTERVAL);
            }
        }));
    }

    // One slow publisher thread (large payload, low rate).
    {
        let session = slow_pub_session.clone();
        let stop = stop.clone();
        let produced = produced_slow.clone();
        handles.push(thread::spawn(move || {
            let publisher = session
                .declare_publisher(SLOW_TOPIC)
                .congestion_control(CongestionControl::BlockFirst)
                .wait()
                .unwrap();
            let payload = vec![b's'; SLOW_PAYLOAD_BYTES];
            while !stop.load(Ordering::Relaxed) {
                publisher.put(payload.clone()).wait().unwrap();
                produced.fetch_add(1, Ordering::Relaxed);
                thread::sleep(SLOW_PUBLISH_INTERVAL);
            }
        }));
    }

    tokio::time::sleep(TEST_DURATION).await;
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.join();
    }

    // Let the fast subscriber drain whatever is still on the wire.
    tokio::time::sleep(CONNECTION_SLEEP).await;

    let produced_fast_v = produced_fast.load(Ordering::Relaxed);
    let produced_slow_v = produced_slow.load(Ordering::Relaxed);
    let fast_fast_v = fast_sub_fast.load(Ordering::Relaxed);
    let fast_slow_v = fast_sub_slow.load(Ordering::Relaxed);
    let slow_fast_v = slow_sub_fast.load(Ordering::Relaxed);
    let slow_slow_v = slow_sub_slow.load(Ordering::Relaxed);

    // Report measured numbers so the regime (loss vs. latency) is visible.
    eprintln!(
        "[router-bottleneck] produced fast={produced_fast_v} slow={produced_slow_v} | \
         fast-sub fast={fast_fast_v} slow={fast_slow_v} | \
         slow-sub fast={slow_fast_v} slow={slow_slow_v}"
    );

    let required = (produced_fast_v as f64 * FAST_DELIVERY_THRESHOLD) as usize;
    assert!(
        fast_fast_v >= required,
        "fast subscriber was starved by the slow subscriber's congestion: received \
         {fast_fast_v}/{produced_fast_v} fast messages (required >= {required}, i.e. {:.0}%). \
         The router egress toward the slow subscriber is not isolated.",
        FAST_DELIVERY_THRESHOLD * 100.0
    );
}
