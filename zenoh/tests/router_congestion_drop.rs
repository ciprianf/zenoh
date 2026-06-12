#![cfg(feature = "unstable")]

//! Subscriber-isolation tests for `Drop` congestion control through a router.
//!
//! The scenario under investigation: a single publisher publishes a single
//! topic through a router; one *fast* subscriber consumes instantly. We want
//! to know whether a (later-added) *slow* subscriber on the same topic can
//! steal delivery from the fast subscriber even though the publisher uses
//! `CongestionControl::Drop` (which is supposed to shed, not block).
//!
//! Topology (single process, wired over TCP loopback so traffic is actually
//! relayed through the router):
//!
//! ```text
//!   pub (client) ─ 512KiB @ 50Hz, Drop ─▶ router ─┬─▶ fast_sub (instant cb)
//!                                                 └─▶ [throttling proxy] ─▶ slow_sub
//! ```
//!
//! Two cases:
//!   * **baseline**: no slow subscriber — the fast subscriber must receive
//!     ~all of the published messages.
//!   * **slow link**: the slow subscriber sits behind an in-process TCP proxy
//!     that rate-caps the router→sub direction. Unlike a hard stall (which
//!     trips the router's eager-drop fast path and *releases* the shared
//!     ingress task), a slow-but-*progressing* link keeps each fragmented
//!     message advancing within the drop deadline, so the router's shared
//!     ingress task stays busy relaying it. Because that same task also drains
//!     the publisher's link and fans out to the fast subscriber, it backpressures
//!     the publisher (whose `Drop` policy then sheds) and starves the fast
//!     subscriber. The slow subscriber meanwhile stays *alive* (~40% delivery),
//!     proving it is genuinely slow rather than dead. This is the **fate-sharing**
//!     we want to surface.
//!
//! Why a throttling TCP proxy instead of a slow callback? The sibling
//! `router_congestion_block_first.rs` reproduces fate-sharing with a trivial
//! 5 s blocking callback, because `BlockFirst` is non-droppable: a hard stall
//! makes the transport *block* the shared fan-out, which is exactly the
//! mechanism under test. `Drop` is the opposite — it has an eager-drop fast
//! path that *detects* a fully stalled link (the `congested` flag latches once
//! a push overruns the drop deadline) and from then on sheds to it *instantly*,
//! **releasing** the shared ingress task. So a hard stall under `Drop` would
//! actually *protect* the fast subscriber, hiding the bug. To keep the shared
//! task busy — and thus fate-share onto the fast subscriber — the slow link
//! must stay *just* fast enough that every fragment keeps making progress
//! within the growing per-fragment drop deadline, so the `congested` flag never
//! latches and the eager-drop path is never taken. A blocking callback cannot
//! express "slow but progressing"; only a rate-limited byte pipe can, hence the
//! in-process proxy that paces the router→sub direction at a fixed bytes/sec.
//!
//! Note on the numbers: the publisher is paced against an absolute clock, so
//! the bounded per-`put` backpressure (≤ the ~50 ms drop deadline, well under
//! the 20 ms interval) is absorbed by catch-up and `produced` stays ≈ nominal
//! (250). The fast subscriber's shortfall therefore comes from `Drop`-shedding
//! at the congested shared egress, *not* from the publisher emitting fewer
//! messages — `Drop` `put()`s that shed still return `Ok` and count as produced.
//!
//! The payload is intentionally larger than the transport batch size (65535)
//! so each message is *fragmented* — this matters for the slow-subscriber case
//! where the per-fragment drop deadline is what blocks the shared router task.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
};
use zenoh::{qos::CongestionControl, Wait};
use zenoh_config::{Config, WhatAmI};
use zenoh_core::ztimeout;
use zenoh_test::get_free_tcp_port;

const TIMEOUT: Duration = Duration::from_secs(60);

// Single topic shared by the publisher and the subscriber(s).
const TOPIC: &str = "unittest/router_congestion_drop/data";

// 512 KiB: not only larger than batch_size (65535) so every message is
// fragmented, but also comfortably larger than the per-priority transmission
// pipeline depth (2 batches ~= 128 KiB). That matters for the slow-subscriber
// case: a message must overflow the pipeline *mid-fragmentation* to pay the
// full `max_wait_before_drop_fragments` (~50 ms) deadline on the shared router
// ingress task. A payload that merely equals the pipeline depth bounces on its
// first fragment after only `wait_before_drop` (~1 ms) and never blocks long
// enough to backpressure the publisher.
const PAYLOAD_BYTES: usize = 512 * 1024;

// Publish cadence and measurement window: 50 Hz for 5 s => 250 messages.
const PUBLISH_INTERVAL: Duration = Duration::from_millis(20);
const TEST_DURATION: Duration = Duration::from_secs(5);
const CONNECTION_SLEEP: Duration = Duration::from_millis(300);

// Fraction of the (computed) nominal count the fast subscriber must receive.
const DELIVERY_THRESHOLD: f64 = 0.90;

// --- Slow-link slow-subscriber knobs (second test) --------------------------
//
// The slow subscriber is reached through an in-process TCP proxy that rate-caps
// the router→sub direction. The cap is chosen so each ~512 KiB message keeps
// making fragment progress (a batch drains in < the growing per-fragment
// drop-deadline step), so the router's congested flag never latches and the
// eager-drop fast path is never taken — yet relaying a single message eats most
// of the ~50 ms drop-deadline budget, so the shared router ingress task falls
// behind the 50 Hz publisher and the fast subscriber is starved. At ~16 MiB/s
// the slow subscriber still receives a meaningful fraction (~40 %), proving it
// is genuinely *slow* rather than dead, while the fast subscriber — on a
// separate, healthy link — is dragged down with it.
const SLOW_LINK_RATE: f64 = 16.0 * 1024.0 * 1024.0; // bytes per second
// Forward a batch-ish chunk at a time. Too small (e.g. 4 KiB) trickles each
// fragment so slowly that every message overruns the ~50 ms deadline and is
// dropped incompletely (the slow sub then reassembles nothing); too large
// (e.g. 64 KiB) delivers whole batches so promptly that the egress never backs
// up and the fast sub is unharmed. 16 KiB is the band where the slow sub stays
// alive *and* the fast sub is starved.
const PROXY_CHUNK: usize = 16 * 1024;
// Small receive buffer on the proxy's router-facing socket so the router can't
// stockpile megabytes in our socket (TCP rx autotuning would otherwise hide the
// egress backpressure we're trying to create).
const PROXY_RCVBUF: u32 = 16 * 1024;
// Pin a small `so_sndbuf` on the router listener to disable send-buffer
// autotuning so the router→proxy egress pipeline actually fills. Shared with
// the fast subscriber's link, but that link never congests (the fast sub drains
// continuously and over loopback a small buffer doesn't cap throughput), so
// it's harmless there.
const ROUTER_SNDBUF: u32 = 16 * 1024;
// Tight publisher->router path: with large (autotuned) buffers the router's
// ingress stall while it is busy relaying to the slow link is simply absorbed
// (the publisher buffers ahead and nothing is dropped), so the fast subscriber
// sees only a small delay. Shrinking the router's receive buffer and the
// publisher's send buffer makes the stall backpressure the publisher, whose
// `Drop` policy then sheds — and those shed messages never reach the fast
// subscriber either.
const ROUTER_RCVBUF: u32 = 16 * 1024;
const PUB_SNDBUF: u32 = 16 * 1024;

// A `put()` that takes longer than this is counted as "backpressured": with
// `Drop` the only thing that can block the publisher is the per-fragment drop
// deadline (up to ~50 ms) while the router's ingress task is held up fanning
// out to the congested slow-sub egress and is therefore not draining the
// publisher's link.
const SLOW_PUT: Duration = Duration::from_millis(5);

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

/// Like [`client_config`] but pins a small TCP send buffer on the link, so the
/// publisher cannot buffer ahead while the router's ingress task is stalled.
fn pub_client_config(port: u16, sndbuf: u32) -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Client)).unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
        .connect
        .endpoints
        .set(vec![format!("tcp/127.0.0.1:{port}#so_sndbuf={sndbuf}")
            .parse()
            .unwrap()])
        .unwrap();
    config
}

/// Like [`router_config`] but pins small TCP buffers on the listener. The small
/// send buffer disables send-buffer autotuning so a stalled subscriber's egress
/// pipeline fills (and the congested flag latches). The small receive buffer
/// keeps the router from stockpiling publisher messages while its ingress task
/// is stalled, so the backpressure reaches the publisher. Shared with the fast
/// subscriber's link, which is harmless since that link never congests.
fn throttled_router_config(port: u16, sndbuf: u32, rcvbuf: u32) -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Router)).unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
        .listen
        .endpoints
        .set(vec![format!(
            "tcp/127.0.0.1:{port}#so_sndbuf={sndbuf};so_rcvbuf={rcvbuf}"
        )
        .parse()
        .unwrap()])
        .unwrap();
    config
}

/// Spawns an in-process TCP proxy that simulates a *slow but progressing* link
/// to the slow subscriber.
///
/// The slow subscriber connects to the returned proxy port; the proxy dials the
/// router on `router_port` and forwards bytes both ways. The router→sub
/// direction (the egress data) is rate-capped with a finely-paced clock so the
/// link makes continuous progress (never a hard stall, which would re-trip the
/// router's eager-drop fast path). The sub→router direction (tiny control
/// traffic) is forwarded freely. The task self-terminates when either side
/// closes at teardown.
async fn spawn_throttling_proxy(router_port: u16) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        // Wait for the slow subscriber to connect to us.
        let (downstream, _) = listener.accept().await.unwrap();

        // Connect upstream to the router with a small receive buffer so the
        // router cannot stockpile megabytes in our socket.
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(PROXY_RCVBUF).ok();
        let router_addr = format!("127.0.0.1:{router_port}").parse().unwrap();
        let upstream = socket.connect(router_addr).await.unwrap();

        downstream.set_nodelay(true).ok();
        upstream.set_nodelay(true).ok();

        let (mut down_rd, mut down_wr) = downstream.into_split();
        let (mut up_rd, mut up_wr) = upstream.into_split();

        // sub -> router: control traffic, forward unthrottled.
        let s2r = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut down_rd, &mut up_wr).await;
        });

        // router -> sub: rate-capped with a paced clock.
        let r2s = tokio::spawn(async move {
            let mut buf = [0u8; PROXY_CHUNK];
            let mut next = tokio::time::Instant::now();
            loop {
                let n = match up_rd.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if down_wr.write_all(&buf[..n]).await.is_err() {
                    break;
                }
                // This chunk is "allowed" to take n / SLOW_LINK_RATE seconds.
                next += Duration::from_secs_f64(n as f64 / SLOW_LINK_RATE);
                let now = tokio::time::Instant::now();
                if next > now {
                    tokio::time::sleep_until(next).await;
                } else {
                    // Fell behind; don't accumulate debt.
                    next = now;
                }
            }
        });

        let _ = tokio::join!(s2r, r2s);
    });

    proxy_port
}

/// Baseline: with no slow subscriber, a `Drop` publisher driving 512 KiB
/// messages at 50 Hz must deliver ~all of them to the fast subscriber.
///
/// Expected to **PASS**: nothing congests the router, so `Drop` never sheds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn router_congestion_drop() {
    // Widen the zenoh RX runtime before any zenoh runtime is initialized, so
    // the (later) slow subscriber's blocking callbacks can't starve the fast
    // path for the wrong reason. `ZRuntime` reads this once, lazily, on first
    // use, so it must be set before the first session opens.
    std::env::set_var("ZENOH_RUNTIME", "(rx: (worker_threads: 16))");

    let port = get_free_tcp_port();

    // Bring the router up first so the clients have something to connect to.
    let _router = ztimeout!(zenoh::open(router_config(port))).unwrap();
    tokio::time::sleep(CONNECTION_SLEEP).await;

    let sub_session = ztimeout!(zenoh::open(client_config(port))).unwrap();
    let pub_session = ztimeout!(zenoh::open(client_config(port))).unwrap();

    // Fast subscriber: instant, no-op callback that just counts deliveries.
    let delivered = Arc::new(AtomicUsize::new(0));
    let _fast_sub = {
        let counter = delivered.clone();
        ztimeout!(sub_session
            .declare_subscriber(TOPIC)
            .callback(move |_sample| {
                counter.fetch_add(1, Ordering::Relaxed);
            }))
        .unwrap()
    };

    // Let the declarations propagate through the router before measuring.
    tokio::time::sleep(CONNECTION_SLEEP).await;

    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicUsize::new(0));

    // Single publisher thread, paced against an absolute clock so the "50 Hz"
    // intent is faithful even if a `put()` occasionally stalls (catch-up keeps
    // the average rate on schedule).
    let publisher_handle = {
        let session = pub_session.clone();
        let stop = stop.clone();
        let produced = produced.clone();
        thread::spawn(move || {
            let publisher = session
                .declare_publisher(TOPIC)
                .congestion_control(CongestionControl::Drop)
                .wait()
                .unwrap();
            let payload = vec![0u8; PAYLOAD_BYTES];
            let start = Instant::now();
            let mut tick: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let target = start + PUBLISH_INTERVAL * tick as u32;
                let now = Instant::now();
                if target > now {
                    thread::sleep(target - now);
                }
                publisher.put(payload.clone()).wait().unwrap();
                produced.fetch_add(1, Ordering::Relaxed);
                tick += 1;
            }
        })
    };

    tokio::time::sleep(TEST_DURATION).await;
    stop.store(true, Ordering::Relaxed);
    let _ = publisher_handle.join();

    // Let the subscriber drain whatever is still on the wire.
    tokio::time::sleep(CONNECTION_SLEEP).await;

    let nominal = (TEST_DURATION.as_secs_f64() / PUBLISH_INTERVAL.as_secs_f64()).round() as usize;
    let produced_v = produced.load(Ordering::Relaxed);
    let delivered_v = delivered.load(Ordering::Relaxed);

    eprintln!(
        "[drop-baseline] nominal={nominal} produced={produced_v} delivered={delivered_v}"
    );

    let required = (nominal as f64 * DELIVERY_THRESHOLD) as usize;
    assert!(
        produced_v >= required,
        "publisher did not sustain its target rate: produced {produced_v} \
         (required >= {required} of nominal {nominal})"
    );
    assert!(
        delivered_v >= required,
        "fast subscriber did not receive the published messages: got \
         {delivered_v}/{produced_v} (required >= {required}, i.e. {:.0}% of nominal {nominal})",
        DELIVERY_THRESHOLD * 100.0
    );
}

/// Same publisher and fast subscriber as the baseline, plus a *slow* subscriber
/// reached through an in-process throttling proxy (a rate-capped router→sub
/// link).
///
/// Unlike a hard stall — which trips the router's eager-drop fast path (the
/// congested flag latches after the ~ms onset and every subsequent push returns
/// instantly, *releasing* the shared ingress task) — a slow-but-progressing
/// link keeps each fragmented 512 KiB message advancing within the per-fragment
/// drop deadline. The congested flag never latches, so the router's shared
/// ingress task stays busy relaying that one message for up to the ~50 ms
/// deadline budget. Since that same task also reads from the publisher and fans
/// out to the fast subscriber, it falls behind the 50 Hz publisher, the
/// publisher is backpressured into `Drop`-shedding, and the fast subscriber is
/// starved.
///
/// This is the **fate-sharing** we want to surface: the test asserts the fast
/// subscriber stays isolated (>= 90% delivery), which is expected to **FAIL on
/// `main`** until the router stops letting one slow subscriber block the shared
/// egress path.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn router_congestion_drop_slow_subscriber() {
    std::env::set_var("ZENOH_RUNTIME", "(rx: (worker_threads: 16))");

    let port = get_free_tcp_port();

    let _router =
        ztimeout!(zenoh::open(throttled_router_config(port, ROUTER_SNDBUF, ROUTER_RCVBUF))).unwrap();
    tokio::time::sleep(CONNECTION_SLEEP).await;

    // Insert the slow link in front of the slow subscriber.
    let proxy_port = spawn_throttling_proxy(port).await;

    let fast_sub_session = ztimeout!(zenoh::open(client_config(port))).unwrap();
    let slow_sub_session = ztimeout!(zenoh::open(client_config(proxy_port))).unwrap();
    let pub_session = ztimeout!(zenoh::open(pub_client_config(port, PUB_SNDBUF))).unwrap();

    // Fast subscriber: instant, no-op callback that just counts deliveries.
    let delivered = Arc::new(AtomicUsize::new(0));
    let _fast_sub = {
        let counter = delivered.clone();
        ztimeout!(fast_sub_session
            .declare_subscriber(TOPIC)
            .callback(move |_sample| {
                counter.fetch_add(1, Ordering::Relaxed);
            }))
        .unwrap()
    };

    // Slow subscriber: plain instant counter — the slowness lives entirely in
    // the rate-capped proxy link, not in the callback.
    let slow_delivered = Arc::new(AtomicUsize::new(0));
    let _slow_sub = {
        let counter = slow_delivered.clone();
        ztimeout!(slow_sub_session
            .declare_subscriber(TOPIC)
            .callback(move |_sample| {
                counter.fetch_add(1, Ordering::Relaxed);
            }))
        .unwrap()
    };

    tokio::time::sleep(CONNECTION_SLEEP).await;

    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicUsize::new(0));
    // Publisher backpressure instrumentation.
    let slow_puts = Arc::new(AtomicUsize::new(0));
    let max_put_us = Arc::new(AtomicU64::new(0));
    let total_put_us = Arc::new(AtomicU64::new(0));

    let publisher_handle = {
        let session = pub_session.clone();
        let stop = stop.clone();
        let produced = produced.clone();
        let slow_puts = slow_puts.clone();
        let max_put_us = max_put_us.clone();
        let total_put_us = total_put_us.clone();
        thread::spawn(move || {
            let publisher = session
                .declare_publisher(TOPIC)
                .congestion_control(CongestionControl::Drop)
                .wait()
                .unwrap();
            let payload = vec![0u8; PAYLOAD_BYTES];
            let start = Instant::now();
            let mut tick: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let target = start + PUBLISH_INTERVAL * tick as u32;
                let now = Instant::now();
                if target > now {
                    thread::sleep(target - now);
                }
                let put_start = Instant::now();
                publisher.put(payload.clone()).wait().unwrap();
                let put_elapsed = put_start.elapsed();
                let put_us = put_elapsed.as_micros() as u64;
                total_put_us.fetch_add(put_us, Ordering::Relaxed);
                max_put_us.fetch_max(put_us, Ordering::Relaxed);
                if put_elapsed >= SLOW_PUT {
                    slow_puts.fetch_add(1, Ordering::Relaxed);
                }
                produced.fetch_add(1, Ordering::Relaxed);
                tick += 1;
            }
        })
    };

    tokio::time::sleep(TEST_DURATION).await;
    stop.store(true, Ordering::Relaxed);
    let _ = publisher_handle.join();

    tokio::time::sleep(CONNECTION_SLEEP).await;

    let nominal = (TEST_DURATION.as_secs_f64() / PUBLISH_INTERVAL.as_secs_f64()).round() as usize;
    let produced_v = produced.load(Ordering::Relaxed);
    let delivered_v = delivered.load(Ordering::Relaxed);
    let slow_delivered_v = slow_delivered.load(Ordering::Relaxed);
    let slow_puts_v = slow_puts.load(Ordering::Relaxed);
    let max_put_ms = max_put_us.load(Ordering::Relaxed) as f64 / 1000.0;
    let avg_put_ms = if produced_v > 0 {
        total_put_us.load(Ordering::Relaxed) as f64 / produced_v as f64 / 1000.0
    } else {
        0.0
    };

    eprintln!(
        "[drop-slow] nominal={nominal} produced={produced_v} \
         fast-delivered={delivered_v} slow-delivered={slow_delivered_v} \
         slow-puts={slow_puts_v} max-put={max_put_ms:.1}ms avg-put={avg_put_ms:.2}ms"
    );

    let required = (nominal as f64 * DELIVERY_THRESHOLD) as usize;
    assert!(
        delivered_v >= required,
        "fast subscriber was starved by the slow-link slow subscriber: got \
         {delivered_v}/{produced_v} fast messages (required >= {required}, i.e. {:.0}% of \
         nominal {nominal}). The router egress toward the slow subscriber is not isolated.",
        DELIVERY_THRESHOLD * 100.0
    );
}
