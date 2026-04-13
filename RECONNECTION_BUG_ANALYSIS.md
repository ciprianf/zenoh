# Zenoh Router Reconnection Bug Analysis

## Problem

Two zenoh routers (A and B) deployed on separate machines connected via WiFi. Router A has `connect.endpoints` pointing to Router B using a hostname. Router B has no connect endpoints pointing to A. When WiFi drops and reconnects, approximately 1 in 100 times the routers permanently fail to reconnect.

# Reproducing the issue

In one terminal run router B:

`cd demo && RUST_LOG=zenoh::net::runtime::orchestrator=debug cargo run --bin router_b`

In the second terminal run router A:

`cd demo && RUST_LOG=zenoh::net::runtime::orchestrator=debug cargo run --bin router_a`

After router A starts, kill router B fast (within 5s window) by pressing `Ctrl+C`

Wait 5 seconds and then start router B again. The router B never gets any message. Permanent disconnect.

## Root Cause

A race condition in `zenoh/src/net/runtime/orchestrator.rs` in the `peers_connector_retry` function (line ~798).

### Background: How Reconnection Works

1. When a link dies and it's the last link, the transport is deleted
2. The `closed_session` callback fires, checks `RuntimeSession.endpoints` for configured endpoints
3. If endpoints are found, it spawns a one-shot `peers_connector_retry` task with exponential backoff (1s -> 2s -> 4s, infinite retries for routers)
4. When the retry succeeds, it registers the endpoint in the **new** `RuntimeSession.endpoints` so future disconnects can trigger another retry
5. The retry task exits

### The Race

`open_transport_unicast` returns a `TransportUnicast` which holds a **Weak reference** to the transport. On success, the code tries to register the endpoint:

```rust
Ok(transport) => {
    if let Ok(Some(orch_transport)) = transport.get_callback() {
        // insert endpoint into RuntimeSession.endpoints
    }
    // consider it a success, exit loop
}
```

`transport.get_callback()` can fail in two ways:
- **`Err`**: The Weak reference can't upgrade because the transport was deleted from the TransportManager's HashMap (it no longer exists in memory)
- **`Ok(None)`**: The transport still exists but `delete()` already called `self.callback.take()`, removing the callback

Both happen when the remote side immediately closes the connection (e.g., WiFi still flaky, or B's old transport cleanup races with the new connection). The remote sends a Close message, and on the local side the RX task spawns `delete()` on a different tokio worker thread, which races with the retry loop on the original thread.

### Why It's Permanent

1. The retry task considers `open_transport_unicast` returning `Ok` as success and **exits**
2. The endpoint was never registered in the new `RuntimeSession.endpoints`
3. When `delete()` calls `closed_session()` on the new RuntimeSession, it sees empty endpoints and returns immediately (line 1221-1222)
4. `closed_link()` never works for hostname-based endpoints because it compares the link's resolved IP against the configured hostname string
5. Router B has no connect config, so it never retries from its side
6. **No reconnection task is ever spawned again**

### Additional Factor: Hostname vs IP Mismatch

`closed_link()` (line 1250) compares `link.dst.to_endpoint()` (resolved IP like `tcp/192.168.1.5:7447`) against the configured endpoints (hostname like `tcp/router-b:7447`). These never match, so `closed_link` is always a no-op in hostname configurations. All reconnection goes through `closed_session`, which works correctly except for the race described above.

## Fix

File: `zenoh/src/net/runtime/orchestrator.rs`, in `peers_connector_retry`.

Track whether the endpoint was actually registered. If `get_callback()` fails (transport died before registration), treat it as a connection failure and push a retry back into the queue instead of exiting.

```rust
Ok(transport) => {
    let mut registered = false;
    if let Ok(Some(orch_transport)) = transport.get_callback() {
        if let Some(orch_transport) = orch_transport
            .as_any()
            .downcast_ref::<super::RuntimeSession>()
        {
            zwrite!(orch_transport.endpoints).insert(peer.clone());
            registered = true;
        }
    }
    if registered {
        // genuine success
    } else {
        // transport died immediately, retry with backoff
    }
}
```

The fix has been applied to the local working copy and compiles cleanly.

## How to Verify

Run with `RUST_LOG="zenoh::net::runtime::orchestrator=debug,zenoh_transport=debug"` and look for:
- `"Successfully connected to configured peer"` followed by `"Connected to configured peer ... but transport was immediately closed"` — confirms the race was hit and the retry kicked in
- Before the fix: `"Successfully connected"` with no subsequent retry and permanent disconnection
