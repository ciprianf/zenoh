# Deadlock: dropping a `Querier` with a pending query whose completion callback re-enters the `Session`

## Summary
Dropping a `Querier` (or calling `Querier::undeclare()`) while one of its queries is still in flight deadlocks if the query's handler has a **completion/drop callback** that calls back into the same `Session`.

The querier destructor removes the pending query **while holding the session `state` write lock**. This drops the query's callback inline, which runs the completion callback, and that callback tries to re-acquire the same lock on the same thread → self-deadlock (`std::sync::RwLock` is non-reentrant).

## Environment
- Zenoh: `main` (crate version `1.9.0`)
- OS: Linux

## Where the problem is
`Session::undeclare_querier_inner` in `zenoh/src/api/session.rs`:

```rust
pub(crate) fn undeclare_querier_inner(&self, querier_id: Id) -> ZResult<()> {
    let mut state = zwrite!(self.0.state);              // (1) WRITE lock held
    ...
    if let Some(querier_state) = state.queriers.remove(&querier_id) {
        // (2) drops each pending query's Callback while the lock is held,
        //     which runs the completion (drop) handler inline
        state.queries.retain(|_, q| q.querier_id != Some(querier_id));
        ...
    }
}
```

The completion callback then calls e.g. `Session::put` → `resolve_put`, which does
`let state = zread!(self.0.state);` — re-acquiring the lock already held at (1).

Relevant locations:
- Lock acquired: `zenoh/src/api/session.rs` — `undeclare_querier_inner`, `zwrite!(self.0.state)`
- Callback dropped under lock: same fn, `state.queries.retain(...)`
- Re-entrant acquire: `zenoh/src/api/session.rs` — `resolve_put`, `zread!(self.0.state)`
- Completion-callback machinery: `zenoh/src/api/handlers/callback.rs` — `CallbackDrop`'s `Drop` impl

This is specific to the **drop/completion** path. The normal **reply** path is safe
because it explicitly `drop(state)` before invoking `callback.call(...)`.

## Why a pending query is required
The deadlock only triggers when the query's callback is dropped *while the lock is held*.
That happens exclusively when the destructor removes a **still-pending** query at (2).
If the query already completed, its callback was dropped earlier (outside the lock), so
there is no re-entrancy. On a standalone session a query with no matching queryables
finalizes immediately, so the reproduction uses a queryable that keeps the query pending.

## Minimal reproduction
- A queryable that stashes incoming `Query` objects and never replies (keeps a query pending).
- A querier issuing `.get()` with a `CallbackDrop { callback, drop }` whose `drop` calls
  `session.put(...).wait()`.
- `drop(querier)` → hang.

```rust
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
```

Result: the program hangs on `drop(querier)`; the watchdog aborts after 10s (exit code 134).

## Suggested fix
Don't drop user callbacks while holding the `state` lock. In `undeclare_querier_inner`,
move the removed queries out under the lock, then drop them after releasing it:

```rust
let mut state = zwrite!(self.0.state);
...
// Collect queries to remove instead of dropping them under the lock.
let to_remove: Vec<_> = state
    .queries
    .keys()
    .filter(|qid| state.queries[qid].querier_id == Some(querier_id))
    .copied()
    .collect();
let removed: Vec<_> = to_remove
    .iter()
    .filter_map(|qid| state.queries.remove(qid))
    .collect();

// ... rest of the logic ...

drop(state);    // release the lock first
drop(removed);  // now run completion callbacks safely
```

The same audit should be applied anywhere a user-provided callback may be dropped while a
Zenoh internal lock is held (e.g. `close`/session-drop paths that flush `state.queries`).

A complementary hardening option is to document that completion/drop callbacks must not
re-enter the `Session`, but releasing the lock before dropping callbacks is the robust fix.
