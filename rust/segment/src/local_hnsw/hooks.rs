//! A test hook of the applies to a local HNSW index: an apply waits before it takes the lock of the index, after the
//! backfill that runs it has committed the same records to SQLite, so that a test reads while SQLite holds records that
//! the index does not have yet.

use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// A wait of an apply: `reached` when the apply comes to it, which then goes on at `go`; `done` once the apply released
/// the lock of the index. A hold is taken once: the apply that reaches it removes it.
#[derive(Debug, Default)]
pub struct Hold {
    pub reached: Notify,
    pub go: Notify,
    pub done: Notify,
}

static HOLDS: Mutex<Vec<(String, Arc<Hold>)>> = Mutex::new(Vec::new());

/// A hold of the next apply whose records include the user id given.
pub fn hold_apply_of(user_id: impl Into<String>) -> Arc<Hold> {
    let hold = Arc::new(Hold::default());
    HOLDS.lock().unwrap().push((user_id.into(), hold.clone()));
    hold
}

/// Notifies `done` of its hold when dropped.
pub(crate) struct Done(Arc<Hold>);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.done.notify_one();
    }
}

/// The wait of an apply before the lock of the index, when a hold is set for one of the user ids of its records.
pub(crate) async fn hold_apply<'a>(ids: impl IntoIterator<Item = &'a str>) -> Option<Done> {
    let hold = {
        let mut holds = HOLDS.lock().unwrap();
        if holds.is_empty() {
            return None;
        }
        let ids = ids.into_iter().collect::<std::collections::HashSet<_>>();
        let position = holds.iter().position(|(id, _)| ids.contains(id.as_str()))?;
        holds.remove(position).1
    };
    hold.reached.notify_one();
    hold.go.notified().await;
    Some(Done(hold))
}
