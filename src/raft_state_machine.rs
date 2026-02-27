use std::io;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::LogId;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::{BasicNode, Entry, EntryPayload};
use tokio::sync::Mutex;

use crate::raft_node::{TrieRaftConfig, TrieResponse};
use crate::store::ShardStore;

pub struct TrieSnapshotBuilder;

impl openraft::RaftSnapshotBuilder<TrieRaftConfig> for TrieSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TrieRaftConfig>, StorageError<u64>> {
        // Snapshotting is disabled by config (SnapshotPolicy::Never). If it is triggered manually,
        // surface a clear error for now.
        Err(StorageError::from_io_error(
            openraft::ErrorSubject::Store,
            openraft::ErrorVerb::Read,
            io::Error::new(io::ErrorKind::Unsupported, "snapshot not implemented"),
        ))
    }
}

#[derive(Clone)]
pub struct TrieStateMachine {
    store: Arc<ShardStore>,
    inner: Arc<Mutex<StateMachineInner>>,
}

#[derive(Default)]
struct StateMachineInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

impl TrieStateMachine {
    pub fn new(
        store: Arc<ShardStore>,
        last_applied: Option<LogId<u64>>,
        last_membership: StoredMembership<u64, BasicNode>,
    ) -> Self {
        let mut inner = StateMachineInner::default();
        inner.last_applied = last_applied;
        inner.last_membership = last_membership;

        Self {
            store,
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl RaftStateMachine<TrieRaftConfig> for TrieStateMachine {
    type SnapshotBuilder = TrieSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let guard = self.inner.lock().await;
        Ok((guard.last_applied, guard.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<TrieResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TrieRaftConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut res = Vec::new();

        for ent in entries {
            let mut applied = false;
            match ent.payload {
                EntryPayload::Blank => {
                    // no-op
                }
                EntryPayload::Normal(cmd) => {
                    self.store
                        .apply_word(&cmd.tenant, &cmd.word)
                        .await
                        .map_err(|e| {
                            StorageError::from_io_error(
                                openraft::ErrorSubject::StateMachine,
                                openraft::ErrorVerb::Write,
                                io::Error::new(io::ErrorKind::Other, e.to_string()),
                            )
                        })?;
                    applied = true;
                }
                EntryPayload::Membership(mem) => {
                    let mut guard = self.inner.lock().await;
                    guard.last_membership = StoredMembership::new(Some(ent.log_id), mem);
                }
            }

            // Openraft expects exactly 1 reply for every entry (including Blank/Membership).
            res.push(TrieResponse { applied });

            let mut guard = self.inner.lock().await;
            guard.last_applied = Some(ent.log_id);
        }

        Ok(res)
    }

    async fn get_snapshot_builder(
        &mut self,
    ) -> <TrieStateMachine as RaftStateMachine<TrieRaftConfig>>::SnapshotBuilder {
        TrieSnapshotBuilder
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<tokio::fs::File>, StorageError<u64>> {
        // Snapshotting is disabled by config (SnapshotPolicy::Never) and unimplemented for now.
        Err(StorageError::from_io_error(
            openraft::ErrorSubject::Snapshot(None),
            openraft::ErrorVerb::Write,
            io::Error::new(io::ErrorKind::Unsupported, "snapshot not implemented"),
        ))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<u64>> {
        let mut guard = self.inner.lock().await;
        // We don't rebuild the trie from snapshot yet. Phase 5 will add real snapshotting.
        guard.last_applied = meta.last_log_id;
        guard.last_membership = meta.last_membership.clone();
        drop(snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TrieRaftConfig>>, StorageError<u64>> {
        Ok(None)
    }
}
