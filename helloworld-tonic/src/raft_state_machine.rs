use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::path::PathBuf;
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
use crate::raft_storage::LogRecord;
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
    data_dir: String,
}

#[derive(Default)]
struct StateMachineInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

impl TrieStateMachine {
    pub async fn new(
        store: Arc<ShardStore>,
        data_dir: String,
        committed: Option<LogId<u64>>,
    ) -> Result<Self, io::Error> {
        let raft_dir = PathBuf::from(&data_dir).join("raft");
        tokio::fs::create_dir_all(&raft_dir).await?;
        let log_path = raft_dir.join("log.aof");

        // Rebuild the in-memory trie by replaying committed entries from the Raft log.
        //
        // This is required because our trie is in-memory only and we haven't implemented snapshots yet.
        // We only apply up to the last persisted committed log id.
        let limit = committed.map(|c| c.index).unwrap_or(0);
        let mut last_membership: StoredMembership<u64, BasicNode> = StoredMembership::default();

        if limit > 0 {
            let mut logs: BTreeMap<u64, Entry<TrieRaftConfig>> = BTreeMap::new();

            let log_bytes = match tokio::fs::read(&log_path).await {
                Ok(d) => d,
                Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(e),
            };

            let mut cur = std::io::Cursor::new(log_bytes);
            loop {
                let mut len_buf = [0u8; 4];
                if let Err(e) = cur.read_exact(&mut len_buf) {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(e);
                }
                let len = u32::from_le_bytes(len_buf) as usize;
                let pos = cur.position() as usize;
                let end = pos.saturating_add(len);
                let buf = cur.get_ref();
                if end > buf.len() {
                    break;
                }
                let bytes = buf[pos..end].to_vec();
                cur.set_position(end as u64);

                let rec: LogRecord = match bincode::deserialize(&bytes) {
                    Ok(v) => v,
                    Err(_) => break,
                };

                match rec {
                    LogRecord::Entry(ent) => {
                        if ent.log_id.index > limit {
                            continue;
                        }
                        logs.insert(ent.log_id.index, ent);
                    }
                    LogRecord::TruncateFrom(t) => {
                        let start = t.index;
                        let keys: Vec<u64> = logs.range(start..).map(|(k, _)| *k).collect();
                        for k in keys {
                            logs.remove(&k);
                        }
                    }
                }
            }

            for (_idx, ent) in logs.iter() {
                match &ent.payload {
                    EntryPayload::Blank => {}
                    EntryPayload::Normal(cmd) => {
                        // Ignore errors here, but surface them as IO error; without applying we can't be correct.
                        store
                            .apply_word(&cmd.tenant, &cmd.word)
                            .await
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    }
                    EntryPayload::Membership(mem) => {
                        last_membership = StoredMembership::new(Some(ent.log_id), mem.clone());
                    }
                }
            }
        }

        let mut inner = StateMachineInner::default();
        inner.last_applied = committed;
        inner.last_membership = last_membership;

        Ok(Self {
            store,
            inner: Arc::new(Mutex::new(inner)),
            data_dir,
        })
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
        let path = PathBuf::from(&self.data_dir).join("raft-receiving.snapshot");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    e,
                )
            })?;
        Ok(Box::new(file))
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

