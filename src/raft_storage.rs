use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::Vote;
use openraft::Entry;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::raft_node::TrieRaftConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum LogRecord {
    Entry(Entry<TrieRaftConfig>),
    TruncateFrom(LogId<u64>),
    // NOTE: Purge is intentionally unsupported (no snapshots/compaction yet).
}

async fn aof_append_record<T: Serialize>(path: &PathBuf, v: &T, fsync: bool) -> Result<(), io::Error> {
    let bytes = bincode::serialize(v).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let len = bytes.len() as u32;

    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    f.write_all(&len.to_le_bytes()).await?;
    f.write_all(&bytes).await?;
    f.flush().await?;
    if fsync {
        f.sync_all().await?;
    }
    Ok(())
}

async fn aof_read_last_record<T: for<'de> Deserialize<'de>>(path: &PathBuf) -> Result<Option<T>, io::Error> {
    let data = match tokio::fs::read(path).await {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut cur = std::io::Cursor::new(data);
    let mut last: Option<T> = None;
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
        match bincode::deserialize::<T>(&bytes) {
            Ok(v) => last = Some(v),
            Err(_) => break,
        }
    }
    Ok(last)
}

#[derive(Clone)]
pub struct FileLogStore {
    inner: Arc<Mutex<FileLogInner>>,
    log_path: PathBuf,
    vote_path: PathBuf,
    committed_path: PathBuf,
    fsync: bool,
}

#[derive(Default)]
struct FileLogInner {
    vote: Option<Vote<u64>>,
    last_purged: Option<LogId<u64>>,
    committed: Option<LogId<u64>>,
    logs: BTreeMap<u64, Entry<TrieRaftConfig>>,
}

impl FileLogStore {
    pub async fn open(data_dir: &str, fsync: bool) -> Result<Self, io::Error> {
        let raft_dir = PathBuf::from(data_dir).join("raft");
        tokio::fs::create_dir_all(&raft_dir).await?;

        let log_path = raft_dir.join("log.aof");
        let vote_path = raft_dir.join("vote.aof");
        let committed_path = raft_dir.join("committed.aof");

        let vote: Option<Vote<u64>> = aof_read_last_record(&vote_path).await?;
        let committed_raw: Option<LogId<u64>> = aof_read_last_record(&committed_path).await?;

        // NOTE: We intentionally do NOT support log purging yet (no snapshots/compaction).
        let last_purged: Option<LogId<u64>> = None;
        let mut logs: BTreeMap<u64, Entry<TrieRaftConfig>> = BTreeMap::new();

        // Replay log records to rebuild the latest view.
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

        // Clamp committed to an index that actually exists in our local log.
        // This avoids a "hole" on startup if the process was killed mid-write.
        let committed: Option<LogId<u64>> = match committed_raw {
            None => None,
            Some(c) => logs.range(..=c.index).next_back().map(|(_, e)| e.log_id),
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(FileLogInner {
                vote,
                last_purged,
                committed,
                logs,
            })),
            log_path,
            vote_path,
            committed_path,
            fsync,
        })
    }
}

impl RaftLogReader<TrieRaftConfig> for FileLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TrieRaftConfig>>, StorageError<u64>> {
        let guard = self.inner.lock().await;
        let mut out = Vec::new();
        for (_idx, ent) in guard.logs.range(range) {
            out.push(ent.clone());
        }
        Ok(out)
    }
}

impl RaftLogStorage<TrieRaftConfig> for FileLogStore {
    type LogReader = FileLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TrieRaftConfig>, StorageError<u64>> {
        let guard = self.inner.lock().await;
        let last_log_id = guard
            .logs
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(guard.last_purged);
        Ok(LogState {
            last_purged_log_id: guard.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> <FileLogStore as RaftLogStorage<TrieRaftConfig>>::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        aof_append_record(&self.vote_path, vote, self.fsync)
            .await
            .map_err(|e| StorageError::from_io_error(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Write, e))?;

        let mut guard = self.inner.lock().await;
        guard.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let guard = self.inner.lock().await;
        Ok(guard.vote.clone())
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        aof_append_record(&self.committed_path, &committed, self.fsync)
            .await
            .map_err(|e| StorageError::from_io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e))?;
        let mut guard = self.inner.lock().await;
        guard.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let guard = self.inner.lock().await;
        Ok(guard.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TrieRaftConfig>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TrieRaftConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut to_persist = Vec::new();
        let mut inserted_idx = Vec::new();
        {
            let mut guard = self.inner.lock().await;
            for ent in entries {
                inserted_idx.push(ent.log_id.index);
                guard.logs.insert(ent.log_id.index, ent.clone());
                to_persist.push(LogRecord::Entry(ent));
            }
        }

        let persist_io: Result<(), io::Error> = async {
            for rec in &to_persist {
                aof_append_record(&self.log_path, rec, self.fsync).await?;
            }
            Ok(())
        }
        .await;

        match &persist_io {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => callback.log_io_completed(Err(io::Error::new(e.kind(), e.to_string()))),
        }

        match persist_io {
            Ok(()) => Ok(()),
            Err(e) => {
                // Best-effort rollback of in-memory view when persistence fails.
                let mut guard = self.inner.lock().await;
                for idx in inserted_idx {
                    guard.logs.remove(&idx);
                }
                Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    e,
                ))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut guard = self.inner.lock().await;
            let start = log_id.index;
            let keys: Vec<u64> = guard.logs.range(start..).map(|(k, _)| *k).collect();
            for k in keys {
                guard.logs.remove(&k);
            }
        }

        aof_append_record(&self.log_path, &LogRecord::TruncateFrom(log_id), self.fsync)
            .await
            .map_err(|e| StorageError::from_io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // No-op until we implement snapshots/compaction.
        let _ = log_id;
        Ok(())
    }
}
