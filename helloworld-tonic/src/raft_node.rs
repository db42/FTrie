use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use openraft::SnapshotPolicy;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::LogId;
use openraft::LogState;
use openraft::Raft;
use openraft::RaftLogReader;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::{BasicNode, Config, Entry, EntryPayload, TokioRuntime};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::raft_proto::raft_client::RaftClient;
use crate::raft_proto::raft_server::Raft as RaftRpcTrait;
use crate::raft_proto::RaftBytes;
use crate::store::ShardStore;

openraft::declare_raft_types!(
    pub TrieRaftConfig:
        D = TrieCommand,
        R = TrieResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<Self>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = TokioRuntime,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrieCommand {
    pub tenant: String,
    pub word: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrieResponse {
    pub applied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LogRecord {
    Entry(Entry<TrieRaftConfig>),
    PurgeTo(LogId<u64>),
    TruncateFrom(LogId<u64>),
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
        // OpenRaft may call `purge()` as an optimization; we treat it as a no-op to keep the
        // full log available across restarts.
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
                LogRecord::PurgeTo(_p) => {
                    // No-op until we implement snapshots/compaction.
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
        for (idx, ent) in guard.logs.range(range) {
            let _ = idx;
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
            .map_err(|e| {
                StorageError::from_io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e)
            })?;
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
        // If we were to persist purges, a restart would require a snapshot to rebuild state.
        let _ = log_id;
        Ok(())
    }
}

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
                    LogRecord::PurgeTo(_p) => {
                        // No-op until we implement snapshots/compaction.
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

pub struct GrpcRaftNetworkFactory;

impl openraft::network::RaftNetworkFactory<TrieRaftConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        GrpcRaftNetwork {
            target,
            addr: node.addr.clone(),
            client: None,
        }
    }
}

pub struct GrpcRaftNetwork {
    target: u64,
    addr: String,
    client: Option<RaftClient<tonic::transport::Channel>>,
}

impl GrpcRaftNetwork {
    async fn client(&mut self) -> Result<&mut RaftClient<tonic::transport::Channel>, NetworkError> {
        if self.client.is_none() {
            let c = RaftClient::connect(self.addr.clone())
                .await
                .map_err(|e| NetworkError::new(&e))?;
            self.client = Some(c);
        }
        Ok(self.client.as_mut().unwrap())
    }

    fn ser<T: Serialize>(&self, v: &T) -> Result<Vec<u8>, NetworkError> {
        bincode::serialize(v).map_err(|e| NetworkError::new(&e))
    }

    fn de<T: for<'de> Deserialize<'de>>(
        &self,
        bytes: &[u8],
    ) -> Result<T, NetworkError> {
        bincode::deserialize(bytes).map_err(|e| NetworkError::new(&e))
    }
}

impl openraft::network::RaftNetwork<TrieRaftConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TrieRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = self.ser(&rpc).map_err(RPCError::Network)?;
        let req = Request::new(RaftBytes { payload });
        let resp = self
            .client()
            .await
            .map_err(RPCError::Network)?
            .append_entries(req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();
        self.de(&resp.payload).map_err(RPCError::Network)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TrieRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        let payload = self.ser(&rpc).map_err(RPCError::Network)?;
        let req = Request::new(RaftBytes { payload });
        let resp = self
            .client()
            .await
            .map_err(RPCError::Network)?
            .install_snapshot(req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();
        self.de(&resp.payload).map_err(RPCError::Network)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = self.ser(&rpc).map_err(RPCError::Network)?;
        let req = Request::new(RaftBytes { payload });
        let resp = self
            .client()
            .await
            .map_err(RPCError::Network)?
            .vote(req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();
        self.de(&resp.payload).map_err(RPCError::Network)
    }
}

pub struct RaftRpc {
    raft: Raft<TrieRaftConfig>,
}

impl RaftRpc {
    pub fn new(raft: Raft<TrieRaftConfig>) -> Self {
        Self { raft }
    }

    fn ser<T: Serialize>(v: &T) -> Result<Vec<u8>, Status> {
        bincode::serialize(v).map_err(|e| Status::internal(format!("serialize failed: {}", e)))
    }

    fn de<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Status> {
        bincode::deserialize(bytes).map_err(|e| Status::internal(format!("deserialize failed: {}", e)))
    }
}

#[tonic::async_trait]
impl RaftRpcTrait for RaftRpc {
    async fn append_entries(&self, request: Request<RaftBytes>) -> Result<Response<RaftBytes>, Status> {
        let rpc: AppendEntriesRequest<TrieRaftConfig> = Self::de(&request.into_inner().payload)?;
        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(format!("append_entries failed: {}", e)))?;
        Ok(Response::new(RaftBytes { payload: Self::ser(&resp)? }))
    }

    async fn vote(&self, request: Request<RaftBytes>) -> Result<Response<RaftBytes>, Status> {
        let rpc: VoteRequest<u64> = Self::de(&request.into_inner().payload)?;
        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(format!("vote failed: {}", e)))?;
        Ok(Response::new(RaftBytes { payload: Self::ser(&resp)? }))
    }

    async fn install_snapshot(&self, request: Request<RaftBytes>) -> Result<Response<RaftBytes>, Status> {
        let rpc: InstallSnapshotRequest<TrieRaftConfig> = Self::de(&request.into_inner().payload)?;
        let resp = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|e| Status::internal(format!("install_snapshot failed: {}", e)))?;
        Ok(Response::new(RaftBytes { payload: Self::ser(&resp)? }))
    }
}

pub async fn build_raft(
    node_id: u64,
    store: Arc<ShardStore>,
    data_dir: String,
    members: BTreeMap<u64, BasicNode>,
    bootstrap: bool,
) -> Result<(Raft<TrieRaftConfig>, Arc<AtomicBool>), Box<dyn std::error::Error>> {
    let mut cfg = Config::default();
    cfg.snapshot_policy = SnapshotPolicy::Never;
    let cfg = Arc::new(cfg.validate()?);

    let raft_fsync = std::env::var("RAFT_FSYNC").ok().as_deref() == Some("1");
    let mut log_store = FileLogStore::open(&data_dir, raft_fsync).await?;
    let committed = log_store.read_committed().await?;
    let sm = TrieStateMachine::new(store, data_dir, committed).await?;
    let net = GrpcRaftNetworkFactory;

    let raft = Raft::new(node_id, cfg, net, log_store, sm).await?;

    // Bootstrap is a control-plane operation; we do it in the background so we don't deadlock
    // startup (peers also need to start serving their Raft RPCs).
    let bootstrapped = Arc::new(AtomicBool::new(!bootstrap));
    if bootstrap && !members.is_empty() {
        let raft_bg = raft.clone();
        let members_bg = members.clone();
        let bootstrapped_bg = bootstrapped.clone();

        tokio::spawn(async move {
            let self_node = members_bg
                .get(&node_id)
                .cloned()
                .unwrap_or_else(|| BasicNode::new(""));

            let mut attempt: u64 = 0;
            loop {
                attempt += 1;

                // 1) Initialize as a single-node cluster (self as voter).
                let init_ok = raft_bg
                    .initialize(BTreeMap::from([(node_id, self_node.clone())]))
                    .await
                    .is_ok();

                // 2) Add peers as learners.
                let mut ok = init_ok;
                for (id, node) in members_bg.iter() {
                    if *id == node_id {
                        continue;
                    }
                    if raft_bg.add_learner(*id, node.clone(), true).await.is_err() {
                        ok = false;
                    }
                }

                // 3) Promote all learners to voters.
                let voter_ids: Vec<u64> = members_bg.keys().copied().collect();
                if raft_bg.change_membership(voter_ids, true).await.is_err() {
                    ok = false;
                }

                if ok {
                    bootstrapped_bg.store(true, Ordering::SeqCst);
                    println!(
                        "raft bootstrap complete: node_id={} voters={}",
                        node_id,
                        members_bg.len()
                    );
                    break;
                }

                if attempt % 20 == 0 {
                    println!(
                        "raft bootstrap still retrying: node_id={} attempt={}",
                        node_id, attempt
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
    }

    Ok((raft, bootstrapped))
}
