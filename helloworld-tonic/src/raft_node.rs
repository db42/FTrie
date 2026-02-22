use std::collections::BTreeMap;
use std::io;
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

#[derive(Clone)]
pub struct MemLogStore {
    inner: Arc<Mutex<MemLogInner>>,
}

#[derive(Default)]
struct MemLogInner {
    vote: Option<Vote<u64>>,
    last_purged: Option<LogId<u64>>,
    logs: BTreeMap<u64, Entry<TrieRaftConfig>>,
}

impl MemLogStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemLogInner::default())),
        }
    }
}

impl RaftLogReader<TrieRaftConfig> for MemLogStore {
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

impl RaftLogStorage<TrieRaftConfig> for MemLogStore {
    type LogReader = MemLogStore;

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

    async fn get_log_reader(&mut self) -> <MemLogStore as RaftLogStorage<TrieRaftConfig>>::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut guard = self.inner.lock().await;
        guard.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let guard = self.inner.lock().await;
        Ok(guard.vote.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TrieRaftConfig>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TrieRaftConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut guard = self.inner.lock().await;
        for ent in entries {
            guard.logs.insert(ent.log_id.index, ent);
        }
        // In-memory: treat as flushed immediately.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut guard = self.inner.lock().await;
        let start = log_id.index;
        let keys: Vec<u64> = guard.logs.range(start..).map(|(k, _)| *k).collect();
        for k in keys {
            guard.logs.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut guard = self.inner.lock().await;
        let end = log_id.index;
        let keys: Vec<u64> = guard.logs.range(..=end).map(|(k, _)| *k).collect();
        for k in keys {
            guard.logs.remove(&k);
        }
        guard.last_purged = Some(log_id);
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
    pub fn new(store: Arc<ShardStore>, data_dir: String) -> Self {
        Self {
            store,
            inner: Arc::new(Mutex::new(StateMachineInner::default())),
            data_dir,
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

    let log_store = MemLogStore::new();
    let sm = TrieStateMachine::new(store, data_dir);
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
