use std::collections::BTreeMap;
use std::io;
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
use openraft::storage::RaftLogStorage;
use openraft::RaftLogReader;
use openraft::Raft;
use openraft::error::InitializeError;
use openraft::StoredMembership;
use openraft::{BasicNode, Config, Entry, EntryPayload, TokioRuntime};
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use crate::raft_proto::raft_client::RaftClient;
use crate::raft_proto::raft_server::Raft as RaftRpcTrait;
use crate::raft_proto::RaftBytes;
use crate::raft_state_machine::TrieStateMachine;
use crate::raft_storage::FileLogStore;
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

    // Minimal recovery: rebuild the in-memory trie by replaying committed Raft log entries
    // before starting the Raft state machine.
    //
    // This keeps the state machine free of persistence/path concerns (Phase 8 boundary cleanup),
    // while still allowing restart recovery without snapshots.
    let mut last_membership: StoredMembership<u64, BasicNode> = StoredMembership::default();
    if let Some(c) = committed {
        let ents = log_store.try_get_log_entries(0..=c.index).await?;
        for ent in ents {
            match ent.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(cmd) => {
                    store
                        .apply_word(&cmd.tenant, &cmd.word)
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                }
                EntryPayload::Membership(mem) => {
                    last_membership = StoredMembership::new(Some(ent.log_id), mem);
                }
            }
        }
    }

    let sm = TrieStateMachine::new(store, committed, last_membership);
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
                let init_ok = match raft_bg
                    .initialize(BTreeMap::from([(node_id, self_node.clone())]))
                    .await
                {
                    Ok(()) => true,
                    // On restart with persisted state, initialize is not allowed. That's fine: it means
                    // the cluster was already initialized earlier.
                    Err(openraft::error::RaftError::APIError(InitializeError::NotAllowed(_))) => true,
                    Err(_) => false,
                };

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
