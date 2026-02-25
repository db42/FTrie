use tonic::{transport::Server, Request, Response, Status};

use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest, PutWordReply, PutWordRequest};

mod indexer;
mod fst_index;
mod partition;
mod store;
mod raft_node;
mod raft_storage;
mod raft_state_machine;
mod flags;

use partition::{env_or_default, PrefixRange};
use tonic_health::server::health_reporter;
use store::ShardStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;
// GRPC server implementation for prefix search service.
// 
// This module implements a GRPC server that provides prefix-based word search functionality
// using the trie data structure. The server supports multiple tenants, with each tenant
// having its own word dictionary and trie.
// 
// # Server Implementation
// 
// The server exposes a single GRPC endpoint `SayHello` that accepts:
// - A prefix string to search for
// - A tenant ID to determine which dictionary to search in
// 
// And returns:
// - A list of words from the tenant's dictionary that match the given prefix
// 
// # Example Usage
// 
// ```bash
// # Start the server
// cargo run --bin helloworld-server
// 
// # Make a GRPC request
// grpcurl -plaintext -import-path ./proto -proto helloworld.proto \
//   -d '{"name": "apr", "tenant": "thoughtspot"}' \
//   '[::1]:50051' helloworld.Greeter/SayHello
// ```
// 
// # Implementation Details
// 
// - Uses Tonic for GRPC server implementation
// - Maintains separate tries per tenant using the `Indexer` 
// - Loads word dictionaries from files at startup
// - Supports concurrent requests across tenants


pub mod hello_world {
    tonic::include_proto!("helloworld");
}

pub mod raft_proto {
    tonic::include_proto!("raft");
}

pub struct MyGreeter {
    node_id: String,
    include_node_id_in_reply: bool,
    include_message_in_reply: bool,
    default_top_k: u32,
    store: Arc<ShardStore>,
    raft: openraft::Raft<raft_node::TrieRaftConfig>,
    raft_bootstrapped: Arc<std::sync::atomic::AtomicBool>,
}

#[tonic::async_trait]
impl Greeter for MyGreeter {

    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let inner = request.into_inner();
        let prefix = inner.name.trim().to_ascii_lowercase();
        let tenant = inner.tenant;
        // `top_k` is proto3 scalar, so if the client omits it we see `0`.
        // Keep backward-compatible behavior: default is unlimited unless DEFAULT_TOP_K is set.
        let effective_top_k = if inner.top_k == 0 {
            self.default_top_k
        } else {
            inner.top_k
        };
        let matches = self
            .store
            .prefix_match_top_k(&tenant, &prefix, effective_top_k)
            .await?;

        let node_suffix = if self.include_node_id_in_reply {
            format!(" node={}", self.node_id)
        } else {
            "".to_string()
        };
        let message = if self.include_message_in_reply {
            format!(
                "Hello {}{} matches={} top_k={}",
                prefix,
                node_suffix,
                matches.len(),
                if effective_top_k == 0 {
                    -1i32
                } else {
                    effective_top_k as i32
                }
            )
        } else {
            "".to_string()
        };
        let reply = HelloReply {
            message,
            matches,
            node_id: if self.include_node_id_in_reply {
                self.node_id.clone()
            } else {
                "".to_string()
            },
        };

        Ok(Response::new(reply))
    }

    async fn put_word(
        &self,
        request: Request<PutWordRequest>,
    ) -> Result<Response<PutWordReply>, Status> {
        let inner = request.into_inner();
        let tenant = inner.tenant;
        if !self.raft_bootstrapped.load(Ordering::SeqCst) {
            return Err(Status::unavailable("raft bootstrapping"));
        }
        let cmd = raft_node::TrieCommand {
            tenant: tenant.clone(),
            word: inner.word.clone(),
        };
        match self.raft.client_write(cmd).await {
            Ok(_resp) => {}
            Err(e) => {
                if let openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::ForwardToLeader(f),
                ) =
                    e
                {
                    let leader = f.leader_node.map(|n| n.addr).unwrap_or_default();
                    // Return a structured leader hint for the LB in metadata.
                    let mut md = tonic::metadata::MetadataMap::new();
                    if !leader.is_empty() {
                        if let Ok(v) = tonic::metadata::MetadataValue::try_from(leader.clone()) {
                            md.insert("x-raft-leader", v);
                        }
                    }
                    return Err(Status::with_metadata(
                        tonic::Code::FailedPrecondition,
                        "not leader",
                        md,
                    ));
                }
                return Err(Status::unavailable(format!("raft write failed: {}", e)));
            }
        }

        Ok(Response::new(PutWordReply {
            applied: true,
            message: "ok".to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env_or_default("BIND_ADDR", "[::]:50051");
    let addr = bind_addr.parse()?;
    let prefix_range = env_or_default("PREFIX_RANGE", "a-z");
    let prefix_range =
        PrefixRange::parse(&prefix_range).unwrap_or_else(|e| panic!("invalid PREFIX_RANGE: {}", e));
    let node_id = env_or_default("NODE_ID", "node");
    let include_node_id_in_reply = env_or_default("INCLUDE_NODE_ID_IN_REPLY", "0") == "1";
    let include_message_in_reply = env_or_default("INCLUDE_MESSAGE", "1") == "1";
    let default_top_k = env_or_default("DEFAULT_TOP_K", "0").parse::<u32>().unwrap_or(0);
    let data_dir = env_or_default("DATA_DIR", "./data");
    let raft_enabled = flags::is_raft_enabled();
    if !raft_enabled {
        return Err("RAFT_ENABLED=0 is not supported; this binary requires Raft mode".into());
    }

    let store = Arc::new(ShardStore::open(prefix_range).await?);

    let raft_node_id: u64 = env_or_default("RAFT_NODE_ID", "1").parse().unwrap_or(1);
    let raft_bootstrap = env_or_default("RAFT_BOOTSTRAP", "0") == "1";

    let mut members = std::collections::BTreeMap::new();
    let m = env_or_default("RAFT_MEMBERS", "");
    // Format: "1=http://127.0.0.1:50051,2=http://127.0.0.1:50053"
    for part in m.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let Some((id_str, addr)) = part.split_once('=') else {
            continue;
        };
        if let Ok(id) = id_str.trim().parse::<u64>() {
            members.insert(id, openraft::BasicNode::new(addr.trim()));
        }
    }
    let (raft, raft_bootstrapped) = raft_node::build_raft(
        raft_node_id,
        store.clone(),
        data_dir.clone(),
        members,
        raft_bootstrap,
    )
    .await?;

    let greeter = MyGreeter {
        node_id: node_id.clone(),
        include_node_id_in_reply,
        include_message_in_reply,
        default_top_k,
        store: store.clone(),
        raft: raft.clone(),
        raft_bootstrapped: raft_bootstrapped.clone(),
    };

    println!(
        "Server configured: NODE_ID={} BIND_ADDR={} PREFIX_RANGE={}-{} DATA_DIR={} INDEX_BACKEND={} INCLUDE_MESSAGE={} DEFAULT_TOP_K={}",
        node_id,
        bind_addr,
        greeter.store.prefix_range().start,
        greeter.store.prefix_range().end,
        data_dir,
        greeter.store.backend_name(),
        if include_message_in_reply { 1 } else { 0 },
        default_top_k
    );

    let (mut reporter, health_service) = health_reporter();
    reporter
        .set_serving::<GreeterServer<MyGreeter>>()
        .await;

    let raft_rpc = raft_node::RaftRpc::new(raft);
    Server::builder()
        .add_service(health_service)
        .add_service(raft_proto::raft_server::RaftServer::new(raft_rpc))
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
