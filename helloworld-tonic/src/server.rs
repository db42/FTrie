use tonic::{transport::Server, Request, Response, Status};

use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest, PutWordReply, PutWordRequest};

mod indexer;
mod partition;
mod wal;

use indexer::Indexer;
use partition::{env_or_default, PrefixRange};
use tonic_health::server::health_reporter;
use tokio::sync::RwLock;
use tokio::fs;
use wal::{append_word, ensure_dir, replay_words, wal_path};
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

pub struct MyGreeter {
    indexer: RwLock<Indexer>,
    node_id: String,
    include_node_id_in_reply: bool,
    prefix_range: PrefixRange,
    data_dir: String,
    fsync: bool,
}

fn normalize_and_validate_word(s: &str) -> Result<String, Status> {
    let w = s.trim().to_ascii_lowercase();
    if w.is_empty() {
        return Err(Status::invalid_argument("word must be non-empty"));
    }
    if !w.chars().all(|c| ('a'..='z').contains(&c)) {
        return Err(Status::invalid_argument("word must contain only a-z"));
    }
    Ok(w)
}

fn normalize_and_validate_prefix(s: &str) -> Result<String, Status> {
    let p = s.trim().to_ascii_lowercase();
    if p.is_empty() {
        return Err(Status::invalid_argument("prefix must be non-empty"));
    }
    if !p.chars().all(|c| ('a'..='z').contains(&c)) {
        return Err(Status::invalid_argument("prefix must contain only a-z"));
    }
    Ok(p)
}

impl MyGreeter {
    async fn new(
        node_id: String,
        include_node_id_in_reply: bool,
        prefix_range: PrefixRange,
        data_dir: String,
        fsync: bool,
    ) -> Self {
        ensure_dir(&data_dir)
            .await
            .unwrap_or_else(|e| panic!("failed to create DATA_DIR {}: {}", data_dir, e));

        let mut indexer = Indexer::new();
        let tenant1 = "thoughtspot";
        let path1 = "./words.txt";
        indexer.indexFileForPrefixRange(&tenant1, &path1, prefix_range.start, prefix_range.end);

        let tenant2 = "power";
        let path2 = "./words_alpha.txt";
        indexer.indexFileForPrefixRange(&tenant2, &path2, prefix_range.start, prefix_range.end);

        // Replay WAL entries for all tenants present in DATA_DIR (in-range only).
        let mut dir = fs::read_dir(&data_dir)
            .await
            .unwrap_or_else(|e| panic!("failed to read DATA_DIR {}: {}", data_dir, e));
        while let Some(entry) = dir
            .next_entry()
            .await
            .unwrap_or_else(|e| panic!("failed to scan DATA_DIR {}: {}", data_dir, e))
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("wal") {
                continue;
            }
            let tenant = match path.file_stem().and_then(|s| s.to_str()) {
                Some(t) if !t.trim().is_empty() => t.to_string(),
                _ => continue,
            };
            let words = replay_words(&path)
                .await
                .unwrap_or_else(|e| panic!("failed to replay WAL {:?}: {}", path, e));
            for w in words {
                if !prefix_range.contains_first_char_of(&w) {
                    continue;
                }
                // Ignore invalid WAL lines (keeps recovery robust).
                if let Ok(w) = normalize_and_validate_word(&w) {
                    indexer.putWord(&tenant, &w);
                }
            }
        }

        MyGreeter {
            indexer: RwLock::new(indexer),
            node_id,
            include_node_id_in_reply,
            prefix_range,
            data_dir,
            fsync,
        }
    }
}

#[tonic::async_trait]
impl Greeter for MyGreeter {

    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request: {:?}", request);
        let inner = request.into_inner();
        let word = normalize_and_validate_prefix(&inner.name)?;
        let tenant = inner.tenant;
        let indexer = self.indexer.read().await;
        let matches = indexer.prefixMatch(&tenant, &word);

        let node_suffix = if self.include_node_id_in_reply {
            format!(" node={}", self.node_id)
        } else {
            "".to_string()
        };
        let reply = HelloReply {
            message: format!("Hello {}{} matches: {:?}!", word, node_suffix, matches),
        };

        Ok(Response::new(reply))
    }

    async fn put_word(
        &self,
        request: Request<PutWordRequest>,
    ) -> Result<Response<PutWordReply>, Status> {
        let inner = request.into_inner();
        let tenant = inner.tenant;
        let word = normalize_and_validate_word(&inner.word)?;

        if tenant.trim().is_empty() {
            return Err(Status::invalid_argument("tenant must be non-empty"));
        }
        if !self.prefix_range.contains_first_char_of(&word) {
            return Err(Status::invalid_argument("word does not belong to this shard"));
        }

        let wal = wal_path(&self.data_dir, &tenant);
        append_word(&wal, &word, self.fsync)
            .await
            .map_err(|e| Status::internal(format!("wal append failed: {}", e)))?;

        let mut indexer = self.indexer.write().await;
        indexer.putWord(&tenant, &word);

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
    let data_dir = env_or_default("DATA_DIR", "./data");
    let fsync = env_or_default("FSYNC", "0") == "1";

    let greeter = MyGreeter::new(
        node_id.clone(),
        include_node_id_in_reply,
        prefix_range,
        data_dir.clone(),
        fsync,
    )
    .await;

    println!(
        "Server configured: NODE_ID={} BIND_ADDR={} PREFIX_RANGE={}-{} DATA_DIR={} FSYNC={}",
        node_id,
        bind_addr,
        greeter.prefix_range.start,
        greeter.prefix_range.end,
        data_dir,
        if fsync { 1 } else { 0 }
    );

    let (mut reporter, health_service) = health_reporter();
    reporter
        .set_serving::<GreeterServer<MyGreeter>>()
        .await;

    Server::builder()
        .add_service(health_service)
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
