use tonic::{transport::Server, Request, Response, Status};

use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};

mod indexer;
mod partition;

use indexer::Indexer;
use partition::{env_or_default, PrefixRange};
use tonic_health::server::health_reporter;
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
    indexer: Indexer,
    node_id: String,
    include_node_id_in_reply: bool,
}

impl MyGreeter {
    fn new() -> Self {
        let bind_addr = env_or_default("BIND_ADDR", "[::]:50051");
        let prefix_range = env_or_default("PREFIX_RANGE", "a-z");
        let prefix_range = PrefixRange::parse(&prefix_range)
            .unwrap_or_else(|e| panic!("invalid PREFIX_RANGE: {}", e));
        let node_id = env_or_default("NODE_ID", "node");
        let include_node_id_in_reply =
            env_or_default("INCLUDE_NODE_ID_IN_REPLY", "0") == "1";

        let mut indexer = Indexer::new();
        let tenant1 = "thoughtspot";
        let path1 = "./words.txt";
        indexer.indexFileForPrefixRange(&tenant1, &path1, prefix_range.start, prefix_range.end);

        let tenant2 = "power";
        let path2 = "./words_alpha.txt";
        indexer.indexFileForPrefixRange(&tenant2, &path2, prefix_range.start, prefix_range.end);

        let greeter = MyGreeter {
            indexer,
            node_id: node_id.clone(),
            include_node_id_in_reply,
        };

        println!(
            "Server configured: NODE_ID={} BIND_ADDR={} PREFIX_RANGE={}-{}",
            node_id, bind_addr, prefix_range.start, prefix_range.end
        );

        greeter
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
        let word = inner.name;
        let tenant = inner.tenant;
        //search for this word in
        let matches = self.indexer.prefixMatch(&tenant, &word);

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env_or_default("BIND_ADDR", "[::]:50051");
    let addr = bind_addr.parse()?;
    // let greeter = MyGreeter::default();
    let greeter = MyGreeter::new();

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
