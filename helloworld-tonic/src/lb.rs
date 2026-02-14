
use tonic::{transport::Server, Request, Response, Status};
use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};
use hello_world::greeter_client::GreeterClient;

mod partition;
use partition::{env_or_default, PartitionMap};

pub mod hello_world {
    tonic::include_proto!("helloworld"); 
}

pub struct LoadBalancer {
    partition_map: PartitionMap,
}

impl LoadBalancer {
    pub fn new() -> Self {
        let pm_str = env_or_default("PARTITION_MAP", "a-z=http://[::1]:50051");
        let partition_map =
            PartitionMap::parse(&pm_str).unwrap_or_else(|e| panic!("invalid PARTITION_MAP: {}", e));

        println!("LB configured: PARTITION_MAP={}", pm_str);

        LoadBalancer { partition_map }
    }
}

#[tonic::async_trait]
impl Greeter for LoadBalancer {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Load balancer received request: {:?}", request);

        let prefix = request.get_ref().name.as_str();
        let partition = self
            .partition_map
            .route(prefix)
            .ok_or_else(|| Status::invalid_argument("prefix must start with a-z"))?;
        let backend = partition.backends[0].clone();

        // Create client to backend node (Phase 1: single backend per partition)
        let mut client = GreeterClient::connect(backend.clone())
            .await
            .map_err(|e| Status::unavailable(format!("Failed to connect to backend: {}", e)))?;

        // Forward the request
        let response = client
            .say_hello(request)
            .await
            .map_err(|e| Status::unavailable(format!("Backend request failed: {}", e)))?;

        Ok(response)
    }
}

#[tokio::main] 
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env_or_default("BIND_ADDR", "[::]:50052");
    let addr = bind_addr.parse()?;
    let lb = LoadBalancer::new();

    println!("Load balancer listening on {}", addr);

    Server::builder()
        .add_service(GreeterServer::new(lb))
        .serve(addr)
        .await?;

    Ok(())
}
