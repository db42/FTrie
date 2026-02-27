use tonic::Request;

use hello_world::greeter_client::GreeterClient;
use hello_world::HelloRequest;

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

// Usage:
//   cargo run --bin ftrie-client -- <prefix> <tenant> [top_k] [addr]
// Example:
//   cargo run --bin ftrie-client -- apr power http://[::1]:50052
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: {} <prefix> <tenant> [top_k] [addr]", args[0]);
        std::process::exit(2);
    }

    let name = args[1].clone();
    let tenant = args[2].clone();
    let top_k: u32 = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let addr = args
        .get(4)
        .cloned()
        .unwrap_or_else(|| "http://[::1]:50052".to_string());

    let mut client = GreeterClient::connect(addr).await?;
    let req = HelloRequest { name, tenant, top_k };
    let resp = client.get_prefix_match(Request::new(req)).await?;
    println!("{:?}", resp.into_inner());
    Ok(())
}
