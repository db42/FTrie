
use tonic::{transport::Server, Request, Response, Status};
use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};
use hello_world::greeter_client::GreeterClient;

mod partition;
use partition::{env_or_default, PartitionMap};

use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic::transport::Channel;

pub mod hello_world {
    tonic::include_proto!("helloworld"); 
}

#[derive(Debug, Clone)]
struct HealthState {
    healthy: bool,
    consecutive_failures: u32,
    consecutive_successes: u32,
}

impl HealthState {
    fn new() -> Self {
        Self {
            healthy: true,
            consecutive_failures: 0,
            consecutive_successes: 0,
        }
    }
}

pub struct LoadBalancer {
    partition_map: PartitionMap,
    health: Arc<RwLock<HashMap<String, HealthState>>>,
    max_attempts: usize,
    fail_after: u32,
    recover_after: u32,
}

async fn mark_backend_result(
    health: &Arc<RwLock<HashMap<String, HealthState>>>,
    backend: &str,
    ok: bool,
    fail_after: u32,
    recover_after: u32,
) {
    let mut table = health.write().await;
    let entry = table
        .entry(backend.to_string())
        .or_insert_with(HealthState::new);

    if ok {
        entry.consecutive_successes = entry.consecutive_successes.saturating_add(1);
        entry.consecutive_failures = 0;
        if !entry.healthy && entry.consecutive_successes >= recover_after {
            entry.healthy = true;
            println!("backend healthy: {}", backend);
        }
    } else {
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        entry.consecutive_successes = 0;
        if entry.healthy && entry.consecutive_failures >= fail_after {
            entry.healthy = false;
            println!("backend unhealthy: {}", backend);
        }
    }
}

async fn is_backend_healthy(
    health: &Arc<RwLock<HashMap<String, HealthState>>>,
    backend: &str,
) -> bool {
    let table = health.read().await;
    table.get(backend).map(|s| s.healthy).unwrap_or(true)
}

async fn probe_backends_forever(
    health: Arc<RwLock<HashMap<String, HealthState>>>,
    backends: Vec<String>,
    interval: Duration,
    fail_after: u32,
    recover_after: u32,
) {
    let mut ticker = time::interval(interval);
    loop {
        ticker.tick().await;
        for backend in &backends {
            let res = async {
                let channel = Channel::from_shared(backend.clone())
                    .map_err(|e| Status::unavailable(format!("health channel init failed: {}", e)))?
                    .connect()
                    .await
                    .map_err(|e| Status::unavailable(format!("health connect failed: {}", e)))?;
                let mut client = HealthClient::new(channel);
                let req = HealthCheckRequest {
                    // Empty service name means "overall server health" in the standard API.
                    service: "".to_string(),
                };
                client
                    .check(Request::new(req))
                    .await
                    .map_err(|e| Status::unavailable(format!("health check failed: {}", e)))?;
                Ok::<(), Status>(())
            }
            .await;

            mark_backend_result(&health, backend, res.is_ok(), fail_after, recover_after).await;
        }
    }
}

#[tonic::async_trait]
impl Greeter for LoadBalancer {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Load balancer received request: {:?}", request);

        let inner = request.get_ref().clone();
        let prefix = inner.name.as_str();
        let partition = self
            .partition_map
            .route(prefix)
            .ok_or_else(|| Status::invalid_argument("prefix must start with a-z"))?;

        // Phase 2: random replica selection + bounded retries.
        let mut candidates: Vec<String> = Vec::new();
        for b in &partition.backends {
            if is_backend_healthy(&self.health, b).await {
                candidates.push(b.clone());
            }
        }
        if candidates.is_empty() {
            // Fall back to trying all replicas if health is unknown/stale.
            candidates = partition.backends.clone();
        }

        candidates.shuffle(&mut rand::thread_rng());

        let attempts = std::cmp::min(self.max_attempts, candidates.len().max(1));
        for backend in candidates.into_iter().take(attempts) {
            let res = async {
                let mut client = GreeterClient::connect(backend.clone())
                    .await
                    .map_err(|e| Status::unavailable(format!("connect failed: {}", e)))?;
                client
                    .say_hello(Request::new(inner.clone()))
                    .await
                    .map_err(|e| Status::unavailable(format!("request failed: {}", e)))
            }
            .await;

            match res {
                Ok(resp) => {
                    mark_backend_result(
                        &self.health,
                        &backend,
                        true,
                        self.fail_after,
                        self.recover_after,
                    )
                    .await;
                    return Ok(resp);
                }
                Err(e) => {
                    mark_backend_result(
                        &self.health,
                        &backend,
                        false,
                        self.fail_after,
                        self.recover_after,
                    )
                    .await;
                    println!("backend attempt failed: backend={} err={}", backend, e);
                    continue;
                }
            }
        }

        Err(Status::unavailable("no healthy backends available"))
    }
}

#[tokio::main] 
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env_or_default("BIND_ADDR", "[::]:50052");
    let addr = bind_addr.parse()?;
    let pm_str = env_or_default("PARTITION_MAP", "a-z=http://[::1]:50051");
    let partition_map =
        PartitionMap::parse(&pm_str).unwrap_or_else(|e| panic!("invalid PARTITION_MAP: {}", e));

    // Extract unique backends for active health probing.
    let mut uniq = HashSet::new();
    let mut backends = Vec::new();
    for entry in pm_str.split(',') {
        if let Some((_, rhs)) = entry.split_once('=') {
            for b in rhs.split('|').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                if uniq.insert(b.to_string()) {
                    backends.push(b.to_string());
                }
            }
        }
    }

    let health = Arc::new(RwLock::new(HashMap::<String, HealthState>::new()));
    let interval_ms: u64 = env_or_default("LB_HEALTH_INTERVAL_MS", "1000")
        .parse()
        .unwrap_or(1000);
    let fail_after: u32 = env_or_default("LB_FAIL_AFTER", "2").parse().unwrap_or(2);
    let recover_after: u32 = env_or_default("LB_RECOVER_AFTER", "1").parse().unwrap_or(1);

    tokio::spawn(probe_backends_forever(
        health.clone(),
        backends,
        Duration::from_millis(interval_ms.max(100)),
        fail_after.max(1),
        recover_after.max(1),
    ));

    // LB_POLICY: Phase 2 only supports random; keep env for forward-compat.
    let policy = env_or_default("LB_POLICY", "random");
    if policy != "random" {
        println!("LB_POLICY={} ignored; using random", policy);
    }

    println!("LB configured: PARTITION_MAP={}", pm_str);

    let lb = LoadBalancer {
        partition_map,
        health,
        max_attempts: env_or_default("LB_MAX_ATTEMPTS", "3")
            .parse()
            .unwrap_or(3)
            .max(1),
        fail_after: fail_after.max(1),
        recover_after: recover_after.max(1),
    };

    println!("Load balancer listening on {}", addr);

    Server::builder()
        .add_service(GreeterServer::new(lb))
        .serve(addr)
        .await?;

    Ok(())
}
