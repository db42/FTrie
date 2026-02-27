
use tonic::{transport::Server, Request, Response, Status};
use ftrie_proto::prefix_matcher_client::PrefixMatcherClient;
use ftrie_proto::prefix_matcher_server::{PrefixMatcher, PrefixMatcherServer};
use ftrie_proto::{HelloReply, HelloRequest, PutWordReply, PutWordRequest};

mod partition;
use partition::{env_or_default, PartitionMap};

use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::sync::RwLock;
use tokio::time;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::server::health_reporter;
use tonic::transport::Channel;
use tonic::Code;

pub mod ftrie_proto {
    tonic::include_proto!("ftrie");
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

fn status_indicates_backend_fault(code: Code) -> bool {
    // Only treat transport-ish statuses as backend health signals.
    // Application errors (INVALID_ARGUMENT, etc.) should not poison health.
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::Unknown
            | Code::ResourceExhausted
    )
}

pub struct LoadBalancer {
    partition_map: PartitionMap,
    health: Arc<RwLock<HashMap<String, HealthState>>>,
    max_attempts: usize,
    fail_after: u32,
    recover_after: u32,
    read_quorum: usize,
    read_timeout: Duration,
    write_timeout: Duration,
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
impl PrefixMatcher for LoadBalancer {
    async fn get_prefix_match(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Load balancer received request: {:?}", request);

        let mut inner = request.get_ref().clone();
        let prefix = normalize_and_validate_prefix(inner.name.as_str())?;
        inner.name = prefix.clone();
        let partition = self
            .partition_map
            .route(&prefix)
            .ok_or_else(|| Status::invalid_argument("prefix must start with a-z"))?;

        if self.read_quorum > partition.backends.len() {
            return Err(Status::invalid_argument(format!(
                "R={} cannot be satisfied with RF={}",
                self.read_quorum,
                partition.backends.len()
            )));
        }

        // Random replica selection + read quorum (R successes).
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

        let attempts = std::cmp::min(self.max_attempts.max(self.read_quorum), candidates.len());
        let selected: Vec<String> = candidates.into_iter().take(attempts).collect();

        let mut set = JoinSet::new();
        for backend in selected {
            let req = inner.clone();
            let timeout = self.read_timeout;
            set.spawn(async move {
                let fut = async {
                    let mut client = PrefixMatcherClient::connect(backend.clone())
                        .await
                        .map_err(|e| Status::unavailable(format!("connect failed: {}", e)))?;
                    let resp = client.get_prefix_match(Request::new(req)).await?;
                    Ok::<HelloReply, tonic::Status>(resp.into_inner())
                };
                let res = time::timeout(timeout, fut).await;
                (backend, res)
            });
        }

        let mut successes = 0usize;
        let mut first_reply: Option<HelloReply> = None;
        while let Some(joined) = set.join_next().await {
            let (backend, res) = joined.map_err(|e| Status::internal(format!("task failed: {}", e)))?;
            match res {
                Ok(Ok(reply)) => {
                    mark_backend_result(&self.health, &backend, true, self.fail_after, self.recover_after).await;
                    successes += 1;
                    if first_reply.is_none() {
                        first_reply = Some(reply);
                    }
                    if successes >= self.read_quorum {
                        set.abort_all();
                        return Ok(Response::new(first_reply.unwrap()));
                    }
                }
                Ok(Err(e)) => {
                    let ok_for_health = !status_indicates_backend_fault(e.code());
                    mark_backend_result(
                        &self.health,
                        &backend,
                        ok_for_health,
                        self.fail_after,
                        self.recover_after,
                    )
                    .await;
                    println!("backend read failed: backend={} err={}", backend, e);
                }
                Err(_) => {
                    mark_backend_result(&self.health, &backend, false, self.fail_after, self.recover_after).await;
                    println!("backend read timeout: backend={}", backend);
                }
            }

            let remaining = set.len();
            if successes + remaining < self.read_quorum {
                set.abort_all();
                break;
            }
        }

        Err(Status::unavailable("no healthy backends available"))
    }

    async fn put_word(
        &self,
        request: Request<PutWordRequest>,
    ) -> Result<Response<PutWordReply>, Status> {
        let mut inner = request.get_ref().clone();
        let word = normalize_and_validate_word(inner.word.as_str())?;
        inner.word = word.clone();
        let partition = self
            .partition_map
            .route(&word)
            .ok_or_else(|| Status::invalid_argument("word must start with a-z"))?;

        // Raft-only mode: shard nodes handle replication. LB only needs to find a leader.
        let mut candidates: Vec<String> = Vec::new();
        for b in &partition.backends {
            if is_backend_healthy(&self.health, b).await {
                candidates.push(b.clone());
            }
        }
        if candidates.is_empty() {
            candidates = partition.backends.clone();
        }
        candidates.shuffle(&mut rand::thread_rng());

        let mut queue = candidates;
        let mut seen = HashSet::new();
        let mut attempts = 0usize;
        while attempts < self.max_attempts.max(1) && !queue.is_empty() {
            let backend = queue.remove(0);
            if !seen.insert(backend.clone()) {
                continue;
            }
            attempts += 1;

            let req = inner.clone();
            let timeout = self.write_timeout;
            let fut = async {
                let mut client = PrefixMatcherClient::connect(backend.clone())
                    .await
                    .map_err(|e| Status::unavailable(format!("connect failed: {}", e)))?;
                client.put_word(Request::new(req)).await?;
                Ok::<(), Status>(())
            };

            match time::timeout(timeout, fut).await {
                Ok(Ok(())) => {
                    mark_backend_result(&self.health, &backend, true, self.fail_after, self.recover_after).await;
                    return Ok(Response::new(PutWordReply {
                        applied: true,
                        message: "ok".to_string(),
                    }));
                }
                Ok(Err(e)) => {
                    // Followers return FAILED_PRECONDITION with a leader hint via metadata `x-raft-leader`.
                    if e.code() == Code::FailedPrecondition {
                        if let Some(v) = e.metadata().get("x-raft-leader") {
                            if let Ok(leader) = v.to_str() {
                                let leader = leader.trim();
                                if !leader.is_empty() {
                                    queue.insert(0, leader.to_string());
                                }
                            }
                        }
                        continue;
                    }

                    let ok_for_health = !status_indicates_backend_fault(e.code());
                    mark_backend_result(
                        &self.health,
                        &backend,
                        ok_for_health,
                        self.fail_after,
                        self.recover_after,
                    )
                    .await;
                    println!("backend write failed: backend={} err={}", backend, e);
                }
                Err(_) => {
                    mark_backend_result(&self.health, &backend, false, self.fail_after, self.recover_after).await;
                    println!("backend write timeout: backend={}", backend);
                }
            }
        }

        Err(Status::unavailable("write failed (no leader available)"))
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

    let read_quorum: usize = env_or_default("R", "1").parse().unwrap_or(1).max(1);
    let read_timeout_ms: u64 = env_or_default("READ_TIMEOUT_MS", "200").parse().unwrap_or(200);
    let write_timeout_ms: u64 = env_or_default("WRITE_TIMEOUT_MS", "500").parse().unwrap_or(500);

    let lb = LoadBalancer {
        partition_map,
        health,
        max_attempts: env_or_default("LB_MAX_ATTEMPTS", "3")
            .parse()
            .unwrap_or(3)
            .max(1),
        fail_after: fail_after.max(1),
        recover_after: recover_after.max(1),
        read_quorum,
        read_timeout: Duration::from_millis(read_timeout_ms.max(10)),
        write_timeout: Duration::from_millis(write_timeout_ms.max(10)),
    };

    println!("Load balancer listening on {}", addr);

    let (mut reporter, health_service) = health_reporter();
    reporter
        .set_serving::<PrefixMatcherServer<LoadBalancer>>()
        .await;

    Server::builder()
        .add_service(health_service)
        .add_service(PrefixMatcherServer::new(lb))
        .serve(addr)
        .await?;

    Ok(())
}
