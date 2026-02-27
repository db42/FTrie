#![allow(dead_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tonic::transport::Channel;
use tonic::{Request, Status};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

pub mod ftrie_proto {
    tonic::include_proto!("ftrie");
}

use ftrie_proto::greeter_client::GreeterClient;
use ftrie_proto::{HelloReply, HelloRequest, PutWordRequest};

pub fn blackbox_enabled() -> bool {
    std::env::var("RUN_BLACKBOX_IT").ok().as_deref() == Some("1")
}

pub fn skip_if_not_blackbox() -> bool {
    if !blackbox_enabled() {
        eprintln!("skipping black-box IT; set RUN_BLACKBOX_IT=1 to enable");
        return true;
    }
    false
}

pub fn pick_unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().unwrap().port()
}

pub fn http_addr(port: u16) -> String {
    format!("http://127.0.0.1:{}", port)
}

fn bin_path(name: &str) -> String {
    match name {
        "ftrie-server" => option_env!("CARGO_BIN_EXE_ftrie-server")
            .expect("CARGO_BIN_EXE_ftrie-server not set; run via cargo test")
            .to_string(),
        "ftrie-lb" => option_env!("CARGO_BIN_EXE_ftrie-lb")
            .expect("CARGO_BIN_EXE_ftrie-lb not set; run via cargo test")
            .to_string(),
        _ => panic!("unknown binary {}", name),
    }
}

pub struct Proc {
    child: Child,
    log_path: PathBuf,
}

impl Proc {
    pub fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    pub fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().ok().flatten()
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub async fn wait_healthy(proc: &mut Proc, addr: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(status) = proc.try_wait() {
            panic!(
                "process exited early (status={}) while waiting for health at {}\nlog:\n{}",
                status,
                addr,
                proc.read_log()
            );
        }

        let res = async {
            let channel = Channel::from_shared(addr.to_string())
                .map_err(|e| Status::unavailable(format!("health channel init failed: {}", e)))?
                .connect()
                .await
                .map_err(|e| Status::unavailable(format!("health connect failed: {}", e)))?;
            let mut client = HealthClient::new(channel);
            let req = HealthCheckRequest {
                service: "".to_string(),
            };
            client.check(Request::new(req)).await?;
            Ok::<(), Status>(())
        }
        .await;

        if res.is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for health at {}\nlog:\n{}",
                addr,
                proc.read_log()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn start_server(
    port: u16,
    node_id: &str,
    prefix_range: &str,
    data_dir: &str,
    include_node_id_in_reply: bool,
) -> (Proc, String) {
    let addr = http_addr(port);
    let mut proc = spawn_server(
        port,
        node_id,
        prefix_range,
        data_dir,
        include_node_id_in_reply,
    );
    wait_healthy(&mut proc, &addr, Duration::from_secs(5)).await;
    (proc, addr)
}

pub fn spawn_server(
    port: u16,
    node_id: &str,
    prefix_range: &str,
    data_dir: &str,
    include_node_id_in_reply: bool,
) -> Proc {
    spawn_server_with_env(
        port,
        node_id,
        prefix_range,
        data_dir,
        include_node_id_in_reply,
        &[],
    )
}

pub fn spawn_server_with_env(
    port: u16,
    node_id: &str,
    prefix_range: &str,
    data_dir: &str,
    include_node_id_in_reply: bool,
    extra_env: &[(String, String)],
) -> Proc {
    let addr = format!("127.0.0.1:{}", port);
    let http = http_addr(port);
    let log_path =
        std::env::temp_dir().join(format!("ftrie-server-{}-{}.log", node_id, port));
    let log = std::fs::File::create(&log_path).expect("create server log");
    let log_err = log.try_clone().expect("clone server log");

    let mut cmd = Command::new(bin_path("ftrie-server"));
    cmd.env("BIND_ADDR", addr)
        .env("NODE_ID", node_id)
        .env("PREFIX_RANGE", prefix_range)
        .env("DATA_DIR", data_dir)
        .env(
            "INCLUDE_NODE_ID_IN_REPLY",
            if include_node_id_in_reply { "1" } else { "0" },
        )
        // Raft-only mode: default to a single-node Raft cluster unless overridden.
        .env("RAFT_ENABLED", "1")
        .env("RAFT_NODE_ID", "1")
        .env("RAFT_BOOTSTRAP", "1")
        .env("RAFT_MEMBERS", format!("1={}", http))
        .env("DISABLE_STATIC_INDEX", "1")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));

    for (k, v) in extra_env {
        cmd.env(k, v);
    }

    let child = cmd.spawn()
        .expect("spawn server");

    Proc { child, log_path }
}

pub async fn start_lb(port: u16, partition_map: &str, r: usize) -> (Proc, String) {
    let addr = http_addr(port);
    let mut proc = spawn_lb(port, partition_map, r);
    wait_healthy(&mut proc, &addr, Duration::from_secs(5)).await;
    (proc, addr)
}

pub fn spawn_lb(port: u16, partition_map: &str, r: usize) -> Proc {
    spawn_lb_with_env(port, partition_map, r, &[])
}

pub fn spawn_lb_with_env(
    port: u16,
    partition_map: &str,
    r: usize,
    extra_env: &[(String, String)],
) -> Proc {
    let addr = format!("127.0.0.1:{}", port);
    let log_path = std::env::temp_dir().join(format!("ftrie-lb-{}.log", port));
    let log = std::fs::File::create(&log_path).expect("create lb log");
    let log_err = log.try_clone().expect("clone lb log");

    let mut cmd = Command::new(bin_path("ftrie-lb"));
    cmd.env("BIND_ADDR", addr)
        .env("PARTITION_MAP", partition_map)
        .env("LB_POLICY", "random")
        .env("LB_MAX_ATTEMPTS", "3")
        .env("LB_HEALTH_INTERVAL_MS", "50")
        .env("LB_FAIL_AFTER", "1")
        .env("LB_RECOVER_AFTER", "1")
        .env("READ_TIMEOUT_MS", "500")
        .env("WRITE_TIMEOUT_MS", "500")
        .env("R", r.to_string())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));

    for (k, v) in extra_env {
        cmd.env(k, v);
    }

    let child = cmd.spawn()
        .expect("spawn lb");

    Proc { child, log_path }
}

pub async fn get_prefix_match_result(addr: &str, tenant: &str, name: &str) -> Result<String, Status> {
    get_prefix_match_result_top_k(addr, tenant, name, 0).await
}

pub async fn get_prefix_match_result_top_k(
    addr: &str,
    tenant: &str,
    name: &str,
    top_k: u32,
) -> Result<String, Status> {
    let mut client = GreeterClient::connect(addr.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connect failed: {}", e)))?;
    let req = HelloRequest {
        name: name.to_string(),
        tenant: tenant.to_string(),
        top_k,
    };
    let resp = client.get_prefix_match(Request::new(req)).await?;
    Ok(resp.into_inner().message)
}

pub async fn get_prefix_match(addr: &str, tenant: &str, name: &str) -> String {
    get_prefix_match_result(addr, tenant, name).await.unwrap()
}

pub async fn get_prefix_match_reply_result(
    addr: &str,
    tenant: &str,
    name: &str,
    top_k: u32,
) -> Result<HelloReply, Status> {
    let mut client = GreeterClient::connect(addr.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connect failed: {}", e)))?;
    let req = HelloRequest {
        name: name.to_string(),
        tenant: tenant.to_string(),
        top_k,
    };
    let resp = client.get_prefix_match(Request::new(req)).await?;
    Ok(resp.into_inner())
}

pub async fn get_prefix_match_reply(addr: &str, tenant: &str, name: &str, top_k: u32) -> HelloReply {
    get_prefix_match_reply_result(addr, tenant, name, top_k)
        .await
        .unwrap()
}

pub async fn put_word(addr: &str, tenant: &str, word: &str) -> Result<(), Status> {
    let mut client = GreeterClient::connect(addr.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connect failed: {}", e)))?;
    let req = PutWordRequest {
        word: word.to_string(),
        tenant: tenant.to_string(),
    };
    client.put_word(Request::new(req)).await?;
    Ok(())
}

pub async fn wait_until_contains(addr: &str, tenant: &str, prefix: &str, needle: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let reply = get_prefix_match_reply(addr, tenant, prefix, 0).await;
        if reply.matches.iter().any(|m| m == needle) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {} to contain {}", addr, needle);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn assert_not_contains_for(
    addr: &str,
    tenant: &str,
    prefix: &str,
    needle: &str,
    dur: Duration,
) {
    let deadline = tokio::time::Instant::now() + dur;
    loop {
        let reply = get_prefix_match_reply(addr, tenant, prefix, 0).await;
        if reply.matches.iter().any(|m| m == needle) {
            panic!("expected {} to not contain {}, but it did", addr, needle);
        }
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
