mod common;

use std::time::Duration;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_full_restart_retains_committed_writes() {
    if skip_if_not_blackbox() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let n1_dir = tmp.path().join("n1");
    let n2_dir = tmp.path().join("n2");
    let n3_dir = tmp.path().join("n3");
    std::fs::create_dir_all(&n1_dir).unwrap();
    std::fs::create_dir_all(&n2_dir).unwrap();
    std::fs::create_dir_all(&n3_dir).unwrap();

    let p1 = pick_unused_port();
    let p2 = pick_unused_port();
    let p3 = pick_unused_port();
    let plb = pick_unused_port();

    let a1 = http_addr(p1);
    let a2 = http_addr(p2);
    let a3 = http_addr(p3);

    let members = format!("1={},2={},3={}", a1, a2, a3);
    let env1_bootstrap = vec![
        ("RAFT_ENABLED".to_string(), "1".to_string()),
        ("RAFT_NODE_ID".to_string(), "1".to_string()),
        ("RAFT_BOOTSTRAP".to_string(), "1".to_string()),
        ("RAFT_MEMBERS".to_string(), members.clone()),
    ];
    let env1_rejoin = vec![
        ("RAFT_ENABLED".to_string(), "1".to_string()),
        ("RAFT_NODE_ID".to_string(), "1".to_string()),
        ("RAFT_MEMBERS".to_string(), members.clone()),
    ];
    let env2 = vec![
        ("RAFT_ENABLED".to_string(), "1".to_string()),
        ("RAFT_NODE_ID".to_string(), "2".to_string()),
        ("RAFT_MEMBERS".to_string(), members.clone()),
    ];
    let env3 = vec![
        ("RAFT_ENABLED".to_string(), "1".to_string()),
        ("RAFT_NODE_ID".to_string(), "3".to_string()),
        ("RAFT_MEMBERS".to_string(), members.clone()),
    ];

    let mut n1 = spawn_server_with_env(
        p1,
        "n1",
        "j-r",
        n1_dir.to_str().unwrap(),
        false,
        &env1_bootstrap,
    );
    let mut n2 = spawn_server_with_env(
        p2,
        "n2",
        "j-r",
        n2_dir.to_str().unwrap(),
        false,
        &env2,
    );
    let mut n3 = spawn_server_with_env(
        p3,
        "n3",
        "j-r",
        n3_dir.to_str().unwrap(),
        false,
        &env3,
    );
    wait_healthy(&mut n1, &a1, Duration::from_secs(5)).await;
    wait_healthy(&mut n2, &a2, Duration::from_secs(5)).await;
    wait_healthy(&mut n3, &a3, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}|{}|{}", a1, a2, a3);
    let mut lb = spawn_lb(plb, &partition_map, 1);
    let lb_addr = http_addr(plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    // Write and verify.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match put_word(&lb_addr, "power", "jokerraftpersist").await {
            Ok(()) => break,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("raft write did not succeed in time: {}", e);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    wait_until_contains(&a1, "power", "joker", "jokerraftpersist").await;
    wait_until_contains(&a2, "power", "joker", "jokerraftpersist").await;
    wait_until_contains(&a3, "power", "joker", "jokerraftpersist").await;

    // Full restart: stop everything.
    drop(lb);
    drop(n1);
    drop(n2);
    drop(n3);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart servers with same DATA_DIR and ports. Node 1 rejoins without bootstrap.
    let mut n1b = spawn_server_with_env(
        p1,
        "n1b",
        "j-r",
        n1_dir.to_str().unwrap(),
        false,
        &env1_rejoin,
    );
    let mut n2b = spawn_server_with_env(
        p2,
        "n2b",
        "j-r",
        n2_dir.to_str().unwrap(),
        false,
        &env2,
    );
    let mut n3b = spawn_server_with_env(
        p3,
        "n3b",
        "j-r",
        n3_dir.to_str().unwrap(),
        false,
        &env3,
    );
    wait_healthy(&mut n1b, &a1, Duration::from_secs(5)).await;
    wait_healthy(&mut n2b, &a2, Duration::from_secs(5)).await;
    wait_healthy(&mut n3b, &a3, Duration::from_secs(5)).await;

    // After restart, the state machine should rebuild from persisted raft state.
    wait_until_contains(&a1, "power", "joker", "jokerraftpersist").await;
    wait_until_contains(&a2, "power", "joker", "jokerraftpersist").await;
    wait_until_contains(&a3, "power", "joker", "jokerraftpersist").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_rejoin_catches_up_after_missing_writes() {
    if skip_if_not_blackbox() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let n1_dir = tmp.path().join("n1");
    let n2_dir = tmp.path().join("n2");
    let n3_dir = tmp.path().join("n3");
    std::fs::create_dir_all(&n1_dir).unwrap();
    std::fs::create_dir_all(&n2_dir).unwrap();
    std::fs::create_dir_all(&n3_dir).unwrap();

    let p1 = pick_unused_port();
    let p2 = pick_unused_port();
    let p3 = pick_unused_port();
    let plb = pick_unused_port();

    let a1 = http_addr(p1);
    let a2 = http_addr(p2);
    let a3 = http_addr(p3);

    let members = format!("1={},2={},3={}", a1, a2, a3);
    let env1 = vec![
        ("RAFT_ENABLED".to_string(), "1".to_string()),
        ("RAFT_NODE_ID".to_string(), "1".to_string()),
        ("RAFT_BOOTSTRAP".to_string(), "1".to_string()),
        ("RAFT_MEMBERS".to_string(), members.clone()),
    ];
    let env2 = vec![
        ("RAFT_ENABLED".to_string(), "1".to_string()),
        ("RAFT_NODE_ID".to_string(), "2".to_string()),
        ("RAFT_MEMBERS".to_string(), members.clone()),
    ];
    let env3 = vec![
        ("RAFT_ENABLED".to_string(), "1".to_string()),
        ("RAFT_NODE_ID".to_string(), "3".to_string()),
        ("RAFT_MEMBERS".to_string(), members.clone()),
    ];

    let mut n1 = spawn_server_with_env(
        p1,
        "n1",
        "j-r",
        n1_dir.to_str().unwrap(),
        false,
        &env1,
    );
    let mut n2 = spawn_server_with_env(
        p2,
        "n2",
        "j-r",
        n2_dir.to_str().unwrap(),
        false,
        &env2,
    );
    let mut n3 = spawn_server_with_env(
        p3,
        "n3",
        "j-r",
        n3_dir.to_str().unwrap(),
        false,
        &env3,
    );
    wait_healthy(&mut n1, &a1, Duration::from_secs(5)).await;
    wait_healthy(&mut n2, &a2, Duration::from_secs(5)).await;
    wait_healthy(&mut n3, &a3, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}|{}|{}", a1, a2, a3);
    let mut lb = spawn_lb(plb, &partition_map, 1);
    let lb_addr = http_addr(plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    // Seed a committed entry.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match put_word(&lb_addr, "power", "jokerraftseed").await {
            Ok(()) => break,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("raft seed write did not succeed in time: {}", e);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    wait_until_contains(&a1, "power", "joker", "jokerraftseed").await;
    wait_until_contains(&a2, "power", "joker", "jokerraftseed").await;
    wait_until_contains(&a3, "power", "joker", "jokerraftseed").await;

    // Take n3 down, write another word, then bring n3 back and expect it to catch up.
    drop(n3);
    tokio::time::sleep(Duration::from_millis(150)).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match put_word(&lb_addr, "power", "jokerraftafterdown").await {
            Ok(()) => break,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("raft write did not succeed in time: {}", e);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    wait_until_contains(&a1, "power", "joker", "jokerraftafterdown").await;
    wait_until_contains(&a2, "power", "joker", "jokerraftafterdown").await;

    let mut n3b = spawn_server_with_env(
        p3,
        "n3b",
        "j-r",
        n3_dir.to_str().unwrap(),
        false,
        &env3,
    );
    wait_healthy(&mut n3b, &a3, Duration::from_secs(5)).await;
    wait_until_contains(&a3, "power", "joker", "jokerraftafterdown").await;
}
