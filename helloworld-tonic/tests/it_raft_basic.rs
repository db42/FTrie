mod common;

use std::time::Duration;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_putword_replicates_across_replicas() {
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
        false,
        &env1,
    );
    let mut n2 = spawn_server_with_env(
        p2,
        "n2",
        "j-r",
        n2_dir.to_str().unwrap(),
        false,
        false,
        &env2,
    );
    let mut n3 = spawn_server_with_env(
        p3,
        "n3",
        "j-r",
        n3_dir.to_str().unwrap(),
        false,
        false,
        &env3,
    );
    wait_healthy(&mut n1, &a1, Duration::from_secs(5)).await;
    wait_healthy(&mut n2, &a2, Duration::from_secs(5)).await;
    wait_healthy(&mut n3, &a3, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}|{}|{}", a1, a2, a3);
    let lb_env = vec![("LB_WRITE_MODE".to_string(), "raft".to_string())];
    let mut lb = spawn_lb_with_env(plb, &partition_map, 1, 1, &lb_env);
    let lb_addr = http_addr(plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    // Give the cluster a moment to elect a leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match put_word(&lb_addr, "power", "jokerraft").await {
            Ok(()) => break,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("raft write did not succeed in time: {}", e);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    // The committed log should be applied on all 3 replicas.
    wait_until_contains(&a1, "power", "joker", "jokerraft").await;
    wait_until_contains(&a2, "power", "joker", "jokerraft").await;
    wait_until_contains(&a3, "power", "joker", "jokerraft").await;
}
