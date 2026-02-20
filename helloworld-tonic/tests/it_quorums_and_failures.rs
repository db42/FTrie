mod common;

use std::time::Duration;

use common::*;
use tonic::Code;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w_equals_rf_fails_if_one_replica_down() {
    if !blackbox_enabled() {
        eprintln!("skipping black-box IT; set RUN_BLACKBOX_IT=1 to enable");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let jr1_dir = tmp.path().join("jr1");
    let jr2_dir = tmp.path().join("jr2");
    std::fs::create_dir_all(&jr1_dir).unwrap();
    std::fs::create_dir_all(&jr2_dir).unwrap();

    let p1 = pick_unused_port();
    let p2 = pick_unused_port();
    let plb = pick_unused_port();
    let jr1_addr = format!("http://127.0.0.1:{}", p1);
    let jr2_addr = format!("http://127.0.0.1:{}", p2);

    let mut jr1 = Some(spawn_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
        false,
    ));
    let mut jr2 = spawn_server(
        p2,
        "jr2",
        "j-r",
        jr2_dir.to_str().unwrap(),
        false,
        false,
    );
    wait_healthy(jr1.as_mut().unwrap(), &jr1_addr, Duration::from_secs(5)).await;
    wait_healthy(&mut jr2, &jr2_addr, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}|{}", jr1_addr, jr2_addr);
    let mut lb = spawn_lb(plb, &partition_map, 1, 2);
    let lb_addr = format!("http://127.0.0.1:{}", plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    drop(jr1.take());
    let err = put_word(&lb_addr, "power", "jokerz").await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_equals_rf_fails_if_one_replica_down() {
    if !blackbox_enabled() {
        eprintln!("skipping black-box IT; set RUN_BLACKBOX_IT=1 to enable");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let jr1_dir = tmp.path().join("jr1");
    let jr2_dir = tmp.path().join("jr2");
    std::fs::create_dir_all(&jr1_dir).unwrap();
    std::fs::create_dir_all(&jr2_dir).unwrap();

    let p1 = pick_unused_port();
    let p2 = pick_unused_port();
    let plb = pick_unused_port();
    let jr1_addr = format!("http://127.0.0.1:{}", p1);
    let jr2_addr = format!("http://127.0.0.1:{}", p2);

    let mut jr1 = Some(spawn_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
        false,
    ));
    let mut jr2 = spawn_server(
        p2,
        "jr2",
        "j-r",
        jr2_dir.to_str().unwrap(),
        false,
        false,
    );
    wait_healthy(jr1.as_mut().unwrap(), &jr1_addr, Duration::from_secs(5)).await;
    wait_healthy(&mut jr2, &jr2_addr, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}|{}", jr1_addr, jr2_addr);
    // Seed: make sure both replicas have some data so reads aren't vacuous.
    // Use W=2 to ensure the seed is present on both replicas deterministically.
    let seed_port = pick_unused_port();
    let mut lb_seed = spawn_lb(seed_port, &partition_map, 1, 2);
    let lb_seed_addr = format!("http://127.0.0.1:{}", seed_port);
    wait_healthy(&mut lb_seed, &lb_seed_addr, Duration::from_secs(5)).await;
    put_word(&lb_seed_addr, "power", "jokerseed").await.unwrap();
    wait_until_contains(&jr1_addr, "power", "joker", "jokerseed").await;
    wait_until_contains(&jr2_addr, "power", "joker", "jokerseed").await;
    drop(lb_seed);

    // Bring LB with R=2, W=1 and kill one replica.
    let mut lb = spawn_lb(plb, &partition_map, 2, 1);
    let lb_addr = format!("http://127.0.0.1:{}", plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    drop(jr1.take());

    let mut client = hello_world::greeter_client::GreeterClient::connect(lb_addr.clone())
        .await
        .unwrap();
    let req = hello_world::HelloRequest {
        name: "joker".to_string(),
        tenant: "power".to_string(),
    };
    let err = client
        .say_hello(tonic::Request::new(req))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_argument_does_not_break_followup_valid_write() {
    if !blackbox_enabled() {
        eprintln!("skipping black-box IT; set RUN_BLACKBOX_IT=1 to enable");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let jr1_dir = tmp.path().join("jr1");
    std::fs::create_dir_all(&jr1_dir).unwrap();

    let p1 = pick_unused_port();
    let plb = pick_unused_port();
    let jr1_addr = format!("http://127.0.0.1:{}", p1);
    let mut jr1 = spawn_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
        false,
    );
    wait_healthy(&mut jr1, &jr1_addr, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}", jr1_addr);
    let mut lb = spawn_lb(plb, &partition_map, 1, 1);
    let lb_addr = format!("http://127.0.0.1:{}", plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    // Invalid word (non a-z) should return INVALID_ARGUMENT.
    let err = put_word(&lb_addr, "power", "joker2").await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // Follow-up valid word should still succeed.
    put_word(&lb_addr, "power", "jokerz").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_greater_than_rf_is_invalid_argument() {
    if !blackbox_enabled() {
        eprintln!("skipping black-box IT; set RUN_BLACKBOX_IT=1 to enable");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let jr1_dir = tmp.path().join("jr1");
    std::fs::create_dir_all(&jr1_dir).unwrap();

    let p1 = pick_unused_port();
    let plb = pick_unused_port();
    let jr1_addr = format!("http://127.0.0.1:{}", p1);
    let mut jr1 = spawn_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
        false,
    );
    wait_healthy(&mut jr1, &jr1_addr, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}", jr1_addr);
    let mut lb = spawn_lb(plb, &partition_map, 2, 1);
    let lb_addr = format!("http://127.0.0.1:{}", plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    let err = say_hello_result(&lb_addr, "power", "joker").await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w_greater_than_rf_is_invalid_argument() {
    if !blackbox_enabled() {
        eprintln!("skipping black-box IT; set RUN_BLACKBOX_IT=1 to enable");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let jr1_dir = tmp.path().join("jr1");
    std::fs::create_dir_all(&jr1_dir).unwrap();

    let p1 = pick_unused_port();
    let plb = pick_unused_port();
    let jr1_addr = format!("http://127.0.0.1:{}", p1);
    let mut jr1 = spawn_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
        false,
    );
    wait_healthy(&mut jr1, &jr1_addr, Duration::from_secs(5)).await;

    let partition_map = format!("j-r={}", jr1_addr);
    let mut lb = spawn_lb(plb, &partition_map, 1, 2);
    let lb_addr = format!("http://127.0.0.1:{}", plb);
    wait_healthy(&mut lb, &lb_addr, Duration::from_secs(5)).await;

    let err = put_word(&lb_addr, "power", "jokerz").await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
