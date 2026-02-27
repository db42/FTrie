mod common;

use std::time::Duration;

use common::*;
use tonic::Code;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn putword_routes_to_correct_shard() {
    if skip_if_not_blackbox() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let ai_dir = tmp.path().join("ai");
    let tz_dir = tmp.path().join("tz");
    std::fs::create_dir_all(&ai_dir).unwrap();
    std::fs::create_dir_all(&tz_dir).unwrap();

    let pai = pick_unused_port();
    let ptz = pick_unused_port();
    let plb = pick_unused_port();

    let (_ai, ai_addr) = start_server(
        pai,
        "ai",
        "a-i",
        ai_dir.to_str().unwrap(),
        false,
    )
    .await;
    let (_tz, tz_addr) = start_server(
        ptz,
        "tz",
        "t-z",
        tz_dir.to_str().unwrap(),
        false,
    )
    .await;

    let partition_map = format!("a-i={},t-z={}", ai_addr, tz_addr);
    let (_lb, lb_addr) = start_lb(plb, &partition_map, 1).await;

    put_word(&lb_addr, "power", "apricot").await.unwrap();
    wait_until_contains(&ai_addr, "power", "apr", "apricot").await;
    assert_not_contains_for(
        &tz_addr,
        "power",
        "apr",
        "apricot",
        Duration::from_secs(1),
    )
    .await;

    put_word(&lb_addr, "power", "terse").await.unwrap();
    wait_until_contains(&tz_addr, "power", "ter", "terse").await;
    assert_not_contains_for(
        &ai_addr,
        "power",
        "ter",
        "terse",
        Duration::from_secs(1),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ha_read_succeeds_when_one_replica_down_with_r1() {
    if skip_if_not_blackbox() {
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

    let (jr1_proc, jr1_addr) = start_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        true,
    )
    .await;
    let mut jr1 = Some(jr1_proc);
    let (_jr2, jr2_addr) = start_server(
        p2,
        "jr2",
        "j-r",
        jr2_dir.to_str().unwrap(),
        true,
    )
    .await;

    let partition_map = format!("j-r={}|{}", jr1_addr, jr2_addr);
    let (_lb, lb_addr) = start_lb(plb, &partition_map, 1).await;

    put_word(&lb_addr, "power", "jokerz").await.unwrap();
    wait_until_contains(&jr1_addr, "power", "joker", "jokerz").await;
    wait_until_contains(&jr2_addr, "power", "joker", "jokerz").await;

    drop(jr1.take());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match get_prefix_match_result(&lb_addr, "power", "joker").await {
            Ok(msg) => {
                assert!(msg.contains("node=jr2"), "expected jr2 reply, got: {}", msg);
                break;
            }
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("timed out waiting for HA read to succeed: {}", e);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lb_rejects_invalid_prefix() {
    if skip_if_not_blackbox() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let jr1_dir = tmp.path().join("jr1");
    std::fs::create_dir_all(&jr1_dir).unwrap();

    let p1 = pick_unused_port();
    let plb = pick_unused_port();

    let (_jr1, jr1_addr) = start_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
    )
    .await;

    let partition_map = format!("j-r={}", jr1_addr);
    let (_lb, lb_addr) = start_lb(plb, &partition_map, 1).await;

    let err = get_prefix_match_result(&lb_addr, "power", "jo2").await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
