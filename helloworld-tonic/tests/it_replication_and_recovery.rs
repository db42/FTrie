mod common;

use std::time::Duration;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn putword_replicates_to_all_replicas_eventually() {
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

    let (_jr1, jr1_addr) = start_server(
        p1,
        "jr1",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
        false,
    )
    .await;
    let (_jr2, jr2_addr) = start_server(
        p2,
        "jr2",
        "j-r",
        jr2_dir.to_str().unwrap(),
        false,
        false,
    )
    .await;

    let partition_map = format!("j-r={}|{}", jr1_addr, jr2_addr);
    let (_lb, lb_addr) = start_lb(plb, &partition_map, 1, 1).await;

    put_word(&lb_addr, "power", "jokerzonlytwo").await.unwrap();

    // With W=1, LB ACKs after one replica succeeds but should still push to the other replica.
    wait_until_contains(&jr1_addr, "power", "joker", "jokerzonlytwo").await;
    wait_until_contains(&jr2_addr, "power", "joker", "jokerzonlytwo").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_restart_replays_wal() {
    if skip_if_not_blackbox() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("jr1");
    std::fs::create_dir_all(&data_dir).unwrap();

    let p1 = pick_unused_port();
    let plb = pick_unused_port();
    let jr1_addr = http_addr(p1);

    {
        let (_jr1, _jr1_addr2) = start_server(
            p1,
            "jr1",
            "j-r",
            data_dir.to_str().unwrap(),
            false,
            true,
        )
        .await;

        let partition_map = format!("j-r={}", jr1_addr);
        let (_lb, lb_addr) = start_lb(plb, &partition_map, 1, 1).await;

        put_word(&lb_addr, "power", "jokerz").await.unwrap();
        wait_until_contains(&jr1_addr, "power", "joker", "jokerz").await;
    }

    // Restart replica with the same DATA_DIR: should replay WAL and still serve jokerz.
    let (_jr1b, _jr1_addr2) = start_server(
        p1,
        "jr1b",
        "j-r",
        data_dir.to_str().unwrap(),
        false,
        true,
    )
    .await;
    wait_until_contains(&jr1_addr, "power", "joker", "jokerz").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_misses_writes_while_down_without_catchup() {
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
        false,
        false,
    )
    .await;
    let mut jr1 = Some(jr1_proc);
    let (_jr2, jr2_addr) = start_server(
        p2,
        "jr2",
        "j-r",
        jr2_dir.to_str().unwrap(),
        false,
        false,
    )
    .await;

    let partition_map = format!("j-r={}|{}", jr1_addr, jr2_addr);
    let (_lb, lb_addr) = start_lb(plb, &partition_map, 1, 1).await;

    // 1) Write with both replicas up. Ensure both replicas eventually have it.
    put_word(&lb_addr, "power", "jokerz").await.unwrap();
    wait_until_contains(&jr1_addr, "power", "joker", "jokerz").await;
    wait_until_contains(&jr2_addr, "power", "joker", "jokerz").await;

    // 2) Stop jr1, write a new word. jr2 should have it; jr1 should miss it.
    drop(jr1.take());
    put_word(&lb_addr, "power", "jokerzonlytwo").await.unwrap();
    wait_until_contains(&jr2_addr, "power", "joker", "jokerzonlytwo").await;

    // 3) Restart jr1 from its WAL dir. It should still have jokerz but not jokerzonlytwo.
    let (_jr1b, _jr1_addr2) = start_server(
        p1,
        "jr1b",
        "j-r",
        jr1_dir.to_str().unwrap(),
        false,
        false,
    )
    .await;
    wait_until_contains(&jr1_addr, "power", "joker", "jokerz").await;
    assert_not_contains_for(
        &jr1_addr,
        "power",
        "joker",
        "jokerzonlytwo",
        Duration::from_secs(1),
    )
    .await;
}
