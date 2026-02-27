mod common;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_prefix_match_honors_top_k_and_returns_matches() {
    if skip_if_not_blackbox() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("node");
    std::fs::create_dir_all(&data_dir).unwrap();

    let p = pick_unused_port();
    let plb = pick_unused_port();

    let (_srv, srv_addr) = start_server(
        p,
        "n1",
        "a-z",
        data_dir.to_str().unwrap(),
        false,
    )
    .await;

    let partition_map = format!("a-z={}", srv_addr);
    let (_lb, lb_addr) = start_lb(plb, &partition_map, 1).await;

    // Seed: a few deterministic completions.
    put_word(&lb_addr, "power", "app").await.unwrap();
    put_word(&lb_addr, "power", "apple").await.unwrap();
    put_word(&lb_addr, "power", "apply").await.unwrap();

    // top_k=1 returns just the first completion (and should include the prefix itself if it's a word).
    let r1 = get_prefix_match_reply(&lb_addr, "power", "app", 1).await;
    assert_eq!(r1.matches, vec!["app".to_string()]);

    // top_k=2 should be stable and bounded.
    let r2 = get_prefix_match_reply(&lb_addr, "power", "app", 2).await;
    assert_eq!(r2.matches.len(), 2);
    assert_eq!(r2.matches[0], "app");
    assert!(r2.matches.contains(&"apple".to_string()) || r2.matches.contains(&"apply".to_string()));

    // top_k=50 should return all 3 in this test.
    let r50 = get_prefix_match_reply(&lb_addr, "power", "app", 50).await;
    assert_eq!(r50.matches, vec!["app".to_string(), "apple".to_string(), "apply".to_string()]);
}
