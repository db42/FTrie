use std::fs::File;
use std::io::{BufRead, BufReader};
use std::mem::size_of;
use std::time::Instant;

#[path = "./partition.rs"]
mod partition;
use partition::PrefixRange;

#[path = "./trie.rs"]
mod trie;
use trie::Node;

fn lines_from_file(path: &str) -> std::io::Result<Vec<String>> {
    let f = File::open(path)?;
    let r = BufReader::new(f);
    let mut out = Vec::new();
    for line in r.lines() {
        out.push(line?);
    }
    Ok(out)
}

fn keep_for_range(range: PrefixRange) -> impl Fn(&str) -> bool {
    let start = range.start.to_ascii_lowercase();
    let end = range.end.to_ascii_lowercase();
    move |word: &str| {
        let c = match word.chars().next() {
            Some(c) => c.to_ascii_lowercase(),
            None => return false,
        };
        ('a'..='z').contains(&c) && c >= start && c <= end
    }
}

fn build_trie_from_file(path: &str, range: PrefixRange) -> std::io::Result<Node> {
    let keep = keep_for_range(range);
    let words = lines_from_file(path)?;
    let mut t = Node::new();
    for w in words.iter() {
        if keep(w) {
            trie::addWord(&mut t, w);
        }
    }
    Ok(t)
}

fn count_nodes(t: &Node) -> u64 {
    trie::node_count(t)
}

fn build_fst_bytes_from_file(path: &str, range: PrefixRange) -> std::io::Result<Vec<u8>> {
    use fst::SetBuilder;

    let keep = keep_for_range(range);
    let words = lines_from_file(path)?;
    let mut filtered: Vec<&str> = words.iter().filter(|w| keep(w)).map(|s| s.as_str()).collect();
    filtered.sort_unstable();
    filtered.dedup();

    let mut bytes = Vec::new();
    {
        let mut b = SetBuilder::new(&mut bytes).expect("fst builder");
        for w in filtered {
            b.insert(w).expect("fst insert");
        }
        b.finish().expect("fst finish");
    }
    Ok(bytes)
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let prefix_range = env_or("PREFIX_RANGE", "a-z");
    let range = PrefixRange::parse(&prefix_range)?;

    let words_ts = "./words.txt";
    let words_power = "./words_alpha.txt";

    println!("PREFIX_RANGE={}-{}", range.start, range.end);
    println!("Input files:");
    println!("  thoughtspot: {}", words_ts);
    println!("  power:      {}", words_power);

    // FST sizing (actual bytes).
    let t0 = Instant::now();
    let fst_ts = build_fst_bytes_from_file(words_ts, range)?;
    let fst_power = build_fst_bytes_from_file(words_power, range)?;
    let fst_ms = t0.elapsed().as_millis();

    println!("");
    println!("FST (actual bytes, in-process Vec<u8>; mmap would be similar on disk):");
    println!("  thoughtspot.fst_bytes={}", fst_ts.len());
    println!("  power.fst_bytes={}", fst_power.len());
    println!("  total_fst_bytes={}", fst_ts.len() + fst_power.len());
    println!("  build_ms={}", fst_ms);

    // Trie sizing (rough estimate).
    //
    // The current trie implementation is extremely allocation-heavy:
    // each node has a Vec with capacity 26 of Option<Node>, where Option<Node>
    // is stored inline and is roughly the size of Node itself.
    //
    // We provide:
    // - a simple per-node byte estimate based on type sizes
    // - node count is not available without making trie internals accessible
    //   (we can add a pub fn for that next, if you want exact counts).
    println!("");
    println!("Trie (estimates):");
    println!("  size_of::<Node>()={}", size_of::<Node>());
    println!("  size_of::<Option<Node>>()={}", size_of::<Option<Node>>());
    println!(
        "  approx_bytes_per_node = size_of::<Node>() + 26*size_of::<Option<Node>>() = {}",
        size_of::<Node>() + 26 * size_of::<Option<Node>>()
    );
    let t1 = Instant::now();
    let trie_ts = build_trie_from_file(words_ts, range)?;
    let trie_power = build_trie_from_file(words_power, range)?;
    let trie_ms = t1.elapsed().as_millis();

    let nodes_ts = count_nodes(&trie_ts);
    let nodes_power = count_nodes(&trie_power);
    let nodes_total = nodes_ts + nodes_power;
    let per = (size_of::<Node>() + 26 * size_of::<Option<Node>>()) as u64;

    println!("  thoughtspot.node_count={}", nodes_ts);
    println!("  power.node_count={}", nodes_power);
    println!("  total_node_count={}", nodes_total);
    println!("  approx_bytes_per_node={}", per);
    println!("  approx_trie_bytes={}", nodes_total.saturating_mul(per));
    println!("  build_ms={}", trie_ms);

    let fst_total = (fst_ts.len() + fst_power.len()) as u64;
    if fst_total > 0 {
        let approx_trie = nodes_total.saturating_mul(per);
        println!("");
        println!("Ratio (approx):");
        println!("  approx_trie_bytes / total_fst_bytes = {:.2}x", (approx_trie as f64) / (fst_total as f64));
    }

    Ok(())
}
