use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use fst::Automaton;
use fst::IntoStreamer;
use fst::Streamer;
use fst::automaton::Str;

/// Read-only per-tenant prefix index backed by an FST.
///
/// This is a Tier-4 style backend: it is optimized for fast concurrent reads and
/// expects updates to happen via rebuild + swap, not per-word mutation.
pub struct FstIndex {
    tenant_sets: HashMap<String, fst::Set<Vec<u8>>>,
}

fn lines_from_file(filename: impl AsRef<Path>) -> std::io::Result<Vec<String>> {
    let file = File::open(filename)?;
    let buf = BufReader::new(file);
    let mut out = Vec::new();
    for line in buf.lines() {
        out.push(line?);
    }
    Ok(out)
}

fn build_set_from_words(mut words: Vec<String>) -> fst::Set<Vec<u8>> {
    // FST keys must be inserted in strictly increasing order.
    words.sort();
    words.dedup();

    let mut bytes = Vec::new();
    {
        let mut builder = fst::SetBuilder::new(&mut bytes).expect("fst builder");
        for w in words {
            // Words are already validated by the caller.
            builder.insert(w).expect("fst insert");
        }
        builder.finish().expect("fst finish");
    }

    fst::Set::new(bytes).expect("fst set from bytes")
}

impl FstIndex {
    pub fn empty() -> Self {
        Self {
            tenant_sets: HashMap::new(),
        }
    }

    pub fn from_words_for_tenant(tenant: &str, words: Vec<String>) -> Self {
        let mut tenant_sets = HashMap::new();
        tenant_sets.insert(tenant.to_string(), build_set_from_words(words));
        Self { tenant_sets }
    }

    pub fn index_file_for_prefix_range<F>(
        &mut self,
        tenant_id: &str,
        file_path: &str,
        keep: F,
    ) -> Result<(), std::io::Error>
    where
        F: Fn(&str) -> bool,
    {
        let words = lines_from_file(file_path)?;
        let mut filtered = Vec::new();
        for w in words.into_iter() {
            if keep(&w) {
                filtered.push(w);
            }
        }
        self.tenant_sets
            .insert(tenant_id.to_string(), build_set_from_words(filtered));
        Ok(())
    }

    pub fn prefix_match_top_k(&self, tenant: &str, prefix: &str, top_k: usize) -> Vec<String> {
        let Some(set) = self.tenant_sets.get(tenant) else {
            return Vec::new();
        };

        // Stream all keys with the prefix and take the first K.
        let aut = Str::new(prefix).starts_with();
        let mut stream = set.search(aut).into_stream();

        let mut out = Vec::with_capacity(top_k.min(64));
        // Keep memory bounded even if callers accidentally pass huge K.
        let limit = top_k.min(1024);
        while out.len() < limit {
            let Some(k) = stream.next() else {
                break;
            };
            // Our keys are ASCII/UTF-8; avoid the overhead of lossy conversion.
            out.push(std::str::from_utf8(k).unwrap_or("").to_string());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fst_prefix_top_k_is_lexicographic_and_bounded() {
        let idx = FstIndex::from_words_for_tenant(
            "t",
            vec![
                "app".to_string(),
                "apple".to_string(),
                "apply".to_string(),
                "apt".to_string(),
                "banana".to_string(),
            ],
        );

        assert_eq!(
            idx.prefix_match_top_k("t", "app", 2),
            vec!["app".to_string(), "apple".to_string()]
        );
        assert_eq!(
            idx.prefix_match_top_k("t", "ap", 10),
            vec![
                "app".to_string(),
                "apple".to_string(),
                "apply".to_string(),
                "apt".to_string()
            ]
        );
        assert!(idx.prefix_match_top_k("t", "zzz", 10).is_empty());
    }
}
