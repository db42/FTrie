use tokio::sync::RwLock;
use tonic::Status;

use crate::indexer::Indexer;
use crate::fst_index::FstIndex;
use crate::partition::PrefixRange;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexBackend {
    Trie,
    Fst,
}

fn index_backend_from_env() -> IndexBackend {
    match std::env::var("INDEX_BACKEND")
        .unwrap_or_else(|_| "trie".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "fst" => IndexBackend::Fst,
        _ => IndexBackend::Trie,
    }
}

pub struct ShardStore {
    backend: IndexBackend,
    indexer: RwLock<Indexer>,
    fst_index: FstIndex,
    prefix_range: PrefixRange,
}

fn words_file_from_env(key: &str, default_value: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default_value.to_string())
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

impl ShardStore {
    pub fn backend_name(&self) -> &'static str {
        match self.backend {
            IndexBackend::Trie => "trie",
            IndexBackend::Fst => "fst",
        }
    }

    pub fn prefix_range(&self) -> PrefixRange {
        self.prefix_range
    }

    pub async fn open(prefix_range: PrefixRange) -> Result<Self, Status> {
        let backend = index_backend_from_env();
        let mut indexer = Indexer::new();
        let mut fst_index = FstIndex::empty();
        let words_thoughtspot = words_file_from_env("WORDS_THOUGHTSPOT_FILE", "./words.txt");
        let words_power = words_file_from_env("WORDS_POWER_FILE", "./words_alpha.txt");
        // Seed from static word lists unless disabled (useful for fast black-box tests).
        let disable_static = std::env::var("DISABLE_STATIC_INDEX").unwrap_or_default() == "1";
        if !disable_static {
            match backend {
                IndexBackend::Trie => {
                    indexer.indexFileForPrefixRange(
                        "thoughtspot",
                        &words_thoughtspot,
                        prefix_range.start,
                        prefix_range.end,
                    );
                    indexer.indexFileForPrefixRange(
                        "power",
                        &words_power,
                        prefix_range.start,
                        prefix_range.end,
                    );
                }
                IndexBackend::Fst => {
                    let start = prefix_range.start.to_ascii_lowercase();
                    let end = prefix_range.end.to_ascii_lowercase();
                    let keep = move |word: &str| {
                        let c = match word.chars().next() {
                            Some(c) => c.to_ascii_lowercase(),
                            None => return false,
                        };
                        ('a'..='z').contains(&c) && c >= start && c <= end
                    };
                    fst_index
                        .index_file_for_prefix_range("thoughtspot", &words_thoughtspot, keep)
                        .map_err(|e| Status::internal(format!("fst load failed: {}", e)))?;
                    // Recreate closure because `keep` was moved above.
                    let start = prefix_range.start.to_ascii_lowercase();
                    let end = prefix_range.end.to_ascii_lowercase();
                    let keep2 = move |word: &str| {
                        let c = match word.chars().next() {
                            Some(c) => c.to_ascii_lowercase(),
                            None => return false,
                        };
                        ('a'..='z').contains(&c) && c >= start && c <= end
                    };
                    fst_index
                        .index_file_for_prefix_range("power", &words_power, keep2)
                        .map_err(|e| Status::internal(format!("fst load failed: {}", e)))?;
                }
            }
        }
        Ok(Self {
            backend,
            indexer: RwLock::new(indexer),
            fst_index,
            prefix_range,
        })
    }

    #[cfg(test)]
    pub async fn open_empty_for_tests(prefix_range: PrefixRange) -> Result<Self, Status> {
        Ok(Self {
            backend: IndexBackend::Trie,
            indexer: RwLock::new(Indexer::new()),
            fst_index: FstIndex::empty(),
            prefix_range,
        })
    }

    pub async fn prefix_match(&self, tenant: &str, prefix: &str) -> Result<Vec<String>, Status> {
        self.prefix_match_top_k(tenant, prefix, 0).await
    }

    pub async fn prefix_match_top_k(
        &self,
        tenant: &str,
        prefix: &str,
        top_k: u32,
    ) -> Result<Vec<String>, Status> {
        if tenant.trim().is_empty() {
            return Err(Status::invalid_argument("tenant must be non-empty"));
        }
        let prefix = normalize_and_validate_prefix(prefix)?;
        let k = if top_k == 0 {
            usize::MAX
        } else {
            top_k as usize
        };
        match self.backend {
            IndexBackend::Trie => {
                let indexer = self.indexer.read().await;
                Ok(indexer.prefixMatchTopK(tenant, &prefix, k))
            }
            IndexBackend::Fst => Ok(self.fst_index.prefix_match_top_k(tenant, &prefix, k)),
        }
    }

    // Applies a validated write to the in-memory index.
    //
    // In a leader-based design, the leader should only call this after a log entry is committed;
    // followers apply committed entries the same way. Durability comes from the Raft log and
    // (future) snapshots.
    pub async fn apply_word(&self, tenant: &str, word: &str) -> Result<(), Status> {
        if tenant.trim().is_empty() {
            return Err(Status::invalid_argument("tenant must be non-empty"));
        }
        let word = normalize_and_validate_word(word)?;
        if !self.prefix_range.contains_first_char_of(&word) {
            return Err(Status::invalid_argument("word does not belong to this shard"));
        }

        if self.backend == IndexBackend::Fst {
            return Err(Status::failed_precondition(
                "INDEX_BACKEND=fst is read-only; rebuild required",
            ));
        }

        let mut indexer = self.indexer.write().await;
        indexer.putWord(tenant, &word);
        Ok(())
    }

    // In Raft mode, durability comes from the Raft log.
    pub async fn put_word(&self, tenant: &str, word: &str) -> Result<(), Status> {
        self.apply_word(tenant, word).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_word_rejects_non_az() {
        let store = ShardStore::open_empty_for_tests(PrefixRange::parse("j-r").unwrap())
            .await
            .unwrap();
        let err = store.put_word("power", "jokerz2").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn put_word_rejects_out_of_range() {
        let store = ShardStore::open_empty_for_tests(PrefixRange::parse("j-r").unwrap())
            .await
            .unwrap();
        let err = store.put_word("power", "apple").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn prefix_match_top_k_is_bounded() {
        let store = ShardStore::open_empty_for_tests(PrefixRange::parse("a-z").unwrap())
            .await
            .unwrap();

        store.put_word("power", "app").await.unwrap();
        store.put_word("power", "apple").await.unwrap();
        store.put_word("power", "apply").await.unwrap();

        let r1 = store.prefix_match_top_k("power", "app", 1).await.unwrap();
        assert_eq!(r1, vec!["app".to_string()]);

        let r2 = store.prefix_match_top_k("power", "app", 2).await.unwrap();
        assert_eq!(r2.len(), 2);
        assert_eq!(r2[0], "app");

        let r50 = store.prefix_match_top_k("power", "app", 50).await.unwrap();
        assert_eq!(
            r50,
            vec!["app".to_string(), "apple".to_string(), "apply".to_string()]
        );
    }
}
