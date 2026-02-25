use tokio::sync::RwLock;
use tonic::Status;

use crate::indexer::Indexer;
use crate::partition::PrefixRange;

pub struct ShardStore {
    indexer: RwLock<Indexer>,
    prefix_range: PrefixRange,
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
    pub fn prefix_range(&self) -> PrefixRange {
        self.prefix_range
    }

    pub async fn open(prefix_range: PrefixRange) -> Result<Self, Status> {
        let mut indexer = Indexer::new();
        // Seed from static word lists unless disabled (useful for fast black-box tests).
        let disable_static = std::env::var("DISABLE_STATIC_INDEX").unwrap_or_default() == "1";
        if !disable_static {
            indexer.indexFileForPrefixRange("thoughtspot", "./words.txt", prefix_range.start, prefix_range.end);
            indexer.indexFileForPrefixRange("power", "./words_alpha.txt", prefix_range.start, prefix_range.end);
        }
        Ok(Self {
            indexer: RwLock::new(indexer),
            prefix_range,
        })
    }

    #[cfg(test)]
    pub async fn open_empty_for_tests(prefix_range: PrefixRange) -> Result<Self, Status> {
        Ok(Self {
            indexer: RwLock::new(Indexer::new()),
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
        let indexer = self.indexer.read().await;
        let k = if top_k == 0 {
            usize::MAX
        } else {
            top_k as usize
        };
        Ok(indexer.prefixMatchTopK(tenant, &prefix, k))
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
