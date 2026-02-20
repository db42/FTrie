use std::path::PathBuf;

use tokio::fs;
use tokio::sync::RwLock;
use tonic::Status;

use crate::indexer::Indexer;
use crate::partition::PrefixRange;
use crate::wal::{append_word, ensure_dir, replay_words, wal_path};

pub struct ShardStore {
    indexer: RwLock<Indexer>,
    prefix_range: PrefixRange,
    data_dir: String,
    fsync: bool,
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

    pub async fn open(
        prefix_range: PrefixRange,
        data_dir: String,
        fsync: bool,
    ) -> Result<Self, Status> {
        ensure_dir(&data_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to create DATA_DIR {}: {}", data_dir, e)))?;

        let mut indexer = Indexer::new();
        // Phase 3 behavior: seed from static word lists unless disabled (useful for fast black-box tests).
        let disable_static = std::env::var("DISABLE_STATIC_INDEX").unwrap_or_default() == "1";
        if !disable_static {
            indexer.indexFileForPrefixRange("thoughtspot", "./words.txt", prefix_range.start, prefix_range.end);
            indexer.indexFileForPrefixRange("power", "./words_alpha.txt", prefix_range.start, prefix_range.end);
        }

        Self::replay_all_wals(&mut indexer, &prefix_range, &data_dir).await?;

        Ok(Self {
            indexer: RwLock::new(indexer),
            prefix_range,
            data_dir,
            fsync,
        })
    }

    #[cfg(test)]
    pub async fn open_empty_for_tests(
        prefix_range: PrefixRange,
        data_dir: String,
        fsync: bool,
    ) -> Result<Self, Status> {
        ensure_dir(&data_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to create DATA_DIR {}: {}", data_dir, e)))?;

        let mut indexer = Indexer::new();
        Self::replay_all_wals(&mut indexer, &prefix_range, &data_dir).await?;

        Ok(Self {
            indexer: RwLock::new(indexer),
            prefix_range,
            data_dir,
            fsync,
        })
    }

    async fn replay_all_wals(
        indexer: &mut Indexer,
        prefix_range: &PrefixRange,
        data_dir: &str,
    ) -> Result<(), Status> {
        let mut dir = fs::read_dir(data_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to read DATA_DIR {}: {}", data_dir, e)))?;
        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|e| Status::internal(format!("failed to scan DATA_DIR {}: {}", data_dir, e)))?
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("wal") {
                continue;
            }
            let tenant = match path.file_stem().and_then(|s| s.to_str()) {
                Some(t) if !t.trim().is_empty() => t.to_string(),
                _ => continue,
            };

            let words = replay_words(&path)
                .await
                .map_err(|e| Status::internal(format!("failed to replay WAL {:?}: {}", path, e)))?;
            for w in words {
                if !prefix_range.contains_first_char_of(&w) {
                    continue;
                }
                if let Ok(w) = normalize_and_validate_word(&w) {
                    indexer.putWord(&tenant, &w);
                }
            }
        }
        Ok(())
    }

    pub async fn prefix_match(&self, tenant: &str, prefix: &str) -> Result<Vec<String>, Status> {
        if tenant.trim().is_empty() {
            return Err(Status::invalid_argument("tenant must be non-empty"));
        }
        let prefix = normalize_and_validate_prefix(prefix)?;
        let indexer = self.indexer.read().await;
        Ok(indexer.prefixMatch(tenant, &prefix))
    }

    pub async fn put_word(&self, tenant: &str, word: &str) -> Result<(), Status> {
        if tenant.trim().is_empty() {
            return Err(Status::invalid_argument("tenant must be non-empty"));
        }
        let word = normalize_and_validate_word(word)?;
        if !self.prefix_range.contains_first_char_of(&word) {
            return Err(Status::invalid_argument("word does not belong to this shard"));
        }

        let wal: PathBuf = wal_path(&self.data_dir, tenant);
        append_word(&wal, &word, self.fsync)
            .await
            .map_err(|e| Status::internal(format!("wal append failed: {}", e)))?;

        let mut indexer = self.indexer.write().await;
        indexer.putWord(tenant, &word);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_word_rejects_non_az() {
        let dir = tempfile::tempdir().unwrap();
        let store = ShardStore::open_empty_for_tests(
            PrefixRange::parse("j-r").unwrap(),
            dir.path().to_str().unwrap().to_string(),
            false,
        )
        .await
        .unwrap();

        let err = store.put_word("power", "jokerz2").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn put_word_rejects_out_of_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = ShardStore::open_empty_for_tests(
            PrefixRange::parse("j-r").unwrap(),
            dir.path().to_str().unwrap().to_string(),
            false,
        )
        .await
        .unwrap();

        let err = store.put_word("power", "apple").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn wal_is_replayed_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        {
            let store = ShardStore::open_empty_for_tests(
                PrefixRange::parse("j-r").unwrap(),
                data_dir.clone(),
                false,
            )
            .await
            .unwrap();
            store.put_word("power", "jokerz").await.unwrap();
        }

        // "Restart": reopen from same DATA_DIR and confirm the word is present.
        let store2 = ShardStore::open_empty_for_tests(
            PrefixRange::parse("j-r").unwrap(),
            data_dir,
            false,
        )
        .await
        .unwrap();

        let matches = store2.prefix_match("power", "joker").await.unwrap();
        assert!(matches.contains(&"jokerz".to_string()));
    }
}
