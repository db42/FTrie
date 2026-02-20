use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

pub fn wal_path(data_dir: &str, tenant: &str) -> PathBuf {
    let mut pb = PathBuf::from(data_dir);
    pb.push(format!("{}.wal", tenant));
    pb
}

pub async fn ensure_dir(data_dir: &str) -> io::Result<()> {
    fs::create_dir_all(data_dir).await
}

pub async fn append_word(path: &Path, word: &str, fsync: bool) -> io::Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    file.write_all(word.as_bytes()).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    if fsync {
        file.sync_all().await?;
    }
    Ok(())
}

pub async fn replay_words(path: &Path) -> io::Result<Vec<String>> {
    let meta = fs::metadata(path).await;
    if meta.is_err() {
        return Ok(Vec::new());
    }

    let file = fs::File::open(path).await?;
    let mut reader = BufReader::new(file);
    let mut out = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let w = line.trim();
        if !w.is_empty() {
            out.push(w.to_string());
        }
    }
    Ok(out)
}

