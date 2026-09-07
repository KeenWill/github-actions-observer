//! Private, immutable snapshots for explicit backfill resumption.

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::{fs, io::AsyncWriteExt};

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) struct ResponseCache {
    directory: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    fetched_at: DateTime<Utc>,
    body: Value,
}

impl ResponseCache {
    pub(crate) async fn open(directory: PathBuf) -> Result<Self> {
        #[cfg(unix)]
        {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&directory).await {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error)
                        .context("cannot create cache directory; its parent must already exist");
                }
            }
            let metadata = fs::symlink_metadata(&directory).await?;
            ensure!(
                metadata.is_dir(),
                "cache directory must be a directory, not a symlink"
            );
            Self::check_permissions(&metadata)?;
            Ok(Self { directory })
        }
        #[cfg(not(unix))]
        {
            let _ = directory;
            anyhow::bail!("private response caching requires Unix file permissions");
        }
    }

    fn path(&self, request_url: &str) -> PathBuf {
        self.directory.join(format!(
            "{}.json",
            hex::encode(Sha256::digest(request_url.as_bytes()))
        ))
    }

    fn check_permissions(metadata: &std::fs::Metadata) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "cache paths must not be accessible to group or other users"
            );
        }
        Ok(())
    }

    pub(crate) async fn read(&self, request_url: &str) -> Result<Option<Value>> {
        let path = self.path(request_url);
        let metadata = match fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("cannot inspect response cache"),
        };
        ensure!(
            metadata.is_file(),
            "cache entry must be a regular file, not a symlink"
        );
        Self::check_permissions(&metadata)?;
        let snapshot: Snapshot = serde_json::from_slice(&fs::read(path).await?)
            .context("invalid cache snapshot; use a new cache directory for a fresh import")?;
        Ok(Some(snapshot.body))
    }

    pub(crate) async fn write(&self, request_url: &str, body: &Value) -> Result<()> {
        let destination = self.path(request_url);
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = self.directory.join(format!(
            ".{}-{}-{sequence}.tmp",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .context("clock outside supported range")?
        ));
        let result = self.write_atomic(&temporary, &destination, body).await;
        if result.is_err() {
            let _ = fs::remove_file(&temporary).await;
        }
        result
    }

    async fn write_atomic(&self, temporary: &Path, destination: &Path, body: &Value) -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(temporary).await?;
        let snapshot = Snapshot {
            fetched_at: Utc::now(),
            body: body.clone(),
        };
        file.write_all(&serde_json::to_vec(&snapshot)?).await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(temporary, destination).await?;
        fs::File::open(&self.directory).await?.sync_all().await?;
        Ok(())
    }
}
