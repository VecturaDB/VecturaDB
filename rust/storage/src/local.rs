// Changed from the original Chroma file; see the git history for the changes and their authors.
use super::config::StorageConfig;
use super::StorageConfigError;
use async_trait::async_trait;
use bytes::Bytes;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use futures::{Stream, StreamExt};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::{hash::DefaultHasher, path::Path};
use thiserror::Error;

use crate::{ETag, PutOptions, StorageError};

#[derive(Debug, Error)]
#[error("Local storage error")]
pub struct LocalStoraegError;

#[derive(Clone)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: &str) -> LocalStorage {
        // Create the local storage with the root path.
        LocalStorage {
            root: Path::new(root).to_path_buf(),
        }
    }

    fn etag_for_bytes(bytes: &[u8]) -> ETag {
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        ETag(hasher.finish().to_string())
    }

    fn path_for_key(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, StorageError> {
        let file_path = self.path_for_key(key);
        if !file_path.exists() {
            return Err(StorageError::NotFound {
                path: file_path
                    .to_str()
                    .expect("File path should be valid string")
                    .to_string(),
                source: Arc::new(LocalStoraegError),
            });
        }
        match std::fs::read(file_path) {
            Ok(bytes_u8) => Ok(Arc::new(bytes_u8)),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn get_with_e_tag(
        &self,
        key: &str,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let bytes = self.get(key).await?;
        let etag = Self::etag_for_bytes(&bytes);
        Ok((bytes, Some(etag)))
    }

    pub async fn confirm_same(&self, _: &str, _: &ETag) -> Result<bool, StorageError> {
        Err(StorageError::NotImplemented)
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        _options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        // TODO: Handle options
        let file_path = self.path_for_key(key);
        let parent = file_path
            .parent()
            .expect("Parent should be present for the file path");
        Self::write_durably(parent, &file_path, bytes).map_err(|e| StorageError::Generic {
            source: Arc::new(e),
        })?;
        Ok(None)
    }

    // A file written in full next to its key, synced, then renamed over the key: a crash leaves the old file or the
    // new one, never a part of one, and the bytes are on disk before the caller records the key.
    // The file gets the permissions that std::fs::write gave it, 0666 less the umask, not the 0600 of a temporary file.
    fn write_durably(parent: &Path, file_path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        std::fs::create_dir_all(parent)?;
        let mut builder = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o666));
        }
        let mut file = builder.tempfile_in(parent)?;
        file.write_all(bytes)?;
        file.as_file().sync_all()?;
        let temp_path = file.into_temp_path();
        std::fs::rename(&temp_path, file_path)?;
        let _ = temp_path.keep();
        #[cfg(unix)]
        Self::folder_synced(std::fs::File::open(parent)?.sync_all())?;
        Ok(())
    }

    // The sync of the folder makes the rename survive a crash. Some file systems refuse to sync a folder, with EINVAL or
    // ENOTSUP (some FUSE, network and CIFS mounts, vboxsf): the file is already in place, so, as in SQLite, that is not
    // an error. Any other error is.
    fn folder_synced(result: std::io::Result<()>) -> std::io::Result<()> {
        match result {
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
                ) =>
            {
                tracing::warn!(error = %err, "the file system does not sync folders");
                Ok(())
            }
            other => other,
        }
    }

    pub async fn put_file(
        &self,
        key: &str,
        path: &str,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let file = std::fs::read(path);
        match file {
            Ok(bytes_u8) => self.put_bytes(key, &bytes_u8, options).await,
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn put_stream<S>(
        &self,
        key: &str,
        _total_size_bytes: usize,
        mut stream: S,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError>
    where
        S: Stream<Item = Result<Bytes, StorageError>> + Send + Unpin,
    {
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        self.put_bytes(key, &buf, options).await
    }

    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let file_path = self.path_for_key(key);

        match std::fs::remove_file(&file_path) {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<crate::s3::DeletedObjects, StorageError> {
        let mut deleted_objects = crate::s3::DeletedObjects::default();
        for key in keys {
            let file_path = self.path_for_key(key.as_ref());
            match std::fs::remove_file(&file_path) {
                Ok(_) => {
                    deleted_objects.deleted.push(key.as_ref().to_string());
                }
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        deleted_objects.errors.push(StorageError::NotFound {
                            path: key.as_ref().to_string(),
                            source: Arc::new(e),
                        });
                    }
                    _ => {
                        deleted_objects.errors.push(StorageError::Generic {
                            source: Arc::new(e),
                        });
                    }
                },
            }
        }
        Ok(deleted_objects)
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let src_path = self.path_for_key(src_key);
        let dst_path = self.path_for_key(dst_key);

        // Create parent directory for destination if it doesn't exist
        if let Some(parent) = std::path::Path::new(&dst_path).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(error = %e, path = %parent.display(), "Failed to create parent directory");
                return Err(StorageError::Generic {
                    source: Arc::new(e),
                });
            }
        }

        match std::fs::rename(&src_path, &dst_path) {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let src_path = self.path_for_key(src_key);
        let dst_path = self.path_for_key(dst_key);

        // Create parent directory for destination if it doesn't exist
        if let Some(parent) = std::path::Path::new(&dst_path).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(error = %e, path = %parent.display(), "Failed to create parent directory");
                return Err(StorageError::Generic {
                    source: Arc::new(e),
                });
            }
        }

        match std::fs::copy(&src_path, &dst_path) {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let search_path = self.path_for_key(prefix);
        if !search_path.exists() {
            return Ok(Vec::new());
        }
        let entries = std::fs::read_dir(search_path).map_err(|e| StorageError::Generic {
            source: Arc::new(e),
        })?;
        entries
            .into_iter()
            .map(|e| {
                e.map_err(|e| StorageError::Generic {
                    source: Arc::new(e),
                })
                .and_then(|e| {
                    e.file_name()
                        .to_str()
                        .map(|s| s.to_string())
                        .ok_or(StorageError::Message {
                            message: "Unable to convert path to string".to_string(),
                        })
                })
            })
            .collect()
    }
}

#[async_trait]
impl Configurable<StorageConfig> for LocalStorage {
    async fn try_from_config(
        config: &StorageConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::Local(local_config) => {
                let storage = LocalStorage::new(&local_config.root);
                Ok(storage)
            }
            _ => Err(Box::new(StorageConfigError::InvalidStorageConfig)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A folder that cannot be synced does not fail a write whose file is in place; any other error does.
    #[test]
    fn a_folder_that_cannot_be_synced_does_not_fail_the_write() {
        for refused in [
            std::io::ErrorKind::InvalidInput,
            std::io::ErrorKind::Unsupported,
        ] {
            assert!(LocalStorage::folder_synced(Err(refused.into())).is_ok());
        }
        assert!(LocalStorage::folder_synced(Err(std::io::ErrorKind::Other.into())).is_err());
        assert!(LocalStorage::folder_synced(Ok(())).is_ok());
    }

    // The files of a key get the permissions of std::fs::write, as before the writes went through a temporary file.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_key_gets_the_permissions_of_a_plain_write() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(root.path().to_str().unwrap());
        storage
            .put_bytes("block/key", b"bytes", PutOptions::default())
            .await
            .unwrap();
        let plain = root.path().join("plain");
        std::fs::write(&plain, b"bytes").unwrap();
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&root.path().join("block/key")), mode(&plain));
        assert_eq!(std::fs::read(root.path().join("block/key")).unwrap(), b"bytes");
    }
}
