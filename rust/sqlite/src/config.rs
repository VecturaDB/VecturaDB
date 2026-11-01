// Changed from the original Chroma file; see the git history for the changes and their authors.
use std::{path::Path, time::Duration};

use crate::db::{SqliteCreationError, SqliteDb};
use async_trait::async_trait;
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::fs::create_dir_all;

#[cfg(feature = "pyo3")]
use pyo3::{pyclass, pymethods};

fn default_hash_type() -> MigrationHash {
    MigrationHash::MD5
}

fn default_migration_mode() -> MigrationMode {
    MigrationMode::Apply
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct SqliteDBConfig {
    #[serde(default = "default_hash_type")]
    pub hash_type: MigrationHash,
    #[serde(default = "default_migration_mode")]
    pub migration_mode: MigrationMode,
    // The SQLite database URL
    // If unspecified, then the database is in memory only
    pub url: Option<String>,
    /// The journal mode of the database file: wal, the default, or delete. With WAL a read does not wait for a write;
    /// delete, the mode of Chroma, is for a data path on a network filesystem (SMB, NFS), where SQLite cannot use WAL.
    #[serde(default)]
    pub journal_mode: JournalMode,
}

impl Default for SqliteDBConfig {
    fn default() -> Self {
        SqliteDBConfig {
            hash_type: default_hash_type(),
            migration_mode: default_migration_mode(),
            url: None,
            journal_mode: JournalMode::default(),
        }
    }
}

/// The journal mode of the SQLite database file
/// - Wal: the log of the writes in a file of its own, chroma.sqlite3-wal, which a normal stop writes back into the
///   database; reads go on during a write
/// - Delete: a rollback journal for each write, as Chroma
#[derive(Clone, Copy, PartialEq, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[serde(rename_all = "lowercase")]
pub enum JournalMode {
    #[default]
    Wal,
    Delete,
}

/// Migration mode for the database
/// - Apply: Apply the migrations
/// - Validate: Validate the applied migrations and ensure none are unappliued
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[serde(rename_all = "lowercase")]
pub enum MigrationMode {
    Apply,
    Validate,
}

/// The hash function to use for the migration files
/// - SHA256: Use SHA256 hash
/// - MD5: Use MD5 hash
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[serde(rename_all = "lowercase")]
pub enum MigrationHash {
    SHA256,
    MD5,
}

//////////////////////// PyMethods Implementation ////////////////////////

#[cfg(feature = "pyo3")]
#[pymethods]
impl SqliteDBConfig {
    #[new]
    #[pyo3(signature = (hash_type, migration_mode, url=None, journal_mode=None))]
    pub fn py_new(
        hash_type: MigrationHash,
        migration_mode: MigrationMode,
        url: Option<String>,
        journal_mode: Option<JournalMode>,
    ) -> Self {
        SqliteDBConfig {
            hash_type,
            migration_mode,
            url,
            journal_mode: journal_mode.unwrap_or_default(),
        }
    }
}

//////////////////////// Configurable Implementation ////////////////////////

impl Injectable for SqliteDb {}

#[async_trait]
impl Configurable<SqliteDBConfig, SqliteCreationError> for SqliteDb {
    async fn try_from_config(
        config: &SqliteDBConfig,
        registry: &Registry,
    ) -> Result<Self, SqliteCreationError> {
        // TODO: copy all other pragmas from python and add basic tests
        let conn_options = SqliteConnectOptions::new()
            // Due to a bug in the python code, foreign_keys is turned off
            // The python code enabled it in a transaction, however,
            // https://www.sqlite.org/pragma.html states that foreign_keys
            // is a no-op in a transaction. In order to be able to run our migrations
            // we turn it off
            .pragma("foreign_keys", "OFF")
            .pragma("case_sensitive_like", "ON")
            // Each connection keeps a page cache of its own (.cargo/config.toml), of 8000 KiB: the default of 2000 KiB
            // holds 483 pages of 4 KiB, fewer than the 985 that a get of 20000 records reads, so every get read all of
            // them again from the file.
            .pragma("cache_size", "-8000")
            .busy_timeout(Duration::from_secs(1000))
            .with_regexp();
        let conn = if let Some(url) = &config.url {
            let path = Path::new(url);
            if let Some(parent) = path.parent() {
                create_dir_all(parent).await?;
            }
            let journal_mode = match config.journal_mode {
                JournalMode::Wal => SqliteJournalMode::Wal,
                JournalMode::Delete => SqliteJournalMode::Delete,
            };
            SqlitePoolOptions::new()
                .connect_with(
                    conn_options
                        .filename(path)
                        .create_if_missing(true)
                        .journal_mode(journal_mode),
                )
                .await?
        } else {
            SqlitePoolOptions::new()
                .max_lifetime(None)
                .idle_timeout(None)
                .max_connections(1)
                .connect_with(conn_options.in_memory(true).shared_cache(true))
                .await?
        };

        let db = SqliteDb::new(conn, config.hash_type);

        db.initialize_migrations_table().await?;
        match config.migration_mode {
            MigrationMode::Apply => {
                db.apply_all_migration().await?;
            }
            MigrationMode::Validate => db.validate_all_migrations().await?,
        }

        registry.register(db.clone());
        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::test_utils::new_test_db_persist_path;

    use super::*;
    use chroma_config::registry::Registry;

    #[tokio::test]
    async fn test_sqlite_db_config_registry() {
        let config = SqliteDBConfig {
            url: new_test_db_persist_path(),
            hash_type: MigrationHash::SHA256,
            migration_mode: MigrationMode::Apply,
            ..Default::default()
        };

        let registry = Registry::new();
        let _db = SqliteDb::try_from_config(&config, &registry).await.unwrap();
        let _retrieved_db = registry
            .get::<SqliteDb>()
            .expect("To be able to retrieve the db");
    }

    async fn database_file(
        journal_mode: JournalMode,
    ) -> (tempfile::TempDir, std::path::PathBuf, SqliteDb) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chroma.sqlite3");
        let config = SqliteDBConfig {
            url: Some(path.to_str().unwrap().to_string()),
            journal_mode,
            ..Default::default()
        };
        let db = SqliteDb::try_from_config(&config, &Registry::new())
            .await
            .unwrap();
        sqlx::query("CREATE TABLE written (value INTEGER)")
            .execute(db.get_conn())
            .await
            .unwrap();
        sqlx::query("INSERT INTO written VALUES (1), (2), (3)")
            .execute(db.get_conn())
            .await
            .unwrap();
        (dir, path, db)
    }

    async fn journal_mode_of(db: &SqliteDb) -> String {
        sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(db.get_conn())
            .await
            .unwrap()
    }

    fn files_of(dir: &std::path::Path) -> Vec<String> {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        files.sort();
        files
    }

    // A new database file is in WAL, with the log of its writes in a file of its own; a normal close writes the log
    // back, and the database file is the only one left, as with a rollback journal.
    #[tokio::test]
    async fn a_database_file_is_in_wal_and_alone_after_a_close() {
        let (dir, _, db) = database_file(JournalMode::default()).await;
        assert_eq!(journal_mode_of(&db).await, "wal");
        assert!(files_of(dir.path()).contains(&"chroma.sqlite3-wal".to_string()));
        db.close().await;
        assert_eq!(files_of(dir.path()), vec!["chroma.sqlite3"]);
    }

    // A database file opened again, as at the next start of the server, is alone after a close too, without reads and
    // after a read.
    #[tokio::test]
    async fn a_database_file_opened_again_is_alone_after_a_close() {
        let (dir, path, db) = database_file(JournalMode::default()).await;
        db.close().await;
        let config = SqliteDBConfig {
            url: Some(path.to_str().unwrap().to_string()),
            ..Default::default()
        };
        let db = SqliteDb::try_from_config(&config, &Registry::new())
            .await
            .unwrap();
        db.close().await;
        assert_eq!(files_of(dir.path()), vec!["chroma.sqlite3"]);
        let db = SqliteDb::try_from_config(&config, &Registry::new())
            .await
            .unwrap();
        let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM written")
            .fetch_one(db.get_conn())
            .await
            .unwrap();
        assert_eq!(written, 3);
        db.close().await;
        assert_eq!(files_of(dir.path()), vec!["chroma.sqlite3"]);
    }

    // journal_mode delete keeps the rollback journal of Chroma, for a data path on a network filesystem.
    #[tokio::test]
    async fn a_database_file_keeps_a_rollback_journal_when_asked() {
        let (dir, _, db) = database_file(JournalMode::Delete).await;
        assert_eq!(journal_mode_of(&db).await, "delete");
        assert_eq!(files_of(dir.path()), vec!["chroma.sqlite3"]);
        db.close().await;
    }

    // Reads from tasks of their own, as those of the server, leave connections that may still be on their way back to the
    // pool when the database closes: the close still leaves the database file alone.
    #[tokio::test]
    async fn a_database_file_is_alone_after_a_close_that_follows_reads() {
        let (dir, _, db) = database_file(JournalMode::default()).await;
        let reads: Vec<_> = (0..20)
            .map(|_| {
                let pool = db.get_conn().clone();
                tokio::spawn(async move {
                    let _: i64 = sqlx::query_scalar("SELECT value FROM written ORDER BY value")
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                })
            })
            .collect();
        for read in reads {
            read.await.unwrap();
        }
        db.close().await;
        assert_eq!(files_of(dir.path()), vec!["chroma.sqlite3"]);
    }

    // A connection still taken when the database closes does not keep the writes in the log: the close writes them
    // back into the database file and empties the log first, then waits for the connection within its grace. With two
    // idle connections besides it, one for the checkpoint and one that sqlx closes, the wait of sqlx alone would end
    // before it.
    #[tokio::test]
    async fn a_close_writes_the_log_back_while_a_connection_is_taken() {
        let (dir, path, db) = database_file(JournalMode::default()).await;
        let held = db.get_conn().acquire().await.unwrap();
        let others = vec![
            db.get_conn().acquire().await.unwrap(),
            db.get_conn().acquire().await.unwrap(),
        ];
        drop(others);
        for _ in 0..1000 {
            if db.get_conn().num_idle() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(db.get_conn().num_idle() >= 2);
        let mut closing = tokio::spawn({
            let db = db.clone();
            async move { db.close().await }
        });
        let wal = dir.path().join("chroma.sqlite3-wal");
        for _ in 0..1000 {
            if std::fs::metadata(&wal).unwrap().len() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut closing)
                .await
                .is_err(),
            "the close ended while a connection was taken, within its grace"
        );
        drop(held);
        closing.await.unwrap();
        assert_eq!(files_of(dir.path()), vec!["chroma.sqlite3"]);
        let copy = dir.path().join("copy.sqlite3");
        std::fs::copy(&path, &copy).unwrap();
        let reader = sqlx::SqlitePool::connect(&format!("sqlite://{}", copy.to_str().unwrap()))
            .await
            .unwrap();
        let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM written")
            .fetch_one(&reader)
            .await
            .unwrap();
        assert_eq!(written, 3);
    }

    // A connection kept past the grace of the close, as by a task that never gives it back, does not keep the close
    // waiting: it ends after its grace, and the writes are in the database, the log written back first.
    #[tokio::test]
    async fn a_close_ends_while_a_connection_is_kept() {
        let (_dir, path, db) = database_file(JournalMode::default()).await;
        let kept = db.get_conn().acquire().await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(30),
                db.close_within(Duration::from_secs(1))
            )
            .await
            .is_ok(),
            "the close waited for a connection that never came back"
        );
        drop(kept);
        let reader = sqlx::SqlitePool::connect(&format!("sqlite://{}", path.to_str().unwrap()))
            .await
            .unwrap();
        let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM written")
            .fetch_one(&reader)
            .await
            .unwrap();
        assert_eq!(written, 3);
    }
}
