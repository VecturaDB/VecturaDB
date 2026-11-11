// Changed from the original Chroma file; see the git history for the changes and their authors.
use std::{path::Path, time::Duration};

use crate::db::{SqliteCreationError, SqliteDb};
use async_trait::async_trait;
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteTransactionManager,
};
use sqlx::{Connection, TransactionManager};
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

/// Syncs the database file, its log and its folder, those there are: on Windows a folder cannot be opened to sync it.
async fn sync_existing(path: &Path) -> std::io::Result<()> {
    let mut files = vec![path.to_path_buf()];
    for suffix in ["-wal", "-shm"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        files.push(name.into());
    }
    for file in files {
        match open_to_sync(&file).await {
            Ok(f) => f.sync_all().await?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            // A file that the process cannot write, as one restored read-only: Windows cannot sync it, and SQLite only
            // reads it. Chroma 1.5.9, which synced nothing, opened it.
            #[cfg(windows)]
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(err) => return Err(err),
        }
    }
    #[cfg(unix)]
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::File::open(parent).await?.sync_all().await?;
    }
    Ok(())
}

// A file opened to be synced. On Windows the sync is FlushFileBuffers, which needs a handle that can write: with one
// opened to read it fails with "Access is denied" (os error 5). Elsewhere fsync syncs a file opened to read.
#[cfg(windows)]
async fn open_to_sync(file: &Path) -> std::io::Result<tokio::fs::File> {
    tokio::fs::OpenOptions::new().write(true).open(file).await
}

#[cfg(not(windows))]
async fn open_to_sync(file: &Path) -> std::io::Result<tokio::fs::File> {
    tokio::fs::File::open(file).await
}

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
            // Each connection keeps a page cache of its own (.cargo/config.toml), of the 2000 KiB of SQLite: a connection
            // takes memory for a page when it reads it and keeps it, so the pool of up to 10 connections holds up to about
            // 20000 KiB of pages. A get that reads more than its 483 pages of 4 KiB reads the rest again from the file.
            .pragma("cache_size", "-2000")
            .busy_timeout(Duration::from_secs(1000))
            .with_regexp();
        let conn = if let Some(url) = &config.url {
            let path = Path::new(url);
            if let Some(parent) = path.parent() {
                create_dir_all(parent).await?;
            }
            // A database copied in just before the start (a restore with cp) can still be only in the page cache. In WAL
            // the writes go to the log and the database file is not written again until a checkpoint, so a power cut
            // after the start loses it, with every collection; with the rollback journal of Chroma each write rewrote
            // and synced it. The files there are and their folder are synced at the open.
            sync_existing(path).await?;
            let journal_mode = match config.journal_mode {
                JournalMode::Wal => SqliteJournalMode::Wal,
                JournalMode::Delete => SqliteJournalMode::Delete,
            };
            SqlitePoolOptions::new()
                // SQLite ends by itself a transaction that fills the disk, so the rollback of sqlx fails and the connection
                // comes back still counted in a transaction. On it the next BEGIN IMMEDIATE is refused, and the savepoint of
                // a plain begin leaves a real transaction open, with the write lock, until the server stops. Such a
                // connection is closed, and SQLite frees what it holds. Not for the database in memory below: it lives in
                // its only connection, and it has no disk to fill.
                .after_release(|connection, _| {
                    Box::pin(async move {
                        if SqliteTransactionManager::get_transaction_depth(connection) > 0 {
                            // The rollback of a dropped transaction may still be queued.
                            connection.ping().await?;
                        }
                        let usable =
                            SqliteTransactionManager::get_transaction_depth(connection) == 0;
                        if !usable {
                            tracing::warn!(
                                "Closing a SQLite connection whose transaction could not be rolled back"
                            );
                        }
                        Ok(usable)
                    })
                })
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

    // A connection that comes back to the pool while the pool closes stays open in it: the wait of sqlx 0.8.6 ends with the
    // last permit, which that connection gives back, and nothing takes it after. The close takes it too, and the database
    // file is alone.
    #[tokio::test]
    async fn a_connection_that_comes_back_during_the_close_is_closed_too() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chroma.sqlite3");
        let first = std::sync::atomic::AtomicBool::new(true);
        let conn = SqlitePoolOptions::new()
            .after_release(move |_, _| {
                let late = first.swap(false, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move {
                    if late {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Ok(true)
                })
            })
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal),
            )
            .await
            .unwrap();
        let db = SqliteDb::new(conn, MigrationHash::MD5);
        sqlx::query("CREATE TABLE written (value INTEGER)")
            .execute(db.get_conn())
            .await
            .unwrap();
        // The connection is on its way back, past the check of the pool on its close.
        tokio::time::sleep(Duration::from_millis(100)).await;
        db.close_within(Duration::from_secs(5)).await;
        assert_eq!(files_of(dir.path()), vec!["chroma.sqlite3"]);
    }

    // Writes more than this connection may hold in the database file: SQLite ends the transaction by itself, as on a full
    // disk. The connection is back in the pool at the end, past the checks of the pool.
    async fn a_transaction_ended_by_a_full_disk(db: &SqliteDb) {
        let mut connection = db.get_conn().acquire().await.unwrap();
        let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        sqlx::query(&format!("PRAGMA max_page_count = {pages}"))
            .execute(&mut *connection)
            .await
            .unwrap();
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let full = sqlx::query("INSERT INTO written VALUES (zeroblob(1048576))")
            .execute(&mut *transaction)
            .await
            .unwrap_err();
        assert_eq!(
            full.as_database_error().unwrap().code().as_deref(),
            Some("13")
        );
        drop(transaction);
        connection.return_to_pool().await;
    }

    // SQLite ends by itself a transaction that fills the disk, so the rollback of sqlx fails and the connection stays counted
    // in a transaction. On that connection the next BEGIN IMMEDIATE is refused, so it is not used again, and the writes go
    // on without a restart.
    #[tokio::test]
    async fn a_connection_left_in_a_transaction_by_a_full_disk_is_not_used_again() {
        for journal_mode in [JournalMode::Wal, JournalMode::Delete] {
            let (_dir, _, db) = database_file(journal_mode).await;
            a_transaction_ended_by_a_full_disk(&db).await;
            // Taken all together, the one that failed is among them if it is still in the pool.
            let mut connections = Vec::new();
            for _ in 0..db.get_conn().size() {
                connections.push(db.get_conn().acquire().await.unwrap());
            }
            for connection in &mut connections {
                let transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
                transaction.commit().await.unwrap();
            }
            drop(connections);
            let mut transaction = db.get_conn().begin_with("BEGIN IMMEDIATE").await.unwrap();
            sqlx::query("INSERT INTO written VALUES (4)")
                .execute(&mut *transaction)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM written")
                .fetch_one(db.get_conn())
                .await
                .unwrap();
            assert_eq!(written, 4);
            db.close().await;
        }
    }

    // A transaction dropped without a commit, as when a request goes away, is rolled back as usual: its connection stays in
    // the pool, even though the rollback is still queued when the connection comes back.
    #[tokio::test]
    async fn a_connection_whose_transaction_was_dropped_stays_in_the_pool() {
        let (_dir, _, db) = database_file(JournalMode::default()).await;
        let mut connection = db.get_conn().acquire().await.unwrap();
        let connections = db.get_conn().size();
        let transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
        drop(transaction);
        connection.return_to_pool().await;
        assert_eq!(db.get_conn().size(), connections);
        db.close().await;
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

    // A database file opened again while it is open, as by a second process or after a kill, when its log and the index
    // of its log are there too: the open syncs the three files, and reads the records that the log holds.
    #[tokio::test]
    async fn a_database_file_opened_again_while_open_reads_its_log() {
        let (dir, path, first) = database_file(JournalMode::default()).await;
        let files = files_of(dir.path());
        assert!(files.contains(&"chroma.sqlite3-wal".to_string()));
        assert!(files.contains(&"chroma.sqlite3-shm".to_string()));
        let config = SqliteDBConfig {
            url: Some(path.to_str().unwrap().to_string()),
            ..Default::default()
        };
        let second = SqliteDb::try_from_config(&config, &Registry::new())
            .await
            .unwrap();
        let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM written")
            .fetch_one(second.get_conn())
            .await
            .unwrap();
        assert_eq!(written, 3);
        second.close().await;
        first.close().await;
    }

    // The sync at the open of a database file that the process cannot write, as one restored read-only: Chroma 1.5.9,
    // which did not sync, opened it, so the sync does not stop the open.
    #[tokio::test]
    async fn a_database_file_opened_again_that_cannot_be_written_is_not_refused_by_the_sync() {
        let (_dir, path, db) = database_file(JournalMode::default()).await;
        db.close().await;
        let writable = std::fs::metadata(&path).unwrap().permissions();
        let mut read_only = writable.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(&path, read_only).unwrap();
        let synced = sync_existing(&path).await;
        std::fs::set_permissions(&path, writable).unwrap();
        synced.unwrap();
    }
}
