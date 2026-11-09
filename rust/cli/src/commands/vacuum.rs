// Changed by VecturaDB contributors for the VecturaDB branding, the lock of the data folder and the limit of the purge.
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
use crate::terminal::{SystemTerminal, Terminal};
use crate::utils::CliError;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::folder_lock::{lock_folder, FolderLock, FolderLockError, FOLDER_WAIT};
use chroma_frontend::{Frontend, FrontendConfig};
use chroma_log::local_compaction_manager::purge_limit;
use chroma_log::sqlite_log::{
    legacy_embeddings_queue_config_default_kind, LegacyEmbeddingsQueueConfig, SqliteLog,
};
use chroma_log::Log;
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::System;
use chroma_types::{CollectionUuid, DatabaseName, ListCollectionsRequest, Schema};
use clap::Parser;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::Row;
use std::error::Error;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use std::{fs, io};
use thiserror::Error;
use tokio::time::timeout;

#[derive(Debug, Error)]
pub enum VacuumError {
    #[error("Path {0} does not exist")]
    PathDoesNotExist(String),
    #[error("Failed to get size of of your VecturaDB directory")]
    DirSizeFailed,
    #[error("Not a VecturaDB path: {0}")]
    NotAChromaPath(String),
    #[error("Cannot find Sqlite config for VecturaDB")]
    SqliteConfigNotFound,
    #[error("Failed to vacuum VecturaDB")]
    VacuumFailed,
    #[error(
        "The data folder {folder} is in use by another VecturaDB program, a server or an embedded client, after \
         {waited} s of wait: stop it, then run vacuum again"
    )]
    FolderInUse { folder: String, waited: u64 },
    #[error(transparent)]
    FolderLock(FolderLockError),
}

#[derive(Parser, Debug)]
pub struct VacuumArgs {
    #[clap(long, help = "The path of your VecturaDB database")]
    path: Option<String>,
    #[clap(long, default_value_t = false, help = "Skip vacuum confirmation")]
    force: bool,
    #[clap(long, help = "Maximum time (in seconds) to wait for vacuum")]
    timeout: Option<u64>,
}

fn sizeof_fmt(num: u64, suffix: Option<&str>) -> String {
    let suffix = suffix.unwrap_or("B");
    let mut n = num as f64;
    let units = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"];
    for unit in &units {
        if n.abs() < 1024.0 {
            return format!("{:3.1}{}{}", n, unit, suffix);
        }
        n /= 1024.0;
    }
    format!("{:.1}Yi{}", n, suffix)
}

fn get_dir_size(path: &Path) -> Result<u64, io::Error> {
    let mut total_size = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = fs::metadata(&entry_path)?;
        if metadata.is_file() {
            total_size += metadata.len();
        } else if metadata.is_dir() {
            total_size += get_dir_size(&entry_path)?;
        }
    }
    Ok(total_size)
}

async fn get_collection_ids_to_migrate(
    sqlite: &SqliteDb,
) -> Result<Vec<CollectionUuid>, Box<dyn Error>> {
    let rows = sqlx::query(
        r#"
                SELECT collection FROM "segments"
                WHERE "id" NOT IN (SELECT "segment_id" FROM "max_seq_id") AND "type" = 'urn:chroma:segment/vector/hnsw-local-persisted'
            "#,
    ).fetch_all(sqlite.get_conn()).await?;

    let collection_ids: Result<Vec<CollectionUuid>, _> = rows
        .into_iter()
        .map(|row| CollectionUuid::from_str(row.get::<&str, _>(0)))
        .collect();

    let collection_ids = collection_ids?;

    Ok(collection_ids)
}

async fn trigger_vector_segments_max_seq_id_migration(
    sqlite: &SqliteDb,
    sysdb: &mut SysDb,
    segment_manager: &LocalSegmentManager,
) -> Result<(), Box<dyn Error>> {
    let collection_ids = get_collection_ids_to_migrate(sqlite).await?;

    for collection_id in collection_ids {
        let mut collection = sysdb
            .get_collection_with_segments(None, collection_id)
            .await?;

        if collection.collection.schema.is_none() {
            collection.collection.schema = Some(
                Schema::try_from(&collection.collection.config)
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?,
            );
        }

        // If collection is uninitialized, that means nothing has been written yet.
        let dim = match collection.collection.dimension {
            Some(dim) => dim,
            None => continue,
        };

        segment_manager
            .get_hnsw_writer(
                &collection.collection,
                &collection.vector_segment,
                dim as usize,
            )
            .await?;
    }

    Ok(())
}

async fn configure_sql_embedding_queue(log: &SqliteLog) -> Result<(), Box<dyn Error>> {
    let config = LegacyEmbeddingsQueueConfig {
        automatically_purge: true,
        kind: legacy_embeddings_queue_config_default_kind(),
    };

    log.update_legacy_embeddings_queue_config(config).await?;
    Ok(())
}

pub async fn vacuum_chroma(config: FrontendConfig) -> Result<(), Box<dyn Error>> {
    let system = System::new();
    let registry = Registry::new();
    let mut frontend = Frontend::try_from_config(&(config.clone(), system), &registry).await?;

    let sqlite = registry.get::<SqliteDb>()?;
    let segment_manager = registry.get::<LocalSegmentManager>()?;
    let mut sysdb = registry.get::<SysDb>()?;
    let mut log = registry.get::<Log>()?;

    println!("Purging the log...\n");

    trigger_vector_segments_max_seq_id_migration(&sqlite, &mut sysdb, &segment_manager).await?;

    let tenant = String::from("default_tenant");
    let database = DatabaseName::new("default_database")
        .ok_or("Invalid database name: must be at least 3 characters")?;

    let list_collections_request = ListCollectionsRequest::try_new(tenant, database, None, 0)?;
    let collections = frontend.list_collections(list_collections_request).await?;

    let pb = ProgressBar::new(collections.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{bar:40.cyan/blue} {pos}/{len} ({percent}%)")?
            .progress_chars("=>-"),
    );

    if let Log::Sqlite(ref log) = log {
        configure_sql_embedding_queue(log).await?;
    } else {
        return Err("Expected a Sqlite log for vacuum".into());
    }

    for collection in collections {
        // The limit of the purge of the server: no record that an attached function has not run on goes.
        let segments = sysdb
            .get_collection_with_segments(None, collection.collection_id)
            .await?;
        let limit = purge_limit(
            &sqlite,
            collection.collection_id,
            &segments.metadata_segment.id,
            &segments.vector_segment.id,
        )
        .await?;

        log.purge_logs(collection.collection_id, limit.purge_seq_id)
            .await?;

        pb.inc(1);
    }

    println!("Vacuuming (this may take a while)...\n");

    sqlx::query("VACUUM").execute(sqlite.get_conn()).await?;

    sqlx::query(
        "INSERT INTO maintenance_log (operation, timestamp)
         VALUES ('vacuum', CURRENT_TIMESTAMP)",
    )
    .execute(sqlite.get_conn())
    .await?;

    Ok(())
}

// The lock of the data folder (chroma_frontend::folder_lock), held until vacuum ends: one program at a time, so vacuum
// does not run beside a server or an embedded client on the same folder. A folder in use is said at once; after the
// wait the refusal is printed, as the error of the command shows only that vacuum failed.
fn hold_folder(folder: &str, term: &mut SystemTerminal) -> Result<FolderLock, VacuumError> {
    let refused = |err| match err {
        FolderLockError::InUse { folder, waited } => VacuumError::FolderInUse { folder, waited },
        err => VacuumError::FolderLock(err),
    };
    let held = match lock_folder(Path::new(folder), Duration::ZERO) {
        Err(FolderLockError::InUse { .. }) => {
            term.println(&format!(
                "The data folder {folder} is in use by another VecturaDB program: waiting up to {} s for it",
                FOLDER_WAIT.as_secs()
            ));
            lock_folder(Path::new(folder), FOLDER_WAIT)
        }
        held => held,
    };
    held.map_err(refused)
        .inspect_err(|err| term.println(&format!("{}", err.to_string().red())))
}

pub fn vacuum(args: VacuumArgs) -> Result<(), CliError> {
    let mut term = SystemTerminal;

    // Vacuum the database. This may result in a small increase in performance.
    // If you recently upgraded Chroma from a version below 0.5.6 to 0.5.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.
    // The execution time of this command scales with the size of your database. It blocks both reads and writes to the database while it is running.
    term.println(&format!("{}", "\nChroma Vacuum\n".underline().bold()));

    let mut config = FrontendServerConfig::single_node_default();
    let persistent_path = args.path.unwrap_or(config.persist_path);

    if !Path::new(&persistent_path).exists() {
        return Err(VacuumError::PathDoesNotExist(persistent_path).into());
    }

    let sqlite_url = format!("{}/{}", persistent_path, config.sqlite_filename);

    if !Path::new(sqlite_url.as_str()).exists() {
        return Err(VacuumError::NotAChromaPath(sqlite_url).into());
    }

    let _held_folder = hold_folder(&persistent_path, &mut term)?;

    let proceed = match args.force {
        true => true,
        false => {
            term.println(&format!(
                "{}",
                "Are you sure you want to vacuum the database?"
                    .bold()
                    .blue()
            ));
            term.prompt_confirm("This will block both reads and writes to the database and may take a while. We recommend shutting down the server before running this command. Continue?")?
        }
    };

    term.println("");

    if !proceed {
        term.println(&format!("{}", "Vacuum cancelled\n".red()));
        return Ok(());
    }

    let initial_size =
        get_dir_size(Path::new(&persistent_path)).map_err(|_| VacuumError::DirSizeFailed)?;

    match config.frontend.sqlitedb.as_mut() {
        Some(sqlite_config) => {
            sqlite_config.url = Some(sqlite_url);
        }
        None => return Err(VacuumError::SqliteConfigNotFound.into()),
    };

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start VecturaDB");

    let res = if let Some(secs) = args.timeout {
        runtime.block_on(async {
            timeout(Duration::from_secs(secs), vacuum_chroma(config.frontend))
                .await
                .unwrap_or_else(|_elapsed| {
                    Err(Box::new(VacuumError::VacuumFailed) as Box<dyn std::error::Error>)
                })
        })
    } else {
        runtime.block_on(vacuum_chroma(config.frontend))
    };
    res.map_err(|_| VacuumError::VacuumFailed)?;

    let post_vacuum_size =
        get_dir_size(Path::new(&persistent_path)).map_err(|_| VacuumError::DirSizeFailed)?;

    let size_diff = initial_size - post_vacuum_size;

    term.println(&format!("🧼 {}", "Vacuum complete!".green().bold()));
    term.println(&format!(
        "Database size reduced by {} (⬇️{:.1}%)",
        sizeof_fmt(size_diff, None).to_string().green(),
        (((size_diff as f64) / (initial_size as f64)) * 100.0)
            .to_string()
            .green()
    ));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_frontend::impls::service_based_frontend::ServiceBasedFrontend;
    use chroma_log::LocalCompactionManager;
    use chroma_system::ComponentHandle;
    use chroma_types::{
        AddCollectionRecordsRequest, AttachFunctionRequest, CreateCollectionRequest,
        InternalCollectionConfiguration, InternalHnswConfiguration, VectorIndexConfiguration,
    };

    // The configuration of a frontend on the folder given, which vacuum opens as it is.
    fn config_on(root: &Path) -> FrontendConfig {
        let mut config = FrontendConfig::sqlite_in_memory();
        config.sqlitedb.as_mut().unwrap().url =
            Some(root.join("chroma.sqlite3").to_str().unwrap().into());
        config.segment_manager.as_mut().unwrap().persist_path = Some(root.to_str().unwrap().into());
        config
    }

    // The records of the log of a collection after the offset given.
    async fn log_rows_after(root: &Path, collection: CollectionUuid, offset: i64) -> i64 {
        let db = sqlx::SqlitePool::connect(root.join("chroma.sqlite3").to_str().unwrap())
            .await
            .unwrap();
        let rows = sqlx::query_scalar(
            "SELECT COUNT(*) FROM embeddings_queue WHERE topic LIKE '%' || ? AND seq_id > ?",
        )
        .bind(collection.to_string())
        .bind(offset)
        .fetch_one(&db)
        .await
        .unwrap();
        db.close().await;
        rows
    }

    // A function behind its input keeps the log it has not run on, as the purge of the server keeps it (the lowest
    // completion offset of the functions of the collection, in local_compaction_manager.rs): a vacuum must keep it too.
    #[tokio::test]
    async fn vacuum_keeps_the_log_a_function_has_not_run_on() {
        let root = tempfile::tempdir().unwrap();
        let config = config_on(root.path());
        let registry = Registry::new();
        let system = System::new();
        let mut frontend =
            ServiceBasedFrontend::try_from_config(&(config.clone(), system.clone()), &registry)
                .await
                .unwrap();
        let collection = frontend
            .create_collection(
                CreateCollectionRequest::try_new(
                    "default_tenant".into(),
                    DatabaseName::new("default_database").unwrap(),
                    "vacuumed".into(),
                    None,
                    Some(InternalCollectionConfiguration {
                        vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                            sync_threshold: 2,
                            ..Default::default()
                        }),
                        embedding_function: None,
                    }),
                    None,
                    false,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        frontend
            .attach_function(
                "default_tenant".into(),
                DatabaseName::new("default_database").unwrap(),
                collection.collection_id.to_string(),
                AttachFunctionRequest::try_new(
                    "counter".into(),
                    "record_counter".into(),
                    "counts".into(),
                    serde_json::json!({}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let db = registry.get::<SqliteDb>().unwrap();
        // The function stays behind its input: with no output collection, a run skips it.
        sqlx::query(
            "UPDATE attached_functions SET output_collection_id = NULL WHERE input_collection_id = ?",
        )
        .bind(collection.collection_id.to_string())
        .execute(db.get_conn())
        .await
        .unwrap();
        for id in 0..10 {
            frontend
                .add(
                    AddCollectionRecordsRequest::try_new(
                        "default_tenant".into(),
                        "default_database".into(),
                        collection.collection_id,
                        vec![format!("r{id}")],
                        vec![vec![1.0, 2.0, 3.0]],
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let offset: i64 = sqlx::query_scalar(
            "SELECT completion_offset FROM attached_functions WHERE input_collection_id = ?",
        )
        .bind(collection.collection_id.to_string())
        .fetch_one(db.get_conn())
        .await
        .unwrap();
        drop(db);
        let mut handle = registry
            .get::<ComponentHandle<LocalCompactionManager>>()
            .unwrap();
        handle.stop();
        handle.join().await.unwrap();
        system.stop().await;
        system.join().await;
        registry.get::<SqliteDb>().unwrap().close().await;
        drop(frontend);
        drop(registry);
        let before = log_rows_after(root.path(), collection.collection_id, offset).await;
        assert!(
            before >= 10,
            "the server kept the log of the function behind: {before} rows"
        );
        vacuum_chroma(config).await.unwrap();
        let after = log_rows_after(root.path(), collection.collection_id, offset).await;
        assert_eq!(
            after, before,
            "the vacuum removed records that the function behind has not run on"
        );
    }

    // One program at a time on a data folder: vacuum on a folder that a server or an embedded client holds waits for it,
    // then refuses, with the folder in its message, and leaves the database as it was.
    #[test]
    fn vacuum_waits_for_a_folder_in_use_then_refuses() {
        let root = tempfile::tempdir().unwrap();
        let folder = root.path().to_str().unwrap().to_string();
        let database = root.path().join("chroma.sqlite3");
        fs::write(&database, b"").unwrap();
        let _server = lock_folder(root.path(), Duration::ZERO).unwrap();

        let started = std::time::Instant::now();
        let refused = vacuum(VacuumArgs {
            path: Some(folder.clone()),
            force: true,
            timeout: None,
        })
        .unwrap_err();

        assert!(started.elapsed() >= FOLDER_WAIT, "{:?}", started.elapsed());
        match refused {
            CliError::Vacuum(VacuumError::FolderInUse { folder: named, .. }) => {
                assert_eq!(named, folder)
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(fs::read(&database).unwrap(), b"");
    }
}
