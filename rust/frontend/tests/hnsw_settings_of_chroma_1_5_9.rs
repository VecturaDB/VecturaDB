#![recursion_limit = "256"]
//! Collections that Chroma 1.5.9 stored with HNSW settings above the bounds that requests have had since Chroma #7825:
//! Chroma 1.5.9 accepted them, and reads them again; the single node reads them too, as they are on the disk.
use std::path::Path;

use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{impls::service_based_frontend::ServiceBasedFrontend, FrontendConfig};
use chroma_sqlite::db::SqliteDb;
use chroma_system::System;
use chroma_types::*;
use sqlx::Row;

const TENANT: &str = "default_tenant";
const DATABASE: &str = "default_database";

async fn open(root: &Path) -> (ServiceBasedFrontend, Registry) {
    let registry = Registry::new();
    let mut config = FrontendConfig::sqlite_in_memory();
    config.sqlitedb.as_mut().unwrap().url =
        Some(root.join("chroma.sqlite3").to_str().unwrap().into());
    config.segment_manager.as_mut().unwrap().persist_path = Some(root.to_str().unwrap().into());
    let frontend = ServiceBasedFrontend::try_from_config(&(config, System::new()), &registry)
        .await
        .unwrap();
    (frontend, registry)
}

fn vector(i: usize) -> Vec<f32> {
    (0..8).map(|d| (i as f32 * 0.37 + d as f32).sin()).collect()
}

// The HNSW settings of every "hnsw" object of a configuration or a schema, as Chroma 1.5.9 stores those of a collection
// made with hnsw:sync_threshold 10000, hnsw:M 200 and hnsw:search_ef 5000.
fn raise(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value {
        if let Some(serde_json::Value::Object(hnsw)) = map.get_mut("hnsw") {
            hnsw.insert("sync_threshold".into(), 10000.into());
            hnsw.insert("max_neighbors".into(), 200.into());
            hnsw.insert("ef_search".into(), 5000.into());
        }
        for child in map.values_mut() {
            raise(child);
        }
    }
}

#[tokio::test]
async fn a_collection_stored_with_hnsw_settings_above_the_bounds_of_a_request_is_read() {
    let root = tempfile::tempdir().unwrap();
    let (mut frontend, registry) = open(root.path()).await;
    let collection = frontend
        .create_collection(
            CreateCollectionRequest::try_new(
                TENANT.to_string(),
                DatabaseName::new(DATABASE).unwrap(),
                "high".to_string(),
                None,
                None,
                None,
                false,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection.collection_id,
                (0..20).map(|i| format!("r{i}")).collect(),
                (0..20).map(vector).collect(),
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let db = registry.get::<SqliteDb>().unwrap();
    let row =
        sqlx::query("SELECT config_json_str, schema_str FROM collections WHERE name = 'high'")
            .fetch_one(db.get_conn())
            .await
            .unwrap();
    // The single node keeps the settings in the schema, and an empty configuration.
    let mut stored = Vec::new();
    for column in [0, 1] {
        let mut value: serde_json::Value =
            serde_json::from_str(row.get::<&str, _>(column)).unwrap();
        raise(&mut value);
        stored.push(value.to_string());
    }
    assert!(
        stored[1].contains("\"sync_threshold\":10000"),
        "{}",
        stored[1]
    );
    sqlx::query("UPDATE collections SET config_json_str = ?, schema_str = ? WHERE name = 'high'")
        .bind(&stored[0])
        .bind(&stored[1])
        .execute(db.get_conn())
        .await
        .unwrap();
    db.close().await;
    drop(frontend);

    let (mut frontend, _registry) = open(root.path()).await;
    let read = frontend
        .get_collection(
            GetCollectionRequest::try_new(
                TENANT.to_string(),
                DatabaseName::new(DATABASE).unwrap(),
                "high".to_string(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        frontend
            .count(
                CountRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    read.collection_id,
                    plan::ReadLevel::default(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        20
    );
    let found = frontend
        .query(
            QueryRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                read.collection_id,
                None,
                None,
                vec![vector(0)],
                1,
                IncludeList(vec![]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found.ids, vec![vec!["r0".to_string()]]);
}
