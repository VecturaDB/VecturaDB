//! The synchronous attached functions of distributed Chroma, for the single node: record_counter, statistics and
//! revision_history. Copies of the worker of distributed Chroma at commit 1cc7c750, to realign with it:
//! - this file: rust/worker/src/execution/operators/execute_task.rs (the trait, HydratedInputBatch, the counter and the
//!   choice of the executor);
//! - statistics.rs: rust/worker/src/execution/functions/statistics.rs;
//! - revision_history.rs: rust/worker/src/execution/functions/revision_history.rs.
//!
//! The one difference: the worker reads the output collection through its record segment (a blockfile
//! RecordSegmentReaderShard), which the output collection of the single node has not; the executors read it here
//! through OutputRecords, which that reader implements as the worker reads it, and the single node with its SQLite.

mod count_to_file_async;
mod revision_history;
pub mod run;
mod statistics;

pub use count_to_file_async::CountToFileAsyncExecutor;
pub use revision_history::RevisionHistoryExecutor;
pub use statistics::{CounterFunctionFactory, StatisticsFunctionExecutor};

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::{RecordSegmentReaderOptions, RecordSegmentReaderShard};
use chroma_segment::types::HydratedMaterializedLogRecord;
use chroma_types::{
    AttachedFunction, Chunk, CollectionUuid, LogRecord, MaterializedLogOperation, Metadata,
    Operation, OperationRecord, UpdateMetadataValue, FUNCTION_COUNT_TO_FILE_ASYNC_ID,
    FUNCTION_DUMMY_ASYNC_ID, FUNCTION_RECORD_COUNTER_ID, FUNCTION_REVISION_HISTORY_ID,
    FUNCTION_STATISTICS_ID,
};
use futures::StreamExt;
use std::sync::Arc;

// Constants for CountAttachedFunction
const COUNT_FUNCTION_OUTPUT_ID: &str = "function_output";
const COUNT_METADATA_KEY: &str = "total_count";

/// The records of the output collection, as the executors read them: the metadata of a record by its id, and the
/// metadata of every record.
#[async_trait]
pub trait OutputRecords: Send + Sync {
    /// The metadata of the record of this id; None when there is no such record, or it cannot be read, as the worker
    /// falls back to no record then.
    async fn metadata_of(&self, user_id: &str) -> Option<Option<Metadata>>;

    /// The metadata of every record.
    async fn all_metadata(&self) -> Result<Vec<Option<Metadata>>, Box<dyn ChromaError>>;
}

// As the worker reads its output collection.
#[async_trait]
impl OutputRecords for RecordSegmentReaderShard<'_> {
    async fn metadata_of(&self, user_id: &str) -> Option<Option<Metadata>> {
        let offset_id = match self
            .get_offset_id_for_user_id(user_id, &RecordSegmentReaderOptions::default())
            .await
        {
            Ok(Some(offset_id)) => offset_id,
            _ => return None,
        };
        match self.get_data_for_offset_id(offset_id).await {
            Ok(Some(data_record)) => Some(data_record.metadata),
            _ => None,
        }
    }

    async fn all_metadata(&self) -> Result<Vec<Option<Metadata>>, Box<dyn ChromaError>> {
        let max_offset_id = self.get_max_offset_id();
        let mut stream = self.get_data_stream(0..=max_offset_id).await;
        let mut metadata = Vec::new();
        while let Some(record_result) = stream.next().await {
            let (_, record) = record_result?;
            metadata.push(record.metadata);
        }
        Ok(metadata)
    }
}

/// Trait for attached function executors that process input records and produce output records.
/// Implementors can read from the output collection to maintain state across executions.
#[async_trait]
pub trait AttachedFunctionExecutor: Send + Sync + std::fmt::Debug {
    /// Execute the attached function logic on input records.
    ///
    /// # Arguments
    /// * `input_batches` - The hydrated materialized log records to process, grouped by input
    ///   collection
    /// * `output_reader` - Optional reader for the output collection's compacted data
    ///
    /// # Returns
    /// The output records to be written to the output collection
    async fn execute(
        &self,
        input_batches: Vec<HydratedInputBatch<'_, '_>>,
        output_reader: Option<&dyn OutputRecords>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>>;
}

/// A simple counting attached function that maintains a running total of records processed.
/// Stores the count in a metadata field called "total_count".
#[derive(Debug)]
pub struct CountAttachedFunction;

impl CountAttachedFunction {
    /// Reads the existing count from the output reader.
    /// Returns 0 if no existing count is found.
    async fn get_existing_count(output_reader: Option<&dyn OutputRecords>) -> i64 {
        let Some(reader) = output_reader else {
            return 0;
        };

        // Try to get the existing record with the function output ID
        let Some(metadata) = reader.metadata_of(COUNT_FUNCTION_OUTPUT_ID).await else {
            return 0;
        };

        // Extract total_count from metadata
        if let Some(metadata) = &metadata {
            if let Some(chroma_types::MetadataValue::Int(count)) = metadata.get(COUNT_METADATA_KEY)
            {
                return *count;
            }
        }

        0
    }
}

#[async_trait]
impl AttachedFunctionExecutor for CountAttachedFunction {
    async fn execute(
        &self,
        input_batches: Vec<HydratedInputBatch<'_, '_>>,
        output_reader: Option<&dyn OutputRecords>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let records_count = input_batches
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>() as i64;

        // NOTE(tanujnay112): Can get all these in one pass but this function is just for
        // testing.
        let delete_count = input_batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .filter(|(record, _)| {
                record.get_operation() == MaterializedLogOperation::DeleteExisting
            })
            .count() as i64;

        let insert_count = input_batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .filter(|(record, _)| record.get_operation() == MaterializedLogOperation::AddNew)
            .count() as i64;

        // Read existing count from output_reader if available
        let existing_count = Self::get_existing_count(output_reader).await;
        let new_total_count = existing_count + insert_count - delete_count;
        println!(
            "Existing count: {}, Insert count: {}, Delete count: {}, New total count: {}",
            existing_count, insert_count, delete_count, new_total_count
        );

        // Create output record with updated count
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            COUNT_METADATA_KEY.to_string(),
            UpdateMetadataValue::Int(new_total_count),
        );

        let output_record = LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: COUNT_FUNCTION_OUTPUT_ID.to_string(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: Some(metadata),
                document: Some(format!(
                    "Last processed {} records (total: {})",
                    records_count, new_total_count
                )),
                operation: Operation::Upsert,
            },
        };

        Ok(Chunk::new(Arc::from(vec![output_record])))
    }
}

/// A dummy attached function for testing that logs a message and returns empty output.
#[derive(Debug)]
pub struct DummyAttachedFunction;

#[async_trait]
impl AttachedFunctionExecutor for DummyAttachedFunction {
    async fn execute(
        &self,
        input_batches: Vec<HydratedInputBatch<'_, '_>>,
        _output_reader: Option<&dyn OutputRecords>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        tracing::info!(
            "DummyAttachedFunction executing with {} input records",
            input_batches
                .iter()
                .map(|batch| batch.records.len())
                .sum::<usize>()
        );

        // Return empty output records
        Ok(Chunk::new(vec![].into()))
    }
}

/// Hydrated records for one input collection, passed to the executor after shard hydration.
pub struct HydratedInputBatch<'me, 'q> {
    pub input_collection_id: CollectionUuid,
    pub input_collection_name: String,
    pub tenant_id: String,
    pub database_id: String,
    /// Carried verbatim from the batch input, so an executor sees the same
    /// database name the orchestrator resolved.
    pub database_name: Option<String>,
    pub pulled_log_offset: u64,
    pub records: Chunk<HydratedMaterializedLogRecord<'me, 'q>>,
}

/// The executor of an asynchronous attached function, by its function_id, as from_attached_function of the worker:
/// dummy_async and count_to_file_async, this one on the storage given; None for one the single node does not run:
/// http_generate and http_currents, part of Foundations, a service of Chroma Cloud (Federico, 04/11).
pub fn async_executor(
    attached_function: &AttachedFunction,
    storage: Option<chroma_storage::Storage>,
) -> Result<Option<Arc<dyn AttachedFunctionExecutor>>, String> {
    Ok(match attached_function.function_id {
        FUNCTION_DUMMY_ASYNC_ID => Some(Arc::new(DummyAttachedFunction)),
        FUNCTION_COUNT_TO_FILE_ASYNC_ID => Some(Arc::new(
            CountToFileAsyncExecutor::from_attached_function(attached_function, storage)
                .map_err(|e| format!("CountToFileAsyncExecutor: {e}"))?,
        )),
        _ => None,
    })
}

/// The executor of a synchronous attached function, by its function_id, as from_attached_function of the worker;
/// None for an asynchronous function, which the single node does not run in its compactor.
pub fn sync_executor(
    attached_function: &AttachedFunction,
) -> Result<Option<Arc<dyn AttachedFunctionExecutor>>, String> {
    Ok(match attached_function.function_id {
        FUNCTION_RECORD_COUNTER_ID => Some(Arc::new(CountAttachedFunction)),
        FUNCTION_STATISTICS_ID => Some(Arc::new(StatisticsFunctionExecutor(Box::new(
            CounterFunctionFactory,
        )))),
        FUNCTION_REVISION_HISTORY_ID => Some(Arc::new(
            RevisionHistoryExecutor::from_attached_function(attached_function)
                .map_err(|e| format!("RevisionHistoryExecutor: {e}"))?,
        )),
        _ => None,
    })
}
