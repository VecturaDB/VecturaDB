// Changed by VecturaDB contributors for the VecturaDB branding.
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
use crate::commands::browse::BrowseError;
use crate::commands::db::get_db_name;
use crate::commands::install::InstallError;
use crate::config_store::{ConfigStore, FileConfigStore};
use crate::terminal::{SystemTerminal, Terminal};
use crate::utils::{
    cloud_client, connect_local, CliError, ErrorResponse, LocalChromaArgs, Profile, UtilsError,
};
use chroma::client::{ChromaHttpClientError, Database};
use chroma::{ChromaCollection, ChromaHttpClient};
use chroma_types::operator::Key;
use chroma_types::plan::SearchPayload;
use chroma_types::{
    GetResponse, Include, IncludeList, Metadata, MetadataValue, Schema, SearchResponse,
    UpdateMetadata, UpdateMetadataValue, ValueTypes, CHROMA_URI_KEY,
};
use clap::Parser;
use crossterm::style::Stylize;
use futures::{stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::StatusCode;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::task::JoinHandle;

/// How long a request can stay without an answer before the copy sends it again.
const ATTEMPT_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(2)
} else {
    Duration::from_secs(120)
};
/// The first wait before the copy sends a request again, when the server does not say how long. It doubles at each try.
const FIRST_WAIT: Duration = Duration::from_millis(500);
/// The longest wait before the copy asks again, also when the server says more: the time it says is an estimate.
const LONGEST_WAIT: Duration = Duration::from_secs(60);
/// How long a request can stay without an answer before the copy says so.
const SAY_WAITING_AFTER: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(20)
};
/// Where the bar shows nothing, as in the Docker command of the guide, how often a line says how the copy goes.
const LINE_EVERY: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
enum Environment {
    Local,
    Cloud,
}

impl Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Environment::Local => write!(f, "Local"),
            Environment::Cloud => write!(f, "Cloud"),
        }
    }
}

#[derive(Debug, Error)]
pub enum CopyError {
    #[error("Can only copy collections from local to Chroma Cloud or from Chroma Cloud to local")]
    InvalidSourceDestination,
    #[error("No collections to copy found")]
    NoCollections,
    #[error("{on_target} records on the target, {expected} expected: the source changed during the copy")]
    SourceChanged { on_target: u32, expected: u32 },
    #[error("Not everything was copied:\n{0}")]
    NotAllCopied(String),
    #[error("Could not ask what to do with collection {0}")]
    NoAnswer(String),
}

#[derive(Parser, Debug)]
pub struct CopyArgs {
    #[clap(
        long = "all",
        conflicts_with = "collections",
        help = "Copy all collections"
    )]
    all: bool,
    #[clap(long = "collections", help = "The names of collections to copy")]
    collections: Vec<String>,
    #[clap(
        long = "from-local",
        conflicts_with_all = ["from_cloud", "to_local"],
        help = "Copy from a local VecturaDB server"
    )]
    from_local: bool,
    #[clap(long = "from-cloud", conflicts_with_all = ["to_cloud", "from_local"], help = "Copy from Chroma Cloud")]
    from_cloud: bool,
    #[clap(
        long = "to-local",
        conflicts_with = "to_cloud",
        help = "Copy to a local VecturaDB server"
    )]
    to_local: bool,
    #[clap(long = "to-cloud", help = "Copy to Chroma Cloud")]
    to_cloud: bool,
    #[clap(long = "db", help = "Chroma Cloud DB with the collections to copy")]
    db: Option<String>,
    #[clap(long = "host", conflicts_with_all = ["path"], help = "Local VecturaDB server host")]
    host: Option<String>,
    #[clap(long = "path", help = "Data path for your local VecturaDB server")]
    path: Option<String>,
    #[clap(
        long = "batch",
        default_value_t = 100,
        value_parser = clap::value_parser!(u32).range(1..=300),
        help = "Batch size for records when copying (min 1, max 300)"
    )]
    batch: u32,
    #[clap(
        long = "resume",
        group = "on_existing",
        help = "On the collections already on the target, add the records they do not have, and leave the others as they are. It asks for each collection"
    )]
    resume: bool,
    #[clap(
        long = "force",
        group = "on_existing",
        help = "Make the collections already on the target exact copies of the source: add what they do not have, write again what is different, remove what is not on the source, and with another schema create them again. It asks for each collection"
    )]
    force: bool,
    #[clap(
        long = "yes",
        requires = "on_existing",
        help = "With --resume or --force, do not ask: for a command without a terminal"
    )]
    yes: bool,
    #[clap(
        long = "concurrent",
        default_value_t = 5,
        value_parser = clap::value_parser!(u32).range(1..=8),
        help = "Number of pages read at the same time when copying, written one at a time in their order (min 1, max 8)"
    )]
    concurrent: u32,
}

fn select_chroma_server_prompt() -> &'static str {
    "What server has the collection(s) you want to copy?"
}

fn select_db_prompt(from: bool) -> String {
    let direction = if from { "from" } else { "to" };
    format!("Which DB do you want to copy collections {}", direction)
}

fn start_copy_prompt(collections_num: usize) -> String {
    format!("Copying {} collection(s)", collections_num)
}

fn verify_db_exists(dbs: &[Database], name: &str) -> Result<(), CliError> {
    if !dbs.iter().any(|db| db.name == name) {
        return Err(CliError::Db(crate::commands::db::DbError::DbNotFound(
            name.to_string(),
        )));
    }
    Ok(())
}

async fn get_cloud_client(
    profile: Profile,
    db_name: Option<String>,
    from: bool,
    term: &mut dyn Terminal,
) -> Result<ChromaHttpClient, CliError> {
    let client = cloud_client(&profile)?;

    if let Some(db_name) = db_name {
        let dbs = client.list_databases().await?;
        verify_db_exists(&dbs, &db_name)?;
        client.set_database_name(db_name);
        return Ok(client);
    }

    let databases = client.list_databases().await?;
    match databases.len() {
        0 => Err(BrowseError::NoDBs.into()),
        1 => {
            client.set_database_name(&databases[0].name);
            Ok(client)
        }
        _ => {
            let input_name = get_db_name(&databases, &select_db_prompt(from), term)?;
            verify_db_exists(&databases, &input_name)?;
            client.set_database_name(input_name);
            Ok(client)
        }
    }
}

async fn get_chroma_clients(
    args: &CopyArgs,
    source: Environment,
    target: Environment,
    profile: Profile,
    term: &mut dyn Terminal,
) -> Result<(ChromaHttpClient, ChromaHttpClient, Option<JoinHandle<()>>), CliError> {
    let local_args = LocalChromaArgs {
        host: args.host.clone(),
        path: args.path.clone(),
    };
    let (local_client, handle) = connect_local(local_args).await?;
    let cloud_client = get_cloud_client(profile, args.db.clone(), args.from_cloud, term).await?;

    match (source, target) {
        (Environment::Cloud, Environment::Local) => Ok((cloud_client, local_client, handle)),
        (Environment::Local, Environment::Cloud) => Ok((local_client, cloud_client, handle)),
        _ => Err(CopyError::InvalidSourceDestination.into()),
    }
}

fn get_target_and_destination(
    args: &CopyArgs,
    term: &mut dyn Terminal,
) -> Result<(Environment, Environment), CliError> {
    let (source, target) = match (
        args.from_cloud,
        args.from_local,
        args.to_cloud,
        args.to_local,
    ) {
        (true, _, _, _) => (Environment::Cloud, Environment::Local),
        (_, true, _, _) => (Environment::Local, Environment::Cloud),
        (_, _, true, _) => (Environment::Local, Environment::Cloud),
        (_, _, _, true) => (Environment::Cloud, Environment::Local),
        _ => {
            let prompt = select_chroma_server_prompt().bold().blue();
            term.println(&format!("{}", prompt));
            let options = vec![
                Environment::Cloud.to_string(),
                Environment::Local.to_string(),
            ];
            let selection = term.prompt_select(&options)?;
            term.println(&format!("{}\n", options[selection]));
            match selection {
                0 => (Environment::Cloud, Environment::Local),
                _ => (Environment::Local, Environment::Cloud),
            }
        }
    };

    Ok((source, target))
}

/// What the user sees of a step of the copy: the bar on a terminal, and elsewhere a line from time to time.
#[derive(Clone)]
struct Progress {
    bar: ProgressBar,
    what: Arc<String>,
    /// When the last line was said, and what it said.
    last_line: Arc<Mutex<Option<(Instant, String)>>>,
    last_wait: Arc<Mutex<Option<(Instant, String)>>>,
}

impl Progress {
    fn new(what: String, records: u64, position: u64) -> Self {
        // A step without records has no bar to show: it only says why it waits.
        let bar = if records == 0 {
            ProgressBar::hidden()
        } else {
            let bar = ProgressBar::new(records);
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("{bar:40.cyan/blue} {pos}/{len} {wide_msg}")
                    .unwrap()
                    .progress_chars("◼◼-"),
            );
            bar.set_position(position);
            bar
        };
        Self {
            bar,
            what: Arc::new(what),
            last_line: Arc::default(),
            last_wait: Arc::default(),
        }
    }

    fn advance(&self, records: u64) {
        self.bar.inc(records);
        self.line(&self.last_line, &self.bar.message());
    }

    /// What the records of the bar do not say.
    fn say(&self, what: String) {
        self.line(&self.last_line, &what);
        self.bar.set_message(what);
    }

    fn waiting(&self, why: &str) {
        self.bar.set_message(why.to_string());
        self.line(&self.last_wait, why);
    }

    fn waited(&self) {
        self.bar.set_message("");
    }

    fn finish(&self) {
        self.bar.finish();
        if let Some(line) = self.next_line(&self.last_line, "", true) {
            eprintln!("{line}");
        }
    }

    fn line(&self, last_line: &Mutex<Option<(Instant, String)>>, why: &str) {
        if let Some(line) = self.next_line(last_line, why, false) {
            eprintln!("{line}");
        }
    }

    /// The line to say where the bar is not shown, if it is time for one: one every LINE_EVERY, and at the end of the
    /// step one that says where it ended, unless the last line said just that.
    fn next_line(
        &self,
        last_line: &Mutex<Option<(Instant, String)>>,
        why: &str,
        at_the_end: bool,
    ) -> Option<String> {
        let records = self.bar.length().unwrap_or_default();
        if !self.bar.is_hidden() || (records == 0 && why.is_empty()) {
            return None;
        }
        let line = match records {
            0 => format!("{}: {why}", self.what),
            records => format!(
                "{}: {} of {records} records. {why}",
                self.what,
                self.bar.position()
            ),
        };
        let mut last_line = last_line.lock().unwrap();
        let quiet = match last_line.as_ref() {
            Some((_, last)) if at_the_end => *last == line,
            Some((time, _)) => time.elapsed() < LINE_EVERY,
            None => false,
        };
        if quiet {
            return None;
        }
        *last_line = Some((Instant::now(), line.clone()));
        Some(line)
    }
}

fn is_quota(message: &str) -> bool {
    message.to_lowercase().contains("quota")
}

/// Whether waiting can solve an error: a refusal for now, a server that fails or restarts, the network. Waiting does
/// not solve a key that is not valid, a collection that is not there, a request that is not valid, a quota or an
/// operation that the server does not have (501): Chroma 1.5.9 answers a search with it.
fn can_wait_for(error: &ChromaHttpClientError) -> bool {
    let status = match error {
        ChromaHttpClientError::RetryAfter(message, _) => return !is_quota(message),
        ChromaHttpClientError::ApiError(message, status) => {
            if is_quota(message) {
                return false;
            }
            *status
        }
        ChromaHttpClientError::RequestError(error) => match error.status() {
            Some(status) => status,
            // No answer, or an answer cut in half.
            None => return true,
        },
        _ => return false,
    };
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
        || (status.is_server_error() && status != StatusCode::NOT_IMPLEMENTED)
}

/// Sends a request until it gets an answer, or an error that waiting does not solve, and says why it waits. It waits as
/// long as the server says: the refusal of a write to a collection with too many records to index says that.
async fn until_done<T, F, Fut>(
    who: &str,
    progress: &Progress,
    mut attempt: F,
) -> Result<T, ChromaHttpClientError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ChromaHttpClientError>>,
{
    let mut next_wait = FIRST_WAIT;
    let mut waited = false;
    loop {
        let (why, said) = match answer_of(who, progress, attempt()).await {
            Some(Ok(value)) => {
                if waited {
                    progress.waited();
                }
                return Ok(value);
            }
            Some(Err(error)) if !can_wait_for(&error) => return Err(error),
            Some(Err(ChromaHttpClientError::RetryAfter(message, wait))) => (message, Some(wait)),
            Some(Err(error)) => (error.to_string(), None),
            None => (
                format!("no answer in {} s", ATTEMPT_TIMEOUT.as_secs()),
                None,
            ),
        };
        let wait = said.map_or(next_wait, |wait| wait.clamp(FIRST_WAIT, LONGEST_WAIT));
        next_wait = (next_wait * 2).min(LONGEST_WAIT);
        waited = true;
        progress.waiting(&format!(
            "Waiting for {who}, {why}. Trying again in {} s",
            wait.as_secs_f32().ceil() as u64
        ));
        tokio::time::sleep(wait).await;
    }
}

/// The answer to a request, or nothing if it does not come in time. Meanwhile it says that it has not come yet, so that a
/// request that hangs, as when the network falls without closing its connections, does not look like a copy that stopped.
async fn answer_of<T, Fut>(who: &str, progress: &Progress, request: Fut) -> Option<T>
where
    Fut: Future<Output = T>,
{
    let started = Instant::now();
    tokio::pin!(request);
    while started.elapsed() < ATTEMPT_TIMEOUT {
        if let Ok(answer) = tokio::time::timeout(SAY_WAITING_AFTER, &mut request).await {
            if started.elapsed() >= SAY_WAITING_AFTER {
                progress.waited();
            }
            return Some(answer);
        }
        progress.waiting(&format!(
            "Waiting for {who}, no answer for {} s",
            started.elapsed().as_secs()
        ));
    }
    None
}

/// A record as a search returns it, on the source and on the target.
#[derive(Clone, Debug, PartialEq)]
struct Record {
    id: String,
    document: Option<String>,
    embedding: Option<Vec<f32>>,
    metadata: Option<Metadata>,
}

impl Record {
    fn metadata(&self) -> Option<&Metadata> {
        self.metadata
            .as_ref()
            .filter(|metadata| !metadata.is_empty())
    }

    /// What of this record is not the same on the other side.
    fn difference(&self, other: &Record) -> Option<&'static str> {
        if self.document != other.document {
            Some("document")
        } else if !same_embedding(self.embedding.as_deref(), other.embedding.as_deref()) {
            Some("embedding")
        } else if self.metadata() != other.metadata() {
            Some("metadata")
        } else {
            None
        }
    }

    /// The metadata of an upsert that leaves on a record exactly those of this one: an upsert keeps the keys it does not
    /// name, unless it names them without a value.
    fn metadata_over(&self, found: &Record) -> Option<UpdateMetadata> {
        let mut metadata: UpdateMetadata = found
            .metadata()
            .into_iter()
            .flatten()
            .map(|(key, _)| (key.clone(), UpdateMetadataValue::None))
            .collect();
        metadata.extend(
            self.metadata()
                .into_iter()
                .flatten()
                .map(|(key, value)| (key.clone(), update_value(value.clone()))),
        );
        (!metadata.is_empty()).then_some(metadata)
    }
}

/// The tolerances of `np.allclose`, with which the tests of Chroma take an embedding read back as the one written
/// (`_field_matches` in chromadb/test/property/invariants.py). HNSW with the cosine space keeps every embedding normalized,
/// with its length, and gives back their product, whose last bit can change.
const RELATIVE_TOLERANCE: f64 = 1e-5;
const ABSOLUTE_TOLERANCE: f64 = 1e-8;

/// Whether the embedding read on the other side is the one written on this side, as `np.allclose(read, written)` says.
fn same_embedding(written: Option<&[f32]>, read: Option<&[f32]>) -> bool {
    match (written, read) {
        (Some(written), Some(read)) => {
            written.len() == read.len()
                && written.iter().zip(read).all(|(&written, &read)| {
                    let (written, read) = (f64::from(written), f64::from(read));
                    (read - written).abs()
                        <= ABSOLUTE_TOLERANCE + RELATIVE_TOLERANCE * written.abs()
                })
        }
        (written, read) => written == read,
    }
}

fn update_value(value: MetadataValue) -> UpdateMetadataValue {
    match value {
        MetadataValue::Bool(value) => value.into(),
        MetadataValue::Int(value) => value.into(),
        MetadataValue::Float(value) => value.into(),
        MetadataValue::Str(value) => value.into(),
        MetadataValue::SparseVector(value) => value.into(),
        MetadataValue::BoolArray(value) => value.into(),
        MetadataValue::IntArray(value) => value.into(),
        MetadataValue::FloatArray(value) => value.into(),
        MetadataValue::StringArray(value) => value.into(),
    }
}

fn records_of(response: SearchResponse) -> Vec<Record> {
    let ids = response.ids.into_iter().next().unwrap_or_default();
    let mut documents = response
        .documents
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_default()
        .into_iter();
    let mut embeddings = response
        .embeddings
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_default()
        .into_iter();
    let mut metadatas = response
        .metadatas
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_default()
        .into_iter();
    ids.into_iter()
        .map(|id| Record {
            id,
            document: documents.next().flatten(),
            embedding: embeddings.next().flatten(),
            metadata: metadatas.next().flatten(),
        })
        .collect()
}

/// The records of a get, as a search gives them: a get gives the URI of a record apart, a search in its metadata, where the
/// write path stores it (`to_records`).
fn records_of_get(response: GetResponse) -> Vec<Record> {
    let mut documents = response.documents.unwrap_or_default().into_iter();
    let mut embeddings = response.embeddings.unwrap_or_default().into_iter();
    let mut uris = response.uris.unwrap_or_default().into_iter();
    let mut metadatas = response.metadatas.unwrap_or_default().into_iter();
    response
        .ids
        .into_iter()
        .map(|id| {
            let mut metadata = metadatas.next().flatten();
            if let Some(uri) = uris.next().flatten() {
                metadata
                    .get_or_insert_with(Metadata::new)
                    .insert(CHROMA_URI_KEY.to_string(), MetadataValue::Str(uri));
            }
            Record {
                id,
                document: documents.next().flatten(),
                embedding: embeddings.next(),
                metadata,
            }
        })
        .collect()
}

fn search_of_records(limit: u32, offset: u32) -> SearchPayload {
    SearchPayload::default().limit(Some(limit), offset).select([
        Key::Document,
        Key::Embedding,
        Key::Metadata,
    ])
}

/// A page of the source, and what the target holds of it when the copy asks. The target is read with a get, which every
/// server has: Chroma 1.5.9 answers a search with 501.
struct Page {
    offset: u32,
    records: Vec<Record>,
    on_target: HashMap<String, Record>,
}

async fn read_page(
    source: &ChromaCollection,
    target: &ChromaCollection,
    offset: u32,
    step: u32,
    look_on_target: bool,
    progress: &Progress,
) -> Result<Page, ChromaHttpClientError> {
    let search = search_of_records(step, offset);
    let response = until_done("the source", progress, || {
        source.search(vec![search.clone()])
    })
    .await?;
    let records = records_of(response);
    let mut on_target = HashMap::new();
    if look_on_target && !records.is_empty() {
        let ids: Vec<String> = records.iter().map(|record| record.id.clone()).collect();
        let include = IncludeList(vec![
            Include::Document,
            Include::Embedding,
            Include::Metadata,
            Include::Uri,
        ]);
        let response = until_done("the target", progress, || {
            target.get(Some(ids.clone()), None, None, None, Some(include.clone()))
        })
        .await?;
        on_target = records_of_get(response)
            .into_iter()
            .map(|record| (record.id.clone(), record))
            .collect();
    }
    Ok(Page {
        offset,
        records,
        on_target,
    })
}

/// A collection on the target, compared record by record with the source.
#[derive(Debug, Default)]
struct Comparison {
    /// The records of the target that are in the source.
    found: u32,
    /// The pages of the source with something to write, and whether a record of the page is missing on the target,
    /// or the page only has records with something different.
    pages: Vec<(u32, bool)>,
    /// The first page of the source after all those that the target has.
    rest: u32,
    /// The records on both sides with something different, and the first of them.
    different: u32,
    first_difference: Option<String>,
}

/// Reads every record of the source that the target should have, and every one of them on the target, until it has found
/// all the records of the target.
async fn compare_with_source(
    source: &ChromaCollection,
    target: &ChromaCollection,
    on_source: u32,
    on_target: u32,
    options: CopyOptions,
    progress: &Progress,
) -> Result<Comparison, ChromaHttpClientError> {
    let step = options.step;
    let mut pages = stream::iter((0..on_source).step_by(step as usize))
        .map(|offset| {
            let source = source.clone();
            let target = target.clone();
            let progress = progress.clone();
            tokio::spawn(
                async move { read_page(&source, &target, offset, step, true, &progress).await },
            )
        })
        .buffered(options.concurrent as usize);
    let mut comparison = Comparison::default();
    while comparison.found < on_target {
        let Some(page) = pages.next().await else {
            break;
        };
        let page = page.expect("the read of a page panicked")?;
        let mut missing = false;
        let mut different = false;
        for record in &page.records {
            let Some(found) = page.on_target.get(&record.id) else {
                missing = true;
                continue;
            };
            comparison.found += 1;
            progress.advance(1);
            if let Some(what) = record.difference(found) {
                different = true;
                comparison.different += 1;
                comparison
                    .first_difference
                    .get_or_insert_with(|| format!("the {what} of record {}", record.id));
            }
        }
        if missing || different {
            comparison.pages.push((page.offset, missing));
        }
        comparison.rest = page.offset + step;
        // With few records in common the bar stays still, while the source is read to its end.
        progress.say(format!(
            "{} records of the source read",
            comparison.rest.min(on_source)
        ));
    }
    progress.finish();
    Ok(comparison)
}

fn metadata_of(collection: &ChromaCollection) -> Option<&Metadata> {
    collection
        .metadata()
        .as_ref()
        .filter(|metadata| !metadata.is_empty())
}

async fn create_on_target(
    source: &ChromaCollection,
    target: &ChromaHttpClient,
    progress: &Progress,
) -> Result<ChromaCollection, ChromaHttpClientError> {
    // Chroma refuses a schema together with the old settings hnsw:* in the metadata of a new collection. They arrive at
    // the end of the copy, with the metadata of the collection.
    let old_settings = metadata_of(source)
        .is_some_and(|metadata| metadata.keys().any(|key| key.starts_with("hnsw:")));
    let metadata = if source.schema().is_some() && old_settings {
        None
    } else {
        source.metadata().clone()
    };
    let created = until_done("the target", progress, || {
        target.create_collection(source.name(), source.schema().clone(), metadata.clone())
    })
    .await;
    match created {
        // The answer to the creation was lost on the way, or someone else has just created it: it is the destination.
        Err(ChromaHttpClientError::ApiError(_, StatusCode::CONFLICT)) => {
            until_done("the target", progress, || {
                target.get_collection(source.name())
            })
            .await
        }
        created => created,
    }
}

fn write_error(error: ChromaHttpClientError) -> CliError {
    if is_quota(&error.to_string()) {
        let msg = serde_json::from_str::<ErrorResponse>(&error.to_string())
            .unwrap_or_default()
            .message;
        return CliError::Utils(UtilsError::Quota(msg));
    }
    CliError::ChromaClient(error)
}

async fn add_records(
    target: &ChromaCollection,
    records: &[Record],
    progress: &Progress,
) -> Result<(), CliError> {
    let ids: Vec<String> = records.iter().map(|record| record.id.clone()).collect();
    let embeddings: Vec<Vec<f32>> = records
        .iter()
        .filter_map(|record| record.embedding.clone())
        .collect();
    let documents = records
        .iter()
        .any(|record| record.document.is_some())
        .then(|| {
            records
                .iter()
                .map(|record| record.document.clone())
                .collect::<Vec<_>>()
        });
    let metadatas = records
        .iter()
        .any(|record| record.metadata.is_some())
        .then(|| {
            records
                .iter()
                .map(|record| record.metadata.clone())
                .collect::<Vec<_>>()
        });
    until_done("the target", progress, || {
        target.add(
            ids.clone(),
            embeddings.clone(),
            documents.clone(),
            None,
            metadatas.clone(),
        )
    })
    .await
    .map_err(write_error)?;
    Ok(())
}

/// Writes again on the target the records that are different, in their place in the collection.
async fn upsert_records(
    target: &ChromaCollection,
    records: &[(Record, Record)],
    progress: &Progress,
) -> Result<(), CliError> {
    let ids: Vec<String> = records
        .iter()
        .map(|(record, _)| record.id.clone())
        .collect();
    let embeddings: Vec<Vec<f32>> = records
        .iter()
        .filter_map(|(record, _)| record.embedding.clone())
        .collect();
    let documents: Vec<Option<String>> = records
        .iter()
        .map(|(record, _)| record.document.clone())
        .collect();
    let metadatas: Vec<Option<UpdateMetadata>> = records
        .iter()
        .map(|(record, found)| record.metadata_over(found))
        .collect();
    until_done("the target", progress, || {
        target.upsert(
            ids.clone(),
            embeddings.clone(),
            Some(documents.clone()),
            None,
            Some(metadatas.clone()),
        )
    })
    .await
    .map_err(write_error)?;
    Ok(())
}

/// Writes what the target does not have of these pages of the source and, when asked, what it has with something
/// different, and returns how many of the second. The pages are read at the same time and written one at a time, in their
/// order: a record takes its place in the collection when it is written, and among equal scores a search follows that
/// place, so a copy written out of order answers in another order.
async fn write_pages(
    source: &ChromaCollection,
    target: &ChromaCollection,
    pages: Vec<(u32, bool)>,
    write_again: bool,
    options: CopyOptions,
    progress: &Progress,
) -> Result<u32, CliError> {
    let step = options.step;
    let mut pages = stream::iter(pages)
        .map(|(offset, look_on_target)| {
            let source = source.clone();
            let target = target.clone();
            let progress = progress.clone();
            tokio::spawn(async move {
                read_page(&source, &target, offset, step, look_on_target, &progress).await
            })
        })
        .buffered(options.concurrent as usize);
    let mut written_again = 0;
    while let Some(page) = pages.next().await {
        let Page {
            records,
            mut on_target,
            ..
        } = page.expect("the read of a page panicked")?;
        let mut to_add = Vec::with_capacity(records.len());
        let mut to_upsert = vec![];
        let mut to_delete = vec![];
        for record in records {
            match on_target.remove(&record.id) {
                None => to_add.push(record),
                Some(found) if !write_again || record.difference(&found).is_none() => {}
                // An upsert does not take a document away.
                Some(found) if record.document.is_none() && found.document.is_some() => {
                    to_delete.push(record.id.clone());
                    to_add.push(record);
                }
                Some(found) => to_upsert.push((record, found)),
            }
        }
        if !to_upsert.is_empty() {
            upsert_records(target, &to_upsert, progress).await?;
        }
        if !to_delete.is_empty() {
            until_done("the target", progress, || {
                target.delete(Some(to_delete.clone()), None, None)
            })
            .await?;
        }
        if !to_add.is_empty() {
            add_records(target, &to_add, progress).await?;
        }
        written_again += (to_upsert.len() + to_delete.len()) as u32;
        progress.advance((to_add.len() - to_delete.len()) as u64);
    }
    Ok(written_again)
}

/// The ids of the records of the target that are not on the source, by reading the ids of the target until it has found
/// as many as the comparison has counted.
async fn ids_only_on_target(
    source: &ChromaCollection,
    target: &ChromaCollection,
    how_many: u32,
    on_target: u32,
    options: CopyOptions,
    progress: &Progress,
) -> Result<Vec<String>, ChromaHttpClientError> {
    let step = options.step;
    let mut pages = stream::iter((0..on_target).step_by(step as usize))
        .map(|offset| {
            let source = source.clone();
            let target = target.clone();
            let progress = progress.clone();
            tokio::spawn(async move {
                only_on_target_in_page(&source, &target, offset, step, &progress).await
            })
        })
        .buffered(options.concurrent as usize);
    let mut only_on_target = vec![];
    while (only_on_target.len() as u32) < how_many {
        let Some(page) = pages.next().await else {
            break;
        };
        let (read, ids) = page.expect("the read of a page panicked")?;
        progress.advance(read);
        only_on_target.extend(ids);
    }
    Ok(only_on_target)
}

/// How many records this page of the target has, and those of them that the source does not have.
async fn only_on_target_in_page(
    source: &ChromaCollection,
    target: &ChromaCollection,
    offset: u32,
    step: u32,
    progress: &Progress,
) -> Result<(u64, Vec<String>), ChromaHttpClientError> {
    let page = until_done("the target", progress, || {
        target.get(
            None,
            None,
            Some(step),
            Some(offset),
            Some(IncludeList::empty()),
        )
    })
    .await?;
    let ids = page.ids;
    if ids.is_empty() {
        return Ok((0, ids));
    }
    let search = SearchPayload::default()
        .limit(Some(ids.len() as u32), 0)
        .r#where(Key::field("#id").is_in(ids.clone()));
    let found = until_done("the source", progress, || {
        source.search(vec![search.clone()])
    })
    .await?;
    let found: HashSet<String> = found
        .ids
        .into_iter()
        .next()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let read = ids.len() as u64;
    Ok((
        read,
        ids.into_iter().filter(|id| !found.contains(id)).collect(),
    ))
}

/// Takes these records away from the target, a page at a time, and says how many are gone.
async fn remove_records(
    target: &ChromaCollection,
    ids: &[String],
    step: u32,
    progress: &Progress,
) -> Result<u32, ChromaHttpClientError> {
    let mut removed = 0;
    for ids in ids.chunks(step as usize) {
        let ids = ids.to_vec();
        until_done("the target", progress, || {
            target.delete(Some(ids.clone()), None, None)
        })
        .await?;
        removed += ids.len() as u32;
        progress.advance(ids.len() as u64);
    }
    Ok(removed)
}

/// What to do with a collection already on the target.
#[derive(Clone, Copy, Debug, PartialEq)]
enum OnExisting {
    /// Add the records it does not have.
    Resume,
    /// Make it an exact copy of the source.
    Force,
}

/// How the copy goes.
#[derive(Clone, Copy)]
struct CopyOptions {
    step: u32,
    concurrent: u32,
    on_existing: Option<OnExisting>,
    /// Do not ask what to do with a collection already on the target.
    yes: bool,
    /// A local server keeps the schema it receives. Chroma Cloud has its own, also after a copy to it: there the schemas
    /// do not say whether two collections are the same.
    compare_schemas: bool,
}

/// A collection already on the target, compared with its source.
struct Existing {
    collection: ChromaCollection,
    on_source: u32,
    on_target: u32,
    comparison: Comparison,
    schema_differs: bool,
}

impl Existing {
    fn same(&self) -> u32 {
        self.comparison.found - self.comparison.different
    }

    // The counts come from another moment than the comparison: the records can change in between.
    fn missing(&self) -> u32 {
        self.on_source.saturating_sub(self.comparison.found)
    }

    fn only_on_target(&self) -> u32 {
        self.on_target.saturating_sub(self.comparison.found)
    }

    fn is_the_same_as(&self, source: &ChromaCollection) -> bool {
        self.missing() == 0
            && self.only_on_target() == 0
            && self.comparison.different == 0
            && metadata_of(source) == metadata_of(&self.collection)
            && !self.schema_differs
    }

    fn report(&self, source: &ChromaCollection) -> String {
        let mut report = format!(
            "Collection {} is already on the target: {} records the same, {} missing, {} different, {} only on the target",
            source.name(),
            self.same(),
            self.missing(),
            self.comparison.different,
            self.only_on_target()
        );
        if let Some(first) = &self.comparison.first_difference {
            report.push_str(&format!(". For example, {first} is different"));
        }
        if metadata_of(source) != metadata_of(&self.collection) {
            report.push_str(". The metadata of the collection are different");
        }
        if self.schema_differs {
            report.push_str(". Its schema or its dimensions are different");
        }
        report
    }

    /// What the copy would do, in words, before it does it.
    fn question(&self, source: &ChromaCollection, what: OnExisting) -> String {
        let name = source.name();
        match what {
            OnExisting::Resume => format!(
                "Add to collection {name} the {} records it does not have, and leave the other {} as they are?",
                self.missing(),
                self.on_target
            ),
            OnExisting::Force if self.schema_differs => format!(
                "Delete collection {name}, with its {} records, and copy it again? Its schema or its dimensions are different, and they do not change",
                self.on_target
            ),
            OnExisting::Force => format!(
                "Make collection {name} an exact copy of its source: {} records added, {} written again, {} REMOVED?",
                self.missing(),
                self.comparison.different,
                self.only_on_target()
            ),
        }
    }
}

/// Compares the collection with the name of the source on the target, if there is one. It writes nothing.
async fn look_at_target(
    source: &ChromaCollection,
    target: &ChromaHttpClient,
    options: CopyOptions,
    target_holds_spann: bool,
    term: &mut dyn Terminal,
) -> Result<Option<Existing>, ChromaHttpClientError> {
    let name = source.name();
    let waiting = Progress::new(format!("Collection {name}"), 0, 0);
    let collection = match until_done("the target", &waiting, || target.get_collection(name)).await
    {
        Ok(collection) => collection,
        Err(ChromaHttpClientError::ApiError(_, StatusCode::NOT_FOUND)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let on_source = until_done("the source", &waiting, || source.count()).await?;
    let on_target = until_done("the target", &waiting, || collection.count()).await?;
    term.println(&format!("Comparing collection {name} with its source"));
    let progress = Progress::new(
        format!("Comparing {name} with its source"),
        on_target as u64,
        0,
    );
    let comparison = compare_with_source(
        source,
        &collection,
        on_source,
        on_target,
        options,
        &progress,
    )
    .await?;
    // A server that does not say it holds SPANN, as Chroma 1.5.9, gives HNSW with its own settings to a collection whose
    // source has SPANN: it is the same collection.
    let dense_index = target_holds_spann || !has_spann(source.schema());
    let schema_differs = options.compare_schemas
        && (chosen_settings(source.schema(), dense_index)
            != chosen_settings(collection.schema(), dense_index)
            || other_dimensions(source, &collection));
    Ok(Some(Existing {
        collection,
        on_source,
        on_target,
        comparison,
        schema_differs,
    }))
}

/// What of a schema someone has chosen. A key of the metadata of the records enters the schema by itself, with the settings
/// of the defaults, when an index first sees it (`Schema::ensure_key_from_metadata`): on the source and on the target at
/// different moments, so those keys do not say that two schemas are different. Without `dense_index`, the settings of
/// SPANN and of HNSW are not a choice either.
fn chosen_settings(schema: &Option<Schema>, dense_index: bool) -> Option<Schema> {
    let mut schema = schema.clone()?;
    if !dense_index {
        for types in std::iter::once(&mut schema.defaults).chain(schema.keys.values_mut()) {
            if let Some(index) = types
                .float_list
                .as_mut()
                .and_then(|list| list.vector_index.as_mut())
            {
                index.config.hnsw = None;
                index.config.spann = None;
            }
        }
    }
    let defaults = schema.defaults.clone();
    schema.keys.retain(|_, types| {
        if types.string == defaults.string {
            types.string = None;
        }
        if types.float_list == defaults.float_list {
            types.float_list = None;
        }
        if types.sparse_vector == defaults.sparse_vector {
            types.sparse_vector = None;
        }
        if types.int == defaults.int {
            types.int = None;
        }
        if types.float == defaults.float {
            types.float = None;
        }
        if types.boolean == defaults.boolean {
            types.boolean = None;
        }
        *types != ValueTypes::default()
    });
    Some(schema)
}

fn has_spann(schema: &Option<Schema>) -> bool {
    schema.as_ref().is_some_and(|schema| {
        std::iter::once(&schema.defaults)
            .chain(schema.keys.values())
            .filter_map(|types| types.float_list.as_ref()?.vector_index.as_ref())
            .any(|index| index.config.spann.is_some())
    })
}

/// Whether the embeddings of the two collections have different sizes: the records of one do not go in the other.
fn other_dimensions(source: &ChromaCollection, target: &ChromaCollection) -> bool {
    let dimension = |collection: &ChromaCollection| collection.to_collection_model().dimension;
    matches!((dimension(source), dimension(target)), (Some(a), Some(b)) if a != b)
}

/// Asks what to do with a collection already on the target, also when the flag of the command has said it.
fn ask_what_to_do(
    source: &ChromaCollection,
    existing: &Existing,
    flag: Option<OnExisting>,
    term: &mut dyn Terminal,
) -> Result<Option<OnExisting>, CliError> {
    if let Some(what) = flag {
        let yes = term.prompt_confirm(&existing.question(source, what))?;
        return Ok(yes.then_some(what));
    }
    term.println(&format!(
        "What should be done with collection {}?",
        source.name()
    ));
    let resume = existing.question(source, OnExisting::Resume);
    let force = existing.question(source, OnExisting::Force);
    Ok(
        match term.prompt_select(&[resume, force, "Leave it as it is".to_string()])? {
            0 => Some(OnExisting::Resume),
            1 => Some(OnExisting::Force),
            _ => None,
        },
    )
}

/// Writes a collection on the target, and says what it did. With a collection already there, it does what was asked.
async fn write_collection(
    source: &ChromaCollection,
    target: &ChromaCollection,
    existing: Option<(&Existing, OnExisting)>,
    options: CopyOptions,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let name = source.name();
    let step = options.step;
    let waiting = Progress::new(format!("Collection {name}"), 0, 0);
    let on_source = until_done("the source", &waiting, || source.count()).await?;
    let nothing = Comparison::default();
    let comparison = existing.map_or(&nothing, |(existing, _)| &existing.comparison);
    let force = existing.is_some_and(|(_, what)| what == OnExisting::Force);
    let on_target = existing.map_or(0, |(existing, _)| existing.on_target);
    let only_on_target = existing.map_or(0, |(existing, _)| existing.only_on_target());
    // The records only on the target stay, unless the target has to be an exact copy.
    let staying = if force { 0 } else { only_on_target };
    term.println(&format!("Copying collection: {name}"));
    let mut removed = 0;
    if force && only_on_target > 0 {
        let looking = Progress::new(
            format!("Looking in {name} for the records to remove"),
            on_target as u64,
            0,
        );
        let ids = ids_only_on_target(source, target, only_on_target, on_target, options, &looking)
            .await?;
        looking.finish();
        let removing = Progress::new(format!("Removing from {name}"), ids.len() as u64, 0);
        removed = remove_records(target, &ids, step, &removing).await?;
        removing.finish();
    }
    let progress = Progress::new(
        format!("Copying {name}"),
        (on_source + staying) as u64,
        on_target.saturating_sub(only_on_target - staying) as u64,
    );
    // The pages with only records that are different have something to do only when those are written again.
    let pages: Vec<(u32, bool)> = comparison
        .pages
        .iter()
        .filter(|(_, missing)| force || *missing)
        .map(|(offset, _)| (*offset, true))
        .chain(
            (comparison.rest..on_source)
                .step_by(step as usize)
                .map(|offset| (offset, false)),
        )
        .collect();
    let written_again = write_pages(source, target, pages, force, options, &progress).await?;
    progress.finish();

    // A new collection gets here its old settings, if it has them: see create_on_target.
    if (existing.is_none() || force) && metadata_of(source) != metadata_of(target) {
        let metadata = source.metadata().clone().unwrap_or_default();
        until_done("the target", &waiting, || {
            let mut target = target.clone();
            let metadata = metadata.clone();
            async move { target.modify(None::<&str>, Some(metadata)).await }
        })
        .await?;
    }

    let on_target_now = until_done("the target", &waiting, || target.count()).await?;
    let on_source_now = until_done("the source", &waiting, || source.count()).await?;
    if on_target_now != on_source_now + staying {
        return Err(CopyError::SourceChanged {
            on_target: on_target_now,
            expected: on_source_now + staying,
        }
        .into());
    }
    term.println(&format!(
        "Collection {name}: {} records added, {written_again} written again, {removed} removed",
        (on_target_now + removed).saturating_sub(on_target)
    ));
    Ok(())
}

/// Does what was decided for a collection, on the target.
async fn copy_collection(
    source: &ChromaCollection,
    target: &ChromaHttpClient,
    existing: Option<(Existing, OnExisting)>,
    options: CopyOptions,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let waiting = Progress::new(format!("Collection {}", source.name()), 0, 0);
    match existing {
        Some((existing, OnExisting::Force)) if existing.schema_differs => {
            // A schema and the dimensions do not change: the collection is created again.
            until_done("the target", &waiting, || {
                target.delete_collection(source.name())
            })
            .await?;
            term.println(&format!(
                "Collection {}: deleted, with its {} records, and copied again",
                source.name(),
                existing.on_target
            ));
            let created = create_on_target(source, target, &waiting).await?;
            write_collection(source, &created, None, options, term).await
        }
        Some((existing, what)) => {
            write_collection(
                source,
                &existing.collection,
                Some((&existing, what)),
                options,
                term,
            )
            .await
        }
        None => {
            let created = create_on_target(source, target, &waiting).await?;
            write_collection(source, &created, None, options, term).await
        }
    }
}

async fn copy_collections(
    source: ChromaHttpClient,
    target: ChromaHttpClient,
    collections: Vec<String>,
    all: bool,
    options: CopyOptions,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    // The copy decides when to send a request again, and says why it waits.
    let source = source.without_retries();
    let target = target.without_retries();
    let waiting = Progress::new("Looking for the collections".to_string(), 0, 0);
    let collections = if all {
        const PAGE: usize = 1000;
        let mut all_collections = vec![];
        loop {
            let page = until_done("the source", &waiting, || {
                source.list_collections(PAGE, Some(all_collections.len()))
            })
            .await?;
            let last = page.len() < PAGE;
            all_collections.extend(page);
            if last {
                break;
            }
        }
        all_collections
    } else {
        let mut source_collections = vec![];
        for collection in collections {
            let source_collection = until_done("the source", &waiting, || {
                source.get_collection(&collection)
            })
            .await?;
            source_collections.push(source_collection);
        }
        source_collections
    };

    if collections.is_empty() {
        return Err(CopyError::NoCollections.into());
    }

    term.println(&format!(
        "{}",
        start_copy_prompt(collections.len()).bold().blue()
    ));

    // What the target says it holds, in the server block of its pre-flight checks. A server that says nothing of
    // itself, as Chroma 1.5.9, or that does not answer them, holds what Chroma 1.5.9 holds.
    let target_holds_spann = options.compare_schemas
        && until_done("the target", &waiting, || target.server_features())
            .await
            .unwrap_or_default()
            .iter()
            .any(|feature| feature == "spann");

    // First the checks, of all the collections, then the questions, all together, and only then the writes: a collection
    // that cannot be copied does not stop the others.
    let mut not_copied = vec![];
    let mut compared = Vec::with_capacity(collections.len());
    for collection in &collections {
        match look_at_target(collection, &target, options, target_holds_spann, term).await {
            Ok(existing) => compared.push((collection, existing)),
            Err(error) => not_copied.push(format!(
                "Collection {}: {error}. When that is solved, run the same command again to go on from there",
                collection.name()
            )),
        }
    }
    let mut to_write = Vec::with_capacity(compared.len());
    for (collection, existing) in compared {
        let Some(existing) = existing else {
            to_write.push((collection, None));
            continue;
        };
        let name = collection.name();
        term.println(&existing.report(collection));
        if existing.is_the_same_as(collection) {
            term.println(&format!("Collection {name} is already copied"));
            continue;
        }
        let what = match options.on_existing {
            Some(what) if options.yes => {
                term.println(&format!("{} Yes", existing.question(collection, what)));
                Some(what)
            }
            flag if term.can_ask() => match ask_what_to_do(collection, &existing, flag, term) {
                Ok(what) => what,
                Err(_) => {
                    not_copied.push(CopyError::NoAnswer(name.to_string()).to_string());
                    continue;
                }
            },
            _ => {
                not_copied.push(format!(
                    "Collection {name} is already on the target and stays as it is, because there is no terminal to ask what to do: add --resume --yes to add the records it does not have, or --force --yes to make it an exact copy of the source"
                ));
                continue;
            }
        };
        match what {
            Some(what) => to_write.push((collection, Some((existing, what)))),
            None => term.println(&format!("Collection {name} stays as it is")),
        }
    }
    for (collection, existing) in to_write {
        if let Err(error) = copy_collection(collection, &target, existing, options, term).await {
            not_copied.push(format!(
                "Collection {}: {error}. When that is solved, run the same command again to go on from there",
                collection.name()
            ));
        }
    }
    if !not_copied.is_empty() {
        return Err(CopyError::NotAllCopied(not_copied.join("\n")).into());
    }

    term.println("Copy Completed!");

    Ok(())
}

pub fn copy(args: CopyArgs) -> Result<(), CliError> {
    let mut term = SystemTerminal;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InstallError::RuntimeError)?;
    runtime.block_on(async {
        if !args.all && args.collections.is_empty() {
            return Err(CopyError::NoCollections.into());
        }

        let store = FileConfigStore::default();
        let (_, profile) = store.get_current_profile()?;
        let (source, target) = get_target_and_destination(&args, &mut term)?;
        let (source_client, target_client, _handle) =
            get_chroma_clients(&args, source, target, profile, &mut term).await?;
        copy_collections(
            source_client,
            target_client,
            args.collections,
            args.all,
            CopyOptions {
                step: args.batch,
                concurrent: args.concurrent,
                on_existing: if args.resume {
                    Some(OnExisting::Resume)
                } else if args.force {
                    Some(OnExisting::Force)
                } else {
                    None
                },
                yes: args.yes,
                compare_schemas: matches!(target, Environment::Local),
            },
            &mut term,
        )
        .await?;
        Ok::<(), CliError>(())
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;

    fn default_args() -> CopyArgs {
        CopyArgs {
            all: false,
            collections: vec![],
            from_local: false,
            from_cloud: false,
            to_local: false,
            to_cloud: false,
            db: None,
            host: None,
            path: None,
            resume: false,
            force: false,
            yes: false,
            batch: 100,
            concurrent: 5,
        }
    }

    #[test]
    fn test_get_target_and_destination_from_cloud() {
        let mut args = default_args();
        args.from_cloud = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Cloud));
        assert!(matches!(target, Environment::Local));
    }

    #[test]
    fn test_get_target_and_destination_from_local() {
        let mut args = default_args();
        args.from_local = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Local));
        assert!(matches!(target, Environment::Cloud));
    }

    #[test]
    fn test_get_target_and_destination_to_cloud() {
        let mut args = default_args();
        args.to_cloud = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Local));
        assert!(matches!(target, Environment::Cloud));
    }

    #[test]
    fn test_get_target_and_destination_to_local() {
        let mut args = default_args();
        args.to_local = true;
        let mut term = TestTerminal::new();
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Cloud));
        assert!(matches!(target, Environment::Local));
    }

    #[test]
    fn test_get_target_and_destination_interactive() {
        let args = default_args();
        // Select index 0 = Cloud (source), so target = Local
        let mut term = TestTerminal::new().with_inputs(vec!["0"]);
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Cloud));
        assert!(matches!(target, Environment::Local));

        // Select index 1 = Local (source), so target = Cloud
        let mut term = TestTerminal::new().with_inputs(vec!["1"]);
        let (source, target) = get_target_and_destination(&args, &mut term).unwrap();
        assert!(matches!(source, Environment::Local));
        assert!(matches!(target, Environment::Cloud));
    }

    fn with_metadata(pairs: &[(&str, MetadataValue)]) -> Option<Metadata> {
        Some(
            pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
        )
    }

    fn record(i: usize) -> Record {
        Record {
            id: format!("r{i}"),
            document: Some(format!("document {i}")),
            embedding: Some(vec![i as f32, 0.5]),
            metadata: with_metadata(&[("n", MetadataValue::Int(i as i64))]),
        }
    }

    fn records(range: std::ops::Range<usize>) -> Vec<Record> {
        range.map(record).collect()
    }

    #[test]
    fn a_record_without_metadata_is_the_same_as_one_with_empty_metadata() {
        let mut one = record(1);
        let mut other = record(1);
        one.metadata = None;
        other.metadata = Some(Metadata::new());
        assert_eq!(one.difference(&other), None);
    }

    #[test]
    fn a_comparison_says_what_of_a_record_is_different() {
        let mut other = record(1);
        other.document = Some("another".to_string());
        assert_eq!(record(1).difference(&other), Some("document"));
        let mut other = record(1);
        other.embedding = Some(vec![9.0, 0.5]);
        assert_eq!(record(1).difference(&other), Some("embedding"));
        let mut other = record(1);
        other.metadata = with_metadata(&[("n", MetadataValue::Int(9))]);
        assert_eq!(record(1).difference(&other), Some("metadata"));
    }

    #[test]
    fn an_embedding_is_the_same_within_the_tolerances_of_np_allclose() {
        let same = |written: &[f32], read: &[f32]| same_embedding(Some(written), Some(read));
        assert!(same(
            &[1.0, 0.2, 0.5],
            &[1.0, f32::from_bits(0x3E4C_CCCC), 0.5]
        ));
        assert!(same(&[2.0], &[2.0 + 2.0 * 0.9e-5]));
        assert!(!same(&[2.0], &[2.0 + 2.0 * 1.1e-5]));
        // A number near zero has the absolute tolerance.
        assert!(same(&[0.0], &[0.9e-8]));
        assert!(!same(&[0.0], &[1.1e-8]));
        assert!(!same(&[1.0, 0.2, 0.5], &[1.0, 0.3, 0.5]));
        assert!(!same(&[1.0, 0.2], &[1.0, 0.2, 0.5]));
        assert!(!same_embedding(Some(&[1.0]), None));
        assert!(same_embedding(None, None));
    }

    #[test]
    fn an_upsert_takes_away_the_keys_the_source_no_longer_has() {
        let mut found = record(1);
        found.metadata = with_metadata(&[
            ("n", MetadataValue::Int(9)),
            ("gone", MetadataValue::Str("x".to_string())),
        ]);
        let metadata = record(1).metadata_over(&found).unwrap();
        assert_eq!(metadata["n"], UpdateMetadataValue::Int(1));
        assert_eq!(metadata["gone"], UpdateMetadataValue::None);
    }

    /// A step as it is said where the bar is not shown, as in a container.
    fn without_bar(what: &str, records: u64) -> Progress {
        let progress = Progress::new(what.to_string(), records, 0);
        progress
            .bar
            .set_draw_target(indicatif::ProgressDrawTarget::hidden());
        progress
    }

    #[test]
    fn where_the_bar_is_not_shown_the_end_of_a_step_is_said_once() {
        let progress = without_bar("Removing", 2);
        progress.bar.inc(2);
        assert_eq!(
            progress
                .next_line(&progress.last_line, "", false)
                .as_deref(),
            Some("Removing: 2 of 2 records. ")
        );
        assert_eq!(progress.next_line(&progress.last_line, "", true), None);

        // A step that went on after its last line says where it ended.
        let progress = without_bar("Copying", 300);
        progress.bar.inc(100);
        assert!(progress.next_line(&progress.last_line, "", false).is_some());
        progress.bar.inc(200);
        assert_eq!(progress.next_line(&progress.last_line, "", false), None);
        assert_eq!(
            progress.next_line(&progress.last_line, "", true).as_deref(),
            Some("Copying: 300 of 300 records. ")
        );
    }

    /// A server with the part of the API that a copy uses, on a real socket, whose answers can fail, and that remembers what
    /// it is asked to write.
    mod server {
        use super::*;
        use axum::body::Body;
        use axum::extract::{Path, State};
        use axum::response::{IntoResponse, Response};
        use axum::routing::{get, post};
        use axum::{Json, Router};
        use chroma_types::{Collection, CollectionUuid, Schema, CHROMA_KEY};
        use serde_json::{json, Value};
        use std::collections::VecDeque;

        #[derive(Clone, Copy, Debug)]
        pub enum Failure {
            /// A 429 with the header Retry-After.
            Refused(u64),
            /// A 429 without it.
            RefusedWithoutTime,
            Status(u16),
            /// An answer whose body stops: what a client sees when the network falls.
            Cut,
            /// No answer before the copy gives up on the request.
            Silent,
        }

        pub struct StoredCollection {
            pub id: CollectionUuid,
            pub name: String,
            pub schema: Option<Schema>,
            pub metadata: Option<Metadata>,
            pub dimension: Option<i32>,
            pub records: Vec<Record>,
        }

        impl StoredCollection {
            pub fn new(name: &str, records: Vec<Record>) -> Self {
                Self {
                    id: CollectionUuid::new(),
                    name: name.to_string(),
                    schema: None,
                    metadata: None,
                    dimension: None,
                    records,
                }
            }
        }

        #[derive(Default)]
        pub struct Server {
            pub collections: Vec<StoredCollection>,
            pub add_failures: VecDeque<Failure>,
            pub read_failures: VecDeque<Failure>,
            /// The answer of the next creation, once the collection is created.
            pub create_failures: VecDeque<Failure>,
            /// The first pages take the longest to read.
            pub first_pages_are_slow: bool,
            /// How long every read takes, a search or a get.
            pub read_delay: Duration,
            /// A server without the Search API, as Chroma 1.5.9: it answers a search with 501.
            pub without_search: bool,
            /// A server that gives HNSW with its own settings to a new collection that asks for SPANN, as Chroma 1.5.9.
            pub spann_becomes_hnsw: bool,
            /// A server whose index keeps every embedding normalized, with its length, and gives back their product, as HNSW
            /// with the cosine space.
            pub rebuilds_embeddings: bool,
            /// The features of the server block of the pre-flight checks. Chroma has no block.
            pub features: Option<Vec<String>>,
            pub reads: usize,
            pub reads_now: usize,
            pub most_reads_at_once: usize,
            /// Records that arrive on this server after the first count.
            pub after_first_count: Vec<Record>,
            /// The ids of every write that arrived, in the order they arrived.
            pub adds: Vec<Vec<String>>,
            pub upserts: Vec<Vec<String>>,
            pub deletes: Vec<Vec<String>>,
            /// The metadata of every creation.
            pub creations: Vec<Option<Metadata>>,
        }

        pub type Shared = Arc<Mutex<Server>>;

        fn failure_body(message: &str) -> Value {
            json!({"error": "ChromaError", "message": message})
        }

        async fn answer(failure: Failure) -> Response {
            match failure {
                Failure::Refused(seconds) => (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", seconds.to_string())],
                    Json(failure_body("log needs compaction")),
                )
                    .into_response(),
                Failure::RefusedWithoutTime => (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(failure_body("rate limited")),
                )
                    .into_response(),
                Failure::Status(status) => (
                    StatusCode::from_u16(status).unwrap(),
                    Json(failure_body("failure")),
                )
                    .into_response(),
                Failure::Cut => {
                    Response::new(Body::from_stream(stream::iter([Err::<Vec<u8>, _>(
                        std::io::Error::other("the network fell"),
                    )])))
                }
                Failure::Silent => {
                    tokio::time::sleep(ATTEMPT_TIMEOUT + Duration::from_secs(1)).await;
                    Json(json!({})).into_response()
                }
            }
        }

        fn collection_json(collection: &StoredCollection) -> Value {
            serde_json::to_value(Collection {
                collection_id: collection.id,
                name: collection.name.clone(),
                schema: collection.schema.clone(),
                metadata: collection.metadata.clone(),
                dimension: collection.dimension,
                tenant: "default_tenant".to_string(),
                database: "default_database".to_string(),
                ..Default::default()
            })
            .unwrap()
        }

        fn by_id<'a>(server: &'a mut Server, id: &str) -> &'a mut StoredCollection {
            server
                .collections
                .iter_mut()
                .find(|collection| collection.id.to_string() == id)
                .unwrap()
        }

        async fn create(State(server): State<Shared>, Json(body): Json<Value>) -> Response {
            let name = body["name"].as_str().unwrap().to_string();
            let schema: Option<Schema> = serde_json::from_value(body["schema"].clone()).unwrap();
            let metadata: Option<Metadata> =
                serde_json::from_value(body["metadata"].clone()).unwrap();
            let failure = {
                let mut server = server.lock().unwrap();
                if server.collections.iter().any(|c| c.name == name) {
                    return (
                        StatusCode::CONFLICT,
                        Json(failure_body("the collection exists")),
                    )
                        .into_response();
                }
                let old_settings = metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.keys().any(|key| key.starts_with("hnsw:")));
                if schema.is_some() && old_settings {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(failure_body(
                            "Cannot set both collection config and schema simultaneously",
                        )),
                    )
                        .into_response();
                }
                server.creations.push(metadata.clone());
                let mut schema = schema;
                if server.spann_becomes_hnsw {
                    for schema in schema.iter_mut() {
                        for types in
                            std::iter::once(&mut schema.defaults).chain(schema.keys.values_mut())
                        {
                            let Some(index) = types
                                .float_list
                                .as_mut()
                                .and_then(|list| list.vector_index.as_mut())
                            else {
                                continue;
                            };
                            if index.config.spann.take().is_some() {
                                index.config.hnsw = Some(Default::default());
                            }
                        }
                    }
                }
                let mut collection = StoredCollection::new(&name, vec![]);
                collection.schema = schema;
                collection.metadata = metadata;
                server.collections.push(collection);
                server.create_failures.pop_front()
            };
            if let Some(failure) = failure {
                return answer(failure).await;
            }
            let server = server.lock().unwrap();
            Json(collection_json(server.collections.last().unwrap())).into_response()
        }

        async fn list(State(server): State<Shared>) -> Json<Value> {
            let server = server.lock().unwrap();
            Json(Value::Array(
                server.collections.iter().map(collection_json).collect(),
            ))
        }

        async fn get_collection(
            State(server): State<Shared>,
            Path((_, _, name)): Path<(String, String, String)>,
        ) -> Response {
            let server = server.lock().unwrap();
            match server.collections.iter().find(|c| c.name == name) {
                Some(collection) => Json(collection_json(collection)).into_response(),
                None => (
                    StatusCode::NOT_FOUND,
                    Json(failure_body("the collection does not exist")),
                )
                    .into_response(),
            }
        }

        async fn delete_collection(
            State(server): State<Shared>,
            Path((_, _, name)): Path<(String, String, String)>,
        ) -> Json<Value> {
            let mut server = server.lock().unwrap();
            server
                .collections
                .retain(|collection| collection.name != name);
            Json(json!({}))
        }

        async fn modify(
            State(server): State<Shared>,
            Path((_, _, id)): Path<(String, String, String)>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            let mut server = server.lock().unwrap();
            by_id(&mut server, &id).metadata =
                serde_json::from_value(body["new_metadata"].clone()).unwrap();
            Json(json!({}))
        }

        async fn count(
            State(server): State<Shared>,
            Path((_, _, id)): Path<(String, String, String)>,
        ) -> Json<Value> {
            let mut server = server.lock().unwrap();
            let arriving = std::mem::take(&mut server.after_first_count);
            let collection = by_id(&mut server, &id);
            let count = collection.records.len();
            collection.records.extend(arriving);
            Json(json!(count))
        }

        /// What a search and a get have in common: they are counted, they take their time, and they can fail.
        async fn read(server: &Shared, offset: usize) -> Option<Failure> {
            let (failure, slow, delay) = {
                let mut server = server.lock().unwrap();
                server.reads += 1;
                server.reads_now += 1;
                server.most_reads_at_once = server.most_reads_at_once.max(server.reads_now);
                (
                    server.read_failures.pop_front(),
                    server.first_pages_are_slow,
                    server.read_delay,
                )
            };
            if slow {
                let delay = 300u64.saturating_sub(offset as u64 * 3);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            tokio::time::sleep(delay).await;
            server.lock().unwrap().reads_now -= 1;
            failure
        }

        async fn search(
            State(server): State<Shared>,
            Path((_, _, id)): Path<(String, String, String)>,
            Json(body): Json<Value>,
        ) -> Response {
            let search = &body["searches"][0];
            let limit = search["limit"]["limit"].as_u64().unwrap() as usize;
            let offset = search["limit"]["offset"].as_u64().unwrap() as usize;
            if let Some(failure) = read(&server, offset).await {
                return answer(failure).await;
            }
            if server.lock().unwrap().without_search {
                return (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(failure_body(
                        "Error executing plan: Not implemented: Search operation is not implemented for local executor",
                    )),
                )
                    .into_response();
            }
            let mut server = server.lock().unwrap();
            let collection = by_id(&mut server, &id);
            let found: Vec<&Record> = match search["filter"]["#id"]["$in"].as_array() {
                Some(ids) => ids
                    .iter()
                    .filter_map(|id| {
                        collection
                            .records
                            .iter()
                            .find(|r| r.id == id.as_str().unwrap())
                    })
                    .collect(),
                None => collection.records.iter().skip(offset).take(limit).collect(),
            };
            Json(json!({
                "ids": [found.iter().map(|r| r.id.clone()).collect::<Vec<_>>()],
                "documents": [found.iter().map(|r| r.document.clone()).collect::<Vec<_>>()],
                "embeddings": [found.iter().map(|r| r.embedding.clone()).collect::<Vec<_>>()],
                "metadatas": [found.iter().map(|r| r.metadata.clone()).collect::<Vec<_>>()],
                "scores": [null],
                "select": [[]],
            }))
            .into_response()
        }

        async fn get_records(
            State(server): State<Shared>,
            Path((_, _, id)): Path<(String, String, String)>,
            Json(body): Json<Value>,
        ) -> Response {
            let offset = body["offset"].as_u64().unwrap_or_default() as usize;
            if let Some(failure) = read(&server, offset).await {
                return answer(failure).await;
            }
            let mut server = server.lock().unwrap();
            let rebuilds = server.rebuilds_embeddings;
            let collection = by_id(&mut server, &id);
            let found: Vec<&Record> = match body["ids"].as_array() {
                Some(ids) => collection
                    .records
                    .iter()
                    .filter(|r| ids.iter().any(|id| id.as_str() == Some(r.id.as_str())))
                    .collect(),
                None => {
                    let limit = body["limit"]
                        .as_u64()
                        .map_or(usize::MAX, |limit| limit as usize);
                    collection.records.iter().skip(offset).take(limit).collect()
                }
            };
            let include: Vec<String> = serde_json::from_value(body["include"].clone()).unwrap();
            let included = |what: &str| include.iter().any(|field| field == what);
            let embedding = |record: &Record| match &record.embedding {
                Some(embedding) if rebuilds => Some(as_the_index_gives_it(embedding)),
                embedding => embedding.clone(),
            };
            // As a get of Chroma (`GetResponse` from a `GetResult`): the URI of a record comes apart from its metadata, which
            // leave the keys of Chroma.
            let uri = |record: &Record| match record.metadata.as_ref()?.get(CHROMA_URI_KEY)? {
                MetadataValue::Str(uri) => Some(uri.clone()),
                _ => None,
            };
            let metadata = |record: &Record| {
                record.metadata.clone().map(|metadata| {
                    metadata
                        .into_iter()
                        .filter(|(key, _)| !key.starts_with(CHROMA_KEY))
                        .collect::<Metadata>()
                })
            };
            Json(json!({
                "ids": found.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
                "documents": included("documents")
                    .then(|| found.iter().map(|r| r.document.clone()).collect::<Vec<_>>()),
                "embeddings": included("embeddings")
                    .then(|| found.iter().map(|r| embedding(r)).collect::<Vec<_>>()),
                "metadatas": included("metadatas")
                    .then(|| found.iter().map(|r| metadata(r)).collect::<Vec<_>>()),
                "uris": included("uris")
                    .then(|| found.iter().map(|r| uri(r)).collect::<Vec<_>>()),
                "include": include,
            }))
            .into_response()
        }

        /// An embedding as HNSW with the cosine space gives it back: `normalize_vector` and `getDataByLabel` in
        /// hnswlib/hnswalg.h, of the hnswlib of Chroma 1.5.9 and of VecturaDB. The last bit of a number can change.
        pub fn as_the_index_gives_it(embedding: &[f32]) -> Vec<f32> {
            let norm = embedding.iter().fold(0.0f32, |norm, x| norm + x * x);
            let length = norm.sqrt();
            let inverse = 1.0f32 / (length + 1e-30f32);
            embedding.iter().map(|x| x * inverse * length).collect()
        }

        /// Chroma's answer, and the server block when the server has one.
        async fn pre_flight_checks(State(server): State<Shared>) -> Json<Value> {
            let mut checks = json!({"max_batch_size": 1000, "supports_base64_encoding": true});
            if let Some(features) = &server.lock().unwrap().features {
                checks["server"] =
                    json!({"name": "VecturaDB", "version": "test", "features": features});
            }
            Json(checks)
        }

        fn sent_records(body: &Value) -> Vec<Record> {
            body["ids"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, id)| Record {
                    id: id.as_str().unwrap().to_string(),
                    document: body["documents"][i].as_str().map(str::to_string),
                    embedding: serde_json::from_value(body["embeddings"][i].clone()).unwrap(),
                    metadata: serde_json::from_value(body["metadatas"][i].clone()).unwrap(),
                })
                .collect()
        }

        async fn add(
            State(server): State<Shared>,
            Path((_, _, id)): Path<(String, String, String)>,
            Json(body): Json<Value>,
        ) -> Response {
            let failure = server.lock().unwrap().add_failures.pop_front();
            if let Some(failure) = failure {
                return answer(failure).await;
            }
            let records = sent_records(&body);
            let mut server = server.lock().unwrap();
            server
                .adds
                .push(records.iter().map(|record| record.id.clone()).collect());
            let collection = by_id(&mut server, &id);
            // As on a real server, an add of an id that is there changes nothing.
            for record in records {
                if !collection.records.iter().any(|r| r.id == record.id) {
                    collection.records.push(record);
                }
            }
            Json(json!({})).into_response()
        }

        async fn upsert(
            State(server): State<Shared>,
            Path((_, _, id)): Path<(String, String, String)>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            let mut server = server.lock().unwrap();
            let ids: Vec<String> = serde_json::from_value(body["ids"].clone()).unwrap();
            server.upserts.push(ids.clone());
            let collection = by_id(&mut server, &id);
            for (i, id) in ids.iter().enumerate() {
                let record = collection.records.iter_mut().find(|r| r.id == *id).unwrap();
                // As on a real server: what the upsert does not name stays, a key without a value goes.
                if let Some(document) = body["documents"][i].as_str() {
                    record.document = Some(document.to_string());
                }
                record.embedding = serde_json::from_value(body["embeddings"][i].clone()).unwrap();
                if let Some(keys) = body["metadatas"][i].as_object() {
                    let metadata = record.metadata.get_or_insert_with(Metadata::new);
                    for (key, value) in keys {
                        if value.is_null() {
                            metadata.remove(key);
                        } else {
                            metadata.insert(
                                key.clone(),
                                serde_json::from_value(value.clone()).unwrap(),
                            );
                        }
                    }
                }
            }
            Json(json!({}))
        }

        async fn delete(
            State(server): State<Shared>,
            Path((_, _, id)): Path<(String, String, String)>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            let mut server = server.lock().unwrap();
            let ids: Vec<String> = serde_json::from_value(body["ids"].clone()).unwrap();
            server.deletes.push(ids.clone());
            by_id(&mut server, &id)
                .records
                .retain(|record| !ids.contains(&record.id));
            Json(json!({"deleted": ids.len()}))
        }

        pub async fn start(server: Server) -> (ChromaHttpClient, Shared) {
            let shared = Arc::new(Mutex::new(server));
            const COLLECTIONS: &str = "/api/v2/tenants/{tenant}/databases/{database}/collections";
            let app = Router::new()
                .route("/api/v2/pre-flight-checks", get(pre_flight_checks))
                .route(COLLECTIONS, post(create).get(list))
                .route(
                    &format!("{COLLECTIONS}/{{collection}}"),
                    get(get_collection).put(modify).delete(delete_collection),
                )
                .route(&format!("{COLLECTIONS}/{{collection}}/count"), get(count))
                .route(
                    &format!("{COLLECTIONS}/{{collection}}/search"),
                    post(search),
                )
                .route(
                    &format!("{COLLECTIONS}/{{collection}}/get"),
                    post(get_records),
                )
                .route(&format!("{COLLECTIONS}/{{collection}}/add"), post(add))
                .route(
                    &format!("{COLLECTIONS}/{{collection}}/upsert"),
                    post(upsert),
                )
                .route(
                    &format!("{COLLECTIONS}/{{collection}}/delete"),
                    post(delete),
                )
                .with_state(shared.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let client = crate::utils::local_client(&format!("http://127.0.0.1:{port}")).unwrap();
            (client, shared)
        }
    }

    use server::{as_the_index_gives_it, start, Failure, Server, Shared, StoredCollection};

    /// A source with one collection of these records, and an empty target.
    async fn source_and_target(
        source_records: Vec<Record>,
    ) -> (ChromaHttpClient, Shared, ChromaHttpClient, Shared) {
        let (source, source_server) = start(Server {
            collections: vec![StoredCollection::new("books", source_records)],
            ..Default::default()
        })
        .await;
        let (target, target_server) = start(Server::default()).await;
        (source, source_server, target, target_server)
    }

    fn options(on_existing: Option<OnExisting>, yes: bool) -> CopyOptions {
        CopyOptions {
            step: 100,
            concurrent: 5,
            on_existing,
            yes,
            compare_schemas: true,
        }
    }

    async fn copy_books_with(
        source: &ChromaHttpClient,
        target: &ChromaHttpClient,
        options: CopyOptions,
        mut term: TestTerminal,
    ) -> (Result<(), CliError>, TestTerminal) {
        let result = copy_collections(
            source.clone(),
            target.clone(),
            vec!["books".to_string()],
            false,
            options,
            &mut term,
        )
        .await;
        (result, term)
    }

    /// The first copy: it asks nothing.
    async fn copy_books(
        source: &ChromaHttpClient,
        target: &ChromaHttpClient,
    ) -> (Result<(), CliError>, TestTerminal) {
        copy_books_with(
            source,
            target,
            options(None, false),
            TestTerminal::new().without_prompts(),
        )
        .await
    }

    /// The command again, with this flag and the answer yes.
    async fn again_with(
        source: &ChromaHttpClient,
        target: &ChromaHttpClient,
        flag: OnExisting,
    ) -> (Result<(), CliError>, TestTerminal) {
        copy_books_with(
            source,
            target,
            options(Some(flag), false),
            TestTerminal::new().with_inputs(vec!["y"]),
        )
        .await
    }

    fn records_on(server: &Shared, name: &str) -> Vec<Record> {
        let server = server.lock().unwrap();
        server
            .collections
            .iter()
            .find(|collection| collection.name == name)
            .unwrap()
            .records
            .clone()
    }

    fn target_with(server: &Shared, name: &str, records: Vec<Record>) {
        server
            .lock()
            .unwrap()
            .collections
            .push(StoredCollection::new(name, records));
    }

    fn said(term: &TestTerminal, text: &str) -> bool {
        term.output.iter().any(|line| line.contains(text))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_copy_writes_the_pages_in_their_order() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..1000)).await;
        // The first pages come last.
        source_server.lock().unwrap().first_pages_are_slow = true;
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        let written: Vec<String> = target_server
            .lock()
            .unwrap()
            .adds
            .iter()
            .flatten()
            .cloned()
            .collect();
        let expected: Vec<String> = records(0..1000).into_iter().map(|r| r.id).collect();
        assert_eq!(written, expected);
        assert_eq!(records_on(&target_server, "books"), records(0..1000));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_copy_goes_on_however_many_times_the_target_refuses_it() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..250)).await;
        // More than the 3 retries of the client: with those, the copy stopped here.
        target_server.lock().unwrap().add_failures = (0..5).map(|_| Failure::Refused(1)).collect();
        let started = Instant::now();
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        assert!(
            started.elapsed() >= Duration::from_secs(5),
            "it did not wait as the server said: {:?}",
            started.elapsed()
        );
        assert_eq!(records_on(&target_server, "books"), records(0..250));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_copy_waits_as_long_as_the_refusal_says() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..50)).await;
        // Without the time of the refusal, the copy would wait 0.5 s and then 1 s.
        target_server.lock().unwrap().add_failures = (0..2).map(|_| Failure::Refused(3)).collect();
        let started = Instant::now();
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        assert!(
            started.elapsed() >= Duration::from_secs(6),
            "it did not wait as the server said: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_copy_goes_on_through_the_errors_of_the_network_and_of_the_servers() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..250)).await;
        let failures = [
            Failure::Status(503),
            Failure::Cut,
            Failure::Silent,
            Failure::RefusedWithoutTime,
            Failure::Status(500),
        ];
        target_server.lock().unwrap().add_failures = failures.into_iter().collect();
        source_server.lock().unwrap().read_failures = failures.into_iter().collect();
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..250));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_lost_answer_to_a_creation_does_not_stop_the_copy() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        target_server
            .lock()
            .unwrap()
            .create_failures
            .push_back(Failure::Cut);
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..150));
    }

    #[test]
    fn the_flags_of_a_collection_on_the_target() {
        let parse = |flags: &[&str]| {
            CopyArgs::try_parse_from(
                ["copy", "--all"]
                    .iter()
                    .chain(flags.iter())
                    .collect::<Vec<_>>()
                    .into_iter()
                    .copied(),
            )
        };
        assert!(parse(&["--resume"]).is_ok());
        assert!(parse(&["--force"]).is_ok());
        assert!(parse(&["--resume", "--yes"]).is_ok());
        assert!(parse(&["--force", "--yes"]).is_ok());
        // --yes says yes to something.
        assert!(parse(&["--yes"]).is_err());
        assert!(parse(&["--resume", "--force"]).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_collection_that_waiting_cannot_solve_does_not_stop_the_others() {
        let (source, _source_server) = start(Server {
            collections: vec![
                StoredCollection::new("first", records(0..10)),
                StoredCollection::new("second", records(0..10)),
            ],
            ..Default::default()
        })
        .await;
        let (target, target_server) = start(Server::default()).await;
        target_server
            .lock()
            .unwrap()
            .add_failures
            .push_back(Failure::Status(403));
        let mut term = TestTerminal::new().without_prompts();
        let error = copy_collections(
            source.clone(),
            target.clone(),
            vec![],
            true,
            options(None, false),
            &mut term,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("Collection first"), "{error}");
        assert!(!error.contains("Collection second"), "{error}");
        assert_eq!(records_on(&target_server, "second"), records(0..10));
        // The same command goes on from there.
        let mut term = TestTerminal::new().with_inputs(vec!["y"]);
        copy_collections(
            source,
            target,
            vec![],
            true,
            options(Some(OnExisting::Resume), false),
            &mut term,
        )
        .await
        .unwrap();
        assert_eq!(records_on(&target_server, "first"), records(0..10));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_checks_come_before_the_writes() {
        let (source, _source_server) = start(Server {
            collections: vec![
                StoredCollection::new("new", records(0..10)),
                StoredCollection::new("there", records(0..10)),
            ],
            ..Default::default()
        })
        .await;
        let (target, target_server) = start(Server::default()).await;
        target_with(&target_server, "there", records(0..5));
        let mut term = TestTerminal::new().with_inputs(vec!["y"]);
        copy_collections(
            source,
            target,
            vec![],
            true,
            options(Some(OnExisting::Resume), false),
            &mut term,
        )
        .await
        .unwrap();
        let position = |text: &str| term.output.iter().position(|line| line.contains(text));
        let compared = position("Comparing collection there").unwrap();
        let reported = position("Collection there is already on the target").unwrap();
        let written = position("Copying collection: new").unwrap();
        assert!(
            compared < reported && reported < written,
            "{:?}",
            term.output
        );
        assert_eq!(records_on(&target_server, "new"), records(0..10));
        assert_eq!(records_on(&target_server, "there"), records(0..10));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_report_says_how_many_records_are_the_same_missing_different_and_only_on_the_target(
    ) {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        let mut there = records(0..50);
        there[7].document = Some("another document".to_string());
        there.extend(records(1000..1020));
        target_with(&target_server, "books", there.clone());
        let term = TestTerminal::new().with_inputs(vec!["n"]);
        let (result, term) = copy_books_with(
            &source,
            &target,
            options(Some(OnExisting::Force), false),
            term,
        )
        .await;
        result.unwrap();
        assert!(
            said(
                &term,
                "49 records the same, 100 missing, 1 different, 20 only on the target"
            ),
            "{:?}",
            term.output
        );
        assert!(said(&term, "the document of record r7 is different"));
        // The answer is no: nothing is written.
        assert_eq!(records_on(&target_server, "books"), there);
        let server = target_server.lock().unwrap();
        assert!(server.adds.is_empty());
        assert!(server.upserts.is_empty());
        assert!(server.deletes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn without_a_terminal_and_without_yes_only_the_new_collections_are_written() {
        let (source, _source_server) = start(Server {
            collections: vec![
                StoredCollection::new("new", records(0..10)),
                StoredCollection::new("there", records(0..10)),
            ],
            ..Default::default()
        })
        .await;
        let (target, target_server) = start(Server::default()).await;
        target_with(&target_server, "there", records(0..5));
        for flag in [None, Some(OnExisting::Resume), Some(OnExisting::Force)] {
            let mut term = TestTerminal::new().without_prompts();
            let error = copy_collections(
                source.clone(),
                target.clone(),
                vec![],
                true,
                options(flag, false),
                &mut term,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                error.contains("Collection there is already on the target"),
                "{error}"
            );
            // The words to add.
            assert!(error.contains("--resume --yes"), "{error}");
            assert!(error.contains("--force --yes"), "{error}");
            assert!(
                said(&term, "5 records the same, 5 missing"),
                "{:?}",
                term.output
            );
            assert_eq!(records_on(&target_server, "there"), records(0..5));
            assert_eq!(records_on(&target_server, "new"), records(0..10));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn yes_answers_where_nobody_can() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..20)).await;
        target_with(&target_server, "books", records(0..10));
        let (result, term) = copy_books_with(
            &source,
            &target,
            options(Some(OnExisting::Resume), true),
            TestTerminal::new().without_prompts(),
        )
        .await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..20));
        // What it does is said also when nobody is asked.
        assert!(
            said(&term, "Add to collection books the 10 records"),
            "{:?}",
            term.output
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_a_terminal_without_a_flag_it_asks_what_to_do() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..20)).await;
        target_with(&target_server, "books", records(0..10));
        // Leave it as it is.
        let term = TestTerminal::new().with_inputs(vec!["2"]);
        let (result, _) = copy_books_with(&source, &target, options(None, false), term).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..10));
        // Add the records it does not have.
        let term = TestTerminal::new().with_inputs(vec!["0"]);
        let (result, _) = copy_books_with(&source, &target, options(None, false), term).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..20));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resume_writes_only_what_is_missing() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..550)).await;
        target_with(&target_server, "books", records(0..300));
        let (result, term) = again_with(&source, &target, OnExisting::Resume).await;
        result.unwrap();
        let server = target_server.lock().unwrap();
        let written: Vec<&String> = server.adds.iter().flatten().collect();
        assert_eq!(written.len(), 250, "{written:?}");
        assert_eq!(written[0], "r300");
        assert!(server.upserts.is_empty());
        assert!(server.deletes.is_empty());
        drop(server);
        assert_eq!(records_on(&target_server, "books"), records(0..550));
        assert!(said(&term, "250 records added, 0 written again, 0 removed"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resume_fills_the_pages_missing_in_the_middle() {
        // What the copy of 1.0.0-preview.5 leaves, writing its pages out of order.
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..500)).await;
        let mut there = records(0..100);
        there.extend(records(200..300));
        there.extend(records(400..500));
        target_with(&target_server, "books", there);
        let (result, _) = again_with(&source, &target, OnExisting::Resume).await;
        result.unwrap();
        let on_target = records_on(&target_server, "books");
        assert_eq!(on_target.len(), 500);
        for record in records(0..500) {
            assert!(on_target.contains(&record), "{}", record.id);
        }
        assert_eq!(target_server.lock().unwrap().adds.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resume_on_a_collection_left_empty_copies_everything() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        target_with(&target_server, "books", vec![]);
        let (result, _) = again_with(&source, &target, OnExisting::Resume).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..150));
        assert!(target_server.lock().unwrap().creations.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resume_leaves_as_they_are_the_records_that_are_different_or_only_on_the_target() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        // One in a page with nothing to write, one in a page with records to add.
        let mut there = records(0..120);
        there[7].document = Some("another document".to_string());
        there[110].document = Some("another document".to_string());
        there.extend(records(1000..1020));
        target_with(&target_server, "books", there.clone());
        let (result, _) = again_with(&source, &target, OnExisting::Resume).await;
        result.unwrap();
        let mut expected = there;
        expected.extend(records(120..150));
        assert_eq!(records_on(&target_server, "books"), expected);
        let server = target_server.lock().unwrap();
        assert!(server.upserts.is_empty());
        assert!(server.deletes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_collection_that_is_the_same_is_not_written_and_asks_nothing() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..250)).await;
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        target_server.lock().unwrap().adds.clear();
        // No answer is ready: a question would be an error.
        let (result, term) = copy_books_with(
            &source,
            &target,
            options(Some(OnExisting::Force), false),
            TestTerminal::new(),
        )
        .await;
        result.unwrap();
        let server = target_server.lock().unwrap();
        assert!(server.adds.is_empty());
        assert!(server.upserts.is_empty());
        assert!(said(&term, "is already copied"), "{:?}", term.output);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_makes_an_exact_copy() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..350)).await;
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        target_server.lock().unwrap().adds.clear();
        // Changed on the source: 3 records different, 2 removed, 1 added.
        let mut expected = records(0..350);
        expected[10].document = Some("another document".to_string());
        expected[110].embedding = Some(vec![9.0, 9.0]);
        expected[210].metadata = None;
        expected.retain(|record| record.id != "r50" && record.id != "r250");
        expected.push(record(1000));
        source_server.lock().unwrap().collections[0].records = expected.clone();
        let (result, term) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        let mut on_target = records_on(&target_server, "books");
        on_target.sort_by_key(|record| record.id.clone());
        let mut sorted = expected.clone();
        sorted.sort_by_key(|record| record.id.clone());
        assert_eq!(on_target.len(), sorted.len());
        for (found, record) in on_target.iter().zip(&sorted) {
            assert_eq!(found.id, record.id);
            assert_eq!(record.difference(found), None, "{}", record.id);
        }
        {
            let server = target_server.lock().unwrap();
            assert_eq!(server.adds.iter().flatten().count(), 1);
            assert_eq!(server.upserts.iter().flatten().count(), 3);
            assert_eq!(
                server.deletes.iter().flatten().collect::<Vec<_>>(),
                ["r50", "r250"]
            );
        }
        assert!(
            said(&term, "1 records added, 3 written again, 2 removed"),
            "{:?}",
            term.output
        );
        // And then there is nothing more to do.
        let (result, term) = copy_books_with(
            &source,
            &target,
            options(Some(OnExisting::Force), false),
            TestTerminal::new(),
        )
        .await;
        result.unwrap();
        assert!(said(&term, "is already copied"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_writes_the_records_changed_on_the_source_in_their_place() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..250)).await;
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        let mut expected = records(0..250);
        expected[10].document = Some("another document".to_string());
        source_server.lock().unwrap().collections[0].records = expected.clone();
        let (result, _) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_takes_away_a_document_the_source_no_longer_has() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..10)).await;
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        source_server.lock().unwrap().collections[0].records[3].document = None;
        let (result, _) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        let on_target = records_on(&target_server, "books");
        assert_eq!(
            on_target.iter().find(|r| r.id == "r3").unwrap().document,
            None
        );
        assert_eq!(on_target.len(), 10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_says_how_many_records_go_before_asking() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..10)).await;
        let mut there = records(0..10);
        there.extend(records(1000..1020));
        target_with(&target_server, "books", there);
        let (result, term) = copy_books_with(
            &source,
            &target,
            options(Some(OnExisting::Force), true),
            TestTerminal::new().without_prompts(),
        )
        .await;
        result.unwrap();
        assert!(said(&term, "20 only on the target"), "{:?}", term.output);
        assert!(said(&term, "20 REMOVED"), "{:?}", term.output);
        assert_eq!(records_on(&target_server, "books"), records(0..10));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_creates_again_a_collection_with_another_schema() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        source_server.lock().unwrap().collections[0].schema = Some(chroma_types::Schema::default());
        target_with(&target_server, "books", records(1000..1020));
        let (result, term) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..150));
        let server = target_server.lock().unwrap();
        assert_eq!(server.collections.len(), 1);
        assert_eq!(server.creations.len(), 1);
        assert_eq!(
            server.collections[0].schema,
            Some(chroma_types::Schema::default())
        );
        drop(server);
        assert!(
            said(&term, "deleted, with its 20 records"),
            "{:?}",
            term.output
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_records_only_on_the_target_are_looked_for_many_pages_at_a_time() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..950)).await;
        let mut there = records(0..950);
        there.extend(records(1000..1003));
        target_with(&target_server, "books", there);
        source_server.lock().unwrap().read_delay = Duration::from_millis(50);
        target_server.lock().unwrap().read_delay = Duration::from_millis(50);
        let source = source.get_collection("books").await.unwrap();
        let target = target.get_collection("books").await.unwrap();
        let progress = Progress::new("Looking".to_string(), 953, 0);
        let ids = ids_only_on_target(&source, &target, 3, 953, options(None, false), &progress)
            .await
            .unwrap();
        assert_eq!(ids, ["r1000", "r1001", "r1002"]);
        assert_eq!(progress.bar.position(), 953);
        for server in [&source_server, &target_server] {
            let most = server.lock().unwrap().most_reads_at_once;
            assert!((2..=5).contains(&most), "{most} reads at once");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_records_only_on_the_target_stop_being_looked_for_when_all_are_found() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..2000)).await;
        let mut there = records(5000..5001);
        there.extend(records(0..2000));
        target_with(&target_server, "books", there);
        let source = source.get_collection("books").await.unwrap();
        let target = target.get_collection("books").await.unwrap();
        let progress = Progress::new("Looking".to_string(), 2001, 0);
        let ids = ids_only_on_target(&source, &target, 1, 2001, options(None, false), &progress)
            .await
            .unwrap();
        assert_eq!(ids, ["r5000"]);
        let reads = target_server.lock().unwrap().reads;
        assert!(reads <= 5, "{reads} pages of 21 read");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_removal_goes_a_page_at_a_time_and_says_how_far_it_is() {
        let (_source, _source_server, target, target_server) = source_and_target(vec![]).await;
        target_with(&target_server, "books", records(0..250));
        let target = target.get_collection("books").await.unwrap();
        let ids: Vec<String> = records(0..250).into_iter().map(|r| r.id).collect();
        let progress = Progress::new("Removing".to_string(), 250, 0);
        let removed = remove_records(&target, &ids, 100, &progress).await.unwrap();
        assert_eq!(removed, 250);
        assert_eq!(progress.bar.position(), 250);
        let server = target_server.lock().unwrap();
        assert_eq!(server.deletes.len(), 3);
        assert!(server.collections[0].records.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_comparison_with_nothing_in_common_says_how_much_of_the_source_it_read() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..250)).await;
        target_with(&target_server, "books", records(1000..1010));
        let source = source.get_collection("books").await.unwrap();
        let target = target.get_collection("books").await.unwrap();
        let progress = Progress::new("Comparing".to_string(), 10, 0);
        let comparison =
            compare_with_source(&source, &target, 250, 10, options(None, false), &progress)
                .await
                .unwrap();
        assert_eq!(comparison.found, 0);
        assert_eq!(progress.bar.message(), "250 records of the source read");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_schema_that_grew_with_the_records_does_not_make_force_create_the_collection_again() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        source_server.lock().unwrap().collections[0].schema = Some(Schema::default());
        target_with(&target_server, "books", records(0..150));
        let mut grown = Schema::default();
        assert!(grown.ensure_key_from_metadata("n", chroma_types::MetadataValueType::Int));
        target_server.lock().unwrap().collections[0].schema = Some(grown);
        let (result, term) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert!(said(&term, "is already copied"), "{:?}", term.output);
        assert!(target_server.lock().unwrap().creations.is_empty());
    }

    #[test]
    fn the_keys_that_enter_a_schema_by_themselves_do_not_make_it_different() {
        let on_source = Schema::default();
        let mut on_target = Schema::default();
        assert!(on_target.ensure_key_from_metadata("topic", chroma_types::MetadataValueType::Str));
        assert!(on_target.ensure_key_from_metadata("n", chroma_types::MetadataValueType::Int));
        assert_ne!(on_source, on_target);
        assert_eq!(
            chosen_settings(&Some(on_source.clone()), true),
            chosen_settings(&Some(on_target.clone()), true)
        );
        // A key with settings of its own is a choice.
        let mut chosen = on_target;
        let topic = chosen.keys.get_mut("topic").unwrap();
        topic.string.as_mut().unwrap().string_inverted_index = None;
        assert_ne!(
            chosen_settings(&Some(on_source), true),
            chosen_settings(&Some(chosen), true)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_creates_again_a_collection_with_other_dimensions() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..10)).await;
        source_server.lock().unwrap().collections[0].dimension = Some(2);
        target_with(&target_server, "books", records(1000..1005));
        target_server.lock().unwrap().collections[0].dimension = Some(8);
        let (result, term) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..10));
        assert!(
            said(&term, "deleted, with its 5 records"),
            "{:?}",
            term.output
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_copy_to_chroma_cloud_does_not_compare_schemas() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        source_server.lock().unwrap().collections[0].schema = Some(chroma_types::Schema::default());
        target_with(&target_server, "books", records(0..150));
        let (result, term) = copy_books_with(
            &source,
            &target,
            CopyOptions {
                compare_schemas: false,
                ..options(Some(OnExisting::Force), false)
            },
            TestTerminal::new(),
        )
        .await;
        result.unwrap();
        assert!(said(&term, "is already copied"), "{:?}", term.output);
        assert!(target_server.lock().unwrap().creations.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resume_leaves_another_schema_and_says_so() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        source_server.lock().unwrap().collections[0].schema = Some(chroma_types::Schema::default());
        target_with(&target_server, "books", records(0..100));
        let (result, term) = again_with(&source, &target, OnExisting::Resume).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..150));
        assert_eq!(target_server.lock().unwrap().collections[0].schema, None);
        assert!(
            said(&term, "Its schema or its dimensions are different"),
            "{:?}",
            term.output
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_aligns_the_metadata_of_a_collection_and_resume_does_not() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..10)).await;
        let metadata = with_metadata(&[("owner", MetadataValue::Str("me".to_string()))]);
        source_server.lock().unwrap().collections[0].metadata = metadata.clone();
        target_with(&target_server, "books", records(0..10));
        let (result, term) = again_with(&source, &target, OnExisting::Resume).await;
        result.unwrap();
        assert_eq!(target_server.lock().unwrap().collections[0].metadata, None);
        assert!(said(&term, "The metadata of the collection are different"));
        let (result, _) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert_eq!(
            target_server.lock().unwrap().collections[0].metadata,
            metadata
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_old_settings_arrive_at_the_end_of_the_copy() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..10)).await;
        let metadata = with_metadata(&[("hnsw:space", MetadataValue::Str("l2".to_string()))]);
        {
            let mut server = source_server.lock().unwrap();
            server.collections[0].metadata = metadata.clone();
            server.collections[0].schema = Some(chroma_types::Schema::default());
        }
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        let server = target_server.lock().unwrap();
        assert_eq!(server.creations, vec![None]);
        assert_eq!(server.collections[0].metadata, metadata);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_source_that_changes_during_the_copy_is_said_and_resume_completes_it() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        // More than the last page takes: that page ends at 200.
        source_server.lock().unwrap().after_first_count = records(150..250);
        let (result, _) = copy_books(&source, &target).await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("the source changed during the copy"),
            "{error}"
        );
        assert!(error.contains("run the same command again"), "{error}");
        let (result, _) = again_with(&source, &target, OnExisting::Resume).await;
        result.unwrap();
        assert_eq!(records_on(&target_server, "books"), records(0..250));
    }

    /// A copy that waits forever would hang the test instead of failing it.
    async fn within_30_s<T>(copy: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(30), copy)
            .await
            .expect("the copy did not end in 30 s")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_target_without_the_search_api_is_compared_all_the_same() {
        let (source, _source_server, target, target_server) =
            source_and_target(records(0..250)).await;
        target_server.lock().unwrap().without_search = true;
        let (result, _) = within_30_s(copy_books(&source, &target)).await;
        result.unwrap();
        let (result, term) = within_30_s(copy_books(&source, &target)).await;
        result.unwrap();
        assert!(said(&term, "is already copied"), "{:?}", term.output);
        target_server.lock().unwrap().collections[0]
            .records
            .extend(records(1000..1003));
        let (result, term) = within_30_s(again_with(&source, &target, OnExisting::Force)).await;
        result.unwrap();
        assert!(
            said(&term, "0 records added, 0 written again, 3 removed"),
            "{:?}",
            term.output
        );
        assert_eq!(records_on(&target_server, "books"), records(0..250));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_operation_that_a_server_does_not_have_is_not_waited_for() {
        // Chroma 1.5.9 as the source: it answers a search with 501.
        let (source, source_server, target, _target_server) =
            source_and_target(records(0..250)).await;
        source_server.lock().unwrap().without_search = true;
        let (result, _) = within_30_s(copy_books(&source, &target)).await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Search operation is not implemented"),
            "{error}"
        );
        let reads = source_server.lock().unwrap().reads;
        assert!(reads <= 3, "{reads} reads of 3 pages");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn where_the_target_does_not_hold_spann_its_hnsw_is_the_same_collection() {
        // Chroma 1.5.9 says nothing of itself, and VecturaDB 1.0.0-preview.5 does not say spann.
        for features in [None, Some(vec!["search".to_string()])] {
            let (source, source_server, target, target_server) =
                source_and_target(records(0..150)).await;
            source_server.lock().unwrap().collections[0].schema =
                Some(Schema::new_default(chroma_types::KnnIndex::Spann));
            {
                let mut server = target_server.lock().unwrap();
                server.spann_becomes_hnsw = true;
                server.features = features;
            }
            let (result, _) = copy_books(&source, &target).await;
            result.unwrap();
            let (result, term) = again_with(&source, &target, OnExisting::Force).await;
            result.unwrap();
            assert!(said(&term, "is already copied"), "{:?}", term.output);
            assert_eq!(target_server.lock().unwrap().creations.len(), 1);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn where_the_target_holds_spann_force_makes_an_hnsw_collection_again() {
        let (source, source_server, target, target_server) =
            source_and_target(records(0..150)).await;
        source_server.lock().unwrap().collections[0].schema =
            Some(Schema::new_default(chroma_types::KnnIndex::Spann));
        target_with(&target_server, "books", records(0..150));
        {
            let mut server = target_server.lock().unwrap();
            server.collections[0].schema = Some(Schema::new_default(chroma_types::KnnIndex::Hnsw));
            server.features = Some(vec!["search".to_string(), "spann".to_string()]);
        }
        let (result, term) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert!(
            said(&term, "Its schema or its dimensions are different"),
            "{:?}",
            term.output
        );
        assert!(
            said(&term, "deleted, with its 150 records"),
            "{:?}",
            term.output
        );
        assert_eq!(
            target_server.lock().unwrap().collections[0].schema,
            Some(Schema::new_default(chroma_types::KnnIndex::Spann))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_record_with_a_uri_is_the_same_on_a_target_that_gives_the_uri_apart() {
        // A search gives the URI of a record in its metadata, a get apart.
        let mut books = records(0..250);
        for book in books.iter_mut().step_by(5) {
            let uri = MetadataValue::Str(format!("s3://bucket/{}", book.id));
            book.metadata
                .get_or_insert_with(Metadata::new)
                .insert(CHROMA_URI_KEY.to_string(), uri);
        }
        let (source, _source_server, target, target_server) =
            source_and_target(books.clone()).await;
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        let (result, term) = copy_books(&source, &target).await;
        result.unwrap();
        assert!(said(&term, "is already copied"), "{:?}", term.output);
        let (result, term) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert!(said(&term, "is already copied"), "{:?}", term.output);
        assert!(target_server.lock().unwrap().upserts.is_empty());
        assert_eq!(records_on(&target_server, "books"), books);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_embedding_that_the_index_of_the_target_gives_back_rounded_is_the_same() {
        // As Chroma 1.5.9 gave back [1.0, 0.2, 0.5], copied from Chroma Cloud to a collection with the cosine space.
        assert_eq!(
            as_the_index_gives_it(&[1.0, 0.2, 0.5]),
            [1.0, f32::from_bits(0x3E4C_CCCC), 0.5]
        );
        let books: Vec<Record> = (0..250)
            .map(|i| Record {
                embedding: Some(vec![1.0, (i % 5) as f32 * 0.2, 0.5]),
                ..record(i)
            })
            .collect();
        let (source, _source_server, target, target_server) =
            source_and_target(books.clone()).await;
        target_server.lock().unwrap().rebuilds_embeddings = true;
        let (result, _) = copy_books(&source, &target).await;
        result.unwrap();
        let (result, term) = copy_books(&source, &target).await;
        result.unwrap();
        assert!(said(&term, "is already copied"), "{:?}", term.output);
        let (result, term) = again_with(&source, &target, OnExisting::Force).await;
        result.unwrap();
        assert!(said(&term, "is already copied"), "{:?}", term.output);
        assert!(target_server.lock().unwrap().upserts.is_empty());
    }
}
