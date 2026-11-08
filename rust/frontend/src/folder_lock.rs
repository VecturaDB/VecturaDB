// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
//! The lock of a data folder of the single node: one program at a time uses a data folder, a server, an embedded client
//! (the Python bindings) or vacuum. Two programs on one folder keep two indexes in memory that write the same files and
//! the same positions in SQLite, which loses records from search or leaves a collection that no longer opens.
//!
//! The lock is a file of the folder, locked for as long as the program holds the folder, with the locks of the standard
//! library (File::try_lock: flock on Linux, LockFileEx on Windows): the system releases it when the program ends,
//! however it ends. A program that finds the folder held waits up to FOLDER_WAIT, so that an update in which the old
//! container stops by itself goes on, then refuses. A lock of the file system: on a network disk it holds only as far as
//! the mount sends locks to the server (SMB mounted with nobrl does not).

use chroma_error::{ChromaError, ErrorCodes};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::time::{Duration, Instant};
use thiserror::Error;

/// The file of the lock, in the data folder. Chroma 1.5.9 does not read it, and a folder keeps working with 1.5.9.
pub const LOCK_FILE: &str = "vecturadb.lock";

/// How long a server or an embedded client waits for a data folder that another program holds.
pub const FOLDER_WAIT: Duration = Duration::from_secs(30);

// How often a program that waits tries the lock again.
const RETRY: Duration = Duration::from_millis(200);

#[derive(Debug, Error)]
pub enum FolderLockError {
    #[error(
        "The data folder {folder} is in use by another VecturaDB program, after {waited} s of wait: one program at a \
         time uses a data folder, a server or an embedded client. To use the data from several processes, run a server \
         on the folder and connect to it."
    )]
    InUse { folder: String, waited: u64 },
    #[error("Cannot lock the data folder {folder}: {source}")]
    Io {
        folder: String,
        #[source]
        source: std::io::Error,
    },
}

impl ChromaError for FolderLockError {
    fn code(&self) -> ErrorCodes {
        match self {
            FolderLockError::InUse { .. } => ErrorCodes::FailedPrecondition,
            FolderLockError::Io { .. } => ErrorCodes::Internal,
        }
    }
}

/// The lock of a data folder, held for as long as this value lives.
#[derive(Debug)]
pub struct FolderLock {
    _file: File,
}

/// The lock of the data folder given, within the wait given: see the module.
pub fn lock_folder(folder: &Path, wait: Duration) -> Result<FolderLock, FolderLockError> {
    let io = |source| FolderLockError::Io {
        folder: folder.display().to_string(),
        source,
    };
    std::fs::create_dir_all(folder).map_err(io)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(folder.join(LOCK_FILE))
        .map_err(io)?;
    let started = Instant::now();
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(FolderLock { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => {}
            Err(std::fs::TryLockError::Error(source)) => return Err(io(source)),
        }
        if started.elapsed() >= wait {
            return Err(FolderLockError::InUse {
                folder: folder.display().to_string(),
                waited: started.elapsed().as_secs(),
            });
        }
        std::thread::sleep(RETRY);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHORT: Duration = Duration::from_millis(300);

    // A second program on a folder that another holds waits, then refuses, with the folder in its message: a server
    // and a client are the same lock, so this is each of two servers, a server and a client, two clients.
    #[test]
    fn a_second_program_waits_then_refuses() {
        let root = tempfile::tempdir().unwrap();
        let _first = lock_folder(root.path(), SHORT).unwrap();
        let started = Instant::now();
        let second = lock_folder(root.path(), SHORT).unwrap_err();
        assert!(started.elapsed() >= SHORT, "{:?}", started.elapsed());
        assert!(matches!(second, FolderLockError::InUse { .. }), "{second}");
        assert!(
            second
                .to_string()
                .contains(&root.path().display().to_string()),
            "{second}"
        );
    }

    // A program that waits gets the folder once the program that held it lets it go, within its wait.
    #[test]
    fn a_program_that_waits_gets_the_folder_once_it_is_free() {
        let root = tempfile::tempdir().unwrap();
        let first = lock_folder(root.path(), SHORT).unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            drop(first);
        });
        let started = Instant::now();
        let _second = lock_folder(root.path(), Duration::from_secs(10)).unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(400),
            "{:?}",
            started.elapsed()
        );
        release.join().unwrap();
    }

    // Once a program lets the folder go, the next one gets it at once.
    #[test]
    fn a_folder_let_go_is_free_at_once() {
        let root = tempfile::tempdir().unwrap();
        drop(lock_folder(root.path(), SHORT).unwrap());
        let started = Instant::now();
        let _again = lock_folder(root.path(), SHORT).unwrap();
        assert!(started.elapsed() < SHORT, "{:?}", started.elapsed());
    }

    // The folder is made when it is not there yet, as the server makes it for its database.
    #[test]
    fn the_folder_is_made_if_missing() {
        let root = tempfile::tempdir().unwrap();
        let folder = root.path().join("data");
        let _lock = lock_folder(&folder, SHORT).unwrap();
        assert!(folder.join(LOCK_FILE).is_file());
    }
}
