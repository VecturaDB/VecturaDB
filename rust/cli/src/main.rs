// Changed by VecturaDB contributors: jemalloc as the global allocator, with its background thread on Linux.
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
#![windows_subsystem = "console"]

use chroma_cli::chroma_cli;
use std::env;

// As the workers of the distributed: with glibc the server kept growing under load (2053 MiB after 30 min of four
// fuzzers, against 839 MiB with jemalloc).
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// jemalloc gives the pages it has freed back to the system after dirty_decay_ms, but without its background thread only
// when the arena that holds them allocates or frees again: on a server that goes idle after a load they stay with the
// process. _RJEM_MALLOC_CONF still overrides it.
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:true\0";

fn main() {
    let args: Vec<String> = env::args().collect();
    chroma_cli(args)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[test]
    fn jemalloc_starts_with_its_background_thread() {
        let on: bool = unsafe { tikv_jemalloc_ctl::raw::read(b"opt.background_thread\0") }.unwrap();
        assert!(on);
    }
}
