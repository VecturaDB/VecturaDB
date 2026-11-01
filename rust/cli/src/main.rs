// Changed by VecturaDB contributors: jemalloc as the global allocator.
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

fn main() {
    let args: Vec<String> = env::args().collect();
    chroma_cli(args)
}
