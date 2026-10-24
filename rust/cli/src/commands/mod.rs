// Changed by VecturaDB contributors for the VecturaDB branding.
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
pub mod browse;
pub mod copy;
// Not commands of VecturaDB: browse, copy and login use only part of them.
#[allow(dead_code)]
pub mod db;
#[allow(dead_code)]
pub mod install;
pub mod login;
pub mod profile;
pub mod run;
pub mod update;
pub mod vacuum;
pub mod webpage;
