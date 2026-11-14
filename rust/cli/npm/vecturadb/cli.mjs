#!/usr/bin/env node
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
//
// The command vecturadb of the npm package, as the command chroma of the npm package chromadb of Chroma
// (clients/new-js/packages/chromadb/src/cli.ts and bindings.ts): update says whether npm has a newer vecturadb, and
// every other command goes as it is to the CLI of this platform, from its package in optionalDependencies. The CLI is
// the binary of the GitHub Release, started as a child process; chromadb loads a binding of its CLI into Node instead.
import { spawn } from "child_process";
import { readFileSync } from "fs";
import { createRequire } from "module";
import os from "os";
import path from "path";
import semver from "semver";

const require = createRequire(import.meta.url);
const platform = os.platform();
const arch = os.arch();

// The packages of the platforms, as in optionalDependencies of package.json.
const packages = {
  "darwin-arm64": "vecturadb-cli-darwin-arm64",
  "darwin-x64": "vecturadb-cli-darwin-x64",
  "linux-arm64": "vecturadb-cli-linux-arm64-gnu",
  "linux-x64": "vecturadb-cli-linux-x64-gnu",
  "win32-x64": "vecturadb-cli-win32-x64-msvc",
};

const installedVersion = JSON.parse(readFileSync(new URL("./package.json", import.meta.url), "utf8")).version;

const getLatestVersion = async (packageName) => {
  const response = await fetch(`https://registry.npmjs.org/${packageName}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch package data: ${response.statusText}`);
  }
  const data = await response.json();
  return data["dist-tags"].latest;
};

const update = async () => {
  try {
    const latestVersion = await getLatestVersion("vecturadb");

    if (semver.lt(installedVersion, latestVersion)) {
      console.log(`\nA new vecturadb version (${latestVersion}) is available!`);
      console.log("\n\x1b[4mUpdate with npm\x1b[0m");
      console.log("npm install vecturadb@latest");

      console.log("\n\x1b[4mUpdate with pnpm\x1b[0m");
      console.log("pnpm add vecturadb@latest");

      console.log("\n\x1b[4mUpdate with yarn\x1b[0m");
      console.log("yarn add vecturadb@latest");

      console.log("\n\x1b[4mUpdate with bun\x1b[0m");
      console.log("bun add vecturadb@latest\n");
    } else {
      console.log(`\nYour vecturadb version (${installedVersion}) is up-to-date!\n`);
    }
  } catch (error) {
    console.error("Error checking versions:", error);
  }
};

const cliPath = () => {
  const packageName = packages[`${platform}-${arch}`];
  if (!packageName) {
    throw new Error(
      `VecturaDB has no CLI on npm for ${platform} ${arch}. It has one for ${Object.keys(packages).join(", ")}.`,
    );
  }
  let packageJson;
  try {
    packageJson = require.resolve(`${packageName}/package.json`);
  } catch {
    throw new Error(
      `${packageName}, the package with the CLI of this machine, is not installed. npm installs it with vecturadb, ` +
        "unless the optional dependencies are left out (--omit=optional, --no-optional)" +
        (platform === "linux" ? ", or the Linux has no glibc 2.17 or later, as Alpine (musl)" : "") +
        ".",
    );
  }
  return path.join(path.dirname(packageJson), platform === "win32" ? "vecturadb.exe" : "vecturadb");
};

const main = async () => {
  const args = process.argv.slice(2);
  if (args.length > 0 && args[0] === "update") {
    await update();
    return;
  }

  let cli;
  try {
    cli = cliPath();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }

  const child = spawn(cli, args, { stdio: "inherit" });

  // Ctrl+C in a terminal reaches the CLI too: the server stops and the CLI exits with 0, as chroma exits with 0 at
  // SIGINT. A SIGINT or SIGTERM sent to this process alone goes on to the CLI. On Windows, Ctrl+C reaches the CLI
  // through the console, and a signal sent from Node would end it at once, before the server stops: this process waits.
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
      if (platform !== "win32") {
        child.kill(signal);
      }
    });
  }

  child.on("error", (error) => {
    console.error(`Failed to start ${cli}: ${error.message}`);
    process.exit(1);
  });
  child.on("exit", (code, signal) => {
    process.exit(signal ? 128 + (os.constants.signals[signal] ?? 0) : code);
  });
};

main();
