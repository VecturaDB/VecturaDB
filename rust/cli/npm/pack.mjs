// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
//
// Makes the npm packages of the CLI with npm pack, for the job npm of ci.yml and the job npm of release.yml:
//   node rust/cli/npm/pack.mjs --cli <folder> --out <folder> [--version <version>] [--only <platform>]...
// --cli is the folder with the binaries of the CLI as the jobs of ci.yml name them, the files of the GitHub Release.
// --version is the version of the release tag; without it, the version of rust/cli/Cargo.toml, which the CLI of a run
// without a tag reports. --only packs only the package of that platform, with vecturadb; without it, all of them.
// Each package is copied into <out>/staging with the version, the binary as vecturadb (vecturadb.exe on Windows),
// LICENSE, NOTICE and, in the packages of the platforms, THIRD-PARTY-NOTICES; the tarballs go to <out>, and
// <out>/packages.json lists them with their integrity and the sha256 of each binary.
import { execFileSync } from "child_process";
import { createHash } from "crypto";
import { chmodSync, copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parseArgs } from "util";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../../..");

// The platforms: the folder of the package in rust/cli/npm, and the binary of the CLI as ci.yml names it.
const platforms = {
  "darwin-arm64": "vecturadb-macos-arm64",
  "darwin-x64": "vecturadb-macos-x64",
  "linux-arm64-gnu": "vecturadb-linux-aarch64",
  "linux-x64-gnu": "vecturadb-linux-x86_64",
  "win32-x64-msvc": "vecturadb-windows-x64.exe",
};

const { values } = parseArgs({
  options: {
    cli: { type: "string" },
    out: { type: "string" },
    version: { type: "string" },
    only: { type: "string", multiple: true },
  },
});
if (!values.cli || !values.out) {
  throw new Error("Usage: node rust/cli/npm/pack.mjs --cli <folder> --out <folder> [--version <version>] [--only <platform>]...");
}

const cargoVersion = () => {
  const match = readFileSync(path.join(root, "rust/cli/Cargo.toml"), "utf8").match(/^version = "(.*)"$/m);
  if (!match) throw new Error("No version in rust/cli/Cargo.toml");
  return match[1];
};
const version = values.version || cargoVersion();
// The versions of release.yml: X.Y.Z, X.Y.Z-preview.N and X.Y.Z-preview.N.postM, all of them semver.
if (!/^\d+\.\d+\.\d+(-preview\.\d+(\.post\d+)?)?$/.test(version)) {
  throw new Error(`${version} is not X.Y.Z, X.Y.Z-preview.N or X.Y.Z-preview.N.postM`);
}

const only = values.only ?? Object.keys(platforms);
for (const platform of only) {
  if (!platforms[platform]) throw new Error(`No package for ${platform}: ${Object.keys(platforms).join(", ")}`);
}

const out = path.resolve(values.out);
const staging = path.join(out, "staging");
rmSync(staging, { recursive: true, force: true });
mkdirSync(staging, { recursive: true });

const readJson = (file) => JSON.parse(readFileSync(file, "utf8"));
const writeJson = (file, data) => writeFileSync(file, JSON.stringify(data, null, 2) + "\n");
const sha256 = (file) => createHash("sha256").update(readFileSync(file)).digest("hex");

// npm pack of a folder, with npm-cli.js of the npm that comes with this node, as nodejs.org and actions/setup-node
// install it: no shell, the same on every platform.
const npmCli = path.join(path.dirname(process.execPath), process.platform === "win32" ? "" : "../lib", "node_modules/npm/bin/npm-cli.js");
if (!existsSync(npmCli)) throw new Error(`No npm next to ${process.execPath}: ${npmCli}`);
const npmPack = (folder) => {
  const output = execFileSync(process.execPath, [npmCli, "pack", folder, "--pack-destination", out, "--json"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  });
  const [result] = JSON.parse(output);
  return { name: result.name, version: result.version, filename: result.filename, integrity: result.integrity, shasum: result.shasum };
};

const stage = (folder, name) => {
  const target = path.join(staging, name);
  mkdirSync(target);
  for (const file of ["README.md"]) copyFileSync(path.join(here, folder, file), path.join(target, file));
  for (const file of ["LICENSE", "NOTICE"]) copyFileSync(path.join(root, file), path.join(target, file));
  return target;
};

const packed = [];

for (const platform of only) {
  const manifest = readJson(path.join(here, platform, "package.json"));
  const target = stage(platform, manifest.name);
  copyFileSync(path.join(root, "THIRD-PARTY-NOTICES"), path.join(target, "THIRD-PARTY-NOTICES"));
  const binary = manifest.files[0];
  const source = path.join(path.resolve(values.cli), platforms[platform]);
  copyFileSync(source, path.join(target, binary));
  // npm keeps the mode of the files in the tarball: the binary is executable where it is packed on Linux or macOS.
  chmodSync(path.join(target, binary), 0o755);
  writeJson(path.join(target, "package.json"), { ...manifest, version });
  packed.push({ ...npmPack(target), binary, sha256: sha256(source), source: platforms[platform] });
}

const main = readJson(path.join(here, "vecturadb", "package.json"));
const mainTarget = stage("vecturadb", main.name);
copyFileSync(path.join(here, "vecturadb", "cli.mjs"), path.join(mainTarget, "cli.mjs"));
const optionalDependencies = Object.fromEntries(Object.keys(main.optionalDependencies).map((name) => [name, version]));
writeJson(path.join(mainTarget, "package.json"), { ...main, version, optionalDependencies });
packed.push(npmPack(mainTarget));

writeJson(path.join(out, "packages.json"), packed);
for (const p of packed) {
  console.log(`${p.filename} ${p.integrity}${p.sha256 ? ` ${p.binary} sha256 ${p.sha256} (${p.source})` : ""}`);
}
