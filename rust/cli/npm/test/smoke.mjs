// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
//
// The test of the npm packages in the job npm of ci.yml, in an app where npm installed vecturadb and chromadb of npm:
// the server starts with npx vecturadb run, as the README of the package says, and chromadb writes and reads; then the
// server stops, starts again on the same folder, this time from the bin of the package, and the data is there. Run in
// the folder of the app:
//   node smoke.mjs
// The stops, on Linux and macOS:
// - npx: SIGINT to its process group, as Ctrl+C in a terminal. npm does not pass signals on to the command: the
//   terminal sends them to every process of the group.
// - The bin, as a process manager starts it: SIGINT to it alone, which the command passes on to the CLI; it then
//   exits with 0, as chroma at SIGINT.
// On Windows both with taskkill /T, as the job of the CLI on Windows stops the server.
import { spawn, spawnSync } from "child_process";
import { readFileSync } from "fs";
import path from "path";
import { setTimeout as sleep } from "timers/promises";
import { ChromaClient } from "chromadb";

const windows = process.platform === "win32";
const host = "127.0.0.1";
const port = 8123;
const run = ["run", "--path", "data", "--host", host, "--port", String(port)];

const check = (ok, what) => {
  if (!ok) throw new Error(`failed: ${what}`);
  console.log(`ok: ${what}`);
};

// On Linux and macOS in a process group of its own, which the stops and the cleanup signal.
const commands = {
  npx: () => spawn("npx", ["vecturadb", ...run], { stdio: "inherit", shell: windows, detached: !windows }),
  bin: () => spawn(path.join("node_modules", ".bin", "vecturadb"), run, { stdio: "inherit", shell: windows, detached: !windows }),
};

const answers = async () => {
  try {
    await new ChromaClient({ host, port }).heartbeat();
    return true;
  } catch {
    return false;
  }
};

const start = async (command) => {
  const server = commands[command]();
  const exit = new Promise((resolve) => server.once("exit", (code, signal) => resolve({ code, signal })));
  let exited = null;
  exit.then((result) => (exited = result));
  for (let i = 0; i < 120; i++) {
    if (exited) throw new Error(`the server stopped before it answered: ${JSON.stringify(exited)}`);
    if (await answers()) {
      check(true, `the server started with ${command} answers the heartbeat`);
      return { command, server, exit };
    }
    await sleep(500);
  }
  throw new Error("the server did not answer in 60 s");
};

const stop = async ({ command, server, exit }) => {
  if (windows) {
    spawnSync("taskkill", ["/pid", String(server.pid), "/T", "/F"], { stdio: "inherit" });
  } else if (command === "npx") {
    process.kill(-server.pid, "SIGINT");
  } else {
    server.kill("SIGINT");
  }
  const result = await Promise.race([exit, sleep(30000).then(() => null)]);
  check(result !== null, `the server started with ${command} stops in 30 s`);
  console.log(`${command} exited with ${JSON.stringify(result)}`);
  if (!windows && command === "bin") check(result.code === 0, "the bin exits with 0 after SIGINT, as chroma");
  check(!(await answers()), "nothing answers on the port after the stop");
};

// After a failure: nothing of the test stays running.
const cleanup = ({ server }) => {
  if (windows) {
    spawnSync("taskkill", ["/pid", String(server.pid), "/T", "/F"], { stdio: "inherit" });
  } else {
    try {
      process.kill(-server.pid, "SIGKILL");
    } catch {}
  }
};

const version = JSON.parse(readFileSync("node_modules/vecturadb/package.json", "utf8")).version;
const reported = spawnSync("npx", ["vecturadb", "--version"], { encoding: "utf8", shell: windows });
check(reported.stdout.trim() === `vecturadb ${version}`, `npx vecturadb --version is "vecturadb ${version}": "${reported.stdout.trim()}"`);

let running = null;
try {
  running = await start("npx");
  const client = new ChromaClient({ host, port });
  const collection = await client.createCollection({ name: "npm", embeddingFunction: null });
  await collection.add({
    ids: ["a", "b", "c"],
    embeddings: [
      [1, 0, 0],
      [0, 1, 0],
      [0, 0, 1],
    ],
    documents: ["one", "two", "three"],
    metadatas: [{ n: 1 }, { n: 2 }, { n: 3 }],
  });
  check((await collection.count()) === 3, "count is 3 after add");
  const query = await collection.query({ queryEmbeddings: [[1, 0, 0.1]], nResults: 2 });
  check(query.ids[0][0] === "a", `query finds a first: ${JSON.stringify(query.ids)}`);
  const got = await collection.get({ ids: ["b"], include: ["documents", "metadatas"] });
  check(got.documents[0] === "two" && got.metadatas[0].n === 2, `get b: ${JSON.stringify(got.documents)} ${JSON.stringify(got.metadatas)}`);
  await collection.delete({ ids: ["c"] });
  check((await collection.count()) === 2, "count is 2 after delete");
  await stop(running);
  running = null;

  running = await start("bin");
  const again = await new ChromaClient({ host, port }).getCollection({ name: "npm" });
  check((await again.count()) === 2, "count is 2 after the start on the same folder");
  const kept = await again.get({ ids: ["a"], include: ["documents"] });
  check(kept.documents[0] === "one", `get a after the start on the same folder: ${JSON.stringify(kept.documents)}`);
  await stop(running);
  running = null;
} finally {
  if (running) cleanup(running);
}
