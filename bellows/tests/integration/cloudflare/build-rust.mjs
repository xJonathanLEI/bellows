// Finite custom Wrangler build, also run before test hooks start their short deadlines.
// Hash source, manifests, tools, and this helper; never silently use stale/prebuilt Wasm.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const examples = resolve(root, "bellows/tests/integration/cloudflare");
const version = execFileSync("worker-build", ["--version"], {
  encoding: "utf8",
}).trim();
if (version !== "0.8.5") {
  throw new Error(
    "Install the pinned tool: cargo install worker-build --version 0.8.5 --locked",
  );
}

const hash = createHash("sha256");
for (const name of ["Cargo.toml", "Cargo.lock", "bellows/Cargo.toml"]) {
  hash.update(readFileSync(resolve(root, name)));
}
function sources(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true }).sort(
    (a, b) => a.name.localeCompare(b.name),
  )) {
    if (["build", ".wrangler", "node_modules"].includes(entry.name)) continue;
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) sources(path);
    else if (/\.(rs|toml)$/.test(entry.name)) {
      hash.update(path);
      hash.update(readFileSync(path));
    }
  }
}
sources(resolve(root, "bellows/src"));
sources(examples);
hash.update(readFileSync(fileURLToPath(import.meta.url)));
hash.update(execFileSync("rustc", ["--version", "--verbose"]));
hash.update(version);
for (const name of [
  "RUSTFLAGS",
  "CARGO_ENCODED_RUSTFLAGS",
  "CARGO_TARGET_DIR",
  "RUSTUP_TOOLCHAIN",
]) {
  hash.update(`${name}=${process.env[name] ?? ""}`);
}
const digest = hash.digest("hex");
const selected = process.argv.slice(2);
for (const name of selected.length
  ? selected
  : ["producer", "processor", "contracts"]) {
  if (!["producer", "processor", "contracts"].includes(name))
    throw new Error(`Unknown Rust Worker: ${name}`);
  const directory = resolve(examples, name);
  const stamp = resolve(directory, "build/source.sha256");
  const current =
    existsSync(stamp) &&
    readFileSync(stamp, "utf8") === digest &&
    existsSync(resolve(directory, "build/worker/shim.mjs")) &&
    existsSync(resolve(directory, "build/index.js")) &&
    existsSync(resolve(directory, "build/index_bg.wasm"));
  if (!current) {
    execFileSync("worker-build", ["--release", "--locked"], {
      cwd: directory,
      stdio: "inherit",
      timeout: 180_000,
      env: { ...process.env, CARGO_TERM_COLOR: "never" },
    });
    writeFileSync(stamp, digest);
  }
  // The SDK's shim only re-exports this already-bundled index.js. Wrangler's documented
  // prebuiltWorkerDir expects <main basename>.js plus its modules. Copy UNMODIFIED SDK output,
  // not another Worker implementation. The harness still loads every real on-disk binding.
  // This avoids rerunning shell custom builds and bundling the same Rust module per fixture.
  const harness = resolve(directory, "build/harness");
  mkdirSync(harness, { recursive: true });
  copyFileSync(
    resolve(directory, "build/index.js"),
    resolve(harness, "shim.js"),
  );
  copyFileSync(
    resolve(directory, "build/index_bg.wasm"),
    resolve(harness, "index_bg.wasm"),
  );
}
