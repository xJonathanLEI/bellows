import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const exec = promisify(execFile);
const root = fileURLToPath(new URL("../../../../", import.meta.url));
const PREPARATION_TIMEOUT_MS = 240_000;
let initializerExecutable: Promise<string> | undefined;

type RustWorker = "producer" | "processor" | "contracts";

async function buildInitializer(): Promise<string> {
  const { stdout } = await exec(
    "cargo",
    [
      "build",
      "-p",
      "bellows",
      "--example",
      "cloudflare_initialize_postgres",
      "--no-default-features",
      "--features",
      "postgres",
      "--locked",
      "--message-format=json",
    ],
    {
      cwd: root,
      timeout: PREPARATION_TIMEOUT_MS,
      killSignal: "SIGKILL",
      maxBuffer: 8 * 1024 * 1024,
      env: { ...process.env, CARGO_TERM_COLOR: "never" },
    },
  );
  for (const line of stdout.trim().split("\n")) {
    const artifact = JSON.parse(line) as {
      reason: string;
      target?: { name: string; kind: string[] };
      executable?: string | null;
    };
    if (
      artifact.reason === "compiler-artifact" &&
      artifact.target?.name === "cloudflare_initialize_postgres" &&
      artifact.target.kind.includes("example") &&
      artifact.executable
    ) {
      // Cargo supplies the actual path, including target-dir overrides and executable suffixes.
      return artifact.executable;
    }
  }
  throw new Error(
    "Cargo did not report the Rust Cloudflare schema initializer",
  );
}

// Explicit preparation only: imports never build bundles or register suites.
// Cold compilation shares one finite budget, before short fixture hooks begin.
export async function prepareRustCloudflare(
  components: readonly RustWorker[] = ["producer", "processor", "contracts"],
): Promise<void> {
  const expires = Date.now() + PREPARATION_TIMEOUT_MS;
  initializerExecutable ??= buildInitializer();
  await initializerExecutable;
  await exec(
    process.execPath,
    [
      fileURLToPath(new URL("./build-rust.mjs", import.meta.url)),
      ...components,
    ],
    {
      cwd: root,
      timeout: Math.max(1, expires - Date.now()),
      killSignal: "SIGKILL",
      maxBuffer: 8 * 1024 * 1024,
    },
  );
}

export async function initializeRustPostgresSchema(
  databaseUrl: string,
  schema: string,
): Promise<void> {
  if (!initializerExecutable) {
    throw new Error("Call prepareRustCloudflare before starting Rust fixtures");
  }
  const executable = await initializerExecutable;
  try {
    await exec(executable, [schema], {
      cwd: root,
      timeout: 3_000,
      killSignal: "SIGKILL",
      env: {
        ...process.env,
        BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL: databaseUrl,
      },
    });
  } catch {
    // execFile settles after the child exits, including forced timeout termination.
    // Do not expose subprocess diagnostics that could contain connection credentials.
    throw new Error(
      "Rust Cloudflare PostgreSQL initialization failed or timed out. Check " +
        "BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL and existing schema privileges.",
    );
  }
}
