import { cloudflareTopology } from "bellows-cloudflare-interop-tests/cloudflare/topology";
import { beforeAll, describe } from "vitest";
import { createCloudflarePostgresFixture } from "./postgres-fixture.js";
import { rustContracts } from "./rust-contracts.js";
import { prepareRustCloudflare } from "./setup.js";

beforeAll(async () => {
  await prepareRustCloudflare();
}, 245_000);

const rust = (component: "producer" | "processor") => {
  const configPath = new URL(`./${component}/wrangler.jsonc`, import.meta.url);
  return {
    configPath,
    prebuiltWorkerDir: new URL("./build/harness/", configPath),
  };
};

// Suites are sequential: local Hyperdrive configuration is process-wide.
describe.sequential("Cloudflare Rust producer/DO -> Rust processor -> PostgreSQL", () =>
  cloudflareTopology(() =>
    createCloudflarePostgresFixture({
      producer: rust("producer"),
      processor: rust("processor"),
    }),
  ));

rustContracts(new URL("./contracts/wrangler.jsonc", import.meta.url));
