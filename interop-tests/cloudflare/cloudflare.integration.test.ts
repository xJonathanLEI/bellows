import { beforeAll, describe } from "vitest";
import { createCloudflarePostgresFixture as createRustFixture } from "../../bellows/tests/integration/cloudflare/postgres-fixture.js";
import { prepareRustCloudflare } from "../../bellows/tests/integration/cloudflare/setup.js";
import { createCloudflarePostgresFixture as createTypeScriptFixture } from "../../bellows-ts/test/integration/cloudflare/postgres-fixture.js";
import { cloudflareTopology } from "./topology.js";

// Only this entry point loads both implementations; neutral exports stay independent.
// Cold compilation finishes before the short per-fixture startup hooks.
beforeAll(async () => {
  await prepareRustCloudflare(["producer", "processor"]);
}, 245_000);

const rust = (component: "producer" | "processor") => {
  const configPath = new URL(
    `../../bellows/tests/integration/cloudflare/${component}/wrangler.jsonc`,
    import.meta.url,
  );
  return {
    configPath,
    prebuiltWorkerDir: new URL("./build/harness/", configPath),
  };
};
const typescript = (component: "producer" | "processor") => ({
  configPath: new URL(
    `../../bellows-ts/test/integration/cloudflare/wrangler.${component}.jsonc`,
    import.meta.url,
  ),
});

// One producer-owned initializer, database URL and schema serve both Workers.
// Never run fixtures concurrently: local Hyperdrive configuration is process-wide.
describe.sequential.each([
  ["TypeScript", "Rust"],
  ["Rust", "TypeScript"],
])("Cloudflare %s producer/DO -> %s processor -> PostgreSQL", (producer, processor) =>
  cloudflareTopology((singleton) => {
    const createFixture =
      producer === "Rust" ? createRustFixture : createTypeScriptFixture;
    return createFixture({
      singleton,
      producer: producer === "Rust" ? rust("producer") : typescript("producer"),
      processor:
        processor === "Rust" ? rust("processor") : typescript("processor"),
    });
  }));
