import { cloudflareTopology } from "bellows-cloudflare-interop-tests/cloudflare/topology";
import { describe } from "vitest";
import { createCloudflarePostgresFixture } from "./postgres-fixture.js";
import { publishingContracts } from "./publishing-contracts.js";

// Fresh workerd instances and schemas per test. Never run these concurrently:
// Wrangler reads a process-wide environment variable for local Hyperdrive.
describe.sequential("Cloudflare TypeScript producer/DO -> TypeScript processor -> PostgreSQL", () =>
  cloudflareTopology(() =>
    createCloudflarePostgresFixture({
      producer: {
        configPath: new URL("./wrangler.producer.jsonc", import.meta.url),
      },
      processor: {
        configPath: new URL("./wrangler.processor.jsonc", import.meta.url),
      },
    }),
  ));

publishingContracts();
