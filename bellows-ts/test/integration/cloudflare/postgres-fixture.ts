import {
  CloudflarePostgresFixture,
  type CloudflareProjects,
} from "bellows-cloudflare-interop-tests/cloudflare/postgres-fixture";
import { initializePostgresSchema } from "../../../src/backends/postgres-operations.js";

export function createCloudflarePostgresFixture(
  project: CloudflareProjects,
): CloudflarePostgresFixture {
  return new CloudflarePostgresFixture(project, {
    databaseUrl:
      process.env.BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL ??
      process.env.BELLOWS_TS_TEST_POSTGRES_URL ??
      "postgres://postgres:postgres@localhost:5432/postgres",
    initializeSchema: async (databaseUrl, schema) => {
      // Use a direct Node connection, never a Worker/LISTEN client. Bound its
      // operations rather than leaving initialization running after teardown.
      const initializationUrl = new URL(databaseUrl);
      initializationUrl.searchParams.set("connect_timeout", "2");
      initializationUrl.searchParams.set("statement_timeout", "1000");
      initializationUrl.searchParams.set("query_timeout", "1500");
      await initializePostgresSchema(initializationUrl.toString(), schema);
    },
  });
}
