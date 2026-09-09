import {
  CloudflarePostgresFixture,
  type CloudflareProjects,
} from "bellows-cloudflare-interop-tests/cloudflare/postgres-fixture";
import { initializeRustPostgresSchema } from "./setup.js";

export function createCloudflarePostgresFixture(
  project: CloudflareProjects,
): CloudflarePostgresFixture {
  return new CloudflarePostgresFixture(project, {
    databaseUrl:
      process.env.BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL ??
      "postgres://postgres:postgres@localhost:5432/postgres",
    initializeSchema: initializeRustPostgresSchema,
  });
}
