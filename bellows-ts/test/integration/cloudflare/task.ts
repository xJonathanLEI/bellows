import { definePublishTask } from "../../../src/index.js";

export const greetingTask = definePublishTask<{ name: string }>(
  "cloudflare_greeting",
);
