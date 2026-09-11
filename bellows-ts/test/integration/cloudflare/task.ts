import { definePublishTask } from "../../../src/index.js";

export const greetingTask = definePublishTask<{ name: string }>(
  "cloudflare_greeting",
);

export const fullNameTask = definePublishTask<{
  firstName: string;
  lastName: string;
}>("cloudflare_full_name");
