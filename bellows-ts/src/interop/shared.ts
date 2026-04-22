import { definePublishTask } from "../index.js";

export const echoTask = definePublishTask<{ name: string }, string>(
  "postgres_interop_echo",
);

export type InteropProcessEvent =
  | { readonly event: "ready" }
  | {
      readonly event: "published";
      readonly taskId: number;
      readonly name: string;
    }
  | {
      readonly event: "processed";
      readonly taskId: number;
      readonly name: string;
    }
  | {
      readonly event: "awaited";
      readonly taskId: number;
      readonly name: string;
    };

export function emitEvent(event: InteropProcessEvent): void {
  process.stdout.write(`${JSON.stringify(event)}\n`);
}
