import { PostgresBackend } from "../backends/postgres.js";
import { TaskSuccess, WorkerDispatcher, type WorkerFactory } from "../index.js";
import { echoTask, emitEvent } from "./shared.js";

const databaseUrl = parseArgs();
const backend = await PostgresBackend.connect(databaseUrl);
await backend.initialize();

let resolveProcessed:
  | ((processed: { readonly taskId: number; readonly name: string }) => void)
  | undefined;
const processedPromise = new Promise<{
  readonly taskId: number;
  readonly name: string;
}>((resolve) => {
  resolveProcessed = resolve;
});

const dispatcher = new WorkerDispatcher(
  backend,
  createWorkerFactory((taskId, name) => {
    resolveProcessed?.({ taskId, name });
  }),
);
const dispatcherHandle = await dispatcher.launch();

emitEvent({ event: "ready" });

const processedTask = await processedPromise;
emitEvent({
  event: "processed",
  taskId: processedTask.taskId,
  name: processedTask.name,
});

await dispatcherHandle.drain();
await backend.close();
process.exit(0);

function parseArgs(): string {
  const [databaseUrl, ...rest] = process.argv.slice(2);

  if (databaseUrl === undefined) {
    throw new Error("expected database URL as the first argument");
  }

  if (rest.length > 0) {
    throw new Error(
      "unexpected extra arguments passed to the TypeScript worker",
    );
  }

  return databaseUrl;
}

function createWorkerFactory(
  onProcessed: (taskId: number, name: string) => void,
): WorkerFactory<typeof echoTask> {
  return {
    task: echoTask,
    build() {
      return {
        async process(taskId, taskPayload) {
          onProcessed(taskId, taskPayload.name);
          return TaskSuccess.done(taskPayload.name);
        },
      };
    },
  };
}
