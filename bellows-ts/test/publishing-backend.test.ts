import { expect, expectTypeOf, test } from "vitest";
import { InMemoryBackend } from "../src/backends/in-memory.js";
import {
  type Backend,
  definePublishTask,
  defineSingletonTask,
  type PublishedTask,
  type TaskExecutionBackend,
  TaskNotFoundError,
  type TaskPublishingBackend,
  TaskUnavailableError,
} from "../src/index.js";

const echoTask = definePublishTask<string, string>("publishing_echo");

// Deliberately exposes only publication, not execution or subscriptions.
function publishingOnly(inner: InMemoryBackend): TaskPublishingBackend {
  return {
    publish: inner.publish.bind(inner),
    publishFuture: inner.publishFuture.bind(inner),
  };
}

async function publishTasks<B extends TaskPublishingBackend>(
  backend: B,
  availableFromMs: number,
): Promise<[PublishedTask, PublishedTask]> {
  const immediate = await backend.publish(echoTask, "immediate");
  const future = await backend.publishFuture(
    echoTask,
    "future",
    availableFromMs,
  );
  return [immediate, future];
}

test("generic producer only requires publishing capability", async () => {
  const inner = new InMemoryBackend();
  const availableFromMs = Date.now() + 60_000;
  const [immediate, future] = await publishTasks(
    publishingOnly(inner),
    availableFromMs,
  );

  expect(immediate.taskId).not.toBe(future.taskId);
  const expiration = availableFromMs + 60_000;
  const claimed = await inner.claimPublished(
    echoTask,
    17,
    immediate.taskId,
    expiration,
  );
  expect(claimed.taskId).toBe(immediate.taskId);
  expect(claimed.taskPayload).toBe("immediate");
  await expect(
    inner.claimPublished(echoTask, 17, future.taskId, expiration),
  ).rejects.toEqual(new TaskUnavailableError(availableFromMs));

  // A non-void callback can be completed without an awaitable publication handle.
  const finished = await inner.finish(
    echoTask,
    17,
    immediate.taskId,
    "done",
    null,
  );
  expect(finished.taskId).toBe(immediate.taskId);
  await expect(
    inner.claimPublished(echoTask, 17, immediate.taskId, expiration),
  ).rejects.toBeInstanceOf(TaskNotFoundError);
});

test("publishing capability preserves task types and the reduced surface", () => {
  expectTypeOf<keyof TaskPublishingBackend>().toEqualTypeOf<
    "publish" | "publishFuture"
  >();
  expectTypeOf<Backend>().toMatchTypeOf<TaskPublishingBackend>();
  expectTypeOf<TaskPublishingBackend>().not.toMatchTypeOf<Backend>();
  expectTypeOf<TaskPublishingBackend>().not.toMatchTypeOf<TaskExecutionBackend>();
  expectTypeOf<TaskExecutionBackend>().not.toMatchTypeOf<TaskPublishingBackend>();

  // These functions are typechecked, not executed.
  expectTypeOf((backend: TaskPublishingBackend) => {
    const singleton = defineSingletonTask("singleton");
    // @ts-expect-error Singleton activation is not publishable.
    backend.publish(singleton, undefined);
    // @ts-expect-error Singleton activation is not publishable in the future.
    backend.publishFuture(singleton, undefined, Date.now());
    // @ts-expect-error The payload must match the task definition.
    backend.publish(echoTask, 42);
    // @ts-expect-error Future payloads must also match the task definition.
    backend.publishFuture(echoTask, 42, Date.now());
    return backend.publish(echoTask, "immediate");
  }).returns.resolves.toEqualTypeOf<PublishedTask>();
  expectTypeOf((backend: TaskPublishingBackend) =>
    backend.publishFuture(echoTask, "future", Date.now()),
  ).returns.resolves.toEqualTypeOf<PublishedTask>();
});
