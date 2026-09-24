/** Logical identity; singleton row IDs belong exclusively to the execution backend. */
export type TaskIdentity =
  | {
      readonly kind: "published";
      readonly taskId: string;
      readonly taskName: string;
    }
  | { readonly kind: "singleton"; readonly taskName: string };

/** Intent affects acceptance only, never processor execution or scheduling. */
export type DispatchTask =
  | { readonly task: TaskIdentity; readonly intent: "run" }
  | {
      readonly task: Extract<TaskIdentity, { kind: "singleton" }>;
      readonly intent: "ensure";
    };

export function trackingKey(task: TaskIdentity): string {
  return task.kind === "published"
    ? `published:${task.taskId}`
    : `singleton:${task.taskName}`;
}

export function dispatchIdentity(value: unknown): TaskIdentity {
  const task = taskIdentity(value);
  if (task.kind === "published") {
    if (task.taskId.length === 0 || task.taskId.length > 200)
      throw new Error(
        "taskId must be a non-empty string no longer than 200 characters",
      );
  } else {
    // Storage keys are limited to 2048 UTF-8 bytes, including both prefixes.
    for (const character of task.taskName) {
      const point = character.codePointAt(0) ?? 0;
      if (point >= 0xd800 && point <= 0xdfff)
        throw new Error("invalid singleton name");
    }
    if (new TextEncoder().encode(`task:${trackingKey(task)}`).length > 2048)
      throw new Error("singleton name exceeds storage key byte limit");
  }
  return task;
}

export function dispatchEntry(value: unknown): DispatchTask {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("invalid dispatch entry");
  const entry = value as Record<string, unknown>;
  const task = dispatchIdentity(entry.task);
  if (Object.keys(entry).length !== 2)
    throw new Error("invalid dispatch entry");
  if (entry.intent === "run") return { task, intent: "run" };
  if (entry.intent === "ensure" && task.kind === "singleton")
    return { task, intent: "ensure" };
  throw new Error("invalid dispatch intent");
}

export function taskIdentity(value: unknown): TaskIdentity {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid task identity");
  }
  const task = value as Record<string, unknown>;
  if (typeof task.taskName !== "string" || task.taskName.length === 0) {
    throw new Error("taskName must be a non-empty string");
  }
  if (task.kind === "singleton" && Object.keys(task).length === 2) {
    return { kind: "singleton", taskName: task.taskName };
  }
  if (
    task.kind === "published" &&
    Object.keys(task).length === 3 &&
    typeof task.taskId === "string"
  ) {
    return { kind: "published", taskId: task.taskId, taskName: task.taskName };
  }
  throw new Error("invalid task identity");
}

export function sameTask(left: TaskIdentity, right: TaskIdentity): boolean {
  return (
    left.kind === right.kind &&
    left.taskName === right.taskName &&
    (left.kind === "singleton" ||
      (right.kind === "published" && left.taskId === right.taskId))
  );
}
