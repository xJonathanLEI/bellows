import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect } from "vitest";

export interface ProcessedTask {
  readonly taskId: number;
  readonly name: string;
}

export class AsyncChannel<T> {
  private readonly values: T[] = [];
  private readonly waiters: Array<(value: T | null) => void> = [];
  private closed = false;

  send(value: T): void {
    if (this.closed) {
      throw new Error("channel is closed");
    }

    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(value);
      return;
    }

    this.values.push(value);
  }

  async recv(): Promise<T | null> {
    const value = this.values.shift();
    if (value !== undefined) {
      return value;
    }

    if (this.closed) {
      return null;
    }

    return await new Promise<T | null>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  close(): void {
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter(null);
    }
  }

  tryRecv(): T | null {
    return this.values.shift() ?? null;
  }
}

export class Gate {
  private permits = 0;
  private readonly waiters: Array<() => void> = [];

  release(): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter();
      return;
    }

    this.permits += 1;
  }

  async wait(): Promise<void> {
    if (this.permits > 0) {
      this.permits -= 1;
      return;
    }

    await new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

export async function assertNamesEchoed(
  channel: AsyncChannel<ProcessedTask>,
  names: readonly string[],
): Promise<void> {
  const processed: ProcessedTask[] = [];

  while (processed.length < names.length) {
    const task = await channel.recv();
    if (task === null) {
      break;
    }

    processed.push(task);
  }

  expect(processed).toHaveLength(names.length);
  for (const name of names) {
    expect(processed.some((task) => task.name === name)).toBe(true);
  }
}

export class TestSqliteDatabase {
  readonly directory = mkdtempSync(join(tmpdir(), "bellows-ts-sqlite-"));
  readonly filePath = join(this.directory, "test.sqlite");
  readonly url = `sqlite://${this.filePath}`;

  cleanup(): void {
    rmSync(this.directory, { recursive: true, force: true });
  }
}
