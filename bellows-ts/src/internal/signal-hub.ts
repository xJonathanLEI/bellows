import type { BackendSignal } from "../types.js";
import { AsyncQueue } from "./async-queue.js";

export class BackendSignalSubscription {
  constructor(
    private readonly queue: AsyncQueue<BackendSignal>,
    private readonly onClose: () => void,
  ) {}

  async recv(): Promise<BackendSignal | null> {
    return await this.queue.shift();
  }

  close(): void {
    this.onClose();
    this.queue.close();
  }
}

export class SignalHub {
  private readonly subscriptions = new Set<AsyncQueue<BackendSignal>>();

  subscribe(): BackendSignalSubscription {
    const queue = new AsyncQueue<BackendSignal>();
    this.subscriptions.add(queue);

    return new BackendSignalSubscription(queue, () => {
      this.subscriptions.delete(queue);
    });
  }

  send(signal: BackendSignal): void {
    for (const subscription of this.subscriptions) {
      subscription.push(signal);
    }
  }

  close(): void {
    for (const subscription of this.subscriptions) {
      subscription.close();
    }

    this.subscriptions.clear();
  }
}
