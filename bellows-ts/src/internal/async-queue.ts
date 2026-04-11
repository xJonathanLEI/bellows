export class AsyncQueue<T> {
  private readonly items: T[] = [];
  private readonly waiters: Array<(value: T | null) => void> = [];
  private closed = false;

  push(value: T): void {
    if (this.closed) {
      return;
    }

    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(value);
      return;
    }

    this.items.push(value);
  }

  async shift(): Promise<T | null> {
    const value = this.items.shift();
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
    if (this.closed) {
      return;
    }

    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter(null);
    }
  }
}
