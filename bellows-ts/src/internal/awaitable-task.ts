import { AwaitTaskError, type TaskCodec } from "../types.js";

class Deferred<T> {
  readonly promise: Promise<T>;
  private resolvePromise!: (value: T) => void;
  private rejectPromise!: (error: unknown) => void;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolvePromise = resolve;
      this.rejectPromise = reject;
    });
    void this.promise.catch(() => undefined);
  }

  resolve(value: T): void {
    this.resolvePromise(value);
  }

  reject(error: unknown): void {
    this.rejectPromise(error);
  }
}

export interface CallbackSink {
  deliver(callbackPayloadJson: string): void;
  drop(): void;
}

export function createCallbackChannel<TCallback>(
  callbackCodec: TaskCodec<TCallback>,
): {
  readonly callbackPromise: Promise<TCallback>;
  readonly callbackSink: CallbackSink;
} {
  const deferred = new Deferred<TCallback>();

  return {
    callbackPromise: deferred.promise,
    callbackSink: {
      deliver(callbackPayloadJson) {
        try {
          deferred.resolve(callbackCodec.decode(callbackPayloadJson));
        } catch {
          deferred.reject(new AwaitTaskError());
        }
      },
      drop() {
        deferred.reject(new AwaitTaskError());
      },
    },
  };
}
