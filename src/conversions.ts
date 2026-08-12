export type ReadableStreamLike<T> =
  | ReadableStream<T>
  | AsyncIterable<T>
  | Iterable<T>;

const STREAM_STRATEGY: QueuingStrategy = { highWaterMark: 0 };

/** Creates a `ReadableStream` that serializes `cancel` after any in-flight `pull`.
 * @remarks
 * Some runtimes (e.g. Cloudflare workerd) only support a single pending read on
 * the underlying stream at a time. Per spec, `cancel` can be called while `pull`
 * is still in-flight, which causes concurrent reads. This wrapper ensures `cancel`
 * waits for any pending `pull` to settle first.
 *
 * The returned stream has a `cancel` method that is safe to call directly,
 * even when the stream is locked. Since this bypasses the stream's own
 * cancellation, which would otherwise reject a repeated `cancel` call, the
 * `cancel` on the source is also only ever called once.
 */
export function createReadableStream<T>(
  source: UnderlyingSource<T> & { expectedLength?: number }
): ReadableStream<T> {
  const { pull, cancel } = source;
  if (pull && cancel) {
    let inFlight: void | PromiseLike<void>;
    let cancelled: PromiseLike<void> | undefined;
    source.pull = function wrappedPull(controller) {
      return (inFlight = pull(controller));
    };
    source.cancel = function wrappedCancel() {
      if (cancelled != null) {
        return cancelled;
      } else if (inFlight != null) {
        const settle = () => cancel();
        return (cancelled = Promise.resolve(inFlight).then(settle, settle));
      }
      return (cancelled = Promise.resolve(cancel()));
    };
  }
  const stream = new ReadableStream<T>(source, STREAM_STRATEGY);
  if (source.cancel) {
    const _cancel = stream.cancel;
    stream.cancel = async function cancel(reason) {
      return (stream.locked ? source.cancel! : _cancel).call(stream, reason);
    };
  }
  return stream;
}

export function streamToAsyncIterable<T>(
  stream: ReadableStream<T>
): AsyncIterable<T> {
  if (!stream[Symbol.asyncIterator]) {
    return (async function* () {
      const reader = stream.getReader();
      let done = false;
      try {
        while (true) {
          const chunk = await reader.read();
          if (chunk.done) {
            done = true;
            return chunk.value;
          }
          yield chunk.value;
        }
      } finally {
        try {
          if (!done) await reader.cancel();
        } finally {
          reader.releaseLock();
        }
      }
    })();
  } else {
    return stream;
  }
}

export async function* streamToSizedAsyncIterable<T extends ArrayBufferView>(
  stream: ReadableStream<T>,
  expectedSize: number | null,
  label: string
): AsyncGenerator<T> {
  if (expectedSize == null) {
    yield* streamToAsyncIterable(stream);
  } else {
    const reader = stream.getReader();
    let remaining = expectedSize;
    let done = false;
    try {
      while (true) {
        const result = await reader.read();
        if (result.done) {
          done = true;
          break;
        }
        remaining -= result.value.byteLength;
        if (remaining < 0) {
          throw new Error(`${label} body exceeds declared size`);
        }
        yield result.value;
      }
      if (remaining > 0) {
        throw new Error(`${label} body is shorter than declared size`);
      }
    } finally {
      try {
        if (!done) await reader.cancel();
      } finally {
        reader.releaseLock();
      }
    }
  }
}

type IteratorReadResult<T> = IteratorResult<T, undefined>;

export interface StreamIterator<T> extends AsyncIterable<T> {
  next(): Promise<IteratorReadResult<T>>;
  return(): Promise<{ done: true; value: undefined }>;
}

export function streamLikeToIterator<T>(
  stream: ReadableStreamLike<T>
): StreamIterator<T> {
  const iterable =
    'getReader' in stream && typeof stream.getReader === 'function'
      ? streamToAsyncIterable(stream)
      : stream;
  const iterator = iterable[Symbol.asyncIterator]
    ? iterable[Symbol.asyncIterator]()
    : iterable[Symbol.iterator]();
  let done = false;
  const output: StreamIterator<T> = {
    [Symbol.asyncIterator]() {
      return output;
    },
    async next() {
      if (done) return { done: true, value: undefined };
      const result = await iterator.next();
      done = !!result.done;
      return result.done ? { done: true, value: undefined } : result;
    },
    async return() {
      if (!done) {
        done = true;
        await iterator.return?.();
      }
      return { done: true, value: undefined };
    },
  };
  return output;
}

interface SafeIteratorSourceOptions {
  signal?: AbortSignal;
  expectedLength?: number | bigint;
}

export interface BodyReadableStream<T> extends ReadableStream<T> {
  signal: AbortSignal;
}

/** Converts an `AsyncIterable` or `Iterable` into a safe `ReadableStream` with a safety `AbortSignal`
 * @remarks
 * This helper converts an iterable to a `ReadableStream` with a paired `AbortSignal`. The `AbortSignal`
 * will abort when the underlying iterable errors.
 *
 * A common problem with Fetch Standard implementations is that a `ReaadbleStream` passed to
 * a request does not propagate its error once the request has started. This prevents the
 * request from being cancelled and the error from propagating to the response when the input
 * Readable Stream errors.
 * This helper provides an AbortSignal to forcefully abort the request when the underlying iterable
 * errors.
 *
 * @param iterable - The AsyncIterable to wrap and catch errors from
 * @param sourceOptions - An optional `expectedLength` and parent `signal` that may propagate to the output `ReadableStream`
 * @returns `BodyReadableStream` that is a converted `ReadableStream` of the iterable with a `signal: AbortSignal` property that aborts when the iterable errors.
 */
export function iterableToStream<T>(
  iterable: AsyncIterable<T> | Iterable<T>,
  sourceOptions?: SafeIteratorSourceOptions
): BodyReadableStream<T> {
  const signal = sourceOptions?.signal;
  const abortController = new AbortController();
  let iterator: AsyncIterator<T> | AsyncIterator<T>;
  return Object.assign(
    new ReadableStream<T>(
      {
        expectedLength: sourceOptions?.expectedLength,
        start() {
          iterator = iterable[Symbol.asyncIterator]
            ? iterable[Symbol.asyncIterator]()
            : iterable[Symbol.iterator]();
          signal?.throwIfAborted();
          signal?.addEventListener('abort', () =>
            abortController.abort(signal.reason)
          );
        },
        async pull(controller) {
          try {
            signal?.throwIfAborted();
            const next = await iterator.next();
            if (next.value) controller.enqueue(next.value);
            if (next.done) controller.close();
          } catch (error) {
            controller.error(error);
            abortController.abort(error);
          }
        },
        async cancel(reason) {
          if (reason) await iterator.throw?.(reason);
          await iterator.return?.();
        },
      },
      { highWaterMark: 0 }
    ),
    { signal: abortController.signal }
  );
}
