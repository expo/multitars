import { describe, it, expect } from 'vitest';
import { createReadableStream, iterableToStream } from '../conversions';

describe('createReadableStream', () => {
  const makeStream = () => {
    const calls: string[] = [];
    let onPullStart: () => void;
    let settlePull: () => void;
    const pullStarted = new Promise<void>(resolve => {
      onPullStart = resolve;
    });
    const pullSettled = new Promise<void>(resolve => {
      settlePull = resolve;
    });
    const stream = createReadableStream<Uint8Array>({
      async pull(controller) {
        calls.push('pull');
        onPullStart();
        await pullSettled;
        controller.enqueue(new Uint8Array(1));
      },
      async cancel() {
        calls.push('cancel');
      },
    });
    return { stream, calls, pullStarted, settlePull: () => settlePull() };
  };

  it('should only cancel the source once for repeated cancellations', async () => {
    const { stream, calls } = makeStream();

    await stream.cancel();
    await stream.cancel();

    expect(calls).toEqual(['cancel']);
  });

  it('should only cancel the source once for concurrent cancellations', async () => {
    const { stream, calls, pullStarted, settlePull } = makeStream();

    // NOTE: A pending pull delays the cancellation of the source, which allows a
    // second cancellation to start while the first one is still in-flight.
    // The stream stays locked, so the second cancellation bypasses the stream
    const reader = stream.getReader();
    const read = reader.read();
    await pullStarted;
    const cancellations = Promise.all([reader.cancel(), stream.cancel()]);
    settlePull();
    await read.catch(() => {});
    await cancellations;

    expect(calls).toEqual(['pull', 'cancel']);
  });

  it('should cancel the source after an in-flight pull settles', async () => {
    const { stream, calls, pullStarted, settlePull } = makeStream();

    const reader = stream.getReader();
    const read = reader.read();
    await pullStarted;
    const cancellation = reader.cancel();
    expect(calls).toEqual(['pull']);

    settlePull();
    await read.catch(() => {});
    await cancellation;

    expect(calls).toEqual(['pull', 'cancel']);
  });
});

describe('safeIteratorToStream', () => {
  it('should propagate errors to an AbortSignal (start errors)', async () => {
    const body = iterableToStream(
      (async function* gen() {
        throw new Error('ohno');
      })()
    );

    try {
      for await (const _chunk of body) {
      }
      expect.fail('Expected stream to error');
    } catch (error: any) {
      expect(error.message).toMatch('ohno');
      expect(body.signal.aborted).toBe(true);
      expect(() => body.signal.throwIfAborted()).toThrow('ohno');
    }
  });

  it('should propagate errors to an AbortSignal (yield errors)', async () => {
    const body = iterableToStream(
      (async function* gen() {
        yield new Uint8Array(0);
        throw new Error('ohno');
      })()
    );

    try {
      for await (const _chunk of body) {
      }
      expect.fail('Expected stream to error');
    } catch (error: any) {
      expect(error.message).toMatch('ohno');
      expect(body.signal.aborted).toBe(true);
      expect(() => body.signal.throwIfAborted()).toThrow('ohno');
    }
  });

  it('should not propagate cancellation', async () => {
    const body = iterableToStream(
      (async function* gen() {
        yield new Uint8Array(0);
        throw new Error('ohno');
      })()
    );

    for await (const _chunk of body) {
      break;
    }

    expect(body.signal.aborted).toBe(false);
  });

  it('should not back-propagate unexpected errors', async () => {
    const body = iterableToStream(
      (async function* gen() {
        while (true) yield new Uint8Array(0);
      })()
    );

    try {
      for await (const _chunk of body) {
        throw new Error('ohno');
      }
    } catch (error) {
      expect(body.signal.aborted).toBe(false);
    }
  });

  it('should cancel if input signal also cancels', async () => {
    const controller = new AbortController();
    const body = iterableToStream(
      (async function* gen() {
        while (true) yield new Uint8Array(0);
      })(),
      { signal: controller.signal }
    );

    try {
      for await (const _chunk of body) {
        controller.abort(new Error('ohno'));
      }
      expect.fail('Expected stream to error');
    } catch (error) {
      expect(body.signal.aborted).toBe(true);
      expect(() => body.signal.throwIfAborted()).toThrow('ohno');
    }
  });
});
