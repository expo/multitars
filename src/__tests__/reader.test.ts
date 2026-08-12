import { describe, it, expect } from 'vitest';
import {
  bytesToSkipTable,
  ReadableStreamBlockReader,
  readUntilBoundary,
} from '../reader';
import {
  utf8Encode,
  iterableToStream,
  streamChunks,
  streamText,
} from './utils';

function gatedStream(chunks: Uint8Array[]) {
  let release: () => void = () => {};
  const gate = () => new Promise<void>(resolve => (release = resolve));
  let pending = gate();
  let index = 0;

  const stream = new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        await pending;
        pending = gate();
        if (index >= chunks.length) return controller.close();
        controller.enqueue(chunks[index++]);
      },
    },
    { highWaterMark: 0 }
  );

  return { stream, release: () => release() };
}

// NOTE(@kitten): This is pretty dense set of tests, but they simply are designed
// to reach 100% test coverage (pnpm test --coverage)
describe(ReadableStreamBlockReader, () => {
  it('allows block-wise reads from a byte stream emitting right-sized chunks', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 3, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([4, 5, 6, 7]));
    await expect(reader.read()).resolves.toEqual(
      new Uint8Array([8, 9, 10, 11])
    );
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows block-wise reads from a byte stream emitting undersized chunks (even)', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 4, chunkSize: 3 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([4, 5, 6, 7]));
    await expect(reader.read()).resolves.toEqual(
      new Uint8Array([8, 9, 10, 11])
    );
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows block-wise reads from a byte stream emitting undersized chunks (uneven)', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 5, chunkSize: 3 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([4, 5, 6, 7]));
    await expect(reader.read()).resolves.toEqual(
      new Uint8Array([8, 9, 10, 11])
    );
    await expect(reader.read()).resolves.toEqual(new Uint8Array([12, 13, 14]));
  });

  it('allows block-wise reads from a byte stream emitting oversized chunks (even)', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 3, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 3);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([3, 4, 5]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7, 8]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([9, 10, 11]));
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows block-wise reads from a byte stream emitting oversized chunks (uneven)', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 2, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 3);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([3, 4, 5]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7]));
    await expect(reader.read()).resolves.toEqual(null);
  });

  it('allows block-wise reads from a byte stream emitting multiply-oversized chunks (even)', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 2, chunkSize: 5 })
    );
    const reader = new ReadableStreamBlockReader(stream, 2);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([2, 3]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([4, 5]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([8, 9]));
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows partial final blocks to be returned', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 2, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 3);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([3, 4, 5]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7]));
    await expect(reader.read()).resolves.toEqual(null);
  });

  it('allows block-wise reads from a byte stream emitting multiply-oversized chunks (uneven)', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 1, chunkSize: 5 })
    );
    const reader = new ReadableStreamBlockReader(stream, 2);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([2, 3]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([4]));
  });

  it('allows block-wise reads from a byte stream emitting multiply-oversized chunks (single)', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 1, chunkSize: 10 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([4, 5, 6, 7]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([8, 9]));
    await expect(reader.read()).resolves.toEqual(null);
  });

  it('allows skipping bytes for undersized chunks at end of blocks', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 4, chunkSize: 2 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.skip(2)).resolves.toBe(0);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7]));
  });

  it('allows skipping bytes for undersized chunks at beginning of blocks', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 4, chunkSize: 2 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.skip(2)).resolves.toBe(0);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([2, 3, 4, 5]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7]));
  });

  it('allows skipping bytes for oversized chunks at end of blocks', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 1, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 2);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1]));
    await expect(reader.skip(2)).resolves.toBe(0);
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows skipping bytes for oversized chunks at beginning of blocks', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 1, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 2);
    await expect(reader.skip(2)).resolves.toBe(0);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([2, 3]));
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows skipping bytes for multiply-oversized chunks', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 1, chunkSize: 6 })
    );
    const reader = new ReadableStreamBlockReader(stream, 2);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1]));
    await expect(reader.skip(2)).resolves.toBe(0);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([4, 5]));
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows skipping uneven number of bytes', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 3, chunkSize: 2 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.skip(2)).resolves.toBe(0);
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows skipping excessive number of bytes', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 3, chunkSize: 2 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    await expect(reader.skip(8)).resolves.toBe(2);
    await expect(reader.read()).resolves.toEqual(null);
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('allows pulling chunks as-is with matching input size', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 2, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 2);
    await expect(reader.pull(8)).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.pull(8)).resolves.toEqual(new Uint8Array([4, 5, 6, 7]));
    await expect(reader.pull(8)).resolves.toEqual(null);
  });

  it('allows pulling chunks as-is with matching output size', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 2, chunkSize: 4 })
    );
    const reader = new ReadableStreamBlockReader(stream, 2);
    await expect(reader.pull(8)).resolves.toEqual(new Uint8Array([0, 1, 2, 3]));
    await expect(reader.pull()).resolves.toEqual(new Uint8Array([4, 5]));
    await expect(reader.pull()).resolves.toEqual(new Uint8Array([6, 7]));
    await expect(reader.pull()).resolves.toEqual(null);
  });

  it('respects rewound data in the middle of a chunk', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 1, chunkSize: 8 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    expect(await reader.read()).toEqual(new Uint8Array([0, 1, 2, 3]));
    reader.rewind(2);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([2, 3, 4, 5]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7]));
    await expect(reader.read()).resolves.toEqual(null);
  });

  it('respects rewound data in a buffered block', async () => {
    const stream = iterableToStream(
      streamChunks({ numChunks: 4, chunkSize: 2 })
    );
    const reader = new ReadableStreamBlockReader(stream, 4);
    expect(await reader.read()).toEqual(new Uint8Array([0, 1, 2, 3]));
    reader.rewind(2);
    await expect(reader.read()).resolves.toEqual(new Uint8Array([2, 3, 4, 5]));
    await expect(reader.read()).resolves.toEqual(new Uint8Array([6, 7]));
    await expect(reader.read()).resolves.toEqual(null);
  });

  it('reaches EOF after overlapping pull and skip calls', async () => {
    const bytes = new Uint8Array(64);
    for (let idx = 0; idx < bytes.length; idx++) bytes[idx] = idx;
    const chunks = [
      bytes.subarray(0, 16),
      bytes.subarray(16, 32),
      bytes.subarray(32),
    ];
    const gated = gatedStream(chunks);
    const reader = new ReadableStreamBlockReader(gated.stream, 16);

    const pulled = reader.pull(8);
    const skipped = reader.skip(8);
    for (let idx = 0; idx <= chunks.length; idx++) {
      gated.release();
      await new Promise(resolve => setTimeout(resolve, 0));
    }
    await expect(Promise.allSettled([pulled, skipped])).resolves.toEqual([
      expect.objectContaining({ status: 'fulfilled' }),
      expect.objectContaining({ status: 'fulfilled' }),
    ]);

    let reads = 0;
    let block: Uint8Array | null;
    do {
      const reading = reader.read();
      gated.release();
      block = await reading;
      reads++;
    } while (block && reads < 32);

    expect(block).toBeNull();
    expect(reads).toBeLessThan(32);
  });
});

describe(readUntilBoundary, () => {
  const BOUNDARY = '--boundary\r\n';

  it('throws if chunk size is smaller than boundary', async () => {
    await expect(async () => {
      const stream = iterableToStream(streamText('', 1));
      const reader = new ReadableStreamBlockReader(stream, 4);
      const boundary = utf8Encode(BOUNDARY);
      for await (const _chunk of readUntilBoundary(
        reader,
        boundary,
        bytesToSkipTable(boundary)
      )) {
        // noop
      }
    }).rejects.toThrow(/Boundary must be shorter/);
  });

  it('returns bytes until boundary, even between two chunks', async () => {
    const stream = iterableToStream(
      streamText(`once upon a time...${BOUNDARY}...the end of the story`, 4)
    );
    const reader = new ReadableStreamBlockReader(stream, 12);

    // Reads data until a boundary across two chunks
    let output = '';
    const decoder = new TextDecoder();
    const boundary = utf8Encode(BOUNDARY);
    for await (const chunk of readUntilBoundary(
      reader,
      boundary,
      bytesToSkipTable(boundary)
    )) {
      expect(chunk).not.toBe(null);
      output += decoder.decode(chunk!);
    }
    expect(output).toBe('once upon a time...');

    // Continues exposing data after the boundary:
    let after = '';
    let chunk: Uint8Array | null;
    while ((chunk = await reader.pull())) after += decoder.decode(chunk);
    expect(after).toBe('...the end of the story');
  });

  it('handles boundary-like strings', async () => {
    const stream = iterableToStream(
      streamText(
        `once upon a time...${BOUNDARY.slice(0, -2)}${BOUNDARY}...the end of the story`,
        4
      )
    );
    const reader = new ReadableStreamBlockReader(stream, 12);

    // Reads data until a boundary across two chunks
    let output = '';
    const decoder = new TextDecoder();
    const boundary = utf8Encode(BOUNDARY);
    for await (const chunk of readUntilBoundary(
      reader,
      boundary,
      bytesToSkipTable(boundary)
    )) {
      expect(chunk).not.toBe(null);
      output += decoder.decode(chunk!);
    }
    expect(output).toBe(`once upon a time...${BOUNDARY.slice(0, -2)}`);

    // Continues exposing data after the boundary:
    let after = '';
    let chunk: Uint8Array | null;
    while ((chunk = await reader.pull())) after += decoder.decode(chunk);
    expect(after).toBe('...the end of the story');
  });

  it('returns immediately when boundary is first item', async () => {
    const stream = iterableToStream(
      streamText(BOUNDARY + 'test', BOUNDARY.length)
    );
    const reader = new ReadableStreamBlockReader(stream, BOUNDARY.length);
    const boundary = utf8Encode(BOUNDARY);
    let chunks = 0;
    for await (const chunk of readUntilBoundary(
      reader,
      boundary,
      bytesToSkipTable(boundary)
    )) {
      expect(chunk).toEqual(new Uint8Array([]));
      chunks++;
    }
    expect(chunks).toBe(1);
    expect(await reader.pull()).toEqual(new Uint8Array(utf8Encode('test')));
  });

  it('aborts with null yield for EOF', async () => {
    const stream = iterableToStream(streamText('some longer string', 4));
    const reader = new ReadableStreamBlockReader(stream, 12);
    const boundary = utf8Encode(BOUNDARY);
    const chunks: (Uint8Array | null)[] = [];
    for await (const chunk of readUntilBoundary(
      reader,
      boundary,
      bytesToSkipTable(boundary)
    )) {
      if (chunk) {
        const copy = new Uint8Array(chunk.byteLength);
        copy.set(chunk);
        chunks.push(copy);
      } else {
        chunks.push(chunk);
      }
    }
    expect(chunks).toMatchInlineSnapshot(`
      [
        Uint8Array [
          115,
          111,
          109,
          101,
          32,
          108,
          111,
          110,
          103,
          101,
          114,
          32,
        ],
        Uint8Array [
          115,
          116,
          114,
          105,
          110,
          103,
        ],
        null,
      ]
    `);
  });

  it('aborts with null yield for EOF while looking at partial boundary', async () => {
    const stream = iterableToStream(
      streamText(`some longer string${BOUNDARY.slice(0, 4)}`, 4)
    );
    const reader = new ReadableStreamBlockReader(stream, 12);
    const boundary = utf8Encode(BOUNDARY);
    const chunks: (Uint8Array | null)[] = [];
    for await (const chunk of readUntilBoundary(
      reader,
      boundary,
      bytesToSkipTable(boundary)
    )) {
      if (chunk) {
        const copy = new Uint8Array(chunk.byteLength);
        copy.set(chunk);
        chunks.push(copy);
      } else {
        chunks.push(chunk);
      }
    }
    expect(chunks).toMatchInlineSnapshot(`
      [
        Uint8Array [
          115,
          111,
          109,
          101,
          32,
          108,
          111,
          110,
          103,
          101,
          114,
          32,
        ],
        null,
      ]
    `);
  });

  it('handles randomized boundaries', async () => {
    function rand(length: number): string {
      let result = '';
      const characters =
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
      const charactersLength = characters.length;
      for (let i = 0; i < length; i++)
        result += characters.charAt(
          Math.floor(Math.random() * charactersLength)
        );
      return result;
    }

    for (let ITERATION = 0; ITERATION < 500; ITERATION++) {
      const before = rand(Math.round(Math.random() * 100));
      const after = rand(Math.round(Math.random() * 100));
      const stream = iterableToStream(
        streamText(`${before}${BOUNDARY}${after}`, 4)
      );
      const reader = new ReadableStreamBlockReader(stream, 12);
      const boundary = utf8Encode(BOUNDARY);
      // Reads data until a boundary across two chunks
      let actual = '';
      const decoder = new TextDecoder();
      for await (const chunk of readUntilBoundary(
        reader,
        boundary,
        bytesToSkipTable(boundary)
      ))
        actual += decoder.decode(chunk!);
      expect(actual).toBe(before);

      // Continues exposing data after the boundary:
      actual = '';
      let chunk: Uint8Array | null;
      while ((chunk = await reader.pull())) actual += decoder.decode(chunk);

      expect(actual).toBe(after);
    }
  });
});
