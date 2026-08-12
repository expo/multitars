import {
  type ReadableStreamLike,
  StreamIterator,
  streamLikeToIterator,
} from './conversions';

/** A reader that can output fixed size pages of underlying byte streams.
 * @remarks
 * The `ReadableStreamBlockReader` accepts a `ReadableStream<Uint8Array>` and a given
 * `blockSize`, and upon calling `read()`, outputs fixed sizes blocks. This is useful
 * for paged readers and parsers.
 */
export class ReadableStreamBlockReader {
  // The block-wise reader has three sets of byte sources from which it'll read in order:
  // 1. this.blockRewind -> read from the prior block's output (like a tape rewinder)
  // 2. this.input -> read from the last chunk the underlying source has emitted
  // 3. this.next -> get the next chunk from the underlying source

  next: StreamIterator<Uint8Array>;

  input: Uint8Array | null;
  inputOffset: number;

  blockLocked: boolean;
  blockRewind: number;
  blockSize: number;
  block: Uint8Array;

  buffer: ArrayBuffer | null;

  constructor(stream: ReadableStreamLike<Uint8Array>, blockSize: number) {
    this.next = streamLikeToIterator(stream);
    this.input = null;
    this.inputOffset = 0;
    this.blockLocked = false;
    this.blockRewind = 0;
    this.blockSize = blockSize;
    this.block = new Uint8Array(blockSize);
    this.buffer = null;
  }

  async close(): Promise<void> {
    await this.next.return();
  }

  /** Outputs the next block of `Uint8Array` data.
   * @remarks
   * Resolves a `Uint8Array` block of `blockSize` length, but will
   * return `null` if the stream is exhausted. It may return a partial
   * block at the end of the stream.
   */
  async read(): Promise<Uint8Array | null> {
    const { blockSize, block } = this;

    // (1): If we've rewound to prior data, copy it within the existing block
    let byteLength = 0;
    if (this.blockRewind > 0) {
      byteLength += this.blockRewind;
      block.copyWithin(0, -this.blockRewind);
      this.blockRewind = 0;
    }

    // (2): If we have the next chunk, read from it first
    let remaining = blockSize - byteLength;
    if (this.input != null && remaining > 0) {
      if (this.input.byteLength - this.inputOffset > remaining) {
        // Optimization: We can return immediately if we have enough data
        // Optimization: If `block` is still empty, we can just return the slice directly
        const slice = this.input.subarray(
          this.inputOffset,
          (this.inputOffset += remaining)
        );
        return (this.blockLocked = byteLength !== 0)
          ? (block.set(slice, byteLength), block)
          : slice;
      } else {
        // We copy partially from input, since the input is exhausted
        const slice = this.input.subarray(this.inputOffset);
        block.set(slice, byteLength);
        byteLength += slice.byteLength;
        this.input = null;
      }
    }

    // (3): If `block` isn't filled yet, start filling it with data from the byte stream
    while ((remaining = blockSize - byteLength) > 0) {
      const { done, value: view } = await this.next.next();
      if (done || !view?.byteLength) {
        break;
      } else if (view.byteLength > remaining) {
        this.input = view;
        const slice = this.input.subarray(0, (this.inputOffset = remaining));
        // Optimization: If `block` is still empty, we can just return the slice directly
        return (this.blockLocked = byteLength !== 0)
          ? (block.set(slice, byteLength), block)
          : slice;
      } else {
        block.set(view, byteLength);
        byteLength += view.byteLength;
      }
    }

    this.blockLocked = true;
    if (byteLength === blockSize) {
      return block;
    } else if (byteLength > 0) {
      // Return a partial block at the end of the stream
      // NOTE: Due to rewind we have to shift this data to the end of the block
      block.copyWithin(blockSize - byteLength, 0, byteLength);
      return block.subarray(blockSize - byteLength);
    } else {
      return null;
    }
  }

  /** Outputs a `Uint8Array` buffer that's at most `maxSize` long.
   * @remarks
   * Resolves a `Uint8Array` block of at most `maxSize` length. When a
   * buffer returned by the underlying byte stream is too long, it's
   * instead buffered. When this method is called, the block it resolves
   * is consumed, and a subsequent method will consume data after this
   * block instead.
   */
  async pull(maxSize = this.blockSize): Promise<Uint8Array | null> {
    const { block, blockRewind } = this;
    if (blockRewind > 0) {
      // (1): If we've rewound to prior data, copy from within the existing block
      // NOTE: If `maxSize < blockRewind`, rewinding isn't safe
      this.blockLocked = true;
      return this.blockRewind <= maxSize
        ? ((this.blockRewind = 0), block.subarray(-blockRewind))
        : block.subarray(-blockRewind, -(this.blockRewind -= maxSize));
    } else if (this.input != null) {
      // (2): If we have a next chunk, return a slice of it
      this.blockLocked = false;
      const inputSize = this.input.byteLength - this.inputOffset;
      if (inputSize <= maxSize) {
        const slice = this.input.subarray(this.inputOffset);
        this.input = null;
        return slice;
      } else {
        return this.input.subarray(
          this.inputOffset,
          (this.inputOffset += maxSize)
        );
      }
    }

    // (3): Otherwise, retrieve the next chunk from the underlying stream
    this.blockLocked = false;
    const { done, value: view } = await this.next.next();
    if (done) {
      return null;
    } else if (view.byteLength > maxSize) {
      this.input = view;
      return view.subarray(0, (this.inputOffset = maxSize));
    } else {
      return view;
    }
  }

  /** Skips `requestedSize` bytes from the underlying byte stream.
   * @remarks
   * Will skip at most `requestedSize` bytes in the underlying byte stream
   * and won't allow these bytes to be used/returned by other methods.
   * The resolved number is the amount of remaining bytes from `requestedSize`
   * that couldn't be skipped due to the end of the underlying byte stream
   * being reached.
   */
  async skip(requestedSize: number): Promise<number> {
    let remaining = requestedSize;

    // (1): If we've rewound to prior data, discard it
    this.blockLocked = false;
    if (this.blockRewind >= remaining) {
      this.blockRewind -= remaining;
      return 0;
    } else if (this.blockRewind > 0) {
      remaining -= this.blockRewind;
      this.blockRewind = 0;
    }

    // (2): If we have the next chunk, skip ahead in it
    if (this.input != null) {
      if (this.input.byteLength - this.inputOffset > remaining) {
        this.inputOffset += remaining;
        return 0;
      } else {
        remaining -= this.input.byteLength - this.inputOffset;
        this.input = null;
      }
    }

    // (3): Otherwise, get more chunks to skip over
    while (remaining > 0) {
      const { done, value: view } = await this.next.next();
      if (done) {
        return remaining;
      } else if (view.byteLength > remaining) {
        this.input = view;
        this.inputOffset = remaining;
        return 0;
      } else {
        remaining -= view.byteLength;
      }
    }

    return remaining;
  }

  /** Re-adds byte array back to buffered data */
  rewind(shiftEnd: number): void {
    if (this.blockLocked) {
      this.blockRewind += shiftEnd;
    } else if (this.input != null) {
      this.inputOffset -= shiftEnd;
    }
  }

  copy(block: Uint8Array): Uint8Array {
    this.buffer ||= new ArrayBuffer(this.blockSize);
    const copy = new Uint8Array(this.buffer, 0, block.byteLength);
    copy.set(block);
    return copy;
  }
}

export function bytesToSkipTable(bytes: Uint8Array): Uint8Array {
  const skipTable = new Uint8Array(256).fill(bytes.byteLength);
  const endIdx = bytes.byteLength - 1;
  for (let idx = 0; idx < endIdx; idx++) skipTable[bytes[idx]] = endIdx - idx;
  return skipTable;
}

function indexOf(
  buffer: Uint8Array,
  boundary: Uint8Array,
  fromIndex: number,
  skipTable: Uint8Array
): number {
  const boundaryEndIdx = boundary.byteLength - 1;
  const bufferEndIdx = buffer.byteLength - boundary.byteLength;
  const boundaryLastByte = boundary[boundaryEndIdx];
  const boundaryStartByte = boundary[0];
  let idx = fromIndex;
  while (idx <= bufferEndIdx) {
    const bufferByte = buffer[idx + boundaryEndIdx];
    if (bufferByte === boundaryLastByte) {
      if (buffer[idx] === boundaryStartByte) {
        return idx;
      }
    }
    idx += skipTable[bufferByte];
  }
  return buffer.indexOf(boundaryStartByte, idx);
}

export async function* readUntilBoundary(
  reader: ReadableStreamBlockReader,
  boundary: Uint8Array,
  skipTable: Uint8Array
): AsyncGenerator<Uint8Array | null> {
  if (boundary.byteLength > reader.blockSize) {
    throw new TypeError(
      `Boundary must be shorter than block size (${boundary.byteLength} > ${reader.blockSize})`
    );
  }
  // Finding a boundary sequence in a byte stream is tricky
  // Say, the boundary is in any way internally repetitive, e.g. `--x--x\r\n`
  // If the boundary is then cut off at its internal repetition point, we
  // have to search the end of the previous buffer repeatedly to find
  // the boundary successfully:
  //   --x--|x--x\r\n
  // If we only search the first buffer once, we risk missing it due to the repetition.
  for (
    let buffer = await reader.read(), nextBuffer: Uint8Array | null = null;
    buffer != null || (buffer = await reader.read()) != null;
    nextBuffer = null
  ) {
    let searchIdx = -1;
    // (1): Search for the starting boundary character from `searchIdx`
    while (
      (searchIdx = indexOf(buffer, boundary, searchIdx + 1, skipTable)) > -1
    ) {
      // (2): Check if boundary matches (partially) at `searchIdx`
      let bufferIdx = searchIdx + 1;
      let boundaryIdx = 1;
      while (
        boundaryIdx < boundary.byteLength &&
        bufferIdx < buffer.byteLength &&
        boundary[boundaryIdx] === buffer[bufferIdx]
      ) {
        boundaryIdx++;
        bufferIdx++;
      }
      // (3): We either have found the boundary or a partial boundary
      if (boundaryIdx === boundary.byteLength) {
        // (3.1): Complete boundary is present in `buffer` at `searchStart`
        reader.rewind(buffer.byteLength - bufferIdx);
        yield buffer.subarray(0, searchIdx);
        return;
      } else if (bufferIdx === buffer.byteLength) {
        // (4): Partial boundary was found at the end of `buffer`
        // Get the next buffer and search the rest of the boundary in `nextBuffer`
        if (!nextBuffer) {
          // Copy last buffer before moving on
          buffer = reader.copy(buffer);
          nextBuffer = await reader.read();
          if (!nextBuffer) {
            // WARN(@kitten): This means we ran out of chunks unexpectedly (EOF) while searching for a boundary
            yield null;
            return;
          }
        }
        // (5): Check if remaining partial boundary matches in `nextBuffer`
        bufferIdx = 0;
        while (
          boundaryIdx < boundary.byteLength &&
          bufferIdx < nextBuffer.byteLength &&
          boundary[boundaryIdx] === nextBuffer[bufferIdx]
        ) {
          boundaryIdx++;
          bufferIdx++;
        }
        if (boundaryIdx === boundary.byteLength) {
          // Boundary found across `buffer` and `nextBuffer`
          reader.rewind(nextBuffer.byteLength - bufferIdx);
          yield buffer.subarray(0, searchIdx);
          return;
        }
      }
    }
    // (6): Boundary wasn't found, so emit the full buffer, and search the next one
    const output = buffer;
    buffer = nextBuffer;
    yield output;
  }
  yield null;
}
