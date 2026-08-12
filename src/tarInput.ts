import { ReadableStreamLike, createReadableStream } from './conversions';
import { ReadableStreamBlockReader } from './reader';
import { decoder } from './shared';

import {
  BLOCK_SIZE,
  CHECKSUM_INITIAL,
  TarTypeFlag,
  InternalTypeFlag,
  TarHeader,
  TarFile,
  TarChunk,
  initTarHeader,
  blockPad,
} from './tarShared';

async function decodePax(
  reader: ReadableStreamBlockReader,
  gax: TarHeader | null,
  header: TarHeader
) {
  let remaining = header.size;
  const chunks: Uint8Array[] = [];
  while (remaining > 0) {
    const block = await reader.read();
    if (!block || block.byteLength !== reader.blockSize)
      throw new Error('Invalid Tar: Unexpected EOF while parsing PAX data');
    const length = Math.min(remaining, block.byteLength);
    chunks.push(block.slice(0, length));
    remaining -= length;
  }
  const pax = new Uint8Array(header.size);
  for (let idx = 0, offset = 0; idx < chunks.length; idx++) {
    pax.set(chunks[idx], offset);
    offset += chunks[idx].byteLength;
  }
  for (let from = 0, to = 0; from < pax.byteLength; to = from) {
    while (to < pax.byteLength && pax[to] !== 32) to++;
    const length = parseInt(decoder.decode(pax.subarray(from, to)), 10);
    if (!length || length != length) break;
    if (pax[from + length - 1] !== 10) break;
    const entry = decoder.decode(pax.subarray(to + 1, from + length - 1));
    const keyIndex = entry.indexOf('=');
    if (keyIndex === -1) break;
    const key = entry.slice(0, keyIndex);
    const value = entry.slice(keyIndex + 1);
    from += length;
    switch (key) {
      case 'path':
        if (gax) gax._paxName = value;
        header._paxName = value;
        break;
      case 'linkpath':
        if (gax) gax._paxLinkName = value;
        header._paxLinkName = value;
        break;
      case 'size':
        if (gax) gax._paxSize = +value;
        header._paxSize = +value;
        break;
      case 'gid':
      case 'uid':
      case 'mode':
      case 'mtime':
        if (gax) gax[key] = +value;
        header[key] = +value;
        break;
      case 'uname':
      case 'gname':
        if (gax) gax[key] = value;
        header[key] = value;
        break;
    }
  }
}

function getTypeFlag(
  bytes: Uint8Array
): TarTypeFlag | InternalTypeFlag | ({} & number) {
  return bytes[156];
}

function checkMagic(bytes: Uint8Array): boolean {
  return (
    bytes[257] === 0x75 &&
    bytes[258] === 0x73 &&
    bytes[259] === 0x74 &&
    bytes[260] === 0x61 &&
    bytes[261] === 0x72 &&
    (bytes[262] === 0x00 || bytes[262] === 0x20)
  );
}

function checkChecksum(bytes: Uint8Array): number {
  let sum = CHECKSUM_INITIAL;
  const chksum = decodeOctal(bytes, 148, 156);
  if (chksum === sum) return sum;
  for (let idx = 0; idx < 148; idx++) sum += bytes[idx];
  for (let idx = 156; idx < 512; idx++) sum += bytes[idx];
  return sum === chksum ? sum : 0;
}

function decodeString(bytes: Uint8Array, from: number, to: number): string {
  let end = from;
  while (end < to && bytes[end] !== 0) end++;
  return end > from ? decoder.decode(bytes.subarray(from, end)) : '';
}

async function decodeLongString(
  reader: ReadableStreamBlockReader,
  size: number
): Promise<string> {
  let remaining = size;
  let output = '';
  let endIndex = -1;
  while (remaining > 0) {
    let block = await reader.read();
    if (!block || block.byteLength !== reader.blockSize)
      throw new Error('Invalid Tar: Unexpected EOF while parsing long string');
    remaining -= block.byteLength;
    if (endIndex === -1) {
      endIndex = block.indexOf(0);
      if (endIndex > -1) block = block.subarray(0, endIndex);
      output += decoder.decode(block, { stream: true });
    }
  }
  output += decoder.decode();
  return output;
}

function decodeOctal(bytes: Uint8Array, from: number, to: number): number {
  let val = 0;
  let idx = to;
  if (bytes[from] === 0x80) {
    let mul = 1;
    while (idx-- > from + 1) {
      val += bytes[idx] * mul;
      mul *= 256;
    }
    return val;
  } else if (bytes[from] === 0xff) {
    let mul = 1;
    let flipped = false;
    while (idx-- > from) {
      const f = flipped
        ? 0xff ^ bytes[idx] // ones comp
        : (0xff ^ bytes[idx]) + 1; // twos comp
      val -= (f & 0xff) * mul;
      mul *= 256;
      flipped ||= bytes[idx] !== 0;
    }
    return val;
  } else {
    idx = from;
    while (idx < to && (bytes[idx] === 32 || bytes[idx] === 0)) idx++;
    while (idx < to && bytes[idx] >= 48 && bytes[idx] <= 55)
      val = val * 8 + (bytes[idx++] - 48);
    return val;
  }
}

function decodeBase(header: TarHeader, buffer: Uint8Array): void {
  /*
  | Field Name | Offset | Length | Field Type                 |
  | ---------- | ------ | ------ | -------------------------- |
  | name       | 0 B    | 100 B  | NUL-terminated if NUL fits |
  | mode       | 100 B  | 8 B    |                            |
  | uid        | 108 B  | 8 B    |                            |
  | gid        | 116 B  | 8 B    |                            |
  | size       | 124 B  | 12 B   |                            |
  | mtime      | 136 B  | 12 B   |                            |
  | chksum     | 148 B  | 8 B    |                            |
  | typeflag   | 156 B  | 1 B    | see below                  |
  | linkname   | 157 B  | 100 B  | NUL-terminated if NUL fits |
  | magic      | 257 B  | 6 B    | must be TMAGIC (NUL term.) |
  | version    | 263 B  | 2 B    | must be TVERSION           |
  | uname      | 265 B  | 32 B   | NUL-terminated             |
  | gname      | 297 B  | 32 B   | NUL-terminated             |
  | devmajor   | 329 B  | 8 B    |                            |
  | devminor   | 337 B  | 8 B    |                            |
  | prefix     | 345 B  | 155 B  | NUL-terminated if NUL fits |
  */
  header.name = decodeString(buffer, 0, 100);
  header.mode ||= decodeOctal(buffer, 100, 108);
  header.uid ||= decodeOctal(buffer, 108, 116);
  header.gid ||= decodeOctal(buffer, 116, 124);
  header.size = decodeOctal(buffer, 124, 136);
  header.mtime ||= decodeOctal(buffer, 136, 148);
  // CHECKSUM: 148 - 156
  header.typeflag = getTypeFlag(buffer);
  header.linkname = decodeString(buffer, 157, 257) || null;
  // TMAGIC: 257 - 263
  // TVERSION: 263 - 265
  header.uname ||= decodeString(buffer, 265, 297) || null;
  header.gname ||= decodeString(buffer, 297, 329) || null;
  header.devmajor = decodeOctal(buffer, 329, 337);
  header.devminor = decodeOctal(buffer, 337, 345);

  if (buffer[345] !== 0) {
    header._prefix = decodeString(buffer, 345, 500);
  }
  if (
    header.typeflag === InternalTypeFlag.OLD_FILE &&
    header.name.endsWith('/')
  ) {
    header.typeflag = TarTypeFlag.DIRECTORY;
  }
}

async function decodeHeader(
  reader: ReadableStreamBlockReader,
  gax: TarHeader
): Promise<TarHeader | undefined> {
  let buffer: Uint8Array | null;
  let header = initTarHeader(gax);
  while ((buffer = await reader.read()) && checkMagic(buffer)) {
    if (buffer.byteLength !== reader.blockSize) {
      throw new Error('Invalid Tar: Unexpected EOF while reading header');
    }
    switch (getTypeFlag(buffer)) {
      case InternalTypeFlag.LONG_NAME:
      case InternalTypeFlag.OLD_LONG_NAME:
        decodeBase(header, buffer);
        header._longName = await decodeLongString(reader, header.size);
        continue;

      case InternalTypeFlag.LONG_LINK_NAME:
        decodeBase(header, buffer);
        header._longLinkName = await decodeLongString(reader, header.size);
        continue;

      case InternalTypeFlag.GAX:
        decodeBase(header, buffer);
        await decodePax(reader, gax, header);
        continue;

      case InternalTypeFlag.PAX:
        decodeBase(header, buffer);
        await decodePax(reader, null, header);
        continue;

      case InternalTypeFlag.OLD_FILE:
      case InternalTypeFlag.CONTIGUOUS_FILE:
      case TarTypeFlag.FILE:
      case TarTypeFlag.LINK:
      case TarTypeFlag.SYMLINK:
      case TarTypeFlag.DIRECTORY:
        decodeBase(header, buffer);
        return header;

      default:
        // Usually we don't check the checksum, but if we don't know the type, we should
        // double check it. If it's invalid we can be sure we have an invalid block
        if (!checkChecksum(buffer)) {
          throw new Error(
            'Invalid Tar: Unexpected block with invalid checksum'
          );
        }
        decodeBase(header, buffer);
        return header;
    }
  }
  for (let idx = 0; buffer && idx < buffer.byteLength; idx++)
    if (buffer[idx] !== 0)
      throw new Error('Invalid Tar: Unexpected non-header block');
  return undefined;
}

/** Provide tar entry iterator */
export async function* untar(
  stream: ReadableStreamLike<Uint8Array>
): AsyncGenerator<TarFile | TarChunk> {
  const reader = new ReadableStreamBlockReader(stream, BLOCK_SIZE);
  try {
    yield* readTar(reader);
  } finally {
    await reader.close();
  }
}

async function* readTar(
  reader: ReadableStreamBlockReader
): AsyncGenerator<TarFile | TarChunk> {
  const gax = initTarHeader(null);

  let header: TarHeader | undefined;
  while ((header = await decodeHeader(reader, gax)) != null) {
    const size = header._paxSize ?? header.size;
    if (!Number.isSafeInteger(size) || size < 0) {
      throw new Error(`Invalid Tar: Invalid entry size ${size}`);
    }
    const pad = blockPad(size);
    let consumedTrailer = pad === 0;
    let remaining = size;
    const stream = createReadableStream<Uint8Array<ArrayBuffer>>({
      // NOTE(@kitten): This is needed in Cloudflare to attach the expected size to the stream
      expectedLength: size,
      async cancel() {
        if (!consumedTrailer) {
          remaining += pad;
        }
        if (remaining > 0) {
          const skipped = await reader.skip(remaining);
          if (skipped > 0) throw new Error('Invalid Tar: Unexpected EOF');
          remaining = 0;
        }
        consumedTrailer = true;
      },
      async pull(controller) {
        if (remaining) {
          const buffer = await reader.pull(remaining);
          if (!buffer) throw new Error('Invalid Tar: Unexpected EOF');
          remaining -= buffer.byteLength;
          controller.enqueue(
            (reader.blockLocked
              ? buffer.slice()
              : buffer) as Uint8Array<ArrayBuffer>
          );
        }
        if (!remaining) {
          if (!consumedTrailer) {
            const skipped = await reader.skip(pad);
            if (skipped > 0) throw new Error('Invalid Tar: Unexpected EOF');
            consumedTrailer = true;
          }
          controller.close();
        }
      },
    });

    let chunk: TarFile | TarChunk;
    switch (header.typeflag) {
      case InternalTypeFlag.OLD_FILE:
      case InternalTypeFlag.CONTIGUOUS_FILE:
      case TarTypeFlag.FILE:
        chunk = new TarFile(stream, header);
        break;
      case TarTypeFlag.LINK:
      case TarTypeFlag.SYMLINK:
      case TarTypeFlag.DIRECTORY:
        chunk = new TarChunk(stream, header);
        break;
      default:
        await stream.cancel();
        continue;
    }

    yield chunk;
    if (remaining > 0 || !consumedTrailer) {
      await stream.cancel();
    }
  }
}
