import {
  type ReadableStreamLike,
  streamLikeToIterator,
  streamToAsyncIterable,
} from './conversions';

import { encoder } from './shared';

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

const MAX_NAME_LEN = 100;
const MAX_PREFIX_LEN = 155;
const MAGIC = 'ustar\0' + '00';
const PAD = new Uint8Array(BLOCK_SIZE);

// See: https://github.com/mafintosh/tar-stream/blob/126968f/constants.js#L1C1-L8C2
function modeToType(mode: number) {
  switch (mode & 61440 /*S_IFMT*/) {
    case 24576: // S_IFBLK
      return InternalTypeFlag.BLOCK_DEV;
    case 8192: // S_IFCHR
      return InternalTypeFlag.CHAR_DEV;
    case 16384: // S_IFDIR
      return TarTypeFlag.DIRECTORY;
    case 4096: // S_IFIFO
      return InternalTypeFlag.FIFO;
    case 40960: // S_IFLNK
      return TarTypeFlag.SYMLINK;
    default:
      return TarTypeFlag.FILE;
  }
}

function encodeString(
  target: Uint8Array,
  from: number,
  to: number,
  value: string | null | undefined
) {
  if (value) encoder.encodeInto(`${value}\0`, target.subarray(from, to));
}

function encodeOctal(
  target: Uint8Array,
  from: number,
  to: number,
  value: number
) {
  const length = to - from;
  const max = length <= 8 ? 0o7777777 : 0o77777777777;
  if (value > max) {
    target[from] = 0x80;
    let num = value;
    for (let idx = to - 1; idx > from; num = Math.floor(num / 0x100), idx--)
      target[idx] = num & 0xff;
  } else if (value < 0) {
    target[from] = 0xff;
    let num = -value;
    let flipped = false;
    for (let idx = to - 1; idx > from; num = Math.floor(num / 0x100), idx--) {
      const byte = num & 0xff;
      target[idx] = flipped
        ? (0xff ^ byte) & 0xff // ones comp
        : ((0xff ^ byte) + 1) & 0xff; // twos comp
      flipped ||= byte !== 0;
    }
  } else if (value) {
    let idx = to - 2;
    let num = value;
    if (num * 8 <= max) target[idx--] = 0x20;
    while (num > 0) {
      const digit = num % 8;
      target[idx--] = 0x30 + digit;
      num = (num - digit) / 8;
    }
    while (idx >= from) target[idx--] = 0x30;
  }
}

function encodeChecksum(bytes: Uint8Array) {
  let sum = CHECKSUM_INITIAL;
  for (let idx = 0; idx < 148; idx++) sum += bytes[idx];
  for (let idx = 156; idx < 512; idx++) sum += bytes[idx];
  encodeOctal(bytes, 148, 156, sum);
}

// Needed if a path is longer than 100 characters
// It attempts to split the path at a slash into a prefix and name
// The prefix capacity is 155 and the name is 100
function indexOfPrefixEnd(path: string): number {
  if (path.length <= 255) {
    let idx = path.length - 1;
    while ((idx = path.lastIndexOf('/', idx - 1)) > 0) {
      const prefixLen = idx;
      const nameLen = path.length - idx - 1;
      if (prefixLen < MAX_PREFIX_LEN && nameLen < MAX_NAME_LEN) return idx;
    }
  }
  return -1;
}

function encodeBase(target: Uint8Array, header: TarHeader): void {
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
  let name = header.name;
  if (!header._paxName && !header._longName && name.length > MAX_NAME_LEN) {
    const idx = indexOfPrefixEnd(name);
    if (idx > -1) {
      name = name.slice(idx + 1);
    }
  }

  if (!header.typeflag) header.typeflag = modeToType(header.mode);
  if (!header.mode)
    header.mode = header.typeflag === TarTypeFlag.DIRECTORY ? 0o755 : 0o644;
  if (!header.mtime) header.mtime = Math.floor(Date.now() / 1000);

  encodeString(target, 0, 100, name);
  encodeOctal(target, 100, 108, header.mode & 0o7777);
  encodeOctal(target, 108, 116, header.uid);
  encodeOctal(target, 116, 124, header.gid);
  encodeOctal(target, 124, 136, header.size);
  encodeOctal(target, 136, 148, header.mtime);
  // CHECKSUM: 148 - 156
  target[156] = header.typeflag;
  encodeString(target, 157, 257, header.linkname);
  encodeString(target, 257, 265, MAGIC); // TMAGIC + TVERSION
  encodeString(target, 265, 297, header.uname);
  encodeString(target, 297, 329, header.gname);
  encodeOctal(target, 329, 337, header.devmajor);
  encodeOctal(target, 337, 345, header.devminor);
  encodeString(target, 345, 500, header._prefix);
  encodeChecksum(target);
}

function encodeHeader(header: TarHeader): Uint8Array<ArrayBuffer> {
  const block = new Uint8Array(BLOCK_SIZE);
  encodeBase(block, header);
  return block;
}

function encodePax(header: TarHeader): Uint8Array<ArrayBuffer> | null {
  function encodePaxEntry(key: string, value: string) {
    const line = ` ${key}=${value}\n`;
    let length = line.length;
    const prefix = `${length}`;
    length += prefix.length;
    // Since the encoded length must include the digits of length itself, we have to make
    // sure we roll over the total number of characters using log10
    if (1 + Math.floor(Math.log10(length)) > prefix.length) length += 1;
    return `${length}${line}`;
  }
  let output = '';
  if (header._paxName) output += encodePaxEntry('path', header._paxName);
  if (header._paxLinkName)
    output += encodePaxEntry('linkpath', header._paxLinkName);
  return output ? encoder.encode(output) : null;
}

function paxName(name: string) {
  const idx = name.lastIndexOf('/');
  const basename = idx > -1 ? name.slice(idx) : name;
  return `PaxHeader/${basename.slice(-99)}`;
}

export async function* tar(
  entries: ReadableStreamLike<TarChunk | TarFile>
): AsyncGenerator<Uint8Array<ArrayBuffer>> {
  const next = streamLikeToIterator(entries);
  let result: Awaited<ReturnType<typeof next>>;
  while (!(result = await next()).done && result.value) {
    const { value: entry } = result;
    const header = initTarHeader(entry);
    if (!Number.isSafeInteger(header.size) || header.size < 0) {
      throw new Error(
        `Invalid Tar: Cannot safely encode part with size ${header.size}`
      );
    }

    if (entry.lastModified && !header.mtime)
      header.mtime = Math.floor(entry.lastModified / 1000);

    if (
      header.typeflag === TarTypeFlag.DIRECTORY &&
      !header.name.endsWith('/')
    ) {
      header.name += '/';
    } else if (header.typeflag === TarTypeFlag.SYMLINK) {
      header.size = 0;
    }

    if (header.name.length > MAX_NAME_LEN) {
      const idx = indexOfPrefixEnd(header.name);
      if (idx > -1) {
        header._prefix = header.name.slice(0, idx);
        header.name = header.name.slice(idx + 1);
      } else {
        header._paxName = header.name;
        header.name = paxName(header.name);
      }
    }

    if (header.linkname && header.linkname.length > MAX_NAME_LEN) {
      header._paxLinkName = header.linkname;
      header.linkname = paxName(header.name);
    }

    const pax = encodePax(header);
    if (pax) {
      const paxHeader = initTarHeader(null);
      paxHeader.typeflag = InternalTypeFlag.PAX;
      paxHeader.size = pax.byteLength;
      yield encodeHeader(paxHeader);
      yield pax;
      const pad = blockPad(pax.byteLength);
      if (pad) yield PAD.subarray(0, pad);
    }

    yield encodeHeader(header);

    const stream = entry.stream();
    if (header.size) {
      yield* streamToAsyncIterable(stream);
    } else if (!stream.locked) {
      await stream.cancel();
    }

    const pad = blockPad(entry.size);
    if (pad) yield PAD.subarray(0, pad);
  }

  yield new Uint8Array(BLOCK_SIZE * 2);
}
