import { StreamFile, StreamFileOptions } from './file';

export const CHECKSUM_INITIAL = 8 * 32;
export const BLOCK_SIZE = 512;

export function blockPad(size: number): number {
  const mask = size & (BLOCK_SIZE - 1);
  return mask && BLOCK_SIZE - mask;
}

export const enum InternalTypeFlag {
  OLD_FILE = 0 /* '\0': regular file */,
  CONTIGUOUS_FILE = 55 /* '7': contiguous file */,
  CHAR_DEV = 51 /* '3': character device (special) */,
  BLOCK_DEV = 52 /* '4': block device (special) */,
  FIFO = 54 /* '6': FIFO (special) */,
  LONG_LINK_NAME = 75 /* 'K': GNU long link name */,
  LONG_NAME = 76 /* 'L': GNU long name */,
  OLD_LONG_NAME = 78 /* 'N': GNU long name */,
  TAPE_VOL = 86 /* 'V': tape/volume header */,
  GAX = 103 /* 'g': global extended header */,
  PAX = 120 /* 'x': extended header (PAX) */,
}

export enum TarTypeFlag {
  FILE = 48 /* '0': regular file */,
  LINK = 49 /* '1': link */,
  SYMLINK = 50 /* '2': symbolic link */,
  DIRECTORY = 53 /* '5': directory */,
}

interface TarChunkHeader {
  name: string;
  mode: number;
  uid: number;
  gid: number;
  size: number;
  mtime: number;
  typeflag: TarTypeFlag | InternalTypeFlag;
  linkname: string | null;
  uname: string | null;
  gname: string | null;
  devmajor: number;
  devminor: number;
}

export interface TarHeader extends TarChunkHeader {
  _prefix?: string;
  _longName?: string;
  _longLinkName?: string;
  _paxName?: string;
  _paxLinkName?: string;
  _paxSize?: number;
}

export function initTarHeader(gax: TarHeader | null): TarHeader {
  return {
    name: gax?.name || '',
    mode: gax?.mode || 0,
    uid: gax?.uid || 0,
    gid: gax?.gid || 0,
    size: gax?.size || 0,
    mtime: gax?.mtime || 0,
    typeflag: gax?.typeflag || TarTypeFlag.FILE,
    linkname: gax?.linkname || null,
    uname: gax?.uname || null,
    gname: gax?.gname || null,
    devmajor: gax?.devmajor || 0,
    devminor: gax?.devminor || 0,
  };
}

const getTarName = (header: TarHeader): string => {
  if (header._longName) {
    return header._longName;
  } else if (header._paxName) {
    return header._paxName;
  } else if (header._prefix) {
    return `${header._prefix}/${header.name}`;
  } else {
    return header.name;
  }
};

const getTarLinkName = (header: TarHeader): string | null =>
  header._longLinkName || header._paxLinkName || header.linkname || null;

const getTarSize = (header: TarHeader): number =>
  header._paxSize ?? header.size;

type TarChunkTypeFlag = Exclude<TarTypeFlag, TarTypeFlag.FILE>;

export class TarChunk extends StreamFile implements TarChunkHeader {
  mode: number;
  uid: number;
  gid: number;
  mtime: number;
  typeflag: TarChunkTypeFlag;
  linkname: string | null;
  uname: string | null;
  gname: string | null;
  devmajor: number;
  devminor: number;

  constructor(
    stream: ReadableStream<Uint8Array<ArrayBuffer>> | BlobPart[],
    header: TarHeader
  ) {
    super(stream, getTarName(header), {
      lastModified: 1000 * header.mtime,
      size: getTarSize(header),
    });

    this.mode = header.mode;
    this.uid = header.uid;
    this.gid = header.gid;
    this.mtime = header.mtime;
    this.typeflag = header.typeflag as TarChunkTypeFlag;
    this.linkname = getTarLinkName(header);
    this.uname = header.uname;
    this.gname = header.gname;
    this.devmajor = header.devmajor;
    this.devminor = header.devminor;
  }
}

export class TarFile extends StreamFile implements TarChunkHeader {
  mode: number;
  uid: number;
  gid: number;
  mtime: number;
  typeflag: TarTypeFlag.FILE;
  linkname: null;
  uname: string | null;
  gname: string | null;
  devmajor: number;
  devminor: number;

  static from(
    stream: ReadableStream<Uint8Array<ArrayBuffer>> | BlobPart[],
    name: string,
    options: StreamFileOptions
  ) {
    const header = initTarHeader(null);
    header.name = name;
    header.mtime = options.lastModified
      ? Math.floor(options.lastModified / 1000)
      : 0;
    header.size = options.size || 0;
    return new TarFile(stream, header);
  }

  constructor(
    stream: ReadableStream<Uint8Array<ArrayBuffer>> | BlobPart[],
    header: TarHeader
  ) {
    super(stream, getTarName(header), {
      lastModified: 1000 * header.mtime,
      size: getTarSize(header),
    });

    this.mode = header.mode;
    this.uid = header.uid;
    this.gid = header.gid;
    this.mtime = header.mtime;
    this.typeflag = TarTypeFlag.FILE;
    this.linkname = null;
    this.uname = header.uname;
    this.gname = header.gname;
    this.devmajor = header.devmajor;
    this.devminor = header.devminor;
  }
}
