import {
  DecompressionStream,
  ReadableStream,
  ReadableStreamDefaultReader,
} from 'node:stream/web';
import { pack } from 'tar-stream';
import * as path from 'node:path';
import { Readable } from 'node:stream';
import { Buffer } from 'node:buffer';
import * as fs from 'node:fs';

import { describe, it, expect } from 'vitest';
import { TarFile, TarTypeFlag } from '../tarShared';
import { untar } from '../tarInput';
import { tar } from '../tarOutput';
import { iterableToStream } from './utils';

const openTarball = () => {
  const tarball = Readable.toWeb(
    fs.createReadStream(path.join(__dirname, 'fixtures/worker-sample.tar.gz'))
  );
  return tarball.pipeThrough(new DecompressionStream('gzip'));
};

function chunk(
  readable: ReadableStream<Uint8Array>,
  chunkSize = 500
): ReadableStream<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array>;
  return new ReadableStream({
    start() {
      reader = readable.getReader();
    },
    async pull(controller) {
      const { done, value } = await reader.read();
      if (done) return controller.close();
      for (let sliceIdx = 0; sliceIdx < value.length; sliceIdx += chunkSize) {
        controller.enqueue(value.subarray(sliceIdx, sliceIdx + chunkSize));
      }
    },
  });
}

interface TestFile {
  name: string;
  data: string | Buffer;
}

function makeTarball(files: Iterable<TestFile>): ReadableStream<any> {
  const tar = pack();
  const readable = Readable.from(tar);
  for (const file of files) {
    tar.entry({ name: file.name, type: 'file' }, file.data);
  }
  tar.finalize();
  return Readable.toWeb(readable);
}

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

const cancellationFiles: TestFile[] = [
  { name: 'dropped/first.bin', data: Buffer.alloc(4096, 1) },
  { name: 'kept/second.txt', data: 'second entry' },
  { name: 'dropped/third.bin', data: Buffer.alloc(2048, 2) },
  { name: 'kept/fourth.txt', data: 'fourth entry' },
  { name: 'dropped/fifth.bin', data: Buffer.alloc(8, 3) },
  { name: 'kept/sixth.txt', data: 'sixth entry' },
];

const cancellationFileNames = cancellationFiles.map(file => file.name);

function patchHeader(header: Buffer) {
  header.fill(0x20, 148, 156);
  let checksum = 0;
  for (const byte of header) checksum += byte;
  header.write(`${checksum.toString(8).padStart(6, '0')}\0 `, 148, 'ascii');
}

describe('untar', () => {
  it('releases the input stream when parsing fails', async () => {
    const tarball = new Blob([new Uint8Array([1])]).stream();

    await expect(async () => {
      for await (const _entry of untar(tarball)) {
        // noop
      }
    }).rejects.toThrow();

    expect(tarball.locked).toBe(false);
  });

  it('releases the input stream when iteration stops early', async () => {
    const tarball = makeTarball([
      { name: 'first.txt', data: 'first' },
      { name: 'second.txt', data: 'second' },
    ]);

    for await (const _entry of untar(tarball)) break;

    expect(tarball.locked).toBe(false);
  });

  it.each(['x', 'g', 'L', 'N', 'K'])(
    'ignores the ordinary metadata of a %s extension header',
    async type => {
      const isPax = type === 'x' || type === 'g';
      const payload = new Blob([isPax ? '10 uid=42\n' : 'x.txt\0']);
      const archive = Buffer.from(
        await new Response(
          iterableToStream(
            tar([
              TarFile.from(payload.stream(), 'extension', {
                size: payload.size,
              }),
              TarFile.from(new Blob(['x']).stream(), 'x.txt', { size: 1 }),
            ])
          )
        ).arrayBuffer()
      );
      const extension = archive.subarray(0, 512);
      const file = archive.subarray(1024, 1536);
      extension[156] = type.charCodeAt(0);
      if (type === 'K') file[156] = TarTypeFlag.SYMLINK;
      for (const [header, mode, uid, gid, mtime, uname, gname] of [
        [extension, 0o4755, 123, 456, 789, 'extension-user', 'extension-group'],
        [file, 0o644, 7, 8, 9, 'file-user', 'file-group'],
      ] as const) {
        for (const [from, to, value] of [
          [100, 108, mode],
          [108, 116, uid],
          [116, 124, gid],
          [136, 148, mtime],
        ]) {
          header.fill(0, from, to);
          header.write(
            value.toString(8).padStart(to - from - 1, '0'),
            from,
            'ascii'
          );
        }
        header.fill(0, 265, 329);
        header.write(uname, 265, 'ascii');
        header.write(gname, 297, 'ascii');
        header.fill(0, 345, 500);
      }
      extension.write('injected', 345, 'ascii');
      patchHeader(extension);
      patchHeader(file);

      const entries: Record<string, unknown>[] = [];
      for await (const entry of untar(new Blob([archive]).stream())) {
        entries.push({
          name: entry.name,
          mode: entry.mode,
          uid: entry.uid,
          gid: entry.gid,
          mtime: entry.mtime,
          uname: entry.uname,
          gname: entry.gname,
          linkname: entry.linkname,
          text: await entry.text(),
        });
      }
      expect(entries).toEqual([
        {
          name: 'x.txt',
          mode: 0o644,
          uid: isPax ? 42 : 7,
          gid: 8,
          mtime: 9,
          uname: 'file-user',
          gname: 'file-group',
          linkname: type === 'K' ? 'x.txt' : null,
          text: 'x',
        },
      ]);
    }
  );

  it.each(['x', 'g'])('ignores mode records in a %s PAX header', async type => {
    const pax = new Blob(['13 mode=2541\n']);
    const archive = Buffer.from(
      await new Response(
        iterableToStream(
          tar([
            TarFile.from(pax.stream(), 'pax', { size: pax.size }),
            TarFile.from(new Blob(['x']).stream(), 'x.txt', { size: 1 }),
          ])
        )
      ).arrayBuffer()
    );
    archive[156] = type.charCodeAt(0);
    patchHeader(archive.subarray(0, 512));

    const entries: { name: string; mode: number; text: string }[] = [];
    for await (const entry of untar(new Blob([archive]).stream())) {
      entries.push({
        name: entry.name,
        mode: entry.mode,
        text: await entry.text(),
      });
    }
    expect(entries).toEqual([{ name: 'x.txt', mode: 0o644, text: 'x' }]);
  });

  it.each([
    ['GNU', 'ustar  \0', 'file.txt'],
    ['POSIX', 'ustar\0' + '00', '15301000244/file.txt'],
  ])(
    'uses the %s header layout for the prefix field',
    async (_format, magic, name) => {
      const archive = Buffer.from(
        await new Response(
          iterableToStream(
            tar([
              TarFile.from(new Blob(['x']).stream(), 'file.txt', { size: 1 }),
            ])
          )
        ).arrayBuffer()
      );
      archive.write(magic, 257, 'ascii');
      // GNU atime/ctime fields occupy the same bytes as the POSIX prefix.
      archive.write('15301000244\0', 345, 'ascii');
      archive.write('15301000244\0', 357, 'ascii');
      patchHeader(archive.subarray(0, 512));

      const entries: { name: string; text: string }[] = [];
      for await (const entry of untar(new Blob([archive]).stream())) {
        entries.push({ name: entry.name, text: await entry.text() });
      }
      expect(entries).toEqual([{ name, text: 'x' }]);
    }
  );

  it.each(['x', 'g'])(
    'preserves zero overrides in a %s PAX header',
    async type => {
      const pax = new Blob(['8 uid=0\n8 gid=0\n11 mtime=0\n']);
      const archive = Buffer.from(
        await new Response(
          iterableToStream(
            tar([
              TarFile.from(pax.stream(), 'pax', { size: pax.size }),
              TarFile.from(new Blob([]).stream(), 'first.txt', { size: 0 }),
              TarFile.from(new Blob([]).stream(), 'second.txt', { size: 0 }),
            ])
          )
        ).arrayBuffer()
      );
      archive[156] = type.charCodeAt(0);
      patchHeader(archive.subarray(0, 512));
      for (const offset of [1024, 1536]) {
        const header = archive.subarray(offset, offset + 512);
        header.write('0000173\0', 108, 'ascii');
        header.write('0000710\0', 116, 'ascii');
        header.write('00000000144\0', 136, 'ascii');
        patchHeader(header);
      }

      const entries: { uid: number; gid: number; mtime: number }[] = [];
      for await (const entry of untar(new Blob([archive]).stream())) {
        entries.push({ uid: entry.uid, gid: entry.gid, mtime: entry.mtime });
        await entry.text();
      }
      expect(entries).toEqual([
        { uid: 0, gid: 0, mtime: 0 },
        type === 'g'
          ? { uid: 0, gid: 0, mtime: 0 }
          : { uid: 123, gid: 456, mtime: 100 },
      ]);
    }
  );

  it('preserves global PAX overrides after a local override', async () => {
    const global = new Blob([
      '19 path=global.txt\n26 linkpath=global-target\n10 uid=42\n',
    ]);
    const local = new Blob([
      '18 path=local.txt\n25 linkpath=local-target\n8 uid=0\n',
    ]);
    const archive = Buffer.from(
      await new Response(
        iterableToStream(
          tar([
            TarFile.from(global.stream(), 'global', { size: global.size }),
            TarFile.from(new Blob([]).stream(), 'first.txt', { size: 0 }),
            TarFile.from(local.stream(), 'local', { size: local.size }),
            TarFile.from(new Blob([]).stream(), 'second.txt', { size: 0 }),
            TarFile.from(new Blob([]).stream(), 'third.txt', { size: 0 }),
          ])
        )
      ).arrayBuffer()
    );
    for (const [offset, type] of [
      [0, 'g'],
      [1024, '2'],
      [1536, 'x'],
      [2560, '2'],
      [3072, '2'],
    ] as const) {
      const header = archive.subarray(offset, offset + 512);
      header[156] = type.charCodeAt(0);
      patchHeader(header);
    }

    const entries: { name: string; linkname: string | null; uid: number }[] =
      [];
    for await (const entry of untar(new Blob([archive]).stream())) {
      entries.push({
        name: entry.name,
        linkname: entry.linkname,
        uid: entry.uid,
      });
      await entry.text();
    }
    expect(entries).toEqual([
      { name: 'global.txt', linkname: 'global-target', uid: 42 },
      { name: 'local.txt', linkname: 'local-target', uid: 0 },
      { name: 'global.txt', linkname: 'global-target', uid: 42 },
    ]);
  });

  async function makeV7Archive() {
    const archive = Buffer.from(
      await new Response(
        iterableToStream(
          tar([
            TarFile.from(new Blob(['x']).stream(), 'file.txt', {
              size: 1,
              lastModified: 100_000,
            }),
          ])
        )
      ).arrayBuffer()
    );
    archive[156] = 0;
    archive.fill(0, 257, 512);
    patchHeader(archive.subarray(0, 512));
    return archive;
  }

  it.each([false, true])(
    'reads V7 entries without interpreting padding as ustar fields (padding: %s)',
    async padding => {
      const archive = await makeV7Archive();
      if (padding) {
        archive.write('ignored-user', 265, 'ascii');
        archive.write('ignored-group', 297, 'ascii');
        archive.write('0000001\0', 329, 'ascii');
        archive.write('0000002\0', 337, 'ascii');
        archive.write('ignored-prefix', 345, 'ascii');
        patchHeader(archive.subarray(0, 512));
      }
      const entries: Record<string, unknown>[] = [];
      for await (const entry of untar(new Blob([archive]).stream())) {
        entries.push({
          name: entry.name,
          mode: entry.mode,
          size: entry.size,
          mtime: entry.mtime,
          uname: entry.uname,
          gname: entry.gname,
          devmajor: entry.devmajor,
          devminor: entry.devminor,
          text: await entry.text(),
        });
      }
      expect(entries).toEqual([
        {
          name: 'file.txt',
          mode: 0o644,
          size: 1,
          mtime: 100,
          uname: null,
          gname: null,
          devmajor: 0,
          devminor: 0,
          text: 'x',
        },
      ]);
    }
  );

  it.each(['corrupted', 'minimum checksum'])(
    'rejects V7 headers with an invalid checksum (%s)',
    async kind => {
      const archive = await makeV7Archive();
      if (kind === 'corrupted') archive[0] ^= 1;
      else archive.write('000400\0 ', 148, 'ascii');
      await expect(async () => {
        for await (const entry of untar(new Blob([archive]).stream())) {
          await entry.text();
        }
      }).rejects.toThrow();
    }
  );

  it('recognizes zero blocks as the end of an empty archive', async () => {
    const entries: string[] = [];
    for await (const entry of untar(
      new Blob([new Uint8Array(1024)]).stream()
    )) {
      entries.push(entry.name);
    }
    expect(entries).toEqual([]);
  });

  it.each([
    ['x', 'L', false],
    ['x', 'L', true],
    ['x', 'K', false],
    ['x', 'K', true],
    ['g', 'L', false],
    ['g', 'L', true],
    ['g', 'K', false],
    ['g', 'K', true],
  ] as const)(
    'prefers %s PAX over GNU %s (PAX first: %s)',
    async (paxType, longType, paxFirst) => {
      const pax = new Blob([
        longType === 'L' ? '17 path=from-pax\n' : '21 linkpath=from-pax\n',
      ]);
      const long = new Blob(['from-gnu\0']);
      const extensions = [
        TarFile.from(pax.stream(), 'pax', { size: pax.size }),
        TarFile.from(long.stream(), 'long', { size: long.size }),
      ];
      if (!paxFirst) extensions.reverse();
      const archive = Buffer.from(
        await new Response(
          iterableToStream(
            tar([
              ...extensions,
              TarFile.from(new Blob([]).stream(), 'base.txt', { size: 0 }),
            ])
          )
        ).arrayBuffer()
      );
      for (const [offset, type] of [
        [0, paxFirst ? paxType : longType],
        [1024, paxFirst ? longType : paxType],
        [2048, longType === 'K' ? '2' : '0'],
      ] as const) {
        const header = archive.subarray(offset, offset + 512);
        header[156] = type.charCodeAt(0);
        patchHeader(header);
      }
      const entries: { name: string; linkname: string | null }[] = [];
      for await (const entry of untar(new Blob([archive]).stream())) {
        entries.push({ name: entry.name, linkname: entry.linkname });
        await entry.text();
      }
      expect(entries).toEqual([
        {
          name: longType === 'L' ? 'from-pax' : 'base.txt',
          linkname: longType === 'K' ? 'from-pax' : null,
        },
      ]);
    }
  );

  it.each([
    [false, false],
    [false, true],
    [true, false],
    [true, true],
  ])(
    'ignores directory sizes (PAX: %s, skip directory: %s)',
    async (paxSize, skipDirectory) => {
      const pax = new Blob(['12 size=512\n']);
      const archive = Buffer.from(
        await new Response(
          iterableToStream(
            tar([
              ...(paxSize
                ? [TarFile.from(pax.stream(), 'pax', { size: pax.size })]
                : []),
              TarFile.from(new Blob([]).stream(), 'dir/', { size: 0 }),
              TarFile.from(new Blob(['x']).stream(), 'kept.txt', { size: 1 }),
            ])
          )
        ).arrayBuffer()
      );
      if (paxSize) {
        archive[156] = 'x'.charCodeAt(0);
        patchHeader(archive.subarray(0, 512));
      }
      const offset = paxSize ? 1024 : 0;
      const directory = archive.subarray(offset, offset + 512);
      directory[156] = TarTypeFlag.DIRECTORY;
      if (!paxSize) directory.write('00000001000\0', 124, 'ascii');
      patchHeader(directory);

      const entries: { name: string; size: number; text?: string }[] = [];
      for await (const entry of untar(new Blob([archive]).stream())) {
        entries.push({
          name: entry.name,
          size: entry.size,
          ...(skipDirectory && entry.typeflag === TarTypeFlag.DIRECTORY
            ? {}
            : { text: await entry.text() }),
        });
      }
      expect(entries).toEqual([
        { name: 'dir/', size: 0, ...(skipDirectory ? {} : { text: '' }) },
        { name: 'kept.txt', size: 1, text: 'x' },
      ]);
    }
  );

  describe.each(['base', 'pax', 'gnu'])(
    'trailing slash in %s names',
    source => {
      it.each([
        [0, 'dir/', TarTypeFlag.DIRECTORY],
        [48, 'dir/', TarTypeFlag.DIRECTORY],
        [0, 'file.txt', TarTypeFlag.FILE],
        [48, 'file.txt', TarTypeFlag.FILE],
      ])('decodes flag %s and name %s', async (type, name, expectedType) => {
        const payload = new Blob([
          source === 'pax'
            ? name === 'dir/'
              ? '13 path=dir/\n'
              : '17 path=file.txt\n'
            : name + '\0',
        ]);
        const archive = Buffer.from(
          await new Response(
            iterableToStream(
              tar([
                ...(source === 'base'
                  ? []
                  : [
                      TarFile.from(payload.stream(), 'extension', {
                        size: payload.size,
                      }),
                    ]),
                TarFile.from(
                  new Blob([]).stream(),
                  source === 'base'
                    ? name
                    : name === 'dir/'
                      ? 'file.txt'
                      : 'dir/',
                  { size: 0 }
                ),
              ])
            )
          ).arrayBuffer()
        );
        if (source !== 'base') {
          archive[156] = (source === 'pax' ? 'x' : 'L').charCodeAt(0);
          patchHeader(archive.subarray(0, 512));
        }
        const offset = source === 'base' ? 0 : 1024;
        archive[offset + 156] = type;
        patchHeader(archive.subarray(offset, offset + 512));
        const entries: { name: string; type: number }[] = [];
        for await (const entry of untar(new Blob([archive]).stream())) {
          entries.push({ name: entry.name, type: entry.typeflag });
          await entry.text();
        }
        expect(entries).toEqual([{ name, type: expectedType }]);
      });
    }
  );

  it('respects a zero size override in a PAX header', async () => {
    const pax = new Blob(['10 size=0\n']);
    const archive = Buffer.from(
      await new Response(
        iterableToStream(
          tar([
            TarFile.from(pax.stream(), 'pax', { size: pax.size }),
            TarFile.from(new Blob([]).stream(), 'empty.txt', { size: 0 }),
          ])
        )
      ).arrayBuffer()
    );

    archive[156] = 'x'.charCodeAt(0);
    patchHeader(archive.subarray(0, 512));
    const header = archive.subarray(1024, 1536);
    header.fill(0, 124, 136);
    header.write('00000000001\0', 124, 'ascii');
    patchHeader(header);

    const entries: { size: number; text: string }[] = [];
    for await (const entry of untar(new Blob([archive]).stream())) {
      entries.push({ size: entry.size, text: await entry.text() });
    }

    expect(entries).toEqual([{ size: 0, text: '' }]);
  });

  it('rejects an invalid PAX size override', async () => {
    const pax = new Blob(['11 size=-1\n']);
    const archive = Buffer.from(
      await new Response(
        iterableToStream(
          tar([
            TarFile.from(pax.stream(), 'pax', { size: pax.size }),
            TarFile.from(new Blob([]).stream(), 'empty.txt', { size: 0 }),
          ])
        )
      ).arrayBuffer()
    );
    archive[156] = 'x'.charCodeAt(0);
    patchHeader(archive.subarray(0, 512));

    await expect(async () => {
      for await (const entry of untar(new Blob([archive]).stream())) {
        await entry.text();
      }
    }).rejects.toThrow(/size/i);
  });

  it('applies a global PAX size override to every following entry', async () => {
    const pax = new Blob(['10 size=1\n']);
    const archive = Buffer.from(
      await new Response(
        iterableToStream(
          tar([
            TarFile.from(pax.stream(), 'pax', { size: pax.size }),
            TarFile.from(new Blob(['a']).stream(), 'first.txt', { size: 1 }),
            TarFile.from(new Blob(['b']).stream(), 'second.txt', { size: 1 }),
          ])
        )
      ).arrayBuffer()
    );

    archive[156] = 'g'.charCodeAt(0);
    patchHeader(archive.subarray(0, 512));
    for (const offset of [1024, 2048]) {
      const header = archive.subarray(offset, offset + 512);
      header.fill(0, 124, 136);
      patchHeader(header);
    }

    const entries: { size: number; text: string }[] = [];
    for await (const entry of untar(new Blob([archive]).stream())) {
      entries.push({ size: entry.size, text: await entry.text() });
    }

    expect(entries).toEqual([
      { size: 1, text: 'a' },
      { size: 1, text: 'b' },
    ]);
  });

  it('decodes multibyte PAX records using byte lengths', async () => {
    const placeholder = 'x'.repeat(360);
    const name = '界'.repeat(120);
    const archive = Buffer.from(
      await new Response(
        iterableToStream(
          tar([TarFile.from(new Blob([]).stream(), placeholder, { size: 0 })])
        )
      ).arrayBuffer()
    );
    const placeholderBytes = Buffer.from(placeholder);
    const nameBytes = Buffer.from(name);
    const pathOffset = archive.indexOf(placeholderBytes, 512);
    expect(pathOffset).toBeGreaterThan(512);
    expect(nameBytes.byteLength).toBe(placeholderBytes.byteLength);
    nameBytes.copy(archive, pathOffset);

    const entries: string[] = [];
    for await (const entry of untar(new Blob([archive]).stream())) {
      entries.push(entry.name);
      await entry.text();
    }

    expect(entries).toEqual([name]);
  });

  it('extract a tarball successfully (unchunked)', async () => {
    const entries: any[] = [];

    const deflate = untar(openTarball() as any);
    for await (const entry of deflate) {
      entries.push({
        name: entry.name,
        size: entry.size,
        text: await entry.text(),
      });
    }

    expect(entries).toMatchSnapshot();
  });

  it('extract a tarball successfully (chunked)', async () => {
    const entries: any[] = [];

    const deflate = untar(chunk(openTarball()) as any);
    for await (const entry of deflate) {
      entries.push({
        name: entry.name,
        size: entry.size,
        text: await entry.text(),
      });
    }

    expect(entries).toMatchSnapshot();
  });

  it('extracts a tar-stream tarball successfully', async () => {
    const tar = makeTarball([
      {
        name: '__main.js',
        data: '/*entrypoint*/',
      },
      {
        name: '__node_compat.js',
        data: '/*node_compat*/',
      },
      {
        name: 'manifest.json',
        data: JSON.stringify({
          env: { TEST_ENV: 'TEST_ENV_VALUE' },
        }),
      },
      {
        name: 'assets.json',
        data: JSON.stringify({
          'favicon.ico': 'hash',
        }),
      },
      {
        name: 'client/robots.txt',
        data: '#robots.txt',
      },
      {
        name: 'server/server.html',
        data: '<!DOCTYPE html>',
      },
    ]);

    const entries: any[] = [];
    const deflate = untar(tar as any);
    for await (const entry of deflate) {
      entries.push({
        name: entry.name,
        size: entry.size,
        text: await entry.text(),
      });
    }

    expect(entries).toMatchSnapshot();
  });

  it('extracts a tar-stream tarball successfully when skipping every other file', async () => {
    const tar = makeTarball([
      {
        name: '__main.js',
        data: '/*entrypoint*/',
      },
      {
        name: '__node_compat.js',
        data: '/*node_compat*/',
      },
      {
        name: 'manifest.json',
        data: JSON.stringify({
          env: { TEST_ENV: 'TEST_ENV_VALUE' },
        }),
      },
      {
        name: 'assets.json',
        data: JSON.stringify({
          'favicon.ico': 'hash',
        }),
      },
      {
        name: 'client/robots.txt',
        data: '#robots.txt',
      },
      {
        name: 'server/server.html',
        data: '<!DOCTYPE html>',
      },
    ]);

    const entries: any[] = [];
    const deflate = untar(tar as any);
    let skip = false;
    for await (const entry of deflate) {
      if (skip) {
        await entry.stream().cancel();
        skip = false;
      } else {
        entries.push({
          name: entry.name,
          size: entry.size,
          text: await entry.text(),
        });
        skip = true;
      }
    }

    expect(entries).toMatchSnapshot();
  });

  it('extracts every entry when skipped entries are not read', async () => {
    const names: string[] = [];
    const contents: string[] = [];

    for await (const entry of untar(chunk(makeTarball(cancellationFiles)))) {
      names.push(entry.name);
      if (entry.name.startsWith('kept/')) contents.push(await entry.text());
    }

    expect(names).toEqual(cancellationFileNames);
    expect(contents).toEqual(['second entry', 'fourth entry', 'sixth entry']);
  });

  it('extracts every entry when a skipped entry has a read in flight', async () => {
    const names: string[] = [];
    const contents: string[] = [];

    for await (const entry of untar(chunk(makeTarball(cancellationFiles)))) {
      names.push(entry.name);
      if (entry.name.startsWith('dropped/')) {
        void entry.stream().getReader().read();
      } else {
        contents.push(await entry.text());
      }
    }

    expect(names).toEqual(cancellationFileNames);
    expect(contents).toEqual(['second entry', 'fourth entry', 'sixth entry']);
  });

  it('extracts every entry when a partially read entry is abandoned', async () => {
    const names: string[] = [];
    const contents: string[] = [];

    for await (const entry of untar(chunk(makeTarball(cancellationFiles)))) {
      names.push(entry.name);
      if (entry.name.startsWith('dropped/')) {
        await entry.stream().getReader().read();
      } else {
        contents.push(await entry.text());
      }
    }

    expect(names).toEqual(cancellationFileNames);
    expect(contents).toEqual(['second entry', 'fourth entry', 'sixth entry']);
  });

  it('extracts an abandoned tiny entry with uneven input chunks', async () => {
    const files: TestFile[] = [
      { name: 'dropped/tiny.bin', data: Buffer.alloc(8, 1) },
      { name: 'kept/second.txt', data: 'second entry' },
    ];
    const names: string[] = [];

    for await (const entry of untar(chunk(makeTarball(files)))) {
      names.push(entry.name);
      if (entry.name.startsWith('dropped/')) {
        await entry.stream().getReader().read();
      } else {
        await entry.text();
      }
    }

    expect(names).toEqual(['dropped/tiny.bin', 'kept/second.txt']);
  });

  it('extracts an abandoned tiny entry with block-sized input chunks', async () => {
    const files: TestFile[] = [
      { name: 'dropped/tiny.bin', data: Buffer.alloc(8, 1) },
      { name: 'kept/second.txt', data: 'second entry' },
    ];
    const names: string[] = [];

    for await (const entry of untar(chunk(makeTarball(files), 512))) {
      names.push(entry.name);
      if (entry.name.startsWith('dropped/')) {
        await entry.stream().getReader().read();
      } else {
        await entry.text();
      }
    }

    expect(names).toEqual(['dropped/tiny.bin', 'kept/second.txt']);
  });

  it('cancels an entry whose pull is waiting for input', async () => {
    const chunks: Uint8Array[] = [];
    const tarball = makeTarball(cancellationFiles).getReader();
    for (;;) {
      const result = await tarball.read();
      if (result.done) break;
      for (let idx = 0; idx < result.value.length; idx += 500) {
        chunks.push(result.value.subarray(idx, idx + 500));
      }
    }

    const gated = gatedStream(chunks);
    const names: string[] = [];
    const parsing = (async () => {
      for await (const entry of untar(gated.stream)) {
        names.push(entry.name);
        if (entry.name.startsWith('dropped/')) {
          void entry.stream().getReader().read();
        } else {
          await entry.text();
        }
      }
    })();

    for (let idx = 0; idx <= chunks.length + 1; idx++) {
      gated.release();
      await new Promise(resolve => setTimeout(resolve, 0));
    }

    await expect(parsing).resolves.toBeUndefined();
    expect(names).toEqual(cancellationFileNames);
  });

  it('handles long names in PAX headers', async () => {
    const tar = makeTarball([
      {
        name: `${'a'.repeat(200)}.txt`,
        data: '/*entrypoint*/',
      },
      {
        name: `${'b'.repeat(400)}.txt`,
        data: '/*entrypoint*/',
      },
      {
        name: `${'c'.repeat(600)}.txt`,
        data: '/*entrypoint*/',
      },
    ]);

    const entries: any[] = [];
    const deflate = untar(tar as any);
    for await (const entry of deflate) {
      entries.push({
        name: entry.name,
        size: entry.size,
        text: await entry.text(),
      });
    }

    expect(entries).toMatchSnapshot();
  });
});

describe('fixtures', () => {
  async function getEntries(relativePath: string) {
    const entries: any[] = [];
    const tarball = Readable.toWeb(
      fs.createReadStream(path.join(__dirname, relativePath))
    );
    const deflate = untar(tarball as any);
    for await (const entry of deflate) {
      entries.push({
        name: entry.name,
        size: entry.size,
        mode: entry.mode,
        uid: entry.uid,
        gid: entry.gid,
        mtime: entry.mtime,
        typeflag: entry.typeflag,
        linkname: entry.linkname,
        uname: entry.uname,
        gname: entry.gname,
        devmajor: entry.devmajor,
        devminor: entry.devminor,
        text: await entry.text(),
      });
    }
    return entries;
  }

  it('bad checksum', async () => {
    // We ignore bad checksums on valid entries
    expect(await getEntries('./fixtures/tar/bad-cksum.tar')).toMatchSnapshot();
  });

  it('body byte counts', async () => {
    const entries = await getEntries('./fixtures/tar/body-byte-counts.tar');
    expect(entries).toMatchSnapshot();
    expect(entries[0]).toMatchObject({
      name: '1024-bytes.txt',
      size: 1024,
      text: expect.stringMatching(/^x{1023}\n$/),
    });
    expect(entries[1]).toMatchObject({
      name: '512-bytes.txt',
      size: 512,
      text: expect.stringMatching(/^x{511}\n$/),
    });
    expect(entries[2]).toMatchObject({
      name: 'one-byte.txt',
      size: 1,
      text: 'a',
    });
    expect(entries[3]).toMatchObject({
      name: 'zero-byte.txt',
      size: 0,
      text: '',
    });
  });

  it('single directory', async () => {
    const entries = await getEntries('./fixtures/tar/dir.tar');
    expect(entries).toMatchSnapshot();
    expect(entries[0]).toMatchObject({
      typeflag: TarTypeFlag.DIRECTORY,
      name: 'dir/',
      size: 0,
    });
  });

  it('single file', async () => {
    const entries = await getEntries('./fixtures/tar/file.tar');
    expect(entries).toMatchInlineSnapshot(`
      [
        {
          "devmajor": 0,
          "devminor": 0,
          "gid": 20,
          "gname": "staff",
          "linkname": null,
          "mode": 420,
          "mtime": 1491843500,
          "name": "one-byte.txt",
          "size": 1,
          "text": "a",
          "typeflag": 48,
          "uid": 501,
          "uname": "isaacs",
        },
      ]
    `);
  });

  it('empty PAX', async () => {
    const entries = await getEntries('./fixtures/tar/emptypax.tar');
    expect(entries).toMatchSnapshot();
  });

  it('global header', async () => {
    const entries = await getEntries('./fixtures/tar/global-header.tar');
    expect(entries).toMatchSnapshot();
  });

  it('links invalid', async () => {
    const entries = await getEntries('./fixtures/tar/links-invalid.tar');
    expect(entries).toMatchSnapshot();
    expect(entries[1]).toMatchObject({
      name: 'hardlink-2',
      typeflag: TarTypeFlag.LINK,
      linkname: null,
    });
    expect(entries[2]).toMatchObject({
      name: 'symlink',
      typeflag: TarTypeFlag.SYMLINK,
      linkname: 'hardlink-2',
    });
  });

  it('links strip', async () => {
    const entries = await getEntries('./fixtures/tar/links-strip.tar');
    expect(entries).toMatchSnapshot();
  });

  it('links', async () => {
    const entries = await getEntries('./fixtures/tar/links.tar');
    expect(entries).toMatchSnapshot();
  });

  it('long paths', async () => {
    const entries = await getEntries('./fixtures/tar/long-paths.tar');
    expect(entries).toMatchSnapshot();
  });

  it('long PAX', async () => {
    const entries = await getEntries('./fixtures/tar/long-pax.tar');
    expect(entries).toMatchSnapshot();
    expect(entries[0]).toMatchObject({
      name: '120-byte-filename-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
    });
  });

  it('long names', async () => {
    const entries = await getEntries('./fixtures/tar/next-file-has-long.tar');
    expect(entries).toMatchSnapshot();
  });

  it('GNU long name spanning multiple blocks', async () => {
    const entries = await getEntries('./fixtures/tar/gnu-long-name.tar');
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      name: 'dir/' + 'a'.repeat(600) + '.txt',
      size: 5,
      text: 'hello',
    });
  });

  it('null byte', async () => {
    const entries = await getEntries('./fixtures/tar/null-byte.tar');
    expect(entries).toMatchSnapshot();
  });

  it('path missing', async () => {
    const entries = await getEntries('./fixtures/tar/path-missing.tar');
    expect(entries).toMatchInlineSnapshot(`
      [
        {
          "devmajor": 0,
          "devminor": 0,
          "gid": 20,
          "gname": "staff",
          "linkname": null,
          "mode": 420,
          "mtime": 1491843500,
          "name": "",
          "size": 1,
          "text": "a",
          "typeflag": 48,
          "uid": 501,
          "uname": "isaacs",
        },
      ]
    `);
  });

  it('trailing slash corner case', async () => {
    const entries = await getEntries(
      './fixtures/tar/trailing-slash-corner-case.tar'
    );
    expect(entries).toMatchSnapshot();
  });

  it('utf8', async () => {
    const entries = await getEntries('./fixtures/tar/utf8.tar');
    expect(entries).toMatchSnapshot();
  });
});
