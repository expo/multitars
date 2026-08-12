import {
  DecompressionStream,
  ReadableStream,
  ReadableStreamDefaultReader,
} from 'node:stream/web';
import { pack } from 'tar-stream';
import * as path from 'node:path';
import { Readable } from 'node:stream';
import * as fs from 'node:fs';

import { describe, it, expect } from 'vitest';
import { TarTypeFlag } from '../tarShared';
import { untar } from '../tarInput';

const openTarball = () => {
  const tarball = Readable.toWeb(
    fs.createReadStream(path.join(__dirname, 'fixtures/worker-sample.tar.gz'))
  );
  return tarball.pipeThrough(new DecompressionStream('gzip'));
};

function chunk(
  readable: ReadableStream<Uint8Array>
): ReadableStream<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array>;
  return new ReadableStream({
    start() {
      reader = readable.getReader();
    },
    async pull(controller) {
      const { done, value } = await reader.read();
      if (done) return controller.close();
      for (let sliceIdx = 0; sliceIdx < value.length; sliceIdx += 500) {
        controller.enqueue(value.subarray(sliceIdx, sliceIdx + 500));
      }
    },
  });
}

interface TestFile {
  name: string;
  data: string;
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
