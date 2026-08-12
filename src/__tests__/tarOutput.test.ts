import { describe, it, expect } from 'vitest';

import { tar } from '../tarOutput';
import { untar } from '../tarInput';
import { TarFile, TarTypeFlag, initTarHeader } from '../tarShared';
import { iterableToStream, streamToBuffer } from './utils';

describe('tar', () => {
  it('releases its input stream when iteration stops early', async () => {
    let cancelled = false;
    const entries = new ReadableStream<TarFile>({
      pull(controller) {
        controller.enqueue(
          TarFile.from(new Blob([]).stream(), 'test.txt', { size: 0 })
        );
      },
      cancel() {
        cancelled = true;
      },
    });

    for await (const _chunk of tar(entries)) break;

    expect(entries.locked).toBe(false);
    expect(cancelled).toBe(true);
  });

  describe('tested via untar()', () => {
    it('compresses a single file readable by untar', async () => {
      const NOW = 1751629979000;

      const tarStream = tar(
        (async function* () {
          const file = new Blob(['hello world!']);
          yield TarFile.from(file.stream(), 'test-file.txt', {
            lastModified: NOW,
            size: file.size,
          });
        })()
      );

      const binaryOutput = await streamToBuffer(iterableToStream(tarStream));
      expect(Buffer.from(binaryOutput).toString('hex')).toMatchSnapshot();

      const entries: any[] = [];
      const blob = new Blob([binaryOutput]);
      for await (const entry of untar(blob.stream())) {
        entries.push({
          name: entry.name,
          size: entry.size,
          lastModified: entry.lastModified,
          text: await entry.text(),
        });
      }

      expect(entries).toEqual([
        {
          name: 'test-file.txt',
          size: 12,
          text: 'hello world!',
          lastModified: NOW,
        },
      ]);
    });

    it('compresses a single file with a longer name readable by untar', async () => {
      const NOW = Math.floor(new Date().valueOf() / 1000) * 1000;
      const NAME = `${'d'.repeat(100)}/${'x'.repeat(50)}.txt`;

      const tarStream = tar(
        (async function* () {
          const file = new Blob(['hello world!']);
          yield TarFile.from(file.stream(), NAME, {
            lastModified: NOW,
            size: file.size,
          });
        })()
      );

      const entries: any[] = [];
      for await (const entry of untar(iterableToStream(tarStream))) {
        entries.push({
          name: entry.name,
          size: entry.size,
          lastModified: entry.lastModified,
          text: await entry.text(),
        });
      }

      expect(entries).toEqual([
        {
          name: NAME,
          size: 12,
          text: 'hello world!',
          lastModified: NOW,
        },
      ]);
    });

    it('compresses a single file with a very long name readable by untar', async () => {
      const NOW = Math.floor(new Date().valueOf() / 1000) * 1000;
      const NAME = `${'d'.repeat(300)}/${'x'.repeat(200)}.txt`;

      const tarStream = tar(
        (async function* () {
          const file = new Blob(['hello world!']);
          yield TarFile.from(file.stream(), NAME, {
            lastModified: NOW,
            size: file.size,
          });
        })()
      );

      const entries: any[] = [];
      for await (const entry of untar(iterableToStream(tarStream))) {
        entries.push({
          name: entry.name,
          size: entry.size,
          lastModified: entry.lastModified,
          text: await entry.text(),
        });
      }

      expect(entries).toEqual([
        {
          name: NAME,
          size: 12,
          text: 'hello world!',
          lastModified: NOW,
        },
      ]);
    });

    it('compresses a long name starting with a slash readable by untar', async () => {
      // Names over 100 characters go through `indexOfPrefixEnd`, which walks backwards from slash
      // to slash. `lastIndexOf` clamps a negative position to 0, so a leading slash used to be
      // found over and over and `idx` never decreased: a synchronous infinite loop with no I/O,
      // which under a CPU limit surfaces as a killed request rather than an error.
      const NAME = `/${'a'.repeat(130)}`;

      const tarStream = tar(
        (async function* () {
          const file = new Blob(['hello world!']);
          yield TarFile.from(file.stream(), NAME, { size: file.size });
        })()
      );

      const entries: any[] = [];
      for await (const entry of untar(iterableToStream(tarStream))) {
        entries.push({
          name: entry.name,
          size: entry.size,
          text: await entry.text(),
        });
      }

      expect(entries).toEqual([{ name: NAME, size: 12, text: 'hello world!' }]);
    });

    it('compresses long names with no usable prefix split readable by untar', async () => {
      // The same walk also has to terminate when no split satisfies both the 155 byte prefix and
      // the 100 byte name limits, with or without a leading slash.
      const NAMES = [
        `/${'b'.repeat(160)}/${'c'.repeat(60)}`,
        `${'d'.repeat(160)}/${'e'.repeat(60)}`,
        `/${'f'.repeat(101)}`,
        `//${'g'.repeat(120)}`,
      ];

      const tarStream = tar(
        (async function* () {
          for (const name of NAMES) {
            const file = new Blob(['hello world!']);
            yield TarFile.from(file.stream(), name, { size: file.size });
          }
        })()
      );

      const names: string[] = [];
      for await (const entry of untar(iterableToStream(tarStream))) {
        names.push(entry.name);
      }

      expect(names).toEqual(NAMES);
    });
  });

  describe('encodeOctal field encoding', () => {
    async function getHeaderBytes(
      overrides: Partial<ReturnType<typeof initTarHeader>>
    ) {
      const header = initTarHeader(null);
      Object.assign(header, overrides);
      if (!header.name) header.name = 'x.txt';
      if (!header.typeflag) header.typeflag = TarTypeFlag.FILE;
      if (!header.mtime) header.mtime = 1;

      const tarStream = tar(
        (async function* () {
          yield new TarFile(new Blob([]).stream(), header);
        })()
      );
      const output = await streamToBuffer(iterableToStream(tarStream));
      return new Uint8Array(output);
    }

    function field(buf: Uint8Array, from: number, to: number) {
      return Buffer.from(buf.slice(from, to)).toString('ascii');
    }

    it('encodes a typical value with zero-padding, space, and NUL', async () => {
      const buf = await getHeaderBytes({ mode: 0o644 });
      expect(field(buf, 100, 108)).toBe('000644 \0');
    });

    it('encodes zero as all zero bytes', async () => {
      const buf = await getHeaderBytes({ uid: 0 });
      expect(field(buf, 108, 116)).toBe('\0\0\0\0\0\0\0\0');
    });

    it('encodes max 8-byte value without trailing space', async () => {
      const buf = await getHeaderBytes({ uid: 0o7777777 });
      expect(field(buf, 108, 116)).toBe('7777777\0');
    });

    it('encodes a 12-byte field with space and NUL', async () => {
      const buf = await getHeaderBytes({ size: 1024 });
      expect(field(buf, 124, 136)).toBe('0000002000 \0');
    });

    it('roundtrips max 8-byte value through untar', async () => {
      const buf = await getHeaderBytes({ uid: 0o7777777 });
      const blob = new Blob([buf]);
      for await (const entry of untar(blob.stream())) {
        expect(entry.uid).toBe(0o7777777);
      }
    });
  });
});
