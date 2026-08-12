import { describe, it, expect } from 'vitest';
import { parseMultipart } from '../multipartInput';
import { iterableToStream } from './utils';
import { MultipartPart } from '../multipartShared';
import * as multipartOutput from '../multipartOutput';
import * as multipartUtils from './utils/multipartUtils';

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

describe('parseMultipart', () => {
  it('releases the input stream when parsing fails', async () => {
    const body = new Blob([]).stream();

    await expect(async () => {
      for await (const _entry of parseMultipart(body, {
        contentType: 'multipart/form-data; boundary=test',
      })) {
        // noop
      }
    }).rejects.toThrow();

    expect(body.locked).toBe(false);
  });

  it('releases the input stream when iteration stops early', async () => {
    const form = new FormData();
    form.set('first', 'first');
    form.set('second', 'second');
    const request = new Request('http://test.com', {
      method: 'POST',
      body: form as any,
    });
    const body = request.body!;

    const entries = parseMultipart(body, {
      contentType: request.headers.get('Content-Type')!,
    });
    for await (const _entry of entries) break;

    expect(body.locked).toBe(false);
  it('rejects a single header over the 16kB byte limit', async () => {
    const boundary = 'test-boundary';
    const body = [
      `--${boundary}`,
      'Content-Disposition: form-data; name="test"',
      `X-Large: ${'界'.repeat(6_000)}`,
      '',
      'content',
      `--${boundary}--`,
      '',
    ].join('\r\n');

    const entries = parseMultipart(new Blob([body]).stream(), {
      contentType: `multipart/form-data; boundary=${boundary}`,
    });
    await expect(async () => {
      for await (const entry of entries) await entry.text();
    }).rejects.toThrow(/maximum length/);
  });

  it('decodes a multibyte header split across input blocks', async () => {
    const boundary = 'test-boundary';
    const value = '界'.repeat(2_000);
    const body = [
      `--${boundary}`,
      'Content-Disposition: form-data; name="test"',
      `X-Multibyte: ${value}`,
      '',
      'content',
      `--${boundary}--`,
      '',
    ].join('\r\n');

    const entries = parseMultipart(new Blob([body]).stream(), {
      contentType: `multipart/form-data; boundary=${boundary}`,
    });
    for await (const entry of entries) {
      expect(entry.headers['x-multibyte']).toBe(value);
      await expect(entry.text()).resolves.toBe('content');
    }
  });

  it('extracts a file from a multipart body successfully (unchunked)', async () => {
    const form = new FormData();
    form.set('filename-a.txt', new File(['test content a'], 'filename-a.txt'));
    form.set('filename-b.txt', new File(['test content b'], 'filename-b.txt'));
    const request = new Request('http://test.com', {
      method: 'POST',
      body: form as any,
    });

    const entries: any[] = [];
    const contentType = request.headers.get('Content-Type')!;
    const multipart = parseMultipart(request.body!, { contentType });
    for await (const entry of multipart) {
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();
  });

  it('extracts a file from a multipart body successfully (chunked)', async () => {
    const form = new FormData();
    form.set(
      'filename-a.txt',
      new File(['test content a'], 'filename-a.txt', { type: 'plain/text' })
    );
    form.set('filename-b.txt', new File(['test content b'], 'filename-b.txt'));
    const request = new Request('http://test.com', {
      method: 'POST',
      body: form as any,
    });

    const entries: any[] = [];
    const contentType = request.headers.get('Content-Type')!;
    const multipart = parseMultipart(chunk(request.body!), { contentType });
    for await (const entry of multipart) {
      entries.push({
        name: entry.name,
        size: entry.size,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();
  });

  it('extracts a file from a multipart body successfully if the first file is skipped (unchunked)', async () => {
    const form = new FormData();
    form.set('filename-a.txt', new File(['test content a'], 'filename-a.txt'));
    form.set('filename-b.txt', new File(['test content b'], 'filename-b.txt'));
    const request = new Request('http://test.com', {
      method: 'POST',
      body: form as any,
    });

    const entries: any[] = [];
    const contentType = request.headers.get('Content-Type')!;
    const multipart = parseMultipart(request.body!, { contentType });
    for await (const entry of multipart) {
      if (entry.name === 'filename-a.txt') {
        await entry.stream().cancel();
        continue;
      }
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();
  });

  it('extracts a file from a multipart body successfully if the first file is skipped (chunked)', async () => {
    const form = new FormData();
    form.set('filename-a.txt', new File(['test content a'], 'filename-a.txt'));
    form.set('filename-b.txt', new File(['test content b'], 'filename-b.txt'));
    const request = new Request('http://test.com', {
      method: 'POST',
      body: form as any,
    });

    const entries: any[] = [];
    const contentType = request.headers.get('Content-Type')!;
    const multipart = parseMultipart(chunk(request.body!), { contentType });
    for await (const entry of multipart) {
      if (entry.name === 'filename-a.txt') {
        await entry.stream().cancel();
        continue;
      }
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();
  });

  it('extracts special characters in filename successfully', async () => {
    const filename = 'newline\nfi+l en"am👍e.txt';
    const form = new FormData();
    form.set(filename, new File(['test content a'], filename));
    const request = new Request('http://test.com', {
      method: 'POST',
      body: form as any,
    });

    const contentType = request.headers.get('Content-Type')!;
    const multipart = parseMultipart(chunk(request.body!), { contentType });
    for await (const entry of multipart) {
      expect(entry.name).toBe(filename);
    }
  });

  it('extracts "multipartOutput" output successfully', async () => {
    const data = new Uint8Array(10 * 1024).fill('0'.charCodeAt(0));

    const body = iterableToStream(
      multipartOutput.streamMultipart(
        (async function* () {
          yield [
            'filename-a',
            new File([data], 'filename-a.txt', {
              type: 'plain/text',
            }),
          ];
          yield ['filename-b', new File(['test content b'], 'filename-b.txt')];
        })()
      )
    );

    const entries: any[] = [];
    const multipart = parseMultipart(body, {
      contentType: multipartOutput.multipartContentType,
    });
    for await (const entry of multipart) {
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();

    expect(entries[0].size).toBe(data.byteLength);
    expect(entries[0].text.length).toBe(data.byteLength);
  });

  it('extracts "multipartOutput" output successfully if the first file is skipped', async () => {
    const body = iterableToStream(
      multipartOutput.streamMultipart(
        (async function* () {
          yield [
            'filename-a.txt',
            new File(['test content a'], 'filename-a.txt', {
              type: 'plain/text',
            }),
          ];
          yield [
            'filename-b.txt',
            new File(['test content b'], 'filename-b.txt'),
          ];
        })()
      )
    );

    const entries: any[] = [];
    const multipart = parseMultipart(body, {
      contentType: multipartOutput.multipartContentType,
    });
    for await (const entry of multipart) {
      if (entry.name === 'filename-a.txt') {
        await entry.stream().cancel();
        continue;
      }
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();
  });

  it('extracts "multipartOutput" output entries successfully if the first entry is skipped', async () => {
    const body = iterableToStream(
      multipartOutput.streamMultipart(
        (async function* () {
          yield ['a', 'test-content-a'];
          yield ['b', 'test-content-b'];
        })()
      )
    );

    const entries: any[] = [];
    const multipart = parseMultipart(body, {
      contentType: multipartOutput.multipartContentType,
    });
    for await (const entry of multipart) {
      if (entry.name === 'a') {
        await entry.stream().cancel();
        continue;
      }
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();
  });

  it('extracts "multipartOutput" stream with custom headers', async () => {
    const body = iterableToStream(
      multipartOutput.streamMultipart(
        (async function* () {
          yield [
            'filename-a.txt',
            new MultipartPart(['1'], '1', {
              headers: { 'custom-signature': '123' },
            }),
          ];
        })()
      )
    );

    const entries: any[] = [];
    const multipart = parseMultipart(body, {
      contentType: multipartOutput.multipartContentType,
    });
    for await (const entry of multipart) {
      expect(entry.headers['custom-signature']).toBe('123');
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        headers: entry.headers,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();
  });

  it('extracts "multipartUtils" stream', async () => {
    const data = new Uint8Array(10 * 1024).fill('0'.charCodeAt(0));

    const body = iterableToStream(
      multipartUtils.createMultipartBodyFromFilesAsync([new File([data], 'x')])
    );

    const entries: any[] = [];
    const multipart = parseMultipart(body, {
      contentType: multipartUtils.multipartContentType,
    });
    for await (const entry of multipart) {
      entries.push({
        name: entry.name,
        size: entry.size,
        type: entry.type,
        headers: entry.headers,
        text: await entry.text(),
      });
    }
    expect(entries).toMatchSnapshot();

    expect(entries[0].size).toBe(data.byteLength);
    expect(entries[0].text.length).toBe(data.byteLength);
  });
});
