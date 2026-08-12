import { describe, it, expect, vi } from 'vitest';
import { streamMultipart } from '../multipartOutput';
import { iterableToStream, streamToText } from './utils';
import { MultipartPart } from '../multipartShared';

vi.mock('../multipartEncoding', async importOriginal => ({
  ...(await importOriginal()),
  BOUNDARY_ID: '----formdata-multitars',
}));

describe('streamMultipart', () => {
  it('releases its input stream when iteration stops early', async () => {
    let cancelled = false;
    const entries = new ReadableStream<readonly [string, string]>({
      pull(controller) {
        controller.enqueue(['name', 'value']);
      },
      cancel() {
        cancelled = true;
      },
    });

    for await (const _chunk of streamMultipart(entries)) break;

    expect(entries.locked).toBe(false);
    expect(cancelled).toBe(true);
  });

  it('creates a multipart stream of strings', async () => {
    const data = streamMultipart(
      (async function* () {
        yield ['a', '1'];
        yield ['b', '2'];
      })()
    );

    expect(await streamToText(iterableToStream(data))).toMatchInlineSnapshot(`
      "------formdata-multitars
      Content-Disposition: form-data; name="a"

      1
      ------formdata-multitars
      Content-Disposition: form-data; name="b"

      2
      ------formdata-multitars--

      "
    `);
  });

  it('creates a multipart stream of files', async () => {
    const data = streamMultipart(
      (async function* () {
        yield ['a', new File(['1'], '1.txt')];
        yield ['b', new File(['2'], '2.txt')];
      })()
    );

    expect(await streamToText(iterableToStream(data))).toMatchInlineSnapshot(`
      "------formdata-multitars
      Content-Disposition: form-data; name="a"; filename="1.txt"
      Content-Length: 1

      1
      ------formdata-multitars
      Content-Disposition: form-data; name="b"; filename="2.txt"
      Content-Length: 1

      2
      ------formdata-multitars--

      "
    `);
  });

  it('creates a multipart stream of files with content types', async () => {
    const data = streamMultipart(
      (async function* () {
        yield ['a', new File(['1'], '1.txt', { type: 'text/plain' })];
        yield ['b', new File(['2'], '2.txt', { type: 'test/plain' })];
      })()
    );

    expect(await streamToText(iterableToStream(data))).toMatchInlineSnapshot(`
      "------formdata-multitars
      Content-Disposition: form-data; name="a"; filename="1.txt"
      Content-Type: text/plain
      Content-Length: 1

      1
      ------formdata-multitars
      Content-Disposition: form-data; name="b"; filename="2.txt"
      Content-Type: test/plain
      Content-Length: 1

      2
      ------formdata-multitars--

      "
    `);
  });

  it('encodes special filenames', async () => {
    const filename = 'newline\nfi+l en"am👍e.txt';
    const data = streamMultipart(
      (async function* () {
        yield [filename, new File(['1'], filename)];
      })()
    );

    expect(await streamToText(iterableToStream(data))).toMatchInlineSnapshot(`
      "------formdata-multitars
      Content-Disposition: form-data; name="newline%0Afi+l en%22am👍e.txt"; filename="newline%0Afi+l en%22am👍e.txt"
      Content-Length: 1

      1
      ------formdata-multitars--

      "
    `);
  });

  it('adds custom header entries', async () => {
    const data = streamMultipart(
      (async function* () {
        const file = new MultipartPart(['1'], '1', {
          headers: { 'custom-signature': '123' },
        });
        yield ['1', file];
      })()
    );

    expect(await streamToText(iterableToStream(data))).toMatchInlineSnapshot(`
      "------formdata-multitars
      Content-Disposition: form-data; name="1"; filename="1"
      Content-Type: application/octet-stream
      custom-signature: 123

      1
      ------formdata-multitars--

      "
    `);
  });

  it('rejects a part body longer than its declared size', async () => {
    const part = new MultipartPart(['too long'], 'file.txt', { size: 1 });
    const data = streamMultipart([['file', part]]);

    await expect(streamToText(iterableToStream(data))).rejects.toThrow(/size/i);
  });

  it('rejects a part body shorter than its declared size', async () => {
    const part = new MultipartPart(['x'], 'file.txt', { size: 8 });
    const data = streamMultipart([['file', part]]);

    await expect(streamToText(iterableToStream(data))).rejects.toThrow(/size/i);
  });
});
