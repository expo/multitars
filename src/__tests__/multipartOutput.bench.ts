import { bench, describe } from 'vitest';

import * as multitars from './fixtures/lib';

const smallContent = new Blob([new Uint8Array(1_024)]);
const largeContent = new Blob([new Uint8Array(5 * 1_024 * 1_024)]);

describe.each([
  { name: '2500 x 1KB', count: 2_500, content: smallContent },
  { name: '15 x 5MB', count: 15, content: largeContent },
])('multipart output ($name)', input => {
  bench('multitars', async () => {
    const entries = (function* (): Generator<multitars.FormEntry> {
      for (let index = 0; index < input.count; index++) {
        yield [`file-${index}.txt`, input.content];
      }
    })();

    let byteLength = 0;
    for await (const chunk of multitars.streamMultipart(entries)) {
      byteLength += chunk.byteLength;
    }
    if (!byteLength) throw new Error('Expected multipart output');
  });
});
