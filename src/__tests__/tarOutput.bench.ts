import { bench, describe } from 'vitest';

import * as multitars from './fixtures/lib';

const smallContent = new Blob([new Uint8Array(1_024)]);
const largeContent = new Blob([new Uint8Array(5 * 1_024 * 1_024)]);
const nestedNames = Array.from(
  { length: 100 },
  (_, index) => `${'directory/'.repeat(index % 20)}file-${index}-界.txt`
);

describe.each([
  { name: '2500 x 1KB', count: 2_500, content: smallContent },
  { name: '20 x 5MB', count: 20, content: largeContent },
  {
    name: '100 nested and UTF-8 names',
    names: nestedNames,
    content: smallContent,
  },
])('tar output ($name)', input => {
  bench('multitars', async () => {
    const entries = (function* () {
      const names = input.names;
      const count = names?.length ?? input.count!;
      for (let index = 0; index < count; index++) {
        yield multitars.TarFile.from(
          input.content.stream(),
          names?.[index] ?? `file-${index}.txt`,
          { size: input.content.size }
        );
      }
    })();

    let byteLength = 0;
    for await (const chunk of multitars.tar(entries)) {
      byteLength += chunk.byteLength;
    }
    if (!byteLength) throw new Error('Expected tar output');
  });
});
