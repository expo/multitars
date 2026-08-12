import { bench, describe } from 'vitest';
import { createTarPacker } from 'modern-tar';

import * as multitars from './fixtures/lib';

const smallContent = new Blob([new Uint8Array(1_024)]);
const largeContent = new Blob([new Uint8Array(5 * 1_024 * 1_024)]);
const nestedNames = Array.from(
  { length: 100 },
  (_, index) => `${'directory/'.repeat(index % 20)}file-${index}-界.txt`
);

async function consume(chunks: AsyncIterable<Uint8Array>) {
  let byteLength = 0;
  for await (const chunk of chunks) byteLength += chunk.byteLength;
  if (!byteLength) throw new Error('Expected tar output');
}

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

    await consume(multitars.tar(entries));
  });

  bench('modern-tar', async () => {
    const { readable, controller } = createTarPacker();
    const consumed = consume(readable);
    const names = input.names;
    const count = names?.length ?? input.count!;
    for (let index = 0; index < count; index++) {
      await input.content.stream().pipeTo(
        controller.add({
          name: names?.[index] ?? `file-${index}.txt`,
          size: input.content.size,
          type: 'file',
        })
      );
    }
    controller.finalize();
    await consumed;
  });
});
