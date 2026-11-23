import { describe, it, expect } from 'vitest';
import { StreamFile } from '../file';

describe('StreamFile', () => {
  it('decodes independent text streams concurrently', async () => {
    const file = () =>
      new StreamFile(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new Uint8Array([0xe7]));
            controller.enqueue(new Uint8Array([0x95, 0x8c]));
            controller.close();
          },
        }),
        '',
        {}
      );
    expect(await Promise.all([file().text(), file().text()])).toEqual([
      '界',
      '界',
    ]);
  });

  it('strips an initial BOM from body text like Blob.text', async () => {
    const text = '\uFEFFcontent';
    expect(await new StreamFile([text], '', {}).text()).toBe(
      await new Blob([text]).text()
    );
  });

  it('extends File', async () => {
    const file = new StreamFile([], '', {});
    expect(file).toBeInstanceOf(File);
  });
});
