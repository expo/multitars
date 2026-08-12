import {
  ReadableStreamBlockReader,
  bytesToSkipTable,
  readUntilBoundary,
} from './reader';
import { encoder } from './shared';
import { decodeName } from './multipartEncoding';
import { ReadableStreamLike, createReadableStream } from './conversions';
import { MultipartHeaders, MultipartPart } from './multipartShared';

const BLOCK_SIZE = 4_096; /*4KiB*/

const CRLF = new Uint8Array([13, 10]);
const CRLF_SKIP_TABLE = bytesToSkipTable(CRLF);
const MAX_PREAMBLE_SIZE = 16_000; /*16kB*/
const MAX_HEADER_SIZE = 16_000; /*16kB*/
const MAX_HEADERS_SIZE = 32_000; /*32kB*/
const boundaryHeaderRe = /boundary="?([^=";]+)"?/i;
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

function utf8Encode(
  content: string | ArrayBufferView | ArrayBufferLike
): Uint8Array {
  return typeof content === 'string'
    ? encoder.encode(content)
    : new Uint8Array('buffer' in content ? content.buffer : content);
}

function parseContentLength(contentLength: string | undefined): number | null {
  if (contentLength) {
    const size = parseInt(contentLength, 10);
    return Number.isSafeInteger(size) && size > 0 ? size : null;
  } else {
    return null;
  }
}

interface ContentDisposition {
  name: string | null;
  filename: string | null;
}

function parseContentDisposition(
  contentDisposition: string | undefined
): ContentDisposition | null {
  let startIdx = 0,
    endIdx = -1;
  if (
    !contentDisposition ||
    (startIdx = contentDisposition.indexOf(';')) < 0 ||
    contentDisposition.slice(0, startIdx).trimEnd() !== 'form-data'
  ) {
    return null;
  }
  const disposition: ContentDisposition = { name: null, filename: null };
  do {
    endIdx = contentDisposition.indexOf(';', startIdx);
    const entry = contentDisposition.slice(
      startIdx,
      endIdx > -1 ? endIdx : undefined
    );
    startIdx = endIdx + 1;
    const equalIdx = entry.indexOf('=');
    if (equalIdx > -1) {
      const key = entry.slice(0, equalIdx).trim();
      let value = entry.slice(equalIdx + 1).trim();
      if (key !== 'name' && key !== 'filename') {
        // NOTE: We don't handle the special `filename*=UTF-8` case
        continue;
      } else if (value[0] === '"' && value[value.length - 1] === '"') {
        disposition[key] = decodeName(value.slice(1, -1));
      } else {
        disposition[key] = decodeName(value);
      }
    }
  } while (endIdx > 0);
  return disposition;
}

interface Boundary {
  /** Initial boundary at the beginning of the multipart stream after the preamble */
  raw: Uint8Array;
  rawSkipTable: Uint8Array;
  /** Trailer boundary after every multipart part.
   * @remarks
   * After every multipart part that isn't the initial one we expect a leading CRLF sequence
   */
  trailer: Uint8Array;
  trailerSkipTable: Uint8Array;
}

/** Create boundary patterns from `contentType` boundary parameter.
 * @remarks
 * - The leading boundary is `--${boundary}\r\n` followed by the first set of headers (leading CRLF is optional)
 * - Subsequent boundary trailers are `\r\n--${boundary}\r\n` followed by headers
 * - The closing boundary is `\r\n--${boundary}--\r\n`
 * Because of the variability of `--` or `\r\n` following the main boundary, we don't add it
 * to the pattern. Instead, we handle `--` in `decodeHeaders`.
 * We have two patterns because the leading CRLF is optional in the first boundary. Afterwards,
 * we use `Boundary.trailer` to include the leading CRLF to remove it from content parts.
 */
function convertToBoundaryBytes(contentType: string): Boundary {
  const boundaryHeader = contentType.match(boundaryHeaderRe);
  const boundaryRaw = `--${boundaryHeader?.[1] || '-'}`;
  const boundaryTrailer = `\r\n${boundaryRaw}`;
  const raw = utf8Encode(boundaryRaw);
  const trailer = utf8Encode(boundaryTrailer);
  return {
    raw,
    rawSkipTable: bytesToSkipTable(raw),
    trailer,
    trailerSkipTable: bytesToSkipTable(trailer),
  };
}

async function expectPreamble(
  reader: ReadableStreamBlockReader,
  boundary: Boundary
): Promise<void> {
  let byteLength = 0;
  for await (const chunk of readUntilBoundary(
    reader,
    boundary.raw,
    boundary.rawSkipTable
  )) {
    if (chunk == null) {
      throw new Error('Invalid Multipart Preamble: Unexpected EOF');
    } else if ((byteLength += chunk?.byteLength) > MAX_PREAMBLE_SIZE) {
      throw new Error(
        'Invalid Multipart Preamble: Boundary not found within the first 16kB'
      );
    }
  }
}

async function expectTrailer(
  reader: ReadableStreamBlockReader,
  boundary: Boundary
): Promise<void> {
  for await (const chunk of readUntilBoundary(
    reader,
    boundary.trailer,
    boundary.trailerSkipTable
  )) {
    if (chunk == null || chunk.byteLength !== 0) {
      throw new Error('Invalid Multipart Part: Expected trailing boundary');
    } else {
      break;
    }
  }
}

async function decodeHeaders(
  reader: ReadableStreamBlockReader
): Promise<MultipartHeaders | null> {
  // NOTE: The characters we're decoding in headers is restricted, and we're therefore
  // more strict here. The `stream` option is also omitted below
  let byteLength = 0;
  const headers: MultipartHeaders = Object.create(null);
  while (byteLength < MAX_HEADERS_SIZE) {
    let header = '';
    for await (const chunk of readUntilBoundary(
      reader,
      CRLF,
      CRLF_SKIP_TABLE
    )) {
      if (chunk == null) {
        throw new Error('Invalid Multipart Headers: Unexpected EOF');
      } else if (
        !byteLength &&
        !header &&
        chunk[0] === 45 /*'-'*/ &&
        chunk[1] === 45 /*'-'*/
      ) {
        // If the first chunk we receive here is `--`, this is the multipart data's closing delimiter
        // This means the multipart stream has ended
        return null;
      } else {
        header += decoder.decode(chunk);
        if (header.length > MAX_HEADER_SIZE)
          throw new Error(
            'Invalid Multipart Headers: A header exceeded its maximum length of 16kB'
          );
      }
    }

    if (header) {
      const colonIdx = header.indexOf(':');
      if (colonIdx > -1) {
        const headerName = header.slice(0, colonIdx).trim().toLowerCase();
        const headerValue = header.slice(colonIdx + 1).trim();
        if (headerValue) headers[headerName] = headerValue;
        byteLength += header.length + CRLF.byteLength;
      } else {
        throw new Error(
          'Invalid Multipart Headers: Invalid header value missing `:`'
        );
      }
    } else if (byteLength > 0) {
      // The first and last CRLF sections are expected to be empty (headerByteLength === 0)
      // since headers start and end with an additional CRLF sequence
      // Hence, if we have already seen some header bytes (byteLenght > 0) we stop decoding headers
      break;
    }
  }
  if (byteLength > MAX_HEADER_SIZE) {
    throw new Error(
      'Invalid Multipart Headers: Headers exceeded their maximum length of 32kB'
    );
  }
  return headers;
}

interface ParseMultipartParams {
  /** The `Content-Type` header value */
  contentType: string;
}

/** Provide tar entry iterator */
export async function* parseMultipart(
  stream: ReadableStreamLike<Uint8Array>,
  params: ParseMultipartParams
): AsyncGenerator<MultipartPart> {
  const reader = new ReadableStreamBlockReader(stream, BLOCK_SIZE);
  try {
    yield* readMultipart(reader, params);
  } finally {
    await reader.close();
  }
}

async function* readMultipart(
  reader: ReadableStreamBlockReader,
  params: ParseMultipartParams
): AsyncGenerator<MultipartPart> {
  const boundary = convertToBoundaryBytes(params.contentType);
  await expectPreamble(reader, boundary);

  let headers: MultipartHeaders | null;
  while ((headers = await decodeHeaders(reader))) {
    const type = headers['content-type'];
    const disposition = parseContentDisposition(headers['content-disposition']);
    const name = disposition?.filename || disposition?.name;
    const size = parseContentLength(headers['content-length']);
    if (!name) {
      throw new Error(
        'Invalid Multipart Part: Missing Content-Disposition name or filename parameter'
      );
    }

    let reachedEnd = false;
    let remaining = 0;
    let stream: ReadableStream;
    if (size !== null) {
      // With a known size, we output a sized stream (similar to tar files)
      remaining = size;
      stream = createReadableStream({
        expectedLength: size,
        async cancel() {
          if (remaining > 0) {
            remaining = await reader.skip(remaining);
            if (remaining > 0)
              throw new Error('Invalid Multipart Part: Unexpected EOF');
          }
          if (!reachedEnd) {
            await expectTrailer(reader, boundary);
            reachedEnd = true;
          }
        },
        async pull(controller) {
          if (remaining) {
            const buffer = await reader.pull(remaining);
            if (!buffer)
              throw new Error('Invalid Multipart Part: Unexpected EOF');
            remaining -= buffer.byteLength;
            controller.enqueue(
              (reader.blockLocked
                ? buffer.slice()
                : buffer) as Uint8Array<ArrayBuffer>
            );
          }
          if (!remaining) {
            await expectTrailer(reader, boundary);
            reachedEnd = true;
            controller.close();
          }
        },
      });
    } else {
      // Without a size, we instead output a stream that ends at the multipart boundary
      const iterator = readUntilBoundary(
        reader,
        boundary.trailer,
        boundary.trailerSkipTable
      );
      stream = createReadableStream({
        async cancel() {
          for await (const chunk of iterator) {
            if (!chunk) {
              throw new Error('Invalid Multipart Part: Unexpected EOF');
            }
          }
          reachedEnd = true;
        },
        async pull(controller) {
          const result = await iterator.next();
          if (result.done) {
            controller.close();
            reachedEnd = true;
          } else if (!result.value) {
            throw new Error('Invalid Multipart Part: Unexpected EOF');
          } else {
            controller.enqueue(result.value.slice());
          }
        },
      });
    }

    yield new MultipartPart(stream, name, {
      type: type ?? undefined,
      size: size ?? undefined,
      headers,
    });

    if (remaining > 0 || !reachedEnd) {
      await stream.cancel();
    }
  }
}
