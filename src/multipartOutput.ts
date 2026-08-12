import { encodeName, BOUNDARY_ID } from './multipartEncoding';
import { encoder } from './shared';
import {
  streamToSizedAsyncIterable,
  type ReadableStreamLike,
  streamLikeToIterator,
} from './conversions';
import { MultipartPart } from './multipartShared';

const CRLF = '\r\n';
const BOUNDARY_HYPHEN_CHARS = '--';

const FORM_FOOTER =
  BOUNDARY_HYPHEN_CHARS + BOUNDARY_ID + BOUNDARY_HYPHEN_CHARS + CRLF + CRLF;

const isBlob = (value: unknown): value is Blob | MultipartPart =>
  typeof value === 'object' &&
  value != null &&
  (value instanceof MultipartPart || value instanceof Blob || 'type' in value);

interface ContentDispositionParams {
  name: string;
  filename?: string;
}

const makeFormHeader = (
  params: ContentDispositionParams,
  part: Blob | MultipartPart | undefined
): string => {
  let header = BOUNDARY_HYPHEN_CHARS + BOUNDARY_ID + CRLF;
  const name = encodeName(params.name);
  header += `Content-Disposition: form-data; name="${name}"`;

  if (params.filename != null) {
    const filename =
      params.filename === params.name ? name : encodeName(params.filename);
    header += `; filename="${filename}"`;
  }

  if (part) {
    if (part.type) {
      header += `${CRLF}Content-Type: ${part.type}`;
    }
    // NOTE(@kitten): When size is zero, we don't send it. Since we're streaming
    // files, some files may not have a known size (See: StreamFile)
    if (part.size) {
      header += `${CRLF}Content-Length: ${part.size}`;
    }
    if ('headers' in part) {
      for (const headerName in part.headers) {
        if (
          headerName !== 'content-length' &&
          headerName !== 'content-type' &&
          headerName !== 'content-disposition'
        ) {
          header += `${CRLF}${headerName}: ${part.headers[headerName]}`;
        }
      }
    }
  }

  header += CRLF;
  header += CRLF;
  return header;
};

export type FormValue =
  | string
  | Uint8Array<ArrayBuffer>
  | MultipartPart
  | Blob
  | File;
export type FormEntry = readonly [name: string, value: FormValue];

export const multipartContentType = `multipart/form-data; boundary=${BOUNDARY_ID}`;

export async function* streamMultipart(
  entries: ReadableStreamLike<FormEntry>
): AsyncGenerator<Uint8Array<ArrayBuffer>> {
  for await (const [name, value] of streamLikeToIterator(entries)) {
    if (isBlob(value)) {
      yield encoder.encode(
        makeFormHeader(
          { name, filename: 'name' in value ? value.name : name },
          value
        )
      );
      const stream = value.stream();
      yield* streamToSizedAsyncIterable(
        stream,
        value.size || null,
        'Invalid Multipart: Part'
      );
    } else {
      yield encoder.encode(makeFormHeader({ name }, undefined));
      yield typeof value === 'string' ? encoder.encode(value) : value;
    }
    yield encoder.encode(CRLF);
  }
  yield encoder.encode(FORM_FOOTER);
}
