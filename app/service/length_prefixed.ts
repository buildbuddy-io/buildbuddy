import { BrowserHeaders } from "browser-headers";
import { google as google_code } from "../../proto/grpc_code_ts_proto";
import { google as google_status } from "../../proto/grpc_status_ts_proto";
import { GRPCStatusError } from "../util/errors";

/**
 * Framing for gRPC-style streaming over plain HTTP: the request and each
 * response message travel as `Length-Prefixed-Message`s, with the final gRPC
 * status encoded as trailers in a frame of its own. Shared by every RPC
 * service the UIs talk to.
 */

// GRPC over HTTP requires protobuf messages to be sent in a series of `Length-Prefixed-Message`s
// Here's what a Length-Prefixed-Message looks like:
// 		Length-Prefixed-Message → Compressed-Flag Message-Length Message
// 		Compressed-Flag → 0 / 1 # encoded as 1 byte unsigned integer
// 		Message-Length → {length of Message} # encoded as 4 byte unsigned integer (big endian)
// 		Message → *{binary octet}
// For more info, see: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
export function lengthPrefixMessage(requestData: Uint8Array) {
  const frame = new ArrayBuffer(requestData.byteLength + 5);
  new DataView(frame, 1, 4).setUint32(0, requestData.length, false /* big endian */);
  new Uint8Array(frame, 5).set(requestData);
  return new Uint8Array(frame);
}

/**
 * Reads length-prefixed message payloads from the stream and invokes the given
 * callback for each one.
 *
 * The returned promise completes when all messages are successfully read from
 * the stream. It resolves with an error if reading from the stream fails or if
 * it contains malformed data.
 */
export async function readLengthPrefixedStream(reader: ReadableStreamDefaultReader, callback: (b: Uint8Array) => void) {
  const bufferedStream = new BufferedStream(reader);

  // Reusable buffer for the flags + length header.
  const headerBytes = new Uint8Array(5);
  const readHeader = async () => {
    const n = await bufferedStream.read(headerBytes);
    if (n === 0) {
      return null; // no more data
    }
    if (n !== headerBytes.length) {
      throw new Error("unexpected data on stream while reading length prefix");
    }
    return {
      flags: headerBytes[0],
      payloadLength: new DataView(headerBytes.buffer, 1, 4).getUint32(0, /*littleEndian=*/ false),
    };
  };

  const readPayload = async (payloadLength: number) => {
    const messageBytes = new Uint8Array(payloadLength);
    const n = await bufferedStream.read(messageBytes);
    if (n < messageBytes.length) {
      throw new Error("stream ended unexpectedly while reading payload");
    }
    return messageBytes;
  };

  while (true) {
    const header = await readHeader();
    if (!header) {
      // No more data, and we haven't read the status.
      // Just assume OK status for now.
      return;
    }

    if ((header.flags & 0x80) === 0x80) {
      // This value indicates that there is no more data and the gRPC status
      // payload will follow.
      const encodedTrailers = await readPayload(header.payloadLength);
      const status = statusFromHeaders(decodeTrailers(encodedTrailers));
      if (status.code === google_code.rpc.Code.OK) {
        return; // OK status - don't throw an error.
      }
      throw new GRPCStatusError(status);
    }

    const message = await readPayload(header.payloadLength);
    callback(message);
  }
}

export function statusFromHeaders(input: Headers | string): google_status.rpc.Status {
  const headers = new BrowserHeaders(input);
  const code = Number(headers.get("grpc-status")?.[0] ?? undefined);
  const message = headers.get("grpc-message")?.[0] ?? undefined;
  return new google_status.rpc.Status({
    code: isNaN(code) ? google_code.rpc.Code.UNKNOWN : code,
    message: message || "unknown error",
  });
}

/**
 * Provides buffering for a ReadableStream.
 */
class BufferedStream {
  private chunks: Uint8Array[] = [];
  private len = 0;
  private done = false;

  constructor(private reader: ReadableStreamDefaultReader) {}

  /**
   * Tries to fill the given buffer with data from the stream, and returns the
   * number of bytes that were successfully read. It returns a value less than
   * the buffer length only if the stream ends while reading.
   */
  async read(out: Uint8Array): Promise<number> {
    // Read chunks until either we can fill the buffer or the stream has no more
    // data.
    while (this.len < out.length && !this.done) {
      const { value, done } = await this.reader.read();
      this.done = done;
      if (value) {
        this.chunks.push(value);
        this.len += value.length ?? 0;
      }
    }
    // Consume chunk data until either we fill the buffer, or the stream is done
    // and we don't have any buffered data left.
    let n = 0;
    while (n < out.length && this.chunks.length) {
      const remainder = out.length - n;
      let data: Uint8Array;
      if (remainder >= this.chunks[0].length) {
        // Consume the full chunk.
        data = this.chunks[0];
        this.chunks.shift();
      } else {
        // Consume a partial chunk.
        data = this.chunks[0].subarray(0, remainder);
        this.chunks[0] = this.chunks[0].subarray(remainder);
      }
      out.set(data, n);
      n += data.length;
    }
    this.len -= n;
    return n;
  }
}

const isAllowedControlChar = (char: number) => char === 0x9 || char === 0xa || char === 0xd;

function isValidHeaderAscii(val: number): boolean {
  return isAllowedControlChar(val) || (val >= 0x20 && val <= 0x7e);
}

function decodeTrailers(bytes: Uint8Array): string {
  for (let i = 0; i !== bytes.length; ++i) {
    if (!isValidHeaderAscii(bytes[i])) {
      throw new Error("gRPC status trailers returned by server are not valid ASCII");
    }
  }
  return new TextDecoder("ASCII").decode(bytes);
}
