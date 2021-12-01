import { invocation } from "../../proto/invocation_ts_proto";
import { MAX_VARINT_LENGTH_64, readVarint } from "../util/varint";

export async function fetchEvents(invocationId: string): Promise<invocation.InvocationEvent[]> {
  const eventsUrl = `${window.location.origin}/rpc/BuildBuddyStreamService/GetBuildEvents?invocation_id=${invocationId}`;
  const events: invocation.InvocationEvent[] = [];
  let buffer = new Uint8Array();
  let nextProtoLength: number | null = null;

  /**
   * Parses as much of the currently fetched buffer as possible into
   * `invocation.InvocationEvent` protos.
   */
  function consumeBuffer(lastCall = false) {
    while (buffer.byteLength > 0) {
      // If we don't know the length of the next proto to consume, look for an encoded length.
      if (nextProtoLength === null) {
        // Don't read the length yet if the current buffer might contain a
        // partial chunk of a varint sequence.
        if (!lastCall && buffer.byteLength < MAX_VARINT_LENGTH_64) return;

        const [length, numBytesRead] = readVarint(buffer);
        // We never expect file sizes that are larger than Number.MAX_SAFE_INTEGER,
        // so the conversion from Long to Number here is OK.
        nextProtoLength = length.toNumber();
        buffer = buffer.subarray(numBytesRead);
        continue;
      }

      // If this is the last call and we don't have the expected number of
      // bytes yet, we won't be able to parse the proto, so give a nicer
      // error in this case.
      if (lastCall && buffer.byteLength < nextProtoLength) {
        throw new Error(
          `Event stream is unexpectedly truncated: expected ${nextProtoLength} bytes, but only ${buffer.byteLength} are available`
        );
      }

      // If our buffer has at least the expected number of bytes, consume an event
      // from the buffer.
      if (buffer.byteLength >= nextProtoLength) {
        const protoBytes = buffer.subarray(0, nextProtoLength);
        const event = invocation.InvocationEvent.decode(protoBytes);
        events.push(event);
        buffer = buffer.subarray(protoBytes.byteLength);
        nextProtoLength = null;
        continue;
      }

      // We couldn't consume a proto length or bytes; nothing left to do.
      return;
    }
  }

  const worker = startFetchWorker(eventsUrl);
  worker.onmessage = (event: MessageEvent) => {
    const data = event.data as Uint8Array | "done";
    if (data === "done") {
      consumeBuffer(/*lastCall=*/ true);
      onDone(events);
      return;
    }
    buffer = concatChunks([buffer, data]);
    consumeBuffer();
  };

  let onDone: (events: invocation.InvocationEvent[]) => void;
  return new Promise<invocation.InvocationEvent[]>((resolve) => {
    onDone = resolve;
  });
}

function startFetchWorker(url: string) {
  const source = `
  (async () => {
    const url = ${JSON.stringify(url)};
    const response = await fetch(url, { credentials: "include" });
    if (response.status >= 400) {
      throw new Error(await response.text());
    }
    const reader = response.body.getReader();
    while (true) {
      const { value, done } = await reader.read();
      if (value) {
        globalThis.postMessage(value);
      }
      if (done) {
        globalThis.postMessage("done");
        return;
      }
    }
  })()
  `;
  return new Worker(URL.createObjectURL(new Blob([source], { type: "application/javascript" })));
}

function concatChunks(chunks: Uint8Array[]) {
  const out = new Uint8Array(chunks.reduce((acc, value) => acc + value.length, 0));
  let offset = 0;
  for (let chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
