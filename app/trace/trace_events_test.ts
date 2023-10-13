import { readProfile } from "./trace_events";

// NOTE: in the following profile data, whitespace is significant (unlike regular JSON):
// - The `"traceEvents":[` list opening has to end with a newline
// - Each entry in the traceEvents list has to end with ",\n", except for the last one
// - The traceEvents list is closed with the exact sequence "\n  ]\n}"

const INCOMPLETE_PROFILE = `
{"otherData":{"build_id":"799cc58b-8695-4e70-a772-7a15472aa55b","output_base":"/output-base","date":"Wed Oct 26 20:24:01 UTC 2022"},"traceEvents":[
  {"name":"thread_name","ph":"M","pid":1,"tid":0,"args":{"name":"Critical Path"}},
  {"name":"thread_sort_index","ph":"M","pid":1,"tid":0,"args":{"sort_index":0}},
  {"name":"thread_name","ph":"M","pid":1,"tid":29,"args":{"name":"Main Thread"}},
  {"name":"thread_sort_index","ph":"M","pid":1,"tid":29,"args":{"sort_index":"1"}},
  {"cat":"build phase marker","name":"Launch Blaze","ph":"X","ts":-899000,"dur":899000,"pid":1,"tid":29},
  {"cat":"build phase marker","name":"Initialize command","ph":"i","ts":0,"pid":1,"tid":29},
  {"cat":"general information","name":"BazelStartupOptionsModule.beforeCommand","ph":"X","ts":200868,"dur":206,"pid":1,"tid":29}`.trim();

const COMPLETE_PROFILE = INCOMPLETE_PROFILE + "\n  ]\n}";

function readableStreamFromString(value: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const reader: ReadableStreamDefaultReader<Uint8Array> = {
    read() {
      if (!value) {
        return Promise.resolve({ done: true });
      }
      // Read a small-ish, random length (between 1 and 10 bytes)
      const length = Math.min(value.length, 1 + Math.floor(Math.random() * 10));
      const before = value.substring(0, length);
      const after = value.substring(length);
      value = after;
      return Promise.resolve({ value: encoder.encode(before), done: false });
    },
    releaseLock() {
      throw new Error("releaseLock(): not implemented");
    },
    cancel() {
      throw new Error("cancel(): not implemented");
    },
    get closed() {
      return Promise.reject("closed: not implemented");
    },
  };
  const stream = {
    getReader(): ReadableStreamDefaultReader<Uint8Array> {
      return reader;
    },
  } as ReadableStream<Uint8Array>;

  return stream;
}

describe("parseProfile", () => {
  it("should parse a complete profile", async () => {
    const stream = readableStreamFromString(COMPLETE_PROFILE);
    let numBytesRead = 0;
    const profile = await readProfile(stream, (n) => {
      numBytesRead = n;
    });
    expect(profile.traceEvents.length).toBe(7);
    expect(numBytesRead).toBe(COMPLETE_PROFILE.length);
  });

  it("should parse an incomplete profile", async () => {
    const stream = readableStreamFromString(INCOMPLETE_PROFILE);
    let numBytesRead = 0;
    const profile = await readProfile(stream, (n) => {
      numBytesRead = n;
    });
    expect(profile.traceEvents.length).toBe(7);
    expect(numBytesRead).toBe(INCOMPLETE_PROFILE.length);
  });
});
