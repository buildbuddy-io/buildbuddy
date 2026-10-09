import {
  lengthPrefixMessage,
  readLengthPrefixedStream,
  statusFromHeaders,
} from "../../../../app/service/length_prefixed";
import { FetchError, GRPCStatusError, HTTPStatusError } from "../../../../app/util/errors";
import { $stream, atlas } from "../../../../proto/atlas_ts_proto";
import { google as google_code } from "../../../../proto/grpc_code_ts_proto";

/** Return type for server-streaming RPCs. */
export type ServerStream<T> = $stream.ServerStream<T>;

/** Stream handler passed when calling a server-streaming RPC. */
export type ServerStreamHandler<T> = $stream.ServerStreamHandler<T>;

/**
 * Talks to the AtlasService over HTTP the way the main app talks to
 * BuildBuddyService: protobuf request and response bodies, length-prefixed so
 * that server-streaming RPCs and structured gRPC errors work over plain fetch.
 */
class RpcService {
  readonly service = new atlas.AtlasService(this.rpc.bind(this));

  private async rpc(
    method: { name: string },
    requestData: Uint8Array,
    callback: (error: any, data?: Uint8Array) => void,
    streamParams?: $stream.StreamingRPCParams
  ): Promise<void> {
    try {
      const response = await fetchOrThrow(`/rpc/AtlasService/${method.name}`, {
        method: "POST",
        headers: { "Content-Type": "application/proto+prefixed" },
        body: lengthPrefixMessage(requestData),
        signal: streamParams?.signal,
      });
      if (response.headers.has("grpc-status")) {
        // Nothing was streamed: the status came back as plain headers.
        const status = statusFromHeaders(response.headers);
        if (status.code !== google_code.rpc.Code.OK) {
          throw new GRPCStatusError(status);
        }
      } else if (response.body) {
        await readLengthPrefixedStream(response.body.getReader(), (message) => callback(null, message));
      }
      streamParams?.complete?.();
    } catch (e) {
      callback(e);
    }
  }
}

async function fetchOrThrow(url: string, init: RequestInit): Promise<Response> {
  let response: Response;
  try {
    response = await fetch(url, init);
  } catch (e) {
    // Cancellation is not an error to report; the generated stream code
    // recognizes it by name.
    if ((e as any)?.name === "AbortError") throw e;
    throw new FetchError(e);
  }
  if (response.status < 200 || response.status >= 400) {
    throw new HTTPStatusError(response.status, await response.text().catch(() => ""));
  }
  return response;
}

export default new RpcService();
