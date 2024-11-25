import { google as google_status } from "../../proto/grpc_status_ts_proto";
import { google as google_code } from "../../proto/grpc_code_ts_proto";

export type ErrorCode = Omit<GRPCCodeName, "OK">;

/**
 * High-level error class representing any error from the BuildBuddy server,
 * either GRPC or HTTP.
 */
// TODO(bduffany): once structured errors are fully rolled out
// (app.streaming_http_enabled), remove this in favor of directly using the
// errors returned by RPCs.
export class BuildBuddyError extends Error {
  constructor(
    public code: ErrorCode,
    public description: string
  ) {
    super(description);
  }

  static parse(e: any): BuildBuddyError {
    if (e instanceof BuildBuddyError) {
      return e;
    }
    if (e instanceof GRPCStatusError) {
      return new BuildBuddyError(grpcCodeToString(e.status.code), e.status.message);
    }

    const message = errorMessage(e).trim();
    if (message === "record not found") {
      return new BuildBuddyError("NotFound", "Not found");
    }

    const status = parseGRPCStatus(message);
    if (!status) {
      return new BuildBuddyError("Unknown", message || "Internal error");
    }

    return new BuildBuddyError(grpcCodeToString(status.code), status.message);
  }

  toString() {
    return this.description;
  }
}

function errorMessage(e: any) {
  if (e instanceof Error) {
    return e.message;
  }
  return String(e);
}

/**
 * An error indicating that an HTTP response was received from the server but it
 * contained a status code other than 2XX.
 */
export class HTTPStatusError extends Error {
  readonly code: number;
  readonly body: string;

  constructor(code: number, body: string) {
    super(`HTTP ${code}: ${body}`);
    this.code = code;
    this.body = body;
  }

  toString(): string {
    return `HTTP error: status ${this.code}: ${this.body}`;
  }
}

/**
 * An error indicating that we successfully got a response from the RPC server,
 * but it contained a gRPC status consisting of an error code and message.
 */
export class GRPCStatusError extends Error {
  readonly status: google_status.rpc.Status;

  constructor(status: google_status.rpc.IStatus) {
    super(`${grpcCodeToString(status.code ?? 0)}: ${status.message ?? "<empty message>"}`);
    this.status = new google_status.rpc.Status(status);
  }

  toString(): string {
    return `gRPC error: ${this.message}`;
  }
}

/**
 * An error that occurred while performing a fetch. These types of errors
 * indicate a network issue and may be retried.
 */
export class FetchError extends Error {
  readonly wrappedError: any;

  constructor(e: any) {
    super(errorMessage(e));
    this.wrappedError = e;
  }

  toString(): string {
    return `Fetch error: ${this.message}`;
  }
}

/**
 * Parses a gRPC status error from an HTTP response returned by the server.
 */
export function parseGRPCStatus(text: string): google_status.rpc.Status | null {
  const grpcErrorPattern = /code = (.*?) desc = (.*)$/;
  const match = text.trim().match(grpcErrorPattern);
  if (!match) return null;
  const [_, codeString, message] = match;
  const code = parseGRPCCode(codeString);
  if (code === null) {
    console.error(`unexpected gRPC status '${codeString}'`);
    return null;
  }
  return new google_status.rpc.Status({ code, message });
}

/**
 * CamelCase string representation of a gRPC code, which is the same
 * representation used by the server for error responses.
 */
export type GRPCCodeName = keyof typeof codesByName;

const Code = google_code.rpc.Code;

const codesByName = {
  OK: Code.OK,
  Cancelled: Code.CANCELLED,
  Unknown: Code.UNKNOWN,
  InvalidArgument: Code.INVALID_ARGUMENT,
  DeadlineExceeded: Code.DEADLINE_EXCEEDED,
  NotFound: Code.NOT_FOUND,
  AlreadyExists: Code.ALREADY_EXISTS,
  PermissionDenied: Code.PERMISSION_DENIED,
  Unauthenticated: Code.UNAUTHENTICATED,
  ResourceExhausted: Code.RESOURCE_EXHAUSTED,
  FailedPrecondition: Code.FAILED_PRECONDITION,
  Aborted: Code.ABORTED,
  OutOfRange: Code.OUT_OF_RANGE,
  Unimplemented: Code.UNIMPLEMENTED,
  Internal: Code.INTERNAL,
  Unavailable: Code.UNAVAILABLE,
  DataLoss: Code.DATA_LOSS,
} as const;

const namesByCode = Object.fromEntries(Object.entries(codesByName).map(([k, v]) => [v, k])) as Record<
  google_code.rpc.Code,
  GRPCCodeName
>;

export function parseGRPCCode(text: string): google_code.rpc.Code | null {
  return (codesByName as Record<string, google_code.rpc.Code>)[text] ?? null;
}

export function grpcCodeToString(code: google_code.rpc.Code): GRPCCodeName {
  return namesByCode[code] || "Unknown";
}
