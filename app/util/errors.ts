export type ErrorCode = "Unknown" | "NotFound" | "OutOfRange" | "PermissionDenied";

export class BuildBuddyError extends Error {
  constructor(public code: ErrorCode, public description: string) {
    super(description);
  }

  static parse(e: any): BuildBuddyError {
    const error = String(e).trim();
    if (error === "Error: record not found") {
      return new BuildBuddyError("NotFound", "Not found");
    }

    const pattern = /code = (.*?) desc = (.*)$/;
    const match = error.match(pattern);
    if (!match) {
      return new BuildBuddyError("Unknown", "Internal error");
    }

    const [_, code, description] = match;

    return new BuildBuddyError(code as ErrorCode, description);
  }
}
