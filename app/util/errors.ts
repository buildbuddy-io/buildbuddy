export type ErrorCode = "Unknown" | "NotFound" | "PermissionDenied" | "Unauthenticated";

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
      return new BuildBuddyError("Unknown", error || "Internal error");
    }

    const [_, code, description] = match;

    return new BuildBuddyError(code as ErrorCode, description);
  }
}
