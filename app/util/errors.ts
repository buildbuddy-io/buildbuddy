export type ErrorCode = "Unknown" | "NotFound" | "AlreadyExists" | "PermissionDenied" | "Unauthenticated";

export class BuildBuddyError extends Error {
  constructor(public code: ErrorCode, public description: string) {
    super(description);
  }

  static parse(e: any): BuildBuddyError {
    if (e instanceof BuildBuddyError) return e;

    const message = (e instanceof Error ? e.message : String(e)).trim();
    if (message === "record not found") {
      return new BuildBuddyError("NotFound", "Not found");
    }

    const pattern = /code = (.*?) desc = (.*)$/;
    const match = message.match(pattern);
    if (!match) {
      return new BuildBuddyError("Unknown", message || "Internal error");
    }

    const [_, code, description] = match;

    return new BuildBuddyError(code as ErrorCode, description);
  }
}
