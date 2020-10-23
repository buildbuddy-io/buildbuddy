export type ErrorCode = "Unknown" | "NotFound";

export type Error = {
  code: ErrorCode;
  description: string;
};

export function parseError(e: any): Error {
  const error = String(e).trim();
  if (error === "Error: record not found") {
    return { code: "NotFound", description: "Not found" };
  }

  const pattern = /code = (.*?) desc = (.*)$/;
  const match = error.match(pattern);
  if (!match) {
    return { code: "Unknown", description: "Internal error" };
  }

  const [_, code, description] = match;

  return { code: code as ErrorCode, description };
}
