export type LogLine =
  | { kind: "text"; text: string }
  | {
      kind: "entry";
      /** The line as written. */
      text: string;
      time?: Date;
      /** Lower-cased: info, warn, error, ... */
      severity: string;
      message: string;
      /** "file.go:123" when the logger recorded where the line came from. */
      source: string;
      fields: [string, string][];
    };

const SOURCE_LOCATION = "logging.googleapis.com/sourceLocation";
const KNOWN = new Set(["severity", "level", "timestamp", "time", "message", "msg", "caller", SOURCE_LOCATION]);

/** Parses one line. Anything but a JSON object with a message stays text. */
export function parseLogLine(text: string): LogLine {
  let o: unknown;
  if (text.startsWith("{") && text.endsWith("}")) {
    try {
      o = JSON.parse(text);
    } catch {
      return { kind: "text", text };
    }
  }
  if (!isRecord(o)) {
    return { kind: "text", text };
  }
  const message = o.message ?? o.msg;
  if (typeof message !== "string") {
    return { kind: "text", text };
  }
  const stamp = o.timestamp ?? o.time;
  const time = typeof stamp === "string" ? new Date(stamp) : undefined;
  const loc = o[SOURCE_LOCATION];
  const source = isRecord(loc) ? `${loc.file ?? ""}:${loc.line ?? ""}` : typeof o.caller === "string" ? o.caller : "";
  const fields: [string, string][] = [];
  for (const [k, v] of Object.entries(o)) {
    if (!KNOWN.has(k)) {
      fields.push([k, typeof v === "string" ? v : JSON.stringify(v)]);
    }
  }
  return {
    kind: "entry",
    text,
    time: time && !isNaN(time.getTime()) ? time : undefined,
    severity: String(o.severity ?? o.level ?? "").toLowerCase(),
    message,
    source,
    fields,
  };
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/** Groups severities into the UI's tones. */
export function severityTone(severity: string): "bad" | "warn" | "" {
  switch (severity) {
    case "error":
    case "fatal":
    case "panic":
    case "critical":
    case "alert":
    case "emergency":
      return "bad";
    case "warn":
    case "warning":
      return "warn";
    default:
      return "";
  }
}

/** Local wall-clock time with milliseconds, the part that matters in a tail. */
export function clockTime(d: Date): string {
  const p = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}
