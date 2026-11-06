import Long from "long";
import { timestampToDate } from "../../../../app/util/proto";
import { atlas } from "../../../../proto/atlas_ts_proto";
import { google as google_timestamp } from "../../../../proto/timestamp_ts_proto";

export function toNumber(value: number | Long | null | undefined): number {
  if (value === null || value === undefined) return 0;
  return typeof value === "number" ? value : value.toNumber();
}

export function toDate(ts?: google_timestamp.protobuf.ITimestamp | null): Date | undefined {
  return ts ? timestampToDate(ts) : undefined;
}

/** A compact age like "45s", "3h" or "12d". */
export function age(ts?: google_timestamp.protobuf.ITimestamp | null): string {
  const date = toDate(ts);
  if (!date) return "";
  const s = Math.max(0, (Date.now() - date.getTime()) / 1000);
  if (s < 120) return `${Math.floor(s)}s`;
  if (s < 7200) return `${Math.floor(s / 60)}m`;
  if (s < 172800) return `${Math.floor(s / 3600)}h`;
  if (s < 2 * 365 * 86400) return `${Math.floor(s / 86400)}d`;
  return `${Math.floor(s / (365 * 86400))}y`;
}

export type Tone = "" | "ok" | "warn" | "bad";

/** The backend's verdict on an entry, as a badge tone. */
export function healthTone(health?: atlas.Health | null): Tone {
  switch (health) {
    case atlas.Health.HEALTH_OK:
      return "ok";
    case atlas.Health.HEALTH_WARN:
      return "warn";
    case atlas.Health.HEALTH_BAD:
      return "bad";
    default:
      return "";
  }
}

export function shortImage(image: string): string {
  return image.split("/").pop() ?? image;
}

/** The muted context shown at the right of a search result. */
export function entryContext(e: atlas.IEntry): string {
  const bits: string[] = [];
  if (e.kind === "Pod") {
    const restarts = toNumber(e.restarts);
    if (restarts) bits.push(`↻${restarts}`);
    if (e.node) bits.push(e.node);
  } else if (e.images?.length) {
    bits.push(e.images.map(shortImage).join(", "));
  } else if (e.kind === "Node" && e.ips?.length) {
    bits.push(e.ips.join(" "));
  } else if (e.ports?.length) {
    bits.push(e.ports.map((p) => p.port).join(","));
  }
  if (e.created) bits.push(age(e.created));
  return bits.join("  ·  ");
}
