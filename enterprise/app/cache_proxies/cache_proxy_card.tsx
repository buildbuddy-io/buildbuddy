import Long from "long";
import { Cloud } from "lucide-react";
import React from "react";
import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import format from "../../../app/format/format";
import { cache_proxy } from "../../../proto/cache_proxy_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";

// Chart slice colors are pulled from the same CSS variables as the legend
// swatches in cache_proxies.css, so the ring and its legend stay in sync
// across theme changes. Recharts paints into SVG `fill` attributes, which
// don't resolve CSS `var(...)` references, so we have to read the computed
// value ourselves; the literal fallbacks are last-resort defaults for
// non-DOM environments (e.g. unit tests).
function cssColor(name: string, fallback: string): string {
  if (typeof document === "undefined") return fallback;
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

const HIT_COLOR = cssColor("--color-green-500", "#4caf50");
const MISS_COLOR = cssColor("--color-status-error", "#f44336");
const READ_COLOR = cssColor("--color-light-blue-500", "#03a9f4");
const WRITE_COLOR = cssColor("--color-indigo-500", "#3f51b5");
const EMPTY_COLOR = "#eee";

interface Props {
  node: cache_proxy.CacheProxyNode;
  lastCheckInTime?: google_timestamp.protobuf.Timestamp | null;
  statistics?: cache_proxy.IStatistics | null;
  // When true, the card is in summary mode and the statistics divider and
  // rings are hidden.
  summary?: boolean;
}

// toNumber accepts the int64-as-number-or-Long values that protobuf-ts
// produces and returns a plain number. Total cache counts could in theory
// exceed Number.MAX_SAFE_INTEGER, but for the purposes of a percentage
// readout that's fine.
function toNumber(value: number | Long | null | undefined): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === "number") return value;
  return value.toNumber();
}

function hitRate(hits: number, misses: number): string {
  const total = hits + misses;
  if (total === 0) return "—";
  return format.percent(hits / total) + "%";
}

// A proxy is considered "fresh" (green) if its last heartbeat is no more
// than freshThresholdMs ago; otherwise it's grey. The server-side
// staleness threshold for removing a proxy from the registry entirely is
// much longer (10 minutes), so a grey card means "still around but the
// most recent heartbeat is stale-ish."
const freshThresholdMs = 60 * 1000;

function isFresh(t?: google_timestamp.protobuf.Timestamp | null): boolean {
  if (!t) return false;
  const ms = +(t.seconds || 0) * 1000 + +(t.nanos || 0) / 1_000_000;
  return Date.now() - ms < freshThresholdMs;
}

export default class CacheProxyCardComponent extends React.Component<Props> {
  render() {
    const fresh = isFresh(this.props.lastCheckInTime);
    return (
      <div className={`card ${fresh ? "card-success" : "card-neutral"}`}>
        <Cloud />
        <div className="content">
          <div className="details">
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Hostname:</div>
              <div>{this.props.node.host}</div>
            </div>
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Proxy Instance ID:</div>
              <div>{this.props.node.proxyId}</div>
            </div>
            {this.props.node.proxyHostId && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Proxy Host ID:</div>
                <div>{this.props.node.proxyHostId}</div>
              </div>
            )}
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Version:</div>
              <div>{this.props.node.version}</div>
            </div>
            {this.props.node.startTime && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Uptime:</div>
                <div>{format.durationSince(this.props.node.startTime)}</div>
              </div>
            )}
            {this.props.lastCheckInTime && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Last Check-in:</div>
                <div>{format.relativeTimeSeconds(this.props.lastCheckInTime)}</div>
              </div>
            )}
            {toNumber(this.props.node.allocatedMemoryBytes) > 0 && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Allocated Memory:</div>
                <div>{format.bytes(toNumber(this.props.node.allocatedMemoryBytes))}</div>
              </div>
            )}
            {toNumber(this.props.node.allocatedCpuMillis) > 0 && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Allocated Milli CPU:</div>
                <div>{toNumber(this.props.node.allocatedCpuMillis)}</div>
              </div>
            )}
            {this.props.node.labels && Object.keys(this.props.node.labels).length > 0 && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Labels:</div>
                <div>
                  {Object.entries(this.props.node.labels)
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([k, v]) => (
                      <div key={k}>
                        <b>{k}</b>: {v}
                      </div>
                    ))}
                </div>
              </div>
            )}
            {!this.props.summary && this.props.statistics && this.renderStatistics(this.props.statistics)}
          </div>
        </div>
      </div>
    );
  }

  renderStatistics(s: cache_proxy.IStatistics) {
    const acReads = toNumber(s.acReadHits) + toNumber(s.acReadMisses);
    const acReadBytes = toNumber(s.acReadHitBytes) + toNumber(s.acReadMissBytes);
    const casReads = toNumber(s.casReadHits) + toNumber(s.casReadMisses);
    const casReadBytes = toNumber(s.casReadHitBytes) + toNumber(s.casReadMissBytes);
    return (
      <>
        <div className="cache-proxy-stats-divider"></div>
        <div className="cache-proxy-stats-heading">Statistics</div>
        <div className="cache-proxy-stat-rings">
          {this.renderReadRing("AC reads (digests)", toNumber(s.acReadHits), toNumber(s.acReadMisses), format.count)}
          {this.renderReadWriteRing("AC reads/writes (digests)", acReads, toNumber(s.acWrites), format.count)}
          {this.renderReadRing(
            "AC reads (bytes)",
            toNumber(s.acReadHitBytes),
            toNumber(s.acReadMissBytes),
            format.bytes
          )}
          {this.renderReadWriteRing("AC reads/writes (bytes)", acReadBytes, toNumber(s.acWriteBytes), format.bytes)}
        </div>
        <div className="cache-proxy-stat-rings">
          {this.renderReadRing("CAS reads (digests)", toNumber(s.casReadHits), toNumber(s.casReadMisses), format.count)}
          {this.renderReadWriteRing("CAS reads/writes (digests)", casReads, toNumber(s.casWrites), format.count)}
          {this.renderReadRing(
            "CAS reads (bytes)",
            toNumber(s.casReadHitBytes),
            toNumber(s.casReadMissBytes),
            format.bytes
          )}
          {this.renderReadWriteRing("CAS reads/writes (bytes)", casReadBytes, toNumber(s.casWriteBytes), format.bytes)}
        </div>
      </>
    );
  }

  renderReadRing(title: string, hits: number, misses: number, formatValue: (v: number) => string) {
    return (
      <div className="cache-proxy-stat-ring">
        <div className="cache-proxy-stat-ring-title">{title}</div>
        <div className="cache-proxy-stat-ring-row">
          {drawReadRing(hits, misses)}
          <div className="cache-proxy-stat-ring-labels">
            <div className="cache-proxy-stat-ring-rate">{hitRate(hits, misses)}</div>
            <div className="cache-chart-label">
              <span className="color-swatch cache-hit-color-swatch"></span>
              <span className="cache-stat">{formatValue(hits)}</span>
              &nbsp;hits
            </div>
            <div className="cache-chart-label">
              <span className="color-swatch cache-miss-color-swatch"></span>
              <span className="cache-stat">{formatValue(misses)}</span>
              &nbsp;misses
            </div>
          </div>
        </div>
      </div>
    );
  }

  renderReadWriteRing(title: string, reads: number, writes: number, formatValue: (v: number) => string) {
    return (
      <div className="cache-proxy-stat-ring">
        <div className="cache-proxy-stat-ring-title">{title}</div>
        <div className="cache-proxy-stat-ring-row">
          {drawReadWriteRing(reads, writes)}
          <div className="cache-proxy-stat-ring-labels">
            <div className="cache-proxy-stat-ring-rate">{readWriteRatio(reads, writes)} read:write</div>
            <div className="cache-chart-label">
              <span className="color-swatch cache-read-color-swatch"></span>
              <span className="cache-stat">{formatValue(reads)}</span>
              &nbsp;read
            </div>
            <div className="cache-chart-label">
              <span className="color-swatch cache-write-color-swatch"></span>
              <span className="cache-stat">{formatValue(writes)}</span>
              &nbsp;written
            </div>
          </div>
        </div>
      </div>
    );
  }
}

function readWriteRatio(reads: number, writes: number): string {
  if (reads === 0 && writes === 0) return "—";
  if (writes === 0) return "∞:1";
  if (reads === 0) return "1:∞";
  if (reads >= writes) {
    const r = reads / writes;
    return `${r < 10 ? r.toFixed(1) : Math.round(r)}:1`;
  }
  const r = writes / reads;
  return `1:${r < 10 ? r.toFixed(1) : Math.round(r)}`;
}

function drawReadRing(hits: number, misses: number) {
  let colorHit = HIT_COLOR;
  let colorMiss = MISS_COLOR;
  let h = hits;
  let m = misses;
  if (h === 0 && m === 0) {
    colorHit = EMPTY_COLOR;
    colorMiss = EMPTY_COLOR;
    h = 1;
  }
  return drawPie([
    { value: h, color: colorHit },
    { value: m, color: colorMiss },
  ]);
}

function drawReadWriteRing(reads: number, writes: number) {
  if (reads === 0 && writes === 0) {
    return drawPie([{ value: 1, color: EMPTY_COLOR }]);
  }
  return drawPie([
    { value: reads, color: READ_COLOR },
    { value: writes, color: WRITE_COLOR },
  ]);
}

function drawPie(data: { value: number; color: string }[]) {
  return (
    <div className="cache-proxy-stat-ring-container">
      <ResponsiveContainer>
        <PieChart>
          <Pie data={data} dataKey="value" outerRadius={32} innerRadius={18} isAnimationActive={false}>
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}
