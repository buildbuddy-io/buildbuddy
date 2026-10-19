import Long from "long";
import { ChevronRight, Cloud } from "lucide-react";
import React from "react";
import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { Link } from "../../../app/components/link/link";
import format from "../../../app/format/format";
import { CACHE_COLORS } from "../../../app/util/color";
import { cache_proxy } from "../../../proto/cache_proxy_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";

const EMPTY_COLOR = "#eee";

interface Props {
  proxy: cache_proxy.CacheProxySummary;
  lastCheckInTime?: google_timestamp.protobuf.Timestamp | null;
  statistics?: cache_proxy.IStatistics | null;
  // When true, the card is in summary mode and flags and statistics are hidden.
  summary?: boolean;
  // The URL of this proxy's details page.
  href: string;
}

// toNumber accepts the int64-as-number-or-Long values that protobuf-ts
// produces and returns a plain number. Total cache counts could in theory
// exceed Number.MAX_SAFE_INTEGER, but for the purposes of a percentage
// readout that's fine.
export function toNumber(value: number | Long | null | undefined): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === "number") return value;
  return value.toNumber();
}

export function hitRate(hits: number, misses: number): string {
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
      <Link className={`card ${fresh ? "card-success" : "card-neutral"} cache-proxy-card-link`} href={this.props.href}>
        <Cloud />
        <div className="content">
          <div className="details">
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Hostname:</div>
              <div>{this.props.proxy.host}</div>
            </div>
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Proxy Instance ID:</div>
              <div>{this.props.proxy.proxyId}</div>
            </div>
            {this.props.proxy.proxyHostId && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Proxy Host ID:</div>
                <div>{this.props.proxy.proxyHostId}</div>
              </div>
            )}
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Version:</div>
              <div>{this.props.proxy.version}</div>
            </div>
            {this.props.proxy.startTime && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Uptime:</div>
                <div>{format.durationSince(this.props.proxy.startTime)}</div>
              </div>
            )}
            {this.props.lastCheckInTime && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Last Check-in:</div>
                <div>{format.relativeTimeSeconds(this.props.lastCheckInTime)}</div>
              </div>
            )}
            {toNumber(this.props.proxy.allocatedMemoryBytes) > 0 && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Allocated Memory:</div>
                <div>{format.bytes(toNumber(this.props.proxy.allocatedMemoryBytes))}</div>
              </div>
            )}
            {toNumber(this.props.proxy.allocatedCpuMillis) > 0 && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Allocated Milli CPU:</div>
                <div>{toNumber(this.props.proxy.allocatedCpuMillis)}</div>
              </div>
            )}
            {this.props.proxy.labels && Object.keys(this.props.proxy.labels).length > 0 && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Labels:</div>
                <div>
                  {Object.entries(this.props.proxy.labels)
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([k, v]) => (
                      <div key={k}>
                        <b>{k}</b>: {v}
                      </div>
                    ))}
                </div>
              </div>
            )}
            {!this.props.summary && (
              <div className="cache-proxy-section cache-proxy-flags-section">
                <div className="cache-proxy-section-title">Configuration:</div>
                <div className="cache-proxy-configured-flags">
                  {this.props.proxy.configuredFlags.length > 0
                    ? this.props.proxy.configuredFlags.map((f) => <div key={f}>{f}</div>)
                    : "No configured flags reported."}
                </div>
              </div>
            )}
            {!this.props.summary &&
              (this.props.statistics ? (
                this.renderStatistics(this.props.statistics)
              ) : (
                <div className="cache-proxy-stats-empty">No statistics reported yet.</div>
              ))}
          </div>
        </div>
        <ChevronRight className="cache-proxy-card-chevron" />
      </Link>
    );
  }

  renderStatistics(s: cache_proxy.IStatistics) {
    const acReads = toNumber(s.acReadHits) + toNumber(s.acReadMisses);
    const acReadBytes = toNumber(s.acReadHitBytes) + toNumber(s.acReadMissBytes);
    const casReads = toNumber(s.casReadHits) + toNumber(s.casReadMisses);
    const casReadBytes = toNumber(s.casReadHitBytes) + toNumber(s.casReadMissBytes);
    return (
      <>
        <div className="cache-proxy-stats-heading">Statistics</div>
        <div className="cache-proxy-stat-rings">
          {this.renderReadRing(
            "Hit rate",
            "CAS Requests",
            toNumber(s.casReadHits),
            toNumber(s.casReadMisses),
            toNumber(s.casReadUncacheable),
            format.count
          )}
          {this.renderReadWriteRing("Read ratio", "CAS Requests", casReads, toNumber(s.casWrites), format.count)}
          {this.renderReadRing(
            "Hit rate",
            "CAS Bytes",
            toNumber(s.casReadHitBytes),
            toNumber(s.casReadMissBytes),
            toNumber(s.casReadUncacheableBytes),
            format.bytes
          )}
          {this.renderReadWriteRing("Read ratio", "CAS Bytes", casReadBytes, toNumber(s.casWriteBytes), format.bytes)}
        </div>
        {toNumber(s.acReadHits) > 0 && (
          <div className="cache-proxy-stat-rings">
            {this.renderReadRing(
              "Hit rate",
              "AC Requests",
              toNumber(s.acReadHits),
              toNumber(s.acReadMisses),
              0,
              format.count
            )}
            {this.renderReadWriteRing("Read ratio", "AC Requests", acReads, toNumber(s.acWrites), format.count)}
            {this.renderReadRing(
              "Hit rate",
              "AC Bytes",
              toNumber(s.acReadHitBytes),
              toNumber(s.acReadMissBytes),
              0,
              format.bytes
            )}
            {this.renderReadWriteRing("Read ratio", "AC Bytes", acReadBytes, toNumber(s.acWriteBytes), format.bytes)}
          </div>
        )}
      </>
    );
  }

  renderReadRing(
    title: string,
    subtitle: string,
    hits: number,
    misses: number,
    uncacheable: number,
    formatValue: (v: number) => string
  ) {
    return (
      <div className="cache-proxy-stat-ring">
        <div className="cache-proxy-stat-ring-title">{title}</div>
        <div className="cache-proxy-stat-ring-subtitle">{subtitle}</div>
        <div className="cache-proxy-stat-ring-row">
          {drawReadRing(hits, misses, uncacheable)}
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
            {uncacheable > 0 && (
              <div className="cache-chart-label">
                <span className="color-swatch cache-uncacheable-color-swatch"></span>
                <span className="cache-stat">{formatValue(uncacheable)}</span>
                &nbsp;uncacheable
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  renderReadWriteRing(
    title: string,
    subtitle: string,
    reads: number,
    writes: number,
    formatValue: (v: number) => string
  ) {
    return (
      <div className="cache-proxy-stat-ring">
        <div className="cache-proxy-stat-ring-title">{title}</div>
        <div className="cache-proxy-stat-ring-subtitle">{subtitle}</div>
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

export function readWriteRatio(reads: number, writes: number): string {
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

function drawReadRing(hits: number, misses: number, uncacheable: number = 0) {
  let colorHit = CACHE_COLORS.hits;
  let colorMiss = CACHE_COLORS.misses;
  let colorUncacheable = CACHE_COLORS.uncacheable;
  let h = hits;
  let m = misses;
  if (h === 0 && m === 0 && uncacheable === 0) {
    colorHit = EMPTY_COLOR;
    colorMiss = EMPTY_COLOR;
    h = 1;
  }
  return drawPie([
    { value: h, color: colorHit },
    { value: m, color: colorMiss },
    { value: uncacheable, color: colorUncacheable },
  ]);
}

function drawReadWriteRing(reads: number, writes: number) {
  if (reads === 0 && writes === 0) {
    return drawPie([{ value: 1, color: EMPTY_COLOR }]);
  }
  return drawPie([
    { value: reads, color: CACHE_COLORS.reads },
    { value: writes, color: CACHE_COLORS.writes },
  ]);
}

function drawPie(data: { value: number; color: string }[]) {
  return (
    <div className="cache-proxy-stat-ring-container">
      <ResponsiveContainer>
        <PieChart accessibilityLayer={false}>
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
