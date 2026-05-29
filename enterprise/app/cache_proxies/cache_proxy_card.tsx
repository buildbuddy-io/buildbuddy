import { Cloud } from "lucide-react";
import React from "react";
import format from "../../../app/format/format";
import { cache_proxy } from "../../../proto/cache_proxy_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";

interface Props {
  node: cache_proxy.CacheProxyNode;
  lastCheckInTime?: google_timestamp.protobuf.Timestamp | null;
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
        <Cloud className="icon" />
        <div className="content">
          <div className="details">
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Hostname:</div>
              <div>{this.props.node.host}</div>
            </div>
            <div className="cache-proxy-section">
              <div className="cache-proxy-section-title">Proxy ID:</div>
              <div>{this.props.node.proxyId}</div>
            </div>
            {this.props.node.proxyHostId && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Host ID:</div>
                <div>{this.props.node.proxyHostId}</div>
              </div>
            )}
            {this.props.node.osFamily && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Operating System:</div>
                <div>{this.props.node.osFamily}</div>
              </div>
            )}
            {this.props.node.arch && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Architecture:</div>
                <div>{this.props.node.arch}</div>
              </div>
            )}
            {this.props.node.version && (
              <div className="cache-proxy-section">
                <div className="cache-proxy-section-title">Version:</div>
                <div>{this.props.node.version}</div>
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
          </div>
        </div>
      </div>
    );
  }
}
