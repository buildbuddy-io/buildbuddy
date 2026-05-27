import { Cloud } from "lucide-react";
import React from "react";
import format from "../../../app/format/format";
import { cache_proxy } from "../../../proto/cache_proxy_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";

interface Props {
  node: cache_proxy.CacheProxyNode;
  lastCheckInTime?: google_timestamp.protobuf.Timestamp | null;
}

export default class CacheProxyCardComponent extends React.Component<Props> {
  render() {
    return (
      <div className="card card-neutral">
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
