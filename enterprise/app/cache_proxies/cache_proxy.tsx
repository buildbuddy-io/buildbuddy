import { ArrowUpDown, Cpu, Gauge, LucideIcon, Server, Settings } from "lucide-react";
import React from "react";
import { Subscription } from "rxjs";
import { User } from "../../../app/auth/auth_service";
import Breadcrumbs from "../../../app/components/breadcrumbs/breadcrumbs";
import DonutChart from "../../../app/components/chart/donut_chart";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import { Link } from "../../../app/components/link/link";
import format from "../../../app/format/format";
import router, { Path } from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { cache_proxy } from "../../../proto/cache_proxy_ts_proto";
import {
  HIT_COLOR,
  hitRate,
  MISS_COLOR,
  READ_COLOR,
  readWriteRatio,
  toNumber,
  UNCACHEABLE_COLOR,
  WRITE_COLOR,
} from "./cache_proxy_card";

// cacheProxyIdFromPath returns the proxy ID from a /cache-proxy/<id> path, or
// "" if the path has no ID or its escapes are malformed.
function cacheProxyIdFromPath(path: string): string {
  try {
    return router.getLastPathComponent(path.replace(/\/+$/, ""), Path.cacheProxyPath) ?? "";
  } catch {
    return "";
  }
}

interface Props {
  user: User;
  path: string;
  // The region the proxy is registered in, for multi-region deployments.
  region?: string;
}

interface State {
  details: cache_proxy.ICacheProxyDetails | null;
  loading: boolean;
  error: BuildBuddyError | null;
  flagFilter: string;
}

export default class CacheProxyComponent extends React.Component<Props, State> {
  state: State = {
    details: null,
    loading: true,
    error: null,
    flagFilter: "",
  };

  subscription?: Subscription;

  proxyId(): string {
    return cacheProxyIdFromPath(this.props.path);
  }

  componentDidMount() {
    document.title = `Cache Proxy | BuildBuddy`;
    this.fetch();
    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetch(),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  async fetch() {
    if (!this.proxyId()) {
      this.setState({ loading: false, error: new BuildBuddyError("NotFound", "No cache proxy specified.") });
      return;
    }
    let service = rpcService.service;
    if (this.props.region) {
      const regional = rpcService.regionalServices.get(this.props.region);
      if (!regional) {
        this.setState({
          loading: false,
          error: new BuildBuddyError("NotFound", `Unknown region "${this.props.region}".`),
        });
        return;
      }
      service = regional;
    }
    this.setState({ loading: true, error: null });
    try {
      const response = await service.getCacheProxy(
        cache_proxy.GetCacheProxyRequest.create({
          selector: cache_proxy.CacheProxySelector.create({ proxyId: this.proxyId() }),
          includeConfiguredFlags: true,
          includeStatistics: true,
        })
      );
      const host = response.details?.summary?.host;
      if (host) {
        document.title = `${host} | Cache Proxy | BuildBuddy`;
      }
      this.setState({ details: response.details ?? null });
    } catch (e) {
      this.setState({ error: BuildBuddyError.parse(e) });
    } finally {
      this.setState({ loading: false });
    }
  }

  render() {
    const summary = this.state.details?.summary;
    return (
      <div className="cache-proxies-page cache-proxy-page">
        <div className="shelf">
          <div className="container">
            <div className="cache-proxy-header-top">
              <Breadcrumbs>
                {this.props.user && <span>{this.props.user.selectedGroupName()}</span>}
                <Link href="/cache-proxies/status">Cache Proxies</Link>
                <span>{summary?.host || this.proxyId()}</span>
              </Breadcrumbs>
            </div>
            <div className="title">{summary?.host || "Cache proxy"}</div>
            {this.props.region && (
              <div className="cache-proxy-chips">
                <span className="cache-proxy-chip">{this.props.region}</span>
              </div>
            )}
          </div>
        </div>
        {this.state.error && <div className="error-message">{this.state.error.message}</div>}
        {this.state.loading && !this.state.details && <div className="loading"></div>}
        {this.state.details && this.renderDetails(this.state.details)}
      </div>
    );
  }

  renderDetails(details: cache_proxy.ICacheProxyDetails) {
    const summary: cache_proxy.ICacheProxySummary = details.summary ?? {};
    const stats = details.statistics;
    const labels = Object.entries(summary.labels ?? {}).sort(([a], [b]) => a.localeCompare(b));
    const cpuMillis = toNumber(summary.allocatedCpuMillis);
    const memoryBytes = toNumber(summary.allocatedMemoryBytes);
    return (
      <div className="container cache-proxy-detail-page">
        <div className="cache-proxy-panels">
          <Panel Icon={Server} title="Identity">
            <KeyValue label="Hostname">{summary.host}</KeyValue>
            <KeyValue label="Proxy instance ID" mono>
              {summary.proxyId}
            </KeyValue>
            <KeyValue label="Proxy host ID" mono>
              {summary.proxyHostId}
            </KeyValue>
            <KeyValue label="Version">{summary.version}</KeyValue>
            <KeyValue label="Operating system">{summary.osFamily}</KeyValue>
            <KeyValue label="Architecture">{summary.arch}</KeyValue>
            <KeyValue label="Labels">
              {labels.length > 0 && (
                <div className="cache-proxy-labels">
                  {labels.map(([k, v]) => (
                    <span
                      key={k}
                      className="cache-proxy-label"
                      style={{ "--label-hue": format.colorHashHue(k) } as React.CSSProperties}>
                      <span className="cache-proxy-label-key">{k}</span>
                      <span className="cache-proxy-label-value">{v}</span>
                    </span>
                  ))}
                </div>
              )}
            </KeyValue>
          </Panel>
          <Panel Icon={Cpu} title="Resources">
            <KeyValue label="CPU">{cpuMillis > 0 && `${cpuMillis / 1000} cores`}</KeyValue>
            <KeyValue label="Memory">{memoryBytes > 0 && format.bytes(memoryBytes)}</KeyValue>
            <KeyValue label="Started">{summary.startTime && format.formatTimestamp(summary.startTime)}</KeyValue>
            <KeyValue label="Uptime">{summary.startTime && format.durationSince(summary.startTime)}</KeyValue>
            <KeyValue label="Last check-in">
              {summary.lastCheckInTime && format.relativeTimeSeconds(summary.lastCheckInTime)}
            </KeyValue>
          </Panel>
        </div>

        {stats && (
          <Panel Icon={Gauge} title="Read Performance">
            <div className="cache-proxy-stat-rings">
              {readChart(
                "AC hits (digests)",
                toNumber(stats.acReadHits),
                toNumber(stats.acReadMisses),
                toNumber(stats.acReadUncacheable),
                format.count
              )}
              {readChart(
                "AC hits (bytes)",
                toNumber(stats.acReadHitBytes),
                toNumber(stats.acReadMissBytes),
                toNumber(stats.acReadUncacheableBytes),
                format.bytes
              )}
              {readChart(
                "CAS hits (digests)",
                toNumber(stats.casReadHits),
                toNumber(stats.casReadMisses),
                toNumber(stats.casReadUncacheable),
                format.count
              )}
              {readChart(
                "CAS hits (bytes)",
                toNumber(stats.casReadHitBytes),
                toNumber(stats.casReadMissBytes),
                toNumber(stats.casReadUncacheableBytes),
                format.bytes
              )}
            </div>
          </Panel>
        )}

        {stats && (
          <Panel Icon={ArrowUpDown} title="Read/Write Performance">
            <div className="cache-proxy-stat-rings">
              {readWriteChart(
                "AC reads/writes (digests)",
                toNumber(stats.acReadHits) + toNumber(stats.acReadMisses),
                toNumber(stats.acWrites),
                format.count
              )}
              {readWriteChart(
                "AC reads/writes (bytes)",
                toNumber(stats.acReadHitBytes) + toNumber(stats.acReadMissBytes),
                toNumber(stats.acWriteBytes),
                format.bytes
              )}
              {readWriteChart(
                "CAS reads/writes (digests)",
                toNumber(stats.casReadHits) + toNumber(stats.casReadMisses),
                toNumber(stats.casWrites),
                format.count
              )}
              {readWriteChart(
                "CAS reads/writes (bytes)",
                toNumber(stats.casReadHitBytes) + toNumber(stats.casReadMissBytes),
                toNumber(stats.casWriteBytes),
                format.bytes
              )}
            </div>
          </Panel>
        )}

        {details.configuredFlags && details.configuredFlags.length > 0 && this.renderFlags(details.configuredFlags)}
      </div>
    );
  }

  renderFlags(flags: string[]) {
    const filter = this.state.flagFilter.toLowerCase();
    const matching = flags.filter((f) => f.toLowerCase().includes(filter));
    return (
      <Panel Icon={Settings} title="Configuration">
        <FilterInput
          className="cache-proxy-flag-filter"
          placeholder="Filter flags..."
          value={this.state.flagFilter}
          onChange={(e) => this.setState({ flagFilter: e.target.value })}
          rightElement={filter ? `${matching.length} of ${flags.length}` : undefined}
        />
        <div className="cache-proxy-flags">
          {matching.map((f) => {
            const eq = f.indexOf("=");
            return (
              <div key={f} className="cache-proxy-flag">
                <span className="cache-proxy-flag-name">{eq >= 0 ? f.substring(0, eq) : f}</span>
                {eq >= 0 && <span className="cache-proxy-flag-value">={f.substring(eq + 1)}</span>}
              </div>
            );
          })}
          {matching.length === 0 && <div className="cache-proxy-muted">No matching flags</div>}
        </div>
      </Panel>
    );
  }
}

function Panel({ Icon, title, children }: { Icon: LucideIcon; title: string; children: React.ReactNode }) {
  return (
    <div className="cache-proxy-panel">
      <div className="cache-proxy-panel-title">
        <Icon />
        {title}
      </div>
      {children}
    </div>
  );
}

function KeyValue({ label, mono, children }: { label: string; mono?: boolean; children?: React.ReactNode }) {
  return (
    <div className="cache-proxy-kv">
      <div className="cache-proxy-kv-label">{label}</div>
      <div className={`cache-proxy-kv-value ${mono ? "mono" : ""}`}>{children || "—"}</div>
    </div>
  );
}

const statColors: Record<string, string> = {
  hits: HIT_COLOR,
  misses: MISS_COLOR,
  uncacheable: UNCACHEABLE_COLOR,
  reads: READ_COLOR,
  writes: WRITE_COLOR,
};

function statChart(
  title: string,
  data: Record<string, number>,
  valueFormatter: (v: number) => string,
  subtitle?: string
) {
  return (
    <div className="cache-proxy-stat-ring">
      <DonutChart
        title={title}
        subtitle={subtitle}
        data={Object.entries(data).map(([name, value]) => ({ name, value }))}
        colorPicker={(name) => statColors[name]}
        valueFormatter={valueFormatter}
        showEmpty
      />
    </div>
  );
}

function readChart(title: string, hits: number, misses: number, uncacheable: number, format: (v: number) => string) {
  return statChart(title, { hits, misses, uncacheable }, format, `${hitRate(hits, misses)} hit rate`);
}

function readWriteChart(title: string, reads: number, writes: number, format: (v: number) => string) {
  return statChart(title, { reads, writes }, format, `${readWriteRatio(reads, writes)} read:write`);
}
