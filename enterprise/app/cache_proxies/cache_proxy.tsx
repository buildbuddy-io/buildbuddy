import { ArrowUpDown, Bug, Cpu, Gauge, LucideIcon, Server, Settings } from "lucide-react";
import React from "react";
import { Subscription } from "rxjs";
import { User } from "../../../app/auth/auth_service";
import Breadcrumbs from "../../../app/components/breadcrumbs/breadcrumbs";
import { OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import { Link } from "../../../app/components/link/link";
import Modal from "../../../app/components/modal/modal";
import format from "../../../app/format/format";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { cache_proxy } from "../../../proto/cache_proxy_ts_proto";
import { ReadRing, ReadWriteRing, toNumber } from "./cache_proxy_card";

interface Props {
  user: User;
  proxyId: string;
  // The region the proxy is registered in, for multi-region deployments.
  region?: string;
}

interface State {
  details: cache_proxy.ICacheProxyDetails | null;
  loading: boolean;
  error: BuildBuddyError | null;
  flagFilter: string;
  debugModalOpen: boolean;
}

export default class CacheProxyComponent extends React.Component<Props, State> {
  state: State = {
    details: null,
    loading: true,
    error: null,
    flagFilter: "",
    debugModalOpen: false,
  };

  subscription?: Subscription;

  componentDidMount() {
    document.title = `Cache Proxy | BuildBuddy`;
    this.fetch();
    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetch(),
    });
  }

  componentDidUpdate(prevProps: Props) {
    if (prevProps.proxyId !== this.props.proxyId || prevProps.region !== this.props.region) {
      this.fetch();
    }
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  async fetch() {
    this.setState({ loading: true, error: null });
    const service = (this.props.region && rpcService.regionalServices.get(this.props.region)) || rpcService.service;
    try {
      const response = await service.getCacheProxy(
        cache_proxy.GetCacheProxyRequest.create({
          selector: cache_proxy.CacheProxySelector.create({ proxyId: this.props.proxyId }),
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
                <span>{summary?.host || this.props.proxyId}</span>
              </Breadcrumbs>
              <OutlinedButton
                className="xsmall-button cache-proxy-debug-button"
                onClick={() => this.setState({ debugModalOpen: true })}>
                <Bug />
                Debug
              </OutlinedButton>
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
        {this.renderDebugModal()}
      </div>
    );
  }

  renderDebugModal() {
    const close = () => this.setState({ debugModalOpen: false });
    return (
      <Modal isOpen={this.state.debugModalOpen} onRequestClose={close}>
        <Dialog>
          <DialogHeader>
            <DialogTitle>Debug</DialogTitle>
          </DialogHeader>
          <DialogBody>hi</DialogBody>
          <DialogFooter>
            <DialogFooterButtons>
              <OutlinedButton onClick={close}>Close</OutlinedButton>
            </DialogFooterButtons>
          </DialogFooter>
        </Dialog>
      </Modal>
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
              <ReadRing
                title="AC hits (digests)"
                hits={toNumber(stats.acReadHits)}
                misses={toNumber(stats.acReadMisses)}
                uncacheable={toNumber(stats.acReadUncacheable)}
                formatValue={format.count}
              />
              <ReadRing
                title="AC hits (bytes)"
                hits={toNumber(stats.acReadHitBytes)}
                misses={toNumber(stats.acReadMissBytes)}
                uncacheable={toNumber(stats.acReadUncacheableBytes)}
                formatValue={format.bytes}
              />
              <ReadRing
                title="CAS hits (digests)"
                hits={toNumber(stats.casReadHits)}
                misses={toNumber(stats.casReadMisses)}
                uncacheable={toNumber(stats.casReadUncacheable)}
                formatValue={format.count}
              />
              <ReadRing
                title="CAS hits (bytes)"
                hits={toNumber(stats.casReadHitBytes)}
                misses={toNumber(stats.casReadMissBytes)}
                uncacheable={toNumber(stats.casReadUncacheableBytes)}
                formatValue={format.bytes}
              />
            </div>
          </Panel>
        )}

        {stats && (
          <Panel Icon={ArrowUpDown} title="Read/Write Performance">
            <div className="cache-proxy-stat-rings">
              <ReadWriteRing
                title="AC reads/writes (digests)"
                reads={toNumber(stats.acReadHits) + toNumber(stats.acReadMisses)}
                writes={toNumber(stats.acWrites)}
                formatValue={format.count}
              />
              <ReadWriteRing
                title="AC reads/writes (bytes)"
                reads={toNumber(stats.acReadHitBytes) + toNumber(stats.acReadMissBytes)}
                writes={toNumber(stats.acWriteBytes)}
                formatValue={format.bytes}
              />
              <ReadWriteRing
                title="CAS reads/writes (digests)"
                reads={toNumber(stats.casReadHits) + toNumber(stats.casReadMisses)}
                writes={toNumber(stats.casWrites)}
                formatValue={format.count}
              />
              <ReadWriteRing
                title="CAS reads/writes (bytes)"
                reads={toNumber(stats.casReadHitBytes) + toNumber(stats.casReadMissBytes)}
                writes={toNumber(stats.casWriteBytes)}
                formatValue={format.bytes}
              />
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
