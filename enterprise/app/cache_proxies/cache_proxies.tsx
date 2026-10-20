import { CheckCircle2, Cpu, ExternalLink, Globe, Hash, Laptop, LucideIcon } from "lucide-react";
import React from "react";
import { Subscription } from "rxjs";
import { User } from "../../../app/auth/auth_service";
import Breadcrumbs from "../../../app/components/breadcrumbs/breadcrumbs";
import LinkButton, { OutlinedLinkButton } from "../../../app/components/button/link_button";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import UpgradePrompt, { mostUrgent } from "../../../app/components/upgrade/upgrade";
import ViewModeToggle, { ViewMode } from "../../../app/components/view_mode_toggle/view_mode_toggle";
import router, { Path } from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { api_key } from "../../../proto/api_key_ts_proto";
import { cache_proxy } from "../../../proto/cache_proxy_ts_proto";
import { capability } from "../../../proto/capability_ts_proto";
import CacheProxyCardComponent from "./cache_proxy_card";

enum FetchType {
  Proxies,
  ApiKeys,
}

interface CacheProxySetupProps {
  user: User;
  proxyKeys: api_key.IApiKey[];
}

class CacheProxySetup extends React.Component<CacheProxySetupProps> {
  render() {
    const keyCount = this.props.proxyKeys.length;
    const canManageKeys = this.props.user.canCall("createApiKey");

    return (
      <div className="cache-proxy-setup">
        <div className="cache-proxy-setup-header">
          <div>
            <h1>Set up self-hosted cache proxies</h1>
          </div>
          <OutlinedLinkButton href="https://www.buildbuddy.io/docs/enterprise-proxy" target="_blank" rel="noreferrer">
            Documentation <ExternalLink className="icon" aria-hidden="true" />
          </OutlinedLinkButton>
        </div>

        <ol className="cache-proxy-setup-steps">
          <li className="cache-proxy-setup-step">
            <span className="cache-proxy-setup-step-number" aria-hidden="true">
              1
            </span>
            <div className="cache-proxy-setup-step-content">
              <h2>Create a registration API key</h2>
              <p>Use an organization API key with the Cache Proxy capability to authorize your proxies.</p>
              {keyCount > 0 && (
                <div className="cache-proxy-setup-key-status">
                  <CheckCircle2 aria-hidden="true" />
                  <span>
                    {keyCount} {keyCount === 1 ? "API key available" : "API keys available"}
                  </span>
                </div>
              )}
              {keyCount === 0 && !canManageKeys && <p>Ask an organization admin to create a Cache Proxy API key.</p>}
              {canManageKeys && (
                <LinkButton href="/settings/org/api-keys" className="small-button">
                  {keyCount > 0 ? "Manage API keys" : "Create API key"}
                </LinkButton>
              )}
            </div>
          </li>
          <li className="cache-proxy-setup-step">
            <span className="cache-proxy-setup-step-number" aria-hidden="true">
              2
            </span>
            <div className="cache-proxy-setup-step-content">
              <h2>Deploy your cache proxies</h2>
              <p>Start each proxy with these flags, using your API key and the app's gRPC target.</p>
              <pre className="cache-proxy-setup-flags">
                <code>{"--cache_proxy.api_key=<key>\n--cache_proxy.app_target=<app gRPC target>"}</code>
              </pre>
              <p className="cache-proxy-setup-hint">Once running, your proxies will appear on the Status tab.</p>
            </div>
          </li>
        </ol>
      </div>
    );
  }
}

// matchesFilter reports whether every whitespace-separated term of the
// (lowercased) filter matches one of the proxy's identifying fields, labels, or
// its region. Terms are further split on '=' and ':', and all of those parts
// must match the same field, so that "env: prod" matches the label "env=prod".
function matchesFilter(region: string, summary: cache_proxy.ICacheProxySummary, filter: string): boolean {
  const terms = filter
    .replace(/\s*([=:])\s*/g, "$1")
    .split(/\s+/)
    .map((t) => t.split(/[=:]/).filter(Boolean))
    .filter((t) => t.length);
  if (!terms.length) return true;
  const fields = [
    region,
    summary.host,
    summary.proxyId,
    summary.proxyHostId,
    summary.version,
    summary.osFamily,
    summary.arch,
    ...Object.entries(summary.labels ?? {}).map(([k, v]) => `${k}=${v}`),
  ];
  const lowered = fields.filter((f): f is string => !!f).map((f) => f.toLowerCase());
  return terms.every((parts) => lowered.some((f) => parts.every((p) => f.includes(p))));
}

interface CacheProxiesListProps {
  regions: RegionalCacheProxyResponse[];
  summary: boolean;
  filter: string;
}

class CacheProxiesList extends React.Component<CacheProxiesListProps> {
  render() {
    // Cluster proxies by region + OS + architecture.
    const filter = this.props.filter.trim().toLowerCase();
    const proxiesByCluster = new Map<string, RegionalCacheProxy[]>();
    for (const region of this.props.regions) {
      for (const proxy of region.response.cacheProxy) {
        if (!proxy.summary || !matchesFilter(region.name, proxy.summary, filter)) {
          continue;
        }
        const summary = proxy.summary as cache_proxy.CacheProxySummary;
        const key = `${region.name}-${summary.osFamily || ""}-${summary.arch || ""}`;
        if (!proxiesByCluster.has(key)) {
          proxiesByCluster.set(key, []);
        }
        proxiesByCluster.get(key)!.push({ region: region.name, proxy, summary });
      }
    }
    const keys = Array.from(proxiesByCluster.keys()).sort();
    if (keys.length === 0) {
      return <div className="cache-proxy-no-matches">No matching cache proxies.</div>;
    }

    return (
      <div className="cache-proxy-cards">
        {keys.map((key) => {
          const proxies = proxiesByCluster.get(key);
          if (!proxies || proxies.length === 0) {
            return null;
          }
          const { region, summary } = proxies[0];
          return (
            <React.Fragment key={key}>
              <div className="cache-proxy-details">
                {region && (
                  <CacheProxyDetail Icon={Globe} label="">
                    {region}
                  </CacheProxyDetail>
                )}
                <CacheProxyDetail Icon={Hash} label="">
                  {proxies.length} {proxies.length === 1 ? "cache proxy" : "cache proxies"}
                </CacheProxyDetail>
                <CacheProxyDetail Icon={Laptop} label="OS">
                  {summary.osFamily || "unknown"}
                </CacheProxyDetail>
                <CacheProxyDetail Icon={Cpu} label="Arch">
                  {summary.arch || "unknown"}
                </CacheProxyDetail>
              </div>
              {proxies.map((p) => (
                <CacheProxyCardComponent
                  key={`${p.region}-${p.summary.proxyId}`}
                  proxy={p.summary}
                  lastCheckInTime={p.proxy.lastCheckInTime}
                  statistics={p.proxy.statistics}
                  summary={this.props.summary}
                  href={
                    `${Path.cacheProxyPath}${encodeURIComponent(p.summary.proxyId)}` +
                    (p.region ? `?region=${encodeURIComponent(p.region)}` : "")
                  }
                />
              ))}
            </React.Fragment>
          );
        })}
      </div>
    );
  }
}

function CacheProxyDetail({ Icon, label, children }: { Icon: LucideIcon; label: string; children: React.ReactNode }) {
  return (
    <span className="cache-proxy-detail">
      <Icon />
      <span>
        {label && <>{label}: </>}
        <b>{children}</b>
      </span>
    </span>
  );
}

type TabId = "status" | "setup";

interface Props {
  user: User;
  path: string;
}

type RegionalCacheProxy = {
  region: string;
  proxy: cache_proxy.GetCacheProxiesResponse.ICacheProxy;
  summary: cache_proxy.CacheProxySummary;
};

type RegionalCacheProxyResponse = {
  name: string;
  response: cache_proxy.GetCacheProxiesResponse;
};

interface State {
  regions: RegionalCacheProxyResponse[];
  proxyKeys: api_key.IApiKey[];
  loading: FetchType[];
  error: BuildBuddyError | null;
  viewMode: ViewMode;
  filter: string;
}

export default class CacheProxiesComponent extends React.Component<Props, State> {
  state: State = {
    regions: [],
    proxyKeys: [],
    loading: [],
    error: null,
    viewMode: "summary",
    filter: "",
  };

  subscription?: Subscription;

  componentWillMount() {
    document.title = `Cache Proxies | BuildBuddy`;
  }

  componentDidMount() {
    this.fetch();
    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetch(),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  async fetchApiKeys() {
    if (!this.props.user) return;
    this.setState((prevState) => ({
      loading: [...prevState.loading, FetchType.ApiKeys],
    }));
    try {
      const response = await rpcService.service.getApiKeys(api_key.GetApiKeysRequest.create({}));
      const proxyKeys = response.apiKey.filter((key) =>
        key.capability.some((cap) => cap == capability.Capability.REGISTER_CACHE_PROXY)
      );
      this.setState({ proxyKeys });
    } catch (e) {
      this.setState({ error: BuildBuddyError.parse(e) });
    } finally {
      this.setState((prevState) => ({
        loading: [...prevState.loading].filter((f) => f != FetchType.ApiKeys),
      }));
    }
  }

  async fetchCacheProxies() {
    this.setState((prevState) => ({
      loading: [...prevState.loading, FetchType.Proxies],
    }));
    try {
      let regions: RegionalCacheProxyResponse[];
      if (rpcService.regionalServices.size) {
        const results = await Promise.allSettled(
          Array.from(rpcService.regionalServices).map(([name, service]) =>
            service.getCacheProxies(cache_proxy.GetCacheProxiesRequest.create({})).then((resp) => {
              return { name: name, response: resp };
            })
          )
        );
        regions = results.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []));
        if (regions.length == 0) {
          const rejected = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
          throw rejected?.reason;
        }
      } else {
        regions = [
          await rpcService.service.getCacheProxies(cache_proxy.GetCacheProxiesRequest.create({})).then((resp) => {
            return { name: "", response: resp };
          }),
        ];
      }
      this.setState({ regions });
    } catch (e) {
      this.setState({ error: BuildBuddyError.parse(e) });
    } finally {
      this.setState((prevState) => ({
        loading: [...prevState.loading].filter((f) => f != FetchType.Proxies),
      }));
    }
  }

  fetch() {
    this.fetchCacheProxies();
    this.fetchApiKeys();
  }

  onClickTab(tabId: TabId) {
    router.navigateTo(`/cache-proxies/${tabId}`);
  }

  onChangeViewMode(viewMode: ViewMode) {
    this.setState({ viewMode });
  }

  renderFilterCount() {
    const filter = this.state.filter.trim().toLowerCase();
    let total = 0;
    let matching = 0;
    for (const region of this.state.regions) {
      for (const proxy of region.response.cacheProxy) {
        if (!proxy.summary) continue;
        total++;
        if (matchesFilter(region.name, proxy.summary, filter)) matching++;
      }
    }
    return `${matching} of ${total}`;
  }

  renderEmpty() {
    return (
      <div className="empty-state">
        <h1>No cache proxies found!</h1>
        <p>
          Cache proxies are self-hosted gRPC proxies that sit in front of the BuildBuddy cache to reduce latency and
          bandwidth.
          <br />
          <br />
          <a className="button" href="https://www.buildbuddy.io/docs/enterprise-proxy">
            Read the docs to get started
          </a>
        </p>
      </div>
    );
  }

  render() {
    const hasProxies = this.state.regions.some((r) => r.response.cacheProxy.some((proxy) => proxy.summary));
    const hasKeys = this.state.proxyKeys.length > 0;
    // When neither proxies nor cache-proxy API keys exist, skip the tab UI
    // and show a single clean empty-state view (matching the executors page
    // pattern when nothing has been set up).
    const showTabs = hasProxies || hasKeys;
    const defaultTabId: TabId = hasProxies ? "status" : "setup";
    const activeTab = (this.props.path.substring("/cache-proxies/".length) || defaultTabId) as TabId;
    return (
      <div className="cache-proxies-page">
        <div className="shelf">
          <div className="container">
            <Breadcrumbs>
              {this.props.user && <span>{this.props.user?.selectedGroupName()}</span>}
              <span>Cache Proxies</span>
            </Breadcrumbs>
            <div className="title">Cache Proxies</div>
          </div>
        </div>
        {this.state.error && <div className="error-message">{this.state.error.message}</div>}
        {this.state.loading.length > 0 && <div className="loading"></div>}
        {this.state.loading.length == 0 && this.state.error == null && (
          <div className="container">
            {!showTabs && this.renderEmpty()}
            {showTabs && (
              <>
                <div className="tabs">
                  <div
                    className={`tab ${activeTab === "status" ? "selected" : ""}`}
                    onClick={this.onClickTab.bind(this, "status")}>
                    Status
                  </div>
                  <div
                    className={`tab ${activeTab === "setup" ? "selected" : ""}`}
                    onClick={this.onClickTab.bind(this, "setup")}>
                    Setup
                  </div>
                  {activeTab === "status" && hasProxies && (
                    <div className="cache-proxy-list-controls">
                      <FilterInput
                        className="cache-proxy-filter"
                        placeholder="Filter cache proxies..."
                        value={this.state.filter}
                        onChange={(e) => this.setState({ filter: e.target.value })}
                        rightElement={this.state.filter.trim() ? this.renderFilterCount() : undefined}
                      />
                      <ViewModeToggle viewMode={this.state.viewMode} onChange={this.onChangeViewMode.bind(this)} />
                    </div>
                  )}
                </div>
                {activeTab === "status" && (
                  <>
                    <UpgradePrompt prompt={mostUrgent(this.state.regions.map((r) => r.response.upgradePrompt))} />
                    {!hasProxies && (
                      <div className="empty-state">
                        <h1>No cache proxies are registered.</h1>
                        <p>Click the "Setup" tab for instructions on self-hosting cache proxies.</p>
                      </div>
                    )}
                    {hasProxies && (
                      <CacheProxiesList
                        regions={this.state.regions}
                        summary={this.state.viewMode === "summary"}
                        filter={this.state.filter}
                      />
                    )}
                  </>
                )}
                {activeTab === "setup" && <CacheProxySetup user={this.props.user} proxyKeys={this.state.proxyKeys} />}
              </>
            )}
          </div>
        )}
      </div>
    );
  }
}
