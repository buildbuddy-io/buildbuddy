import { Hash } from "lucide-react";
import React from "react";
import { Subscription } from "rxjs";
import { User } from "../../../app/auth/auth_service";
import Breadcrumbs from "../../../app/components/breadcrumbs/breadcrumbs";
import LinkButton from "../../../app/components/button/link_button";
import router from "../../../app/router/router";
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
    return (
      <>
        <h1>Set up self-hosted cache proxies</h1>
        <hr />
        <h2>1. Create an API key for cache proxy registration</h2>
        {this.props.proxyKeys.length == 0 && (
          <div>
            <p>There are no API keys with the cache proxy capability configured for your organization.</p>
            <p>API keys are used to authorize self-hosted cache proxies.</p>
            {this.props.user.canCall("createApiKey") && (
              <LinkButton href="/settings/org/api-keys" className="manage-keys-button">
                Manage keys
              </LinkButton>
            )}
          </div>
        )}
        {this.props.proxyKeys.length > 0 && (
          <>
            <div>
              {this.props.proxyKeys.length == 1 && <p>You have one Cache Proxy API key available.</p>}
              {this.props.proxyKeys.length > 1 && (
                <p>You have {this.props.proxyKeys.length} Cache Proxy API keys available.</p>
              )}
              <p>These API keys can be used to register your cache proxies.</p>
            </div>
            <h2>2. Deploy cache proxies</h2>
            <p>
              Start each cache proxy with <code>--cache_proxy.api_key=&lt;key&gt;</code> and{" "}
              <code>--cache_proxy.app_target=&lt;app gRPC target&gt;</code>. Once running, the proxy will appear on the
              Status tab.
            </p>
          </>
        )}
      </>
    );
  }
}

interface CacheProxiesListProps {
  regions: RegionalCacheProxyResponse[];
}

class CacheProxiesList extends React.Component<CacheProxiesListProps> {
  render() {
    const proxiesByKey = new Map<string, RegionalCacheProxy>();
    for (const r of this.props.regions) {
      for (const proxy of r.response.cacheProxy) {
        if (!proxy.node) {
          continue;
        }
        proxiesByKey.set(`${r.name}-${proxy.node.proxyId}`, {
          region: r.name,
          proxy,
          node: proxy.node as cache_proxy.CacheProxyNode,
        });
      }
    }
    const keys = Array.from(proxiesByKey.keys()).sort();

    return (
      <div className="cache-proxy-cards">
        <div className="cache-proxy-summary">
          <Hash />
          <span>
            <b>
              {keys.length} {keys.length === 1 ? "cache proxy" : "cache proxies"}
            </b>
          </span>
        </div>
        {keys.map((key) => {
          const regionalProxy = proxiesByKey.get(key);
          if (!regionalProxy) {
            return null;
          }
          const { region, proxy, node } = regionalProxy;
          return (
            <CacheProxyCardComponent
              key={key}
              region={region}
              node={node}
              lastCheckInTime={proxy.lastCheckInTime}
              statistics={proxy.statistics}
            />
          );
        })}
      </div>
    );
  }
}

type TabId = "status" | "setup";

interface Props {
  user: User;
  path: string;
}

type RegionalCacheProxy = {
  region: string;
  proxy: cache_proxy.GetCacheProxiesResponse.ICacheProxy;
  node: cache_proxy.CacheProxyNode;
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
}

export default class CacheProxiesComponent extends React.Component<Props, State> {
  state: State = {
    regions: [],
    proxyKeys: [],
    loading: [],
    error: null,
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
    const hasProxies = this.state.regions.some((r) => r.response.cacheProxy.some((proxy) => proxy.node));
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
                </div>
                {activeTab === "status" && (
                  <>
                    {!hasProxies && (
                      <div className="empty-state">
                        <h1>No cache proxies are registered.</h1>
                        <p>Click the "Setup" tab for instructions on self-hosting cache proxies.</p>
                      </div>
                    )}
                    {hasProxies && <CacheProxiesList regions={this.state.regions} />}
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
