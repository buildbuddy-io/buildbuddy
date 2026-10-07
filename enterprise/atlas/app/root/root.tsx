import React from "react";
import TextInput from "../../../../app/components/input/input";
import { atlas } from "../../../../proto/atlas_ts_proto";
import ClustersComponent from "../clusters/clusters";
import ClusterPicker from "../components/cluster_picker";
import Link from "../components/link";
import ThemeToggle from "../components/theme_toggle";
import router, { Route, paths } from "../lib/router";
import rpcService from "../lib/rpc_service";
import * as theme from "../lib/theme";
import ObjectComponent from "../object/object";
import SearchComponent from "../search/search";

interface State {
  route: Route;
  /** The search box's text; follows the URL and drives it. */
  query: string;
  /** Keyboard selection within the current results; -1 for none. */
  selectedIndex: number;
  results: atlas.Entry[];
  searchMeta: string;
  status?: { text: string; error: boolean };
}

const SEARCH_DEBOUNCE_MS = 120;
const STATUS_REFRESH_MS = 15_000;

export default class RootComponent extends React.Component<{}, State> {
  state: State = {
    route: router.current(),
    query: queryOf(router.current()),
    selectedIndex: -1,
    results: [],
    searchMeta: "",
  };
  private input = React.createRef<HTMLInputElement>();
  private searchTimeout?: number;
  private statusTimer?: number;
  private unsubscribe?: () => void;

  componentDidMount() {
    this.unsubscribe = router.subscribe(this.onRouteChange);
    document.addEventListener("keydown", this.onDocumentKeyDown);
    theme.apply();
    this.refreshStatus();
    this.statusTimer = window.setInterval(this.refreshStatus, STATUS_REFRESH_MS);
  }

  componentWillUnmount() {
    this.unsubscribe?.();
    document.removeEventListener("keydown", this.onDocumentKeyDown);
    window.clearInterval(this.statusTimer);
    window.clearTimeout(this.searchTimeout);
  }

  private onRouteChange = () => {
    // A query still waiting to reach the URL must not replace the page just
    // navigated to.
    window.clearTimeout(this.searchTimeout);
    const route = router.current();
    this.setState((state) => ({
      route,
      query: route.kind === "search" ? route.query : route.kind === "home" ? "" : state.query,
    }));
  };

  private onQueryChange = (query: string) => {
    this.setState({ query });
    window.clearTimeout(this.searchTimeout);
    this.searchTimeout = window.setTimeout(() => router.replace(paths.search(query)), SEARCH_DEBOUNCE_MS);
  };

  private onInputKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    switch (e.key) {
      case "ArrowDown":
        this.moveSelection(1);
        e.preventDefault();
        break;
      case "ArrowUp":
        this.moveSelection(-1);
        e.preventDefault();
        break;
      case "Enter": {
        const entry = this.state.results[this.state.selectedIndex];
        if (entry) router.navigateTo(paths.object(entry));
        break;
      }
      case "Escape":
        this.input.current?.blur();
        break;
    }
  };

  private onDocumentKeyDown = (e: KeyboardEvent) => {
    if (e.key === "/" && document.activeElement !== this.input.current) {
      this.input.current?.focus();
      this.input.current?.select();
      e.preventDefault();
    }
  };

  private moveSelection(delta: number) {
    this.setState((state) => ({
      selectedIndex: Math.min(state.results.length - 1, Math.max(0, state.selectedIndex + delta)),
    }));
  }

  private onResults = (response: atlas.SearchResponse | undefined, results: atlas.Entry[]) => {
    this.setState({
      results,
      // The top hit is selected from the start, so Enter opens what is
      // highlighted.
      selectedIndex: results.length ? 0 : -1,
      searchMeta: response ? `${response.total} in ${(Number(response.tookUsec) / 1000).toFixed(1)}ms` : "",
    });
  };

  private refreshStatus = () => {
    rpcService.service
      .getStatus(new atlas.GetStatusRequest())
      .then((response) => {
        const total = response.clusters.reduce((n, c) => n + c.totalObjects, 0);
        this.setState({
          status: {
            text: `${response.clusters.length} clusters · ${total.toLocaleString()} objects`,
            error: response.clusters.some((c) => c.discoveryError || c.resources.some((r) => r.error)),
          },
        });
      })
      .catch(() => {
        // The header chip is best-effort.
      });
  };

  private renderRoute() {
    const route = this.state.route;
    switch (route.kind) {
      case "home":
      case "search":
        return (
          <SearchComponent
            query={route.kind === "search" ? route.query : ""}
            selectedIndex={this.state.selectedIndex}
            onResults={this.onResults}
          />
        );
      case "object":
        // Keyed, so a response for one object can never render on another's page.
        return <ObjectComponent key={paths.object(route.ref)} objectRef={route.ref} />;
      case "clusters":
        return <ClustersComponent />;
      case "unknown":
        return <div className="atlas-hint">nothing at {route.path}</div>;
    }
  }

  render() {
    const showMeta = this.state.route.kind === "search" || this.state.route.kind === "home";
    return (
      <div className="atlas-root">
        <header className="atlas-header">
          <Link to={paths.home} className="atlas-logo">
            🗺️ <span>atlas</span>
          </Link>
          <div className="atlas-searchbox">
            <TextInput
              ref={this.input}
              className="atlas-search-input"
              value={this.state.query}
              placeholder="Search everything…   (try: kind:pod ns:executor cluster:sjc label:app=web crashloop)"
              autoFocus
              autoComplete="off"
              spellCheck={false}
              onChange={(e) => this.onQueryChange(e.target.value)}
              onKeyDown={this.onInputKeyDown}
            />
            {showMeta && <span className="atlas-search-meta">{this.state.searchMeta}</span>}
          </div>
          <Link to={paths.clusters} className={`atlas-navlink ${this.state.status?.error ? "error" : ""}`}>
            {this.state.status?.text ?? "clusters"}
          </Link>
          <ClusterPicker />
          <ThemeToggle />
        </header>
        <main className="atlas-main">{this.renderRoute()}</main>
      </div>
    );
  }
}

function queryOf(route: Route): string {
  return route.kind === "search" ? route.query : "";
}
