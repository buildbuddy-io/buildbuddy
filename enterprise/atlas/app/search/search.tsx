import React from "react";
import Spinner from "../../../../app/components/spinner/spinner";
import { BuildBuddyError } from "../../../../app/util/errors";
import { atlas } from "../../../../proto/atlas_ts_proto";
import EntryRow from "../components/entry_row";
import rpcService from "../lib/rpc_service";

interface Props {
  query: string;
  /** Index into the flattened results, for keyboard navigation; -1 for none. */
  selectedIndex: number;
  /** Reports the response (or none) and its results flattened in display order. */
  onResults: (response: atlas.SearchResponse | undefined, entries: atlas.Entry[]) => void;
}

interface State {
  loading: boolean;
  response?: atlas.SearchResponse;
  errorMessage?: string;
}

/** Search results grouped by kind, or the landing hint when there is no query. */
export default class SearchComponent extends React.Component<Props, State> {
  state: State = { loading: false };
  // Responses to superseded queries are dropped.
  private latestRequest = 0;

  componentDidMount() {
    this.search();
  }

  componentDidUpdate(prevProps: Props) {
    if (prevProps.query !== this.props.query) {
      this.search();
    }
  }

  private search() {
    const query = this.props.query.trim();
    const request = ++this.latestRequest;
    if (!query) {
      this.setState({ loading: false, response: undefined, errorMessage: undefined });
      this.props.onResults(undefined, []);
      return;
    }
    this.setState({ loading: true });
    rpcService.service
      .search(new atlas.SearchRequest({ query }))
      .then((response) => {
        if (request !== this.latestRequest) return;
        this.setState({ loading: false, response, errorMessage: undefined });
        this.props.onResults(response, flatten(response));
      })
      .catch((e) => {
        if (request !== this.latestRequest) return;
        this.setState({ loading: false, response: undefined, errorMessage: BuildBuddyError.parse(e).description });
        this.props.onResults(undefined, []);
      });
  }

  render() {
    const query = this.props.query.trim();
    if (!query) {
      return (
        <div className="atlas-hint">
          <div className="atlas-hint-big">🗺️</div>
          <p>Search resources in the cluster.</p>
          <p className="atlas-small">
            <kbd>/</kbd> to search · <kbd>↑</kbd>
            <kbd>↓</kbd> to pick · <kbd>Enter</kbd> to open
            <br />
            <br />
            filters: <code>kind:pod</code> <code>ns:executor</code> <code>cluster:sjc</code> <code>label:app=web</code>{" "}
            — free text matches names, images, nodes, IPs and labels
          </p>
        </div>
      );
    }
    if (this.state.errorMessage) {
      return <div className="atlas-error-panel">{this.state.errorMessage}</div>;
    }
    const response = this.state.response;
    if (!response) {
      return this.state.loading ? <Spinner className="atlas-spinner" /> : null;
    }
    if (!response.total) {
      return (
        <div className="atlas-hint">
          <div className="atlas-hint-big">🫧</div>
          nothing matches <b>{query}</b>
        </div>
      );
    }
    // Unrelated kinds can share a name across API groups; name the group
    // only when that happens.
    const kindCount = new Map<string, number>();
    for (const group of response.groups) {
      kindCount.set(group.kind, (kindCount.get(group.kind) ?? 0) + 1);
    }
    let index = 0;
    return (
      <>
        {response.groups.map((group) => (
          <div className="atlas-group" key={`${group.group}/${group.kind}`}>
            <h2>
              {group.kind}s{(kindCount.get(group.kind) ?? 0) > 1 && group.group ? ` (${group.group})` : ""}{" "}
              <span className="atlas-count">
                {group.results.length < group.total ? `${group.results.length} of ${group.total}` : group.total}
              </span>
            </h2>
            <div className="atlas-card">
              {group.results.map((result) => (
                <EntryRow
                  key={entryKey(entryOf(result))}
                  entry={entryOf(result)}
                  links={result.links}
                  selected={index++ === this.props.selectedIndex}
                />
              ))}
            </div>
          </div>
        ))}
      </>
    );
  }
}

function flatten(response: atlas.SearchResponse): atlas.Entry[] {
  return response.groups.flatMap((group) => group.results.map(entryOf));
}

function entryOf(result: atlas.SearchResult): atlas.Entry {
  return (result.entry ?? new atlas.Entry()) as atlas.Entry;
}

function entryKey(e: atlas.Entry): string {
  return [e.cluster, e.group, e.version, e.resource, e.namespace, e.name].join("/");
}
