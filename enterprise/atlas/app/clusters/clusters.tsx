import React from "react";
import Spinner from "../../../../app/components/spinner/spinner";
import { BuildBuddyError } from "../../../../app/util/errors";
import { atlas } from "../../../../proto/atlas_ts_proto";
import { Badge } from "../components/badges";
import rpcService from "../lib/rpc_service";

interface State {
  response?: atlas.GetStatusResponse;
  errorMessage?: string;
}

/** Per-cluster index status: what is watched, how much of it, and what failed. */
export default class ClustersComponent extends React.Component<{}, State> {
  state: State = {};

  componentDidMount() {
    rpcService.service
      .getStatus(new atlas.GetStatusRequest())
      .then((response) => this.setState({ response }))
      .catch((e) => this.setState({ errorMessage: BuildBuddyError.parse(e).description }));
  }

  render() {
    if (this.state.errorMessage) {
      return <div className="atlas-error-panel">{this.state.errorMessage}</div>;
    }
    const response = this.state.response;
    if (!response) {
      return <Spinner className="atlas-spinner" />;
    }
    if (!response.clusters.length) {
      return <div className="atlas-hint">no clusters configured</div>;
    }
    return (
      <>
        {response.clusters.map((c) => (
          <div className="atlas-card atlas-cluster-card" key={c.name}>
            <div className="atlas-cluster-head">
              <span className="atlas-cluster-name">{c.name}</span>
              <span className="atlas-muted">
                {c.totalObjects} objects · {c.resources.length} resource types
              </span>
              <span className="atlas-muted atlas-small">
                {c.svcZone ? `svc: ${c.svcZone}` : ""} {c.podZone ? ` pod: ${c.podZone}` : ""}
              </span>
              {c.discoveryError && <span className="atlas-error-text">discovery: {c.discoveryError}</span>}
            </div>
            <table className="atlas-table">
              <thead>
                <tr>
                  <th>resource</th>
                  <th>count</th>
                  <th></th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {[...c.resources]
                  .sort((a, b) => b.count - a.count)
                  .map((r) => (
                    <tr key={`${r.resource?.group}/${r.resource?.version}/${r.resource?.resource}`}>
                      <td className="atlas-mono">
                        {r.resource?.resource}
                        {r.resource?.group && <span className="atlas-muted atlas-small">.{r.resource.group}</span>}
                      </td>
                      <td>{r.count}</td>
                      <td>{r.synced ? <Badge tone="ok">synced</Badge> : <Badge tone="warn">syncing</Badge>}</td>
                      <td className="atlas-error-text atlas-wrap">{r.error}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        ))}
      </>
    );
  }
}
