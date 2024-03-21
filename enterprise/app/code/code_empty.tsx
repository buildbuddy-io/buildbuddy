import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import format from "../../../app/format/format";

interface State {
  repoStats: invocation.InvocationStat[];
  loading: boolean;
}

export default class CodeEmptyStateComponent extends React.Component {
  state: State = {
    repoStats: [],
    loading: false,
  };

  componentDidMount() {
    const request = new invocation.GetInvocationStatRequest({
      aggregationType: invocation.AggType.REPO_URL_AGGREGATION_TYPE,
    });
    rpcService.service
      .getInvocationStat(request)
      .then((response) => {
        console.log(response);
        this.setState({ repoStats: response.invocationStat.filter((stat) => stat.name) });
      })
      .finally(() => this.setState({ loading: false }));

    // TODO(siggisim): Get repos accessible through installations.
    // rpcService.service.getGitHubAppInstallations({}).then((r) => {
    //   console.log(r);
    // });

    // rpcService.service.getAccessibleGitHubRepos({}).then((r) => {
    //   console.log(r);
    // });

    // rpcService.service.getLinkedGitHubRepos({}).then((r) => {
    //   console.log(r);
    // });

    return;
  }

  render() {
    return (
      <div className="repo-empty">
        <div className="repo-options">
          <div className="repo-logo">BuildBuddy Code</div>
          <div className="repo-start">
            <div className="repo-title">Start</div>
            <div className="repo-section">
              <a href="/repo/?mode=code" className="repo-link">
                New repo
              </a>
            </div>
          </div>
          {this.state.repoStats?.length > 0 && (
            <div className="repo-recent">
              <div className="repo-title">Recent</div>
              <div className="repo-section">
                {this.state.repoStats.slice(0, 5).map((repo) => (
                  <a href={`/code/${format.formatGitUrl(repo.name)}`} className="repo-link">
                    {format.formatGitUrl(repo.name)}
                  </a>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }
}
