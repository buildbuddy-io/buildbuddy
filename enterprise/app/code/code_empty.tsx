import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import format from "../../../app/format/format";

interface State {
  reposWithStats: string[];
  linkedRepos: string[];
  loading: boolean;
}

export default class CodeEmptyStateComponent extends React.Component<{}, State> {
  state: State = {
    reposWithStats: [],
    linkedRepos: [],
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
        this.setState({ reposWithStats: response.invocationStat.map((stat) => stat.name).filter((name) => name) });
      })
      .finally(() => this.setState({ loading: false }));

    // TODO(siggisim): Get repos accessible through installations.
    // rpcService.service.getGitHubAppInstallations({}).then((r) => {
    //   console.log(r);
    // });

    // rpcService.service.getAccessibleGitHubRepos({}).then((r) => {
    //   console.log(r);
    // });

    rpcService.service.getLinkedGitHubRepos({}).then((response) => {
      console.log(response);
      this.setState({ linkedRepos: response.repoUrls });
    });

    return;
  }

  render() {
    let recentRepos = new Set<string>();
    this.state.reposWithStats.forEach((r) => recentRepos.add(r));
    this.state.linkedRepos.forEach((r) => recentRepos.add(r));

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
          {recentRepos.size > 0 && (
            <div className="repo-recent">
              <div className="repo-title">Recent</div>
              <div className="repo-section">
                {[...recentRepos.values()].slice(0, 5).map((repo) => (
                  <a href={`/code/${format.formatGitUrl(repo)}`} className="repo-link">
                    {format.formatGitUrl(repo)}
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
