import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import format from "../../../app/format/format";
import { Code } from "lucide-react";

interface State {
  repoStats: invocation.IInvocationStat[];
  loading: boolean;
}

const RECOMMENDED_REPOS = [
  "buildbuddy-io/buildbuddy",
  "bazelbuild/bazel",
  "bazelbuild/bazel-gazelle",
  "abseil/abseil-cpp",
  "tensorflow/tensorflow",
  "cockroachdb/cockroach",
];

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

    return;
  }

  render() {
    return (
      <div className="">
        <div className="code-menu">
          <div className="code-menu-logo">
            <a href="/">
              <img alt="BuildBuddy Code" src="/image/logo_dark.svg" className="logo" /> Code{" "}
              <Code className="icon code-logo" />
            </a>
          </div>
        </div>
        <div className="repo-previews">
          {this.state.repoStats?.length > 0 && (
            <>
              <div className="repo-previews-title">Your Repos</div>
              <div className="repo-previews-section">
                {this.state.repoStats.map((repo) => this.renderRepo(format.formatGitUrl(repo.name)))}
              </div>
            </>
          )}
          <div className="repo-previews-title">Recommended Repos</div>
          <div className="repo-previews-section">{RECOMMENDED_REPOS.map(this.renderRepo)}</div>
        </div>
      </div>
    );
  }

  renderRepo(repo: string) {
    return (
      <div className="repo-preview">
        <a href={`/code/${repo}`}>
          <img
            src={`https://opengraph.githubassets.com/678d0aac73c1525b882f63e6a2978a53b80d99d1788ddb16863183a38a66f98a/${repo}`}
          />
        </a>
      </div>
    );
  }
}
