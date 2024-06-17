import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import router, { Path } from "../../../app/router/router";
import format from "../../../app/format/format";
import Select, { Option } from "../../../app/components/select/select";
import capabilities from "../../../app/capabilities/capabilities";
import errorService from "../../../app/errors/error_service";
import { normalizeRepoURL } from "../../../app/util/git";
import TestGridComponent from "./grid";
import FlakesComponent from "./flakes";
import GridSortControlsComponent from "./grid_sort_controls";

interface Props {
  user: User;
  tab: string;
  search: URLSearchParams;
  dark: boolean;
}

interface State {
  selectedRepo?: string;
  repos: string[];
}

type Tab = "grid" | "flakes";

const LAST_SELECTED_REPO_LOCALSTORAGE_KEY = "tests__last_selected_repo";

export default class TapComponent extends React.Component<Props, State> {
  state: State = {
    repos: [],
  };

  isV2 = Boolean(capabilities.config.testGridV2Enabled);

  componentWillMount() {
    document.title = `Tests | BuildBuddy`;
    this.fetchRepos();
  }

  componentDidUpdate() {
    localStorage[LAST_SELECTED_REPO_LOCALSTORAGE_KEY] = this.selectedRepo();
  }

  getSelectedTab(): Tab {
    if (capabilities.config.targetFlakesUiEnabled && this.props.tab === "#flakes") {
      return "flakes";
    }
    return "grid";
  }

  updateSelectedTab(tab: Tab) {
    router.navigateTo(Path.tapPath + "#" + tab);
  }

  fetchRepos(): Promise<void> {
    if (!this.isV2) return Promise.resolve();

    // If we've already got a repo selected (from the last time we visited the page),
    // keep the repo selected and populate the full repo list in the background.
    const selectedRepo = this.selectedRepo();
    if (selectedRepo) this.setState({ repos: [selectedRepo] });

    const fetchPromise = rpcService.service
      .getInvocationStat(
        invocation.GetInvocationStatRequest.create({
          aggregationType: invocation.AggType.REPO_URL_AGGREGATION_TYPE,
        })
      )
      .then((response) => {
        const repos = response.invocationStat.filter((stat) => stat.name).map((stat) => stat.name);
        if (selectedRepo && !repos.includes(selectedRepo)) {
          repos.push(selectedRepo);
        }
        this.setState({ repos: repos.sort() });
      })
      .catch((e) => errorService.handleError(e));

    return selectedRepo ? Promise.resolve() : fetchPromise;
  }

  selectedRepo(): string {
    const repo = this.props.search.get("repo");
    if (repo) return normalizeRepoURL(repo);

    const lastSelectedRepo = localStorage[LAST_SELECTED_REPO_LOCALSTORAGE_KEY];
    if (lastSelectedRepo) return normalizeRepoURL(lastSelectedRepo);

    return this.state?.repos[0] || "";
  }

  handleRepoChange(event: React.ChangeEvent<HTMLSelectElement>) {
    const repo = event.target.value;
    router.replaceParams({ repo });
  }

  render() {
    const tab = this.getSelectedTab();
    let tabContent;
    const repo = this.selectedRepo();
    if (tab === "flakes") {
      tabContent = <FlakesComponent repo={repo} search={this.props.search} dark={this.props.dark}></FlakesComponent>;
    } else {
      tabContent = (
        <TestGridComponent repo={repo} search={this.props.search} user={this.props.user}></TestGridComponent>
      );
    }

    const title = capabilities.config.targetFlakesUiEnabled ? "Test history" : "Test grid";

    return (
      <div className={`tap ${this.isV2 ? "v2" : ""}`}>
        <div className={`tap-top-bar  ${tab !== "flakes" ? "stick" : ""}`}>
          <div className="container">
            <div className="tap-header-group">
              <div className="tap-header">
                <div className="tap-header-left-section">
                  <div className="tap-title">{title}</div>
                  {this.isV2 && this.state.repos.length > 0 && (
                    <Select
                      onChange={this.handleRepoChange.bind(this)}
                      value={this.selectedRepo()}
                      className="repo-picker">
                      {this.state.repos.map((repo) => (
                        <Option key={repo} value={repo}>
                          {format.formatGitUrl(repo)}
                        </Option>
                      ))}
                    </Select>
                  )}
                </div>
                {tab === "grid" && (
                  <div className="controls">
                    <GridSortControlsComponent search={this.props.search}></GridSortControlsComponent>
                  </div>
                )}
              </div>
              {capabilities.config.targetFlakesUiEnabled && (
                <div className="tabs">
                  <div
                    onClick={() => this.updateSelectedTab("grid")}
                    className={`tab ${this.getSelectedTab() === "grid" ? "selected" : ""}`}>
                    Test Grid
                  </div>
                  <div
                    onClick={() => this.updateSelectedTab("flakes")}
                    className={`tab ${this.getSelectedTab() === "flakes" ? "selected" : ""}`}>
                    Flakes
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
        {tabContent}
      </div>
    );
  }
}
