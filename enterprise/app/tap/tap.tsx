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
import DatePickerButton from "../filter/date_picker_button";
import { getProtoFilterParams } from "../filter/filter_util";

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

    const filterParams = getProtoFilterParams(this.props.search);

    const fetchPromise = rpcService.service
      .getInvocationStat(
        invocation.GetInvocationStatRequest.create({
          aggregationType: invocation.AggType.REPO_URL_AGGREGATION_TYPE,
          query: new invocation.InvocationStatQuery({
            updatedBefore: filterParams.updatedBefore,
            updatedAfter: filterParams.updatedAfter,
          }),
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
    const repo = this.selectedRepo();
    let title;
    let tabContent;
    if (tab === "flakes") {
      title = "Flakes";
      tabContent = <FlakesComponent repo={repo} search={this.props.search} dark={this.props.dark}></FlakesComponent>;
    } else {
      title = "Tests";
      tabContent = (
        <TestGridComponent repo={repo} search={this.props.search} user={this.props.user}></TestGridComponent>
      );
    }

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
                <div className="controls">
                  {tab === "grid" && <GridSortControlsComponent search={this.props.search}></GridSortControlsComponent>}
                  {tab === "flakes" && <DatePickerButton search={this.props.search}></DatePickerButton>}
                </div>
              </div>
            </div>
          </div>
        </div>
        {tabContent}
      </div>
    );
  }
}
