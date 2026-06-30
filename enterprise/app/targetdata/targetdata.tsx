import React from "react";
import { User } from "../../../app/auth/user";
import errorService from "../../../app/errors/error_service";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { getProtoFilterParams } from "../filter/filter_util";

interface Props {
  user: User;
  search: URLSearchParams;
}

interface State {
  loading: boolean;
  timeline?: execution_stats.GetExecutionTimelineResponse;
}

export default class TargetDataComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
  };

  private currentTarget?: string;
  private pendingTimelineRequest?: CancelablePromise;

  private getPageTitle() {
    return "Target Data";
  }

  private updateDocumentTitle() {
    document.title = `${this.getPageTitle()} | BuildBuddy`;
  }

  componentDidMount(): void {
    this.updateDocumentTitle();
    this.fetchExecutionTimeline();
  }

  componentDidUpdate(): void {
    this.updateDocumentTitle();
    this.fetchExecutionTimeline();
  }

  componentWillUnmount(): void {
    this.pendingTimelineRequest?.cancel();
  }

  private fetchExecutionTimeline(): void {
    const target = this.props.search.get("target") ?? "";
    // Avoid re-fetching when the target hasn't changed across updates.
    if (target === this.currentTarget) {
      return;
    }
    this.currentTarget = target;

    this.pendingTimelineRequest?.cancel();

    if (!target) {
      this.setState({ loading: false, timeline: undefined });
      return;
    }

    const filterParams = getProtoFilterParams(this.props.search);
    let query = new execution_stats.ExecutionQuery({
      invocationHost: filterParams.host,
      invocationUser: filterParams.user,
      repoUrl: filterParams.repo,
      branchName: filterParams.branch,
      commitSha: filterParams.commit,
      command: filterParams.command,
      pattern: filterParams.pattern,
      tags: filterParams.tags,
      role: filterParams.role || [],
      updatedAfter: filterParams.updatedAfter,
      updatedBefore: filterParams.updatedBefore,
      invocationStatus: filterParams.status || [],
      filter: [],
      dimensionFilter: filterParams.dimensionFilters,
      genericFilters: filterParams.genericFilters,
    });

    const request = new execution_stats.GetExecutionTimelineRequest({
      target,
      query,
    });

    this.setState({ loading: true, timeline: undefined });
    this.pendingTimelineRequest = rpcService.service
      .getExecutionTimeline(request)
      .then((response) => {
        console.log(response);
        this.setState({ timeline: response });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  render(): React.ReactNode {
    return (
      <div className="target-data">
        <div className="container">
          <div className="target-data-header">
            <div className="target-data-title">{this.getPageTitle()}</div>
          </div>
        </div>
      </div>
    );
  }
}
