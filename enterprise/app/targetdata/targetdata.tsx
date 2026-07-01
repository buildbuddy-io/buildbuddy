import moment from "moment";
import React from "react";
import { User } from "../../../app/auth/user";
import errorService from "../../../app/errors/error_service";
import * as format from "../../../app/format/format";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { getProtoFilterParams } from "../filter/filter_util";
import TrendsChartComponent, { ChartColor, ChartDataSeries } from "../trends/trends_chart";

const MICROSECONDS_PER_SECOND = 1e6;

const COLORS = [
  ChartColor.BLUE,
  ChartColor.RED,
  ChartColor.GREEN,
  ChartColor.ORANGE,
  ChartColor.GREY,
  ChartColor.BASICALLY_BLACK,
];

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
    return this.props.search.get("target") ?? "Target Data";
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

  private renderTimelineCharts(rsp: execution_stats.GetExecutionTimelineResponse): React.ReactNode {
    const startTimes: number[] = [];
    const durationSeries: ChartDataSeries[] = [];
    const cpuSeries: ChartDataSeries[] = [];
    const memorySeries: ChartDataSeries[] = [];
    let i = 0;
    for (const timeline of rsp.timelines) {
      // X-axis values are the execution start times (in microseconds), and each
      // maps to its duration so the line series can extract it.
      startTimes.push(...timeline.execution.map((e) => +(e.startTimeUsec ?? 0)));
      const durationByStartTime = new Map<number, number>();
      const memoryByStartTime = new Map<number, number>();
      const cpuByStartTime = new Map<number, number>();
      for (const e of timeline.execution) {
        durationByStartTime.set(+(e.startTimeUsec ?? 0), +(e.durationUsec ?? 0));
        memoryByStartTime.set(+(e.startTimeUsec ?? 0), +(e.peakMemoryBytes ?? 0));
        cpuByStartTime.set(+(e.startTimeUsec ?? 0), +(e.cpuNanos ?? 0));
      }
      durationSeries.push({
        name: "duration" + i,
        isLine: true,
        extractValue: (startTimeUsec) => {
          const d = durationByStartTime.get(startTimeUsec);
          return d ? d / MICROSECONDS_PER_SECOND : null;
        },
        formatHoverValue: (value) => format.durationSec(value || 0),
        color: COLORS[i % COLORS.length],
      });
      cpuSeries.push({
        name: "cpu" + i,
        isLine: true,
        extractValue: (startTimeUsec) => cpuByStartTime.get(startTimeUsec) ?? null,
        formatHoverValue: (value) => format.durationMillis((value ?? 0) / 1e6),
        color: COLORS[i % COLORS.length],
      });
      memorySeries.push({
        name: "memory" + i,
        isLine: true,
        extractValue: (startTimeUsec) => memoryByStartTime.get(startTimeUsec) ?? null,
        formatHoverValue: (value) => format.bytes(value || 0),
        color: COLORS[i % COLORS.length],
      });

      i++;
    }

    startTimes.sort();

    return (
      <>
        <TrendsChartComponent
          title="Execution duration"
          data={startTimes}
          ticks={[]}
          dataSeries={durationSeries}
          primaryYAxis={{
            formatTickValue: format.durationSec,
            allowDecimals: false,
          }}
          formatXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("MMM D, h:mm a")}
          formatHoverXAxisLabel={(startTimeUsec) =>
            moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")
          }
        />
        <TrendsChartComponent
          title="CPU usage"
          data={startTimes}
          ticks={[]}
          dataSeries={cpuSeries}
          primaryYAxis={{
            formatTickValue: (value) => format.durationMillis(value / 1e6),
            allowDecimals: false,
          }}
          formatXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("MMM D, h:mm a")}
          formatHoverXAxisLabel={(startTimeUsec) =>
            moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")
          }
        />
        <TrendsChartComponent
          title="Peak memory usage"
          data={startTimes}
          ticks={[]}
          dataSeries={memorySeries}
          primaryYAxis={{
            formatTickValue: (value) => format.bytes(value),
            allowDecimals: false,
          }}
          formatXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("MMM D, h:mm a")}
          formatHoverXAxisLabel={(startTimeUsec) =>
            moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")
          }
        />
      </>
    );
  }

  render(): React.ReactNode {
    return (
      <div className="target-data">
        <div className="container">
          <div className="target-data-header">
            <div className="target-data-title">{this.getPageTitle()}</div>
          </div>
          {this.state.timeline && this.renderTimelineCharts(this.state.timeline)}
        </div>
      </div>
    );
  }
}
