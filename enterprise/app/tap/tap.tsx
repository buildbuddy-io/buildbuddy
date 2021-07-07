import React from "react";
import Long from "long";
import moment from "moment";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { target } from "../../../proto/target_ts_proto";
import { Subscription } from "rxjs";
import router from "../../../app/router/router";
import format from "../../../app/format/format";
import Select, { Option } from "../../../app/components/select/select";

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State {
  targetHistory: target.TargetHistory[];
  loading: boolean;
  coloring: "status" | "timing";
  sort: "target" | "count" | "pass" | "avgDuration" | "maxDuration";
  direction: "asc" | "desc";
  targetLimit: number;
  invocationLimit: number;
  stats: Map<string, Stat>;
  maxInvocations: number;
  maxDuration: number;
}

interface Stat {
  count: number;
  pass: number;
  totalDuration: number;
  maxDuration: number;
  avgDuration: number;
}

const MIN_OPACITY = 0.1;
const DAYS_OF_DATA_TO_FETCH = 7;

export default class TapComponent extends React.Component<Props> {
  props: Props;

  state: State = {
    targetHistory: [],
    loading: true,
    sort: "target",
    direction: "asc",
    coloring: "status",
    targetLimit: 100,
    invocationLimit: Math.min(100, Math.max(20, (window.innerWidth - 312) / 34)),
    stats: new Map<string, Stat>(),
    maxInvocations: 0,
    maxDuration: 1,
  };

  subscription: Subscription;

  componentWillMount() {
    document.title = `Tests | BuildBuddy`;
    this.updateColoring();
    this.fetchTargets();

    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetchTargets(),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.hash !== prevProps.hash) {
      this.updateColoring();
    }
  }

  updateColoring() {
    this.setState({ coloring: this.props.hash == "#timing" ? "timing" : "status" });
  }

  fetchTargets() {
    let request = new target.GetTargetRequest();

    request.startTimeUsec = Long.fromNumber(moment().subtract(DAYS_OF_DATA_TO_FETCH, "day").utc().valueOf() * 1000);
    request.endTimeUsec = Long.fromNumber(moment().utc().valueOf() * 1000);

    this.setState({ loading: true });
    rpcService.service.getTarget(request).then((response: target.GetTargetResponse) => {
      console.log(response);
      this.updateState(response);
    });
  }

  updateState(response: target.GetTargetResponse) {
    this.state.stats.clear();

    let maxInvocations = 0;
    let maxDuration = 1;
    for (let targetHistory of response.invocationTargets) {
      let stats: Stat = { count: 0, pass: 0, totalDuration: 0, maxDuration: 0, avgDuration: 0 };
      for (let status of targetHistory.targetStatus) {
        stats.count += 1;
        let duration = this.durationToNum(status.timing.duration);
        stats.totalDuration += duration;
        stats.maxDuration = Math.max(stats.maxDuration, this.durationToNum(status.timing.duration));
        if (status.status == 5 /*PASSED*/) {
          stats.pass += 1;
        }
      }
      stats.avgDuration = stats.totalDuration / stats.count;
      maxInvocations = Math.max(maxInvocations, stats.count);
      maxDuration = Math.max(maxDuration, stats.maxDuration);
      this.state.stats.set(targetHistory.target.id, stats);
    }

    this.setState({
      loading: false,
      targetHistory: response.invocationTargets,
      stats: this.state.stats,
      maxInvocations: maxInvocations,
      maxDuration: maxDuration,
    });
  }

  navigateTo(event: any, destination: string) {
    router.navigateTo(destination);
    event.stopPropagation();
  }

  handleFilterChange(event: any) {
    let value = encodeURIComponent(event.target.value);
    router.updateParams({ filter: value });
  }

  durationToNum(duration: any) {
    return +duration.seconds + +duration.nanos / 1000000000;
  }

  statusToString(s: number) {
    // TODO(siggisim): Import the right proto here and clean this up.
    switch (s) {
      case 0:
        return "Unknown";
      case 1:
        return "Building";
      case 2:
        return "Built";
      case 3:
        return "Failed to build";
      case 4:
        return "Testing";
      case 5:
        return "Passed";
      case 6:
        return "Failed";
      case 7:
        return "Timed out";
      case 8:
        return "Cancelled";
      case 9:
        return "Tool failed";
      case 10:
        return "Incomplete";
      case 11:
        return "Flaky";
      case 12:
        return "Unknown";
      case 13:
        return "Skipped";
    }
  }

  loadMoreTargets() {
    this.setState({ targetLimit: this.state.targetLimit + 50 });
  }

  loadMoreInvocations() {
    this.setState({ invocationLimit: this.state.invocationLimit + 50 });
  }

  handleSortChange(event: any) {
    this.setState({
      sort: event.target.value,
    });
  }

  handleDirectionChange(event: any) {
    this.setState({
      direction: event.target.value,
    });
  }

  handleColorChange(event: any) {
    window.location.hash = event?.target?.value;
  }

  sort(a: target.TargetHistory, b: target.TargetHistory) {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    let firstStats = this.state.stats.get(first?.target?.id);
    let secondStats = this.state.stats.get(second?.target?.id);

    switch (this.state.sort) {
      case "target":
        return first?.target?.label.localeCompare(second?.target?.label);
      case "count":
        return firstStats.count - secondStats.count;
      case "pass":
        return firstStats.pass / firstStats.count - secondStats.pass / secondStats.count;
      case "avgDuration":
        return firstStats.avgDuration - secondStats.avgDuration;
      case "maxDuration":
        return firstStats.maxDuration - secondStats.maxDuration;
    }
  }

  render() {
    if (this.state.loading) {
      return <div className="loading"></div>;
    }

    if (!this.state.targetHistory.length) {
      return (
        <div className="tap">
          <div className="container narrow">
            <div className="tap-header">
              <div className="tap-title">Test grid</div>
            </div>
            <div className="empty-state history">
              <h2>No CI tests found in the last week!</h2>
              <p>
                Seems like you haven't done any builds marked as CI. Add <b>--build_metadata=ROLE=CI</b> to your builds
                to see a CI test grid. You'll likely also want to provide a commit SHA and optionally a git repo url.
                <br />
                <br />
                Check out the Build Metadata Guide below for more information on configuring these.
                <br />
                <br />
                <a className="button" target="_blank" href="https://buildbuddy.io/docs/guide-metadata">
                  View the Build Metadata Guide
                </a>
              </p>
            </div>
          </div>
        </div>
      );
    }

    let filter = this.props.search.get("filter") ? decodeURIComponent(this.props.search.get("filter")) : "";

    return (
      <div className="tap">
        <div className="container">
          <div className="tap-header">
            <div className="tap-title">Test grid</div>

            <div className="controls">
              <div className="tap-sort-controls">
                <div className="tap-sort-control">
                  <span className="tap-sort-title">Sort by</span>
                  <Select onChange={this.handleSortChange.bind(this)} value={this.state.sort}>
                    <Option value="target">Target name</Option>
                    <Option value="count">Invocation count</Option>
                    <Option value="pass">Pass percentage</Option>
                    <Option value="avgDuration">Average duration</Option>
                    <Option value="maxDuration">Max duration</Option>
                  </Select>
                </div>
                <div className="tap-sort-control">
                  <span className="tap-sort-title">Direction</span>
                  <Select onChange={this.handleDirectionChange.bind(this)} value={this.state.direction}>
                    <Option value="asc">Asc</Option>
                    <Option value="desc">Desc</Option>
                  </Select>
                </div>
                <div className="tap-sort-control">
                  <span className="tap-sort-title">Color</span>
                  <Select onChange={this.handleColorChange.bind(this)} value={this.state.coloring}>
                    <Option value="status">Test status</Option>
                    <Option value="timing">Test duration</Option>
                  </Select>
                </div>
              </div>
            </div>
          </div>

          <div className="filter">
            <img src="/image/filter.svg" />
            <input
              value={filter}
              className="filter-input"
              placeholder="Filter..."
              onChange={this.handleFilterChange.bind(this)}
            />
          </div>

          <div className="tap-grid-container">
            <div className="tap-grid">
              {/* 
            // TODO(siggisim): Figure out the best way to give a consistent x-axis
            <div className="tap-row">
              {this.state.targetHistory.length > 0 &&
                this.state.targetHistory[0].targetStatus.map((status: target.TargetStatus) => {
                  let destinationUrl = `/history/commit/${status.commitSha}`;
                  return (
                    <a
                      href={destinationUrl}
                      onClick={this.navigateTo.bind(this, destinationUrl)}
                      className={`tap-block tap-block-header`}>
                      {format.compactDurationSec(Date.now() / 1000 - +status.timing.startTime.seconds)} ago
                      <br />
                      {status.commitSha}
                    </a>
                  );
                })}
            </div> */}

              {this.state.targetHistory
                .filter((targetHistory) => (filter ? targetHistory.target.label.includes(filter) : true))
                .sort(this.sort.bind(this))
                .slice(0, this.state.targetLimit)
                .map((targetHistory) => {
                  let targetParts = targetHistory.target.label.split(":");
                  let targetPath = targetParts.length > 0 ? targetParts[0] : "";
                  let targetName = targetParts.length > 1 ? targetParts[1] : "";
                  let stats = this.state.stats.get(targetHistory.target.id);
                  return (
                    <div key={targetHistory.target.id} className="tap-target">
                      <div title={targetHistory.target.ruleType} className="tap-target-label">
                        <span className="tap-target-path">{targetPath}</span>:
                        <span className="tap-target-name">{targetName}</span>
                        <span className="tap-target-stats">
                          ({format.formatWithCommas(stats.count)} invocations,{" "}
                          {format.percent(stats.pass / stats.count)}% pass, {format.durationSec(stats.avgDuration)} avg,{" "}
                          {format.durationSec(stats.maxDuration)} max)
                        </span>
                      </div>
                      <div className="tap-row">
                        {targetHistory.targetStatus
                          .slice(0, this.state.invocationLimit)
                          .map((status: target.TargetStatus) => {
                            let destinationUrl = `/invocation/${status.invocationId}?target=${targetHistory.target.label}`;
                            return (
                              <a
                                key={targetHistory.target.id + status.invocationId}
                                href={destinationUrl}
                                onClick={this.navigateTo.bind(this, destinationUrl)}
                                title={
                                  this.state.coloring == "timing"
                                    ? `${this.durationToNum(status.timing.duration).toFixed(2)}s`
                                    : this.statusToString(status.status)
                                }
                                style={{
                                  opacity:
                                    this.state.coloring == "timing"
                                      ? Math.max(
                                          MIN_OPACITY,
                                          (1.0 * this.durationToNum(status.timing.duration)) / stats.maxDuration
                                        )
                                      : undefined,
                                }}
                                className={`tap-block ${
                                  this.state.coloring == "status" ? `status-${status.status}` : "timing"
                                } clickable`}></a>
                            );
                          })}
                      </div>
                    </div>
                  );
                })}
              {this.state.targetHistory.length > this.state.targetLimit && (
                <button className="more-targets-button" onClick={this.loadMoreTargets.bind(this)}>
                  Load more targets
                </button>
              )}
            </div>
            {this.state.maxInvocations > this.state.invocationLimit && (
              <button className="more-invocations-button" onClick={this.loadMoreInvocations.bind(this)}>
                Load more
              </button>
            )}
          </div>
        </div>
      </div>
    );
  }
}
