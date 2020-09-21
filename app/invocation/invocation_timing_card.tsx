import pako from "pako";
import React from "react";
import capabilities from "../capabilities/capabilities";
import SetupCodeComponent from "../docs/setup_code";
import FlameChart from "../flame_chart/flame_chart";
import rpcService from "../service/rpc_service";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
}

interface State {
  threadNumPages: number;
  threadToNumEventPagesMap: Map<number, number>;
  profile: any;
  threadMap: Map<number, Thread>;
  timingEnabled: boolean;
  buildInProgress: boolean;
  /** Whether the "command.profile.gz" file is missing from BuildToolLogs. */
  isMissingProfile: boolean;
  sortBy: string;
  groupBy: string;
  threadPageSize: number;
  eventPageSize: number;
  error: string;
}

interface Thread {
  id: number;
  totalDuration: number;
  name: string;
  events: any[];
}

const sortByStorageKey = "InvocationTimingCardComponent.sortBy";
const groupByStorageKey = "InvocationTimingCardComponent.groupBy";
const threadPageSizeStorageKey = "InvocationTimingCardComponent.threadPageSize";
const eventPageSizeStorageKey = "InvocationTimingCardComponent.eventPageSize";
const sortByTimeAscStorageValue = "time-asc";
const sortByDurationDescStorageValue = "duration-desc";
const groupByThreadStorageValue = "thread";
const groupByAllStorageValue = "all";

export default class InvocationTimingCardComponent extends React.Component {
  props: Props;

  state: State = {
    threadNumPages: 1,
    threadToNumEventPagesMap: new Map<number, number>(),
    profile: null,
    threadMap: new Map<number, Thread>(),
    timingEnabled: true,
    buildInProgress: false,
    isMissingProfile: false,
    sortBy: window.localStorage[sortByStorageKey] || sortByTimeAscStorageValue,
    groupBy: window.localStorage[groupByStorageKey] || groupByThreadStorageValue,
    threadPageSize: window.localStorage[threadPageSizeStorageKey] || 10,
    eventPageSize: window.localStorage[eventPageSizeStorageKey] || 100,
    error: "",
  };

  componentDidMount() {
    this.fetchProfile();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.fetchProfile();
    }
  }

  fetchProfile() {
    let profileUrl = this.props.model.buildToolLogs?.log.find((log: any) => log.name == "command.profile.gz")?.uri;

    if (!profileUrl) {
      const hasBuildToolLogs = this.props.model.buildToolLogs;
      this.setState({
        ...this.state,
        buildInProgress: !hasBuildToolLogs,
        isMissingProfile: hasBuildToolLogs,
      });
      return;
    }

    if (!profileUrl.startsWith("bytestream://")) {
      this.setState({
        ...this.state,
        timingEnabled: false,
        buildInProgress: false,
      });
      return;
    }

    if (this.state.profile) {
      // Already fetched
      return;
    }

    rpcService
      .fetchBytestreamFile(profileUrl, this.props.model.getId(), "arraybuffer")
      .then((contents: Uint8Array) => {
        let decompressedResponse = pako.inflate(contents, { to: "string" });
        this.updateProfile(JSON.parse(decompressedResponse));
      })
      .catch(() => {
        console.error("Error loading bytestream timing profile!");
        this.setState({
          ...this.state,
          error: "Error loading timing profile. Make sure your cache is correctly configured.",
        });
      });
  }

  updateProfile(profile: any) {
    console.log(profile);
    this.state.profile = profile;
    for (let event of this.state.profile?.traceEvents || []) {
      let thread = this.state.threadMap.get(event.tid) || {
        name: "",
        totalDuration: 0,
        id: event.tid,
        events: [] as any[],
      };

      if (event.ph == "X") {
        // Duration events
        thread.events.push(event);
        thread.totalDuration += event.dur;
      } else if (event.ph == "M" && event.name == "thread_name") {
        // Metadata events
        thread.name = event.args.name;
      }

      this.state.threadMap.set(event.tid, thread);
    }
    this.state.buildInProgress = false;
    this.state.error = "";
    this.setState(this.state);
  }

  sortIdAsc(a: any, b: any) {
    return a.id - b.id;
  }

  sortDurationDesc(a: any, b: any) {
    return b.dur - a.dur;
  }

  sortTimeAsc(a: any, b: any) {
    return a.ts - b.ts;
  }

  handleMoreEventsClicked(threadId: number) {
    this.state.threadToNumEventPagesMap.set(threadId, this.getNumPagesForThread(threadId) + 1);
    this.setState({
      ...this.state,
      threadToNumEventPagesMap: this.state.threadToNumEventPagesMap,
    });
  }

  handleMoreThreadsClicked() {
    this.setState({
      ...this.state,
      threadNumPages: this.state.threadNumPages + 1,
    });
  }

  handleSortByClicked(sortBy: string) {
    window.localStorage[sortByStorageKey] = sortBy;
    this.setState({ ...this.state, sortBy: sortBy });
  }

  handleGroupByClicked(groupBy: string) {
    window.localStorage[groupByStorageKey] = groupBy;
    this.setState({ ...this.state, groupBy: groupBy });
  }

  handleThreadPageSizeClicked(pageSize: number) {
    window.localStorage[threadPageSizeStorageKey] = pageSize;
    this.setState({ ...this.state, threadPageSize: pageSize });
  }

  handleEventPageSizeClicked(pageSize: number) {
    window.localStorage[eventPageSizeStorageKey] = pageSize;
    this.setState({ ...this.state, eventPageSize: pageSize });
  }

  getNumPagesForThread(threadId: number) {
    return this.state.threadToNumEventPagesMap.get(threadId) || 1;
  }

  formatDuration(durationMicros: number) {
    let durationSeconds = durationMicros / 1000000;
    if (durationSeconds < 100) {
      return durationSeconds.toPrecision(3);
    }
    return durationSeconds.toFixed(0);
  }

  render() {
    let threads = Array.from(this.state.threadMap.values());
    return (
      <>
        {this.state.profile && <FlameChart profile={this.state.profile} />}
        <div className="card timing">
          <img className="icon" src="/image/clock-regular.svg" />
          <div className="content">
            <div className="title">All events</div>
            <div className="sort-controls">
              <div className="sort-control">
                Sort by&nbsp;
                <u
                  onClick={this.handleSortByClicked.bind(this, sortByTimeAscStorageValue)}
                  className={`clickable ${this.state.sortBy == sortByTimeAscStorageValue && "selected"}`}>
                  start time
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleSortByClicked.bind(this, sortByDurationDescStorageValue)}
                  className={`clickable ${this.state.sortBy == sortByDurationDescStorageValue && "selected"}`}>
                  duration
                </u>
              </div>
              <div className="sort-control">
                Group by&nbsp;
                <u
                  onClick={this.handleGroupByClicked.bind(this, groupByThreadStorageValue)}
                  className={`clickable ${this.state.groupBy == "thread" && "selected"}`}>
                  thread
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleGroupByClicked.bind(this, groupByAllStorageValue)}
                  className={`clickable ${this.state.groupBy == groupByAllStorageValue && "selected"}`}>
                  flat
                </u>
              </div>
              <div className="sort-control">
                Threads&nbsp;
                <u
                  onClick={this.handleThreadPageSizeClicked.bind(this, 10)}
                  className={`clickable ${this.state.threadPageSize == 10 && "selected"}`}>
                  10
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleThreadPageSizeClicked.bind(this, 100)}
                  className={`clickable ${this.state.threadPageSize == 100 && "selected"}`}>
                  100
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleThreadPageSizeClicked.bind(this, 1000)}
                  className={`clickable ${this.state.threadPageSize == 1000 && "selected"}`}>
                  1000
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleThreadPageSizeClicked.bind(this, 10000)}
                  className={`clickable ${this.state.threadPageSize == 10000 && "selected"}`}>
                  10000
                </u>
              </div>
              <div className="sort-control">
                Events&nbsp;
                <u
                  onClick={this.handleEventPageSizeClicked.bind(this, 10)}
                  className={`clickable ${this.state.eventPageSize == 10 && "selected"}`}>
                  10
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleEventPageSizeClicked.bind(this, 100)}
                  className={`clickable ${this.state.eventPageSize == 100 && "selected"}`}>
                  100
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleEventPageSizeClicked.bind(this, 1000)}
                  className={`clickable ${this.state.eventPageSize == 1000 && "selected"}`}>
                  1000
                </u>{" "}
                |&nbsp;
                <u
                  onClick={this.handleEventPageSizeClicked.bind(this, 10000)}
                  className={`clickable ${this.state.eventPageSize == 10000 && "selected"}`}>
                  10000
                </u>
              </div>
            </div>
            {this.state.buildInProgress && <div className="empty-state">Build is in progress...</div>}
            {this.state.isMissingProfile && (
              <div className="empty-state">
                Could not find profile info. This might be because Bazel was invoked with a non-default{" "}
                <span className="inline-code">--profile</span> flag.
              </div>
            )}
            {!this.state.timingEnabled && (
              <div className="empty-state">
                Profiling isn't enabled for this invocation.
                <br />
                <br />
                To enable profiling you must add GRPC remote caching. You can do so by checking <b>Enable cache</b>{" "}
                below, updating your <b>.bazelrc</b> accordingly, and re-running your invocation:
                <SetupCodeComponent />
              </div>
            )}
            {this.state.error && <div className="empty-state">{this.state.error}</div>}
            {this.state.groupBy == groupByThreadStorageValue && (
              <div className="details">
                {threads
                  .sort(this.sortIdAsc)
                  .slice(0, this.state.threadNumPages * this.state.threadPageSize)
                  .map((thread: Thread) => (
                    <div>
                      <div className="list-title">
                        <div>{thread.name}</div>
                      </div>
                      <ul>
                        {thread.events
                          .sort(
                            this.state.sortBy == sortByTimeAscStorageValue ? this.sortTimeAsc : this.sortDurationDesc
                          )
                          .slice(0, this.state.eventPageSize * this.getNumPagesForThread(thread.id))
                          .map((event) => (
                            <li>
                              <div className="list-grid">
                                <div>
                                  {event.name} {event.args?.target}
                                </div>
                                <div>{this.formatDuration(event.dur)} seconds</div>
                              </div>
                              <div
                                className="list-percent"
                                data-percent={`${(100 * (event.dur / this.props.model.getDurationMicros())).toFixed(
                                  0
                                )}%`}
                                style={{
                                  width: `${(100 * (event.dur / this.props.model.getDurationMicros())).toPrecision(
                                    3
                                  )}%`,
                                }}></div>
                            </li>
                          ))}
                      </ul>
                      {thread.events.length > this.state.eventPageSize * this.getNumPagesForThread(thread.id) &&
                        !!this.state.eventPageSize && (
                          <div className="more" onClick={this.handleMoreEventsClicked.bind(this, thread.id)}>
                            See more events
                          </div>
                        )}
                    </div>
                  ))}
              </div>
            )}
            {this.state.groupBy == groupByAllStorageValue && (
              <div className="details">
                <div>
                  <div className="list-title">
                    <div>All events</div>
                  </div>
                  <ul>
                    {threads
                      .flatMap((thread: Thread) => thread.events)
                      .sort(this.state.sortBy == sortByTimeAscStorageValue ? this.sortTimeAsc : this.sortDurationDesc)
                      .slice(0, this.state.eventPageSize * this.getNumPagesForThread(0))
                      .map((event) => (
                        <li>
                          <div className="list-grid">
                            <div>{event.name}</div>
                            <div>{this.formatDuration(event.dur)} seconds</div>
                          </div>
                          <div
                            className="list-percent"
                            data-percent={`${(100 * (event.dur / this.props.model.getDurationMicros())).toFixed(0)}%`}
                            style={{
                              width: `${(100 * (event.dur / this.props.model.getDurationMicros())).toPrecision(3)}%`,
                            }}></div>
                        </li>
                      ))}
                  </ul>
                  {threads.flatMap((thread: Thread) => thread.events).length >
                    this.state.eventPageSize * this.getNumPagesForThread(0) &&
                    !!this.state.eventPageSize && (
                      <div className="more" onClick={this.handleMoreEventsClicked.bind(this, 0)}>
                        See more events
                      </div>
                    )}
                </div>
              </div>
            )}
            {this.state.groupBy == groupByThreadStorageValue &&
              threads.length > this.state.threadPageSize * this.state.threadNumPages &&
              !!this.state.threadPageSize && (
                <div className="more" onClick={this.handleMoreThreadsClicked.bind(this)}>
                  See more threads
                </div>
              )}
          </div>
        </div>
      </>
    );
  }
}
