import pako from "pako";
import React from "react";
import SetupCodeComponent from "../docs/setup_code";
import FlameChart from "../flame_chart/flame_chart";
import { Profile, parseProfile } from "../flame_chart/profile_model";
import rpcService from "../service/rpc_service";
import InvocationModel from "./invocation_model";
import Button from "../components/button/button";
import { Clock, HelpCircle } from "lucide-react";
import errorService from "../errors/error_service";
import format from "../format/format";
import InvocationBreakdownCardComponent from "./invocation_breakdown_card";
import { getTimingDataSuggestion, SuggestionComponent } from "./invocation_suggestion_card";

interface Props {
  model: InvocationModel;
}

interface State {
  profile: Profile | null;
  loading: boolean;
  threadNumPages: number;
  threadToNumEventPagesMap: Map<number, number>;
  threadMap: Map<number, Thread>;
  durationMap: Map<string, number>;
  sortBy: string;
  groupBy: string;
  threadPageSize: number;
  eventPageSize: number;
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

export default class InvocationTimingCardComponent extends React.Component<Props, State> {
  state: State = {
    profile: null,
    loading: false,
    threadNumPages: 1,
    threadToNumEventPagesMap: new Map<number, number>(),
    threadMap: new Map<number, Thread>(),
    durationMap: new Map<string, number>(),
    sortBy: window.localStorage[sortByStorageKey] || sortByTimeAscStorageValue,
    groupBy: window.localStorage[groupByStorageKey] || groupByThreadStorageValue,
    threadPageSize: window.localStorage[threadPageSizeStorageKey] || 10,
    eventPageSize: window.localStorage[eventPageSizeStorageKey] || 100,
  };

  componentDidMount() {
    this.fetchProfile();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.fetchProfile();
    }
  }

  getProfileFile() {
    return this.props.model.buildToolLogs?.log.find((log: any) => log.uri);
  }

  isTimingEnabled() {
    return Boolean(this.getProfileFile()?.uri?.startsWith("bytestream://"));
  }

  fetchProfile() {
    if (!this.isTimingEnabled()) return;

    // Already fetched
    if (this.state.profile) return;

    let profileFile = this.getProfileFile();
    let compressionOption = this.props.model.optionsMap.get("json_trace_compression");
    let isGzipped = compressionOption === undefined ? profileFile.name?.endsWith(".gz") : compressionOption == "1";

    this.setState({ loading: true });
    rpcService
      .fetchBytestreamFile(profileFile?.uri, this.props.model.getId(), isGzipped ? "arraybuffer" : "json")
      .then((contents: any) => {
        if (isGzipped) {
          contents = parseProfile(pako.inflate(contents, { to: "string" }));
        }
        this.updateProfile(contents);
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  downloadProfile() {
    let profileFile = this.getProfileFile();

    try {
      rpcService.downloadBytestreamFile("timing_profile.gz", profileFile?.uri, this.props.model.getId());
    } catch {
      console.error("Error downloading bytestream timing profile");
    }
  }

  updateProfile(profile: Profile) {
    this.state.profile = profile;
    for (let event of this.state.profile?.traceEvents || []) {
      let thread = this.state.threadMap.get(event.tid) || {
        name: "",
        totalDuration: 0,
        id: event.tid,
        events: [] as any[],
      };

      if (event.dur) {
        if (this.state.durationMap.get(event.name)) {
          this.state.durationMap.set(event.name, this.state.durationMap.get(event.name) + event.dur);
        } else {
          this.state.durationMap.set(event.name, event.dur);
        }
      }

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
      threadToNumEventPagesMap: this.state.threadToNumEventPagesMap,
    });
  }

  handleMoreThreadsClicked() {
    this.setState({
      threadNumPages: this.state.threadNumPages + 1,
    });
  }

  handleSortByClicked(sortBy: string) {
    window.localStorage[sortByStorageKey] = sortBy;
    this.setState({ sortBy: sortBy });
  }

  handleGroupByClicked(groupBy: string) {
    window.localStorage[groupByStorageKey] = groupBy;
    this.setState({ groupBy: groupBy });
  }

  handleThreadPageSizeClicked(pageSize: number) {
    window.localStorage[threadPageSizeStorageKey] = pageSize;
    this.setState({ threadPageSize: pageSize });
  }

  handleEventPageSizeClicked(pageSize: number) {
    window.localStorage[eventPageSizeStorageKey] = pageSize;
    this.setState({ eventPageSize: pageSize });
  }

  getNumPagesForThread(threadId: number) {
    return this.state.threadToNumEventPagesMap.get(threadId) || 1;
  }

  renderEmptyState() {
    if (this.state.loading) {
      return <div className="loading" />;
    }

    if (!this.props.model.buildToolLogs) {
      return <>Build is in progress...</>;
    }

    // Note: This profile file should be present even if remote cache is disabled,
    // so enabling remote cache won't fix a missing profile. Show a special message
    // for this case.
    if (!this.getProfileFile()) {
      return (
        <>
          Could not find profile info. This might be because Bazel was invoked with a non-default{" "}
          <span className="inline-code">--profile</span> flag.
        </>
      );
    }

    return (
      <>
        <p>Profiling isn't enabled for this invocation.</p>
        <p>
          To enable profiling you must add gRPC remote caching. You can do so by checking <b>Enable cache</b> below,
          updating your <b>.bazelrc</b> accordingly, and re-running your invocation:
        </p>
        <SetupCodeComponent />
      </>
    );
  }

  renderTimingSuggestionCard() {
    const suggestion = getTimingDataSuggestion({ model: this.props.model, buildLogs: "" });
    return suggestion ? <SuggestionComponent suggestion={suggestion} /> : null;
  }

  render() {
    let threads = Array.from(this.state.threadMap.values());

    if (!this.state.profile) {
      return (
        <div className="card timing">
          <Clock className="icon" />
          <div className="content">
            <div className="header">
              <div className="title">Timing</div>
            </div>
            <div className="empty-state">{this.renderEmptyState()}</div>
          </div>
        </div>
      );
    }

    return (
      <>
        <FlameChart profile={this.state.profile} />
        <InvocationBreakdownCardComponent durationMap={this.state.durationMap} />

        {this.renderTimingSuggestionCard()}

        <div className="card timing">
          <Clock className="icon" />
          <div className="content">
            <div className="header">
              <div className="title">All events</div>
              <div className="button">
                <Button className="download-gz-file" onClick={this.downloadProfile.bind(this)}>
                  Download profile
                </Button>
              </div>
            </div>
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
                                <div>{format.durationUsec(event.dur)}</div>
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
                            <div>{format.durationUsec(event.dur)}</div>
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
