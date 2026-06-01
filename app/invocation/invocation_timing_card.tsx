import { Clock, Download } from "lucide-react";
import React from "react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import capabilities from "../capabilities/capabilities";
import Button, { OutlinedButton } from "../components/button/button";
import LinkButton from "../components/button/link_button";
import SetupCodeComponent from "../docs/setup_code";
import errorService from "../errors/error_service";
import format from "../format/format";
import rpcService, { CancelablePromise, FileEncoding } from "../service/rpc_service";
import { Profile, readProfile, Thread } from "../trace/compact_trace";
import TimingProfileDropTarget from "../trace/timing_profile_drop_target";
import TraceViewer from "../trace/trace_viewer";
import InvocationBreakdownCardComponent from "./invocation_breakdown_card";
import InvocationModel from "./invocation_model";
import { getTimingDataSuggestion, SuggestionComponent } from "./invocation_suggestion_card";

interface Props {
  model: InvocationModel;
  dark: boolean;
}

interface State {
  profile: Profile | null;
  loading: boolean;
  threadNumPages: number;
  threadToNumEventPagesMap: Map<number, number>;
  threadMap: Map<number, Thread>;
  durationByNameMap: Map<string, number>;
  durationByCategoryMap: Map<string, number>;
  sortBy: string;
  groupBy: string;
  threadPageSize: number;
  eventPageSize: number;
  localProfileName: string;
  viewerKey: number;
}

interface TraceEventRef {
  thread: Thread;
  eventIndex: number;
}

const sortByStorageKey = "InvocationTimingCardComponent.sortBy";
const groupByStorageKey = "InvocationTimingCardComponent.groupBy";
const threadPageSizeStorageKey = "InvocationTimingCardComponent.threadPageSize";
const eventPageSizeStorageKey = "InvocationTimingCardComponent.eventPageSize";
const sortByTimeAscStorageValue = "time-asc";
const sortByDurationDescStorageValue = "duration-desc";
const groupByThreadStorageValue = "thread";
const groupByAllStorageValue = "all";

function createEmptyProfileState() {
  return {
    profile: null,
    loading: true,
    threadNumPages: 1,
    threadToNumEventPagesMap: new Map<number, number>(),
    threadMap: new Map<number, Thread>(),
    durationByNameMap: new Map<string, number>(),
    durationByCategoryMap: new Map<string, number>(),
    localProfileName: "",
  };
}

export default class InvocationTimingCardComponent extends React.Component<Props, State> {
  state: State = {
    ...createEmptyProfileState(),
    sortBy: window.localStorage[sortByStorageKey] || sortByTimeAscStorageValue,
    groupBy: window.localStorage[groupByStorageKey] || groupByThreadStorageValue,
    threadPageSize: window.localStorage[threadPageSizeStorageKey] || 10,
    eventPageSize: window.localStorage[eventPageSizeStorageKey] || 100,
    viewerKey: 0,
  };

  private progressRef = React.createRef<HTMLDivElement>();
  private profileRPC?: CancelablePromise;

  componentDidMount() {
    this.fetchProfile();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.cancelProfileLoad();
      this.setState(createEmptyProfileState(), () => this.fetchProfile());
    }
  }

  componentWillUnmount() {
    this.cancelProfileLoad();
  }

  getProfileFile(): build_event_stream.File | undefined {
    // To override the auto-loaded profile (for debugging):
    // - Upload a profile with `bb upload --target=grpc://localhost:1985 <profile_path>`
    // - Copy the resulting resource name that it prints
    // - Check whether the file is gzipped by running `file <profile_path>`
    // - If it's gzipped, set ?debug_profile_gz=<resource_name> in the URL.
    //   Otherwise, set ?debug_profile_json=<resource_name>
    const params = new URLSearchParams(window.location.search);
    const debugProfileGzippedRN = params.get("debug_profile_gz");
    if (debugProfileGzippedRN) {
      return new build_event_stream.File({
        name: "timing_profile.gz",
        uri: "bytestream://localhost:1985/" + debugProfileGzippedRN.replace(/^\/+/, ""),
      });
    }
    const debugProfileRN = params.get("debug_profile_json");
    if (debugProfileRN) {
      return new build_event_stream.File({
        name: "timing_profile.json",
        uri: "bytestream://localhost:1985/" + debugProfileRN.replace(/^\/+/, ""),
      });
    }

    // Bazel 8 semi-fixed the profile name with: https://github.com/bazelbuild/bazel/pull/22345
    const version = this.props.model?.getBazelVersion();
    if (version && version.major >= 8) {
      return this.props.model.buildToolLogs?.log.find(
        (log: build_event_stream.File) => log.name.startsWith("command.profile.") && log.uri
      );
    }

    const profilePath =
      this.props.model.structuredCommandLine
        ?.find((scl) => scl.commandLineLabel == "canonical")
        ?.sections?.find((s) => s.sectionLabel == "command options")
        ?.optionList?.option?.find((o) => o.optionName == "profile")
        ?.optionValue.replaceAll("\\", "/") ?? "command.profile.gz";
    const profileName = profilePath.substring(profilePath.lastIndexOf("/") + 1);

    return this.props.model.buildToolLogs?.log.find(
      (log: build_event_stream.File) => log.name == profileName && log.uri
    );
  }

  isTimingEnabled() {
    return Boolean(this.getProfileFile()?.uri?.startsWith("bytestream://"));
  }

  setProgress(bytesLoaded: number, digestSize: number, encoding: FileEncoding, done = false) {
    const container = this.progressRef.current;
    if (!container) return;

    let approxCompressionRatio = 1;
    if (encoding === "gzip") {
      approxCompressionRatio = 11.3;
    }

    const compressedBytesLoaded = done ? digestSize : Math.min(bytesLoaded / approxCompressionRatio, digestSize);
    const progressPercent = 100 * Math.min(1, compressedBytesLoaded / digestSize);

    const spinner = container.querySelector(".loading") as HTMLElement;
    spinner.style.display = "none";

    const progressContainer = container.querySelector(".timing-profile-progress")!;
    progressContainer.removeAttribute("hidden");
    const progressLabel = progressContainer.querySelector(".progress-label")!;
    const label = done ? "Finalizing profile" : "Loading profile";
    progressLabel.innerHTML = `${label} (${format.bytes(compressedBytesLoaded)} / ${format.bytes(digestSize)})`;

    const progressBarInner = progressContainer.querySelector(".progress-bar-inner") as HTMLElement;
    progressBarInner.style.width = `${progressPercent}%`;
  }

  fetchProfile(ignoreSizeLimit = false) {
    this.cancelProfileLoad();

    if (!this.isTimingEnabled()) {
      this.setState({ loading: false });
      return;
    }

    // Already fetched
    if (this.state.profile) return;

    let profileFile = this.getProfileFile();
    if (!profileFile?.uri) return;

    if (!ignoreSizeLimit && isProfileTooLarge(profileFile)) {
      this.setState({ loading: false });
      return;
    }

    let compressionOption = this.props.model.optionsMap.get("json_trace_compression");
    let storedEncoding: FileEncoding = "";
    if (compressionOption === "1" || (profileFile?.name ?? "").endsWith(".gz")) {
      storedEncoding = "gzip";
    }

    const digestSize = Number(profileFile.uri.split("/").pop());

    this.setState({ loading: true });
    const init = {
      // Set the stored encoding header to prevent the server from double-gzipping.
      headers: { "X-Stored-Encoding-Hint": storedEncoding },
    };
    this.profileRPC = rpcService
      .fetchBytestreamFile(profileFile.uri, this.props.model.getInvocationId(), "stream", { init })
      .then((response) => {
        if (!response.body) throw new Error("response body is null");
        return readProfile(response.body, (n, done) => this.setProgress(n, digestSize, storedEncoding, done));
      })
      .then((profile) => this.updateProfile(profile))
      .catch((e) => errorService.handleError(e))
      .finally(() => {
        this.profileRPC = undefined;
        this.setState({ loading: false });
      });
  }

  private cancelProfileLoad() {
    this.profileRPC?.cancel();
    this.profileRPC = undefined;
  }

  private buildDerivedProfileState(profile: Profile) {
    const threadMap = new Map<number, Thread>();
    const durationByNameMap = new Map<string, number>();
    const durationByCategoryMap = new Map<string, number>();

    for (const thread of profile.threads) {
      threadMap.set(thread.tid, thread);
      for (let i = 0; i < thread.length; i++) {
        const dur = thread.dur[i];
        const name = thread.getName(i);
        const cat = thread.getCat(i);
        durationByNameMap.set(name, (durationByNameMap.get(name) || 0) + dur);
        durationByCategoryMap.set(cat, (durationByCategoryMap.get(cat) || 0) + dur);
      }
    }

    return {
      profile,
      threadNumPages: 1,
      threadToNumEventPagesMap: new Map<number, number>(),
      threadMap,
      durationByNameMap,
      durationByCategoryMap,
      viewerKey: this.state.viewerKey + 1,
    };
  }

  updateProfile(profile: Profile, localProfileName = "") {
    this.setState({
      ...this.buildDerivedProfileState(profile),
      loading: false,
      localProfileName,
    });
  }

  private restoreInvocationProfile() {
    this.setState(createEmptyProfileState(), () => this.fetchProfile());
  }

  private renderTraceViewer() {
    return (
      <TimingProfileDropTarget
        className="timing-profile-drop-target"
        onProfileLoaded={this.updateProfile.bind(this)}
        onProfileLoadError={(e) => errorService.handleError(e)}>
        {({ dragActive, loadingMessage }) => (
          <>
            <TraceViewer key={this.state.viewerKey} profile={this.state.profile!} dark={this.props.dark} />
            {(dragActive || loadingMessage) && (
              <div className="timing-profile-drop-overlay">
                <div className="timing-profile-drop-overlay-text">
                  {loadingMessage || "Drop a timing profile to render it"}
                </div>
              </div>
            )}
          </>
        )}
      </TimingProfileDropTarget>
    );
  }

  sortIdAsc(a: Thread, b: Thread) {
    return a.tid - b.tid;
  }

  sortDurationDesc(a: TraceEventRef, b: TraceEventRef) {
    return b.thread.dur[b.eventIndex] - a.thread.dur[a.eventIndex];
  }

  sortTimeAsc(a: TraceEventRef, b: TraceEventRef) {
    return a.thread.ts[a.eventIndex] - b.thread.ts[b.eventIndex];
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
      return (
        <div ref={this.progressRef}>
          <div className="loading" />
          <div className="timing-profile-progress" hidden>
            <div className="progress-label" />
            <div className="progress-bar">
              <div className="progress-bar-inner" />
            </div>
          </div>
        </div>
      );
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

    const profileFile = this.getProfileFile();
    if (isProfileTooLarge(profileFile)) {
      const sizeBytes = getProfileSizeBytes(profileFile);
      const downloadHref = getProfileDownloadHref(profileFile, this.props.model.getInvocationId());
      return (
        <>
          <div>
            Large timing profiles may crash the browser and are not shown by default
            {sizeBytes ? (
              <>
                {" "}
                (profile size: <b>{format.bytes(sizeBytes)}</b>)
              </>
            ) : null}
            .
          </div>
          <div className="timing-profile-too-large-actions">
            <LinkButton className="small-button" href={downloadHref} target="_blank">
              <Download className="icon" />
              Download profile
            </LinkButton>
            <OutlinedButton className="small-button" onClick={this.fetchProfile.bind(this, true)}>
              Try loading anyway
            </OutlinedButton>
          </div>
        </>
      );
    }

    return (
      <>
        <p>Profiling isn't enabled for this invocation. To enable profiling you must add gRPC remote caching.</p>
        <SetupCodeComponent
          requireCacheEnabled
          instructionsHeader={
            <p>
              To enable remote caching, check <b>Enable cache</b> below, update your <b>.bazelrc</b> accordingly, and
              re-run your invocation:
            </p>
          }
        />
      </>
    );
  }

  renderTimingSuggestionCard() {
    const suggestion = getTimingDataSuggestion({ model: this.props.model, buildLogs: "" });
    return suggestion ? <SuggestionComponent suggestion={suggestion} /> : null;
  }

  private getEventRefs(thread: Thread) {
    const refs: TraceEventRef[] = [];
    for (let eventIndex = 0; eventIndex < thread.length; eventIndex++) {
      refs.push({ thread, eventIndex });
    }
    return refs;
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

    const profileFile = this.getProfileFile();
    const downloadHref = getProfileDownloadHref(profileFile, this.props.model.getInvocationId());
    const eventSort = this.state.sortBy == sortByTimeAscStorageValue ? this.sortTimeAsc : this.sortDurationDesc;
    const allEventRefs =
      this.state.groupBy == groupByAllStorageValue
        ? threads.flatMap((thread: Thread) => this.getEventRefs(thread)).sort(eventSort)
        : [];
    const allEventsPageSize = this.state.eventPageSize * this.getNumPagesForThread(0);

    return (
      <>
        {this.state.localProfileName && (
          <div className="timing-profile-local-banner">
            <div>
              Showing local profile <span className="inline-code">{this.state.localProfileName}</span>.
            </div>
            <Button onClick={this.restoreInvocationProfile.bind(this)}>Show invocation profile</Button>
          </div>
        )}
        {this.renderTraceViewer()}
        <InvocationBreakdownCardComponent
          durationByNameMap={this.state.durationByNameMap}
          durationByCategoryMap={this.state.durationByCategoryMap}
        />

        {this.renderTimingSuggestionCard()}

        <div className="card timing">
          <Clock className="icon" />
          <div className="content">
            <div className="header">
              <div className="title">All events</div>
              {downloadHref && (
                <div className="button">
                  <LinkButton className="download-gz-file" href={downloadHref} target="_blank">
                    {this.state.localProfileName ? "Download invocation profile" : "Download profile"}
                  </LinkButton>
                </div>
              )}
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
                        {this.getEventRefs(thread)
                          .sort(eventSort)
                          .slice(0, this.state.eventPageSize * this.getNumPagesForThread(thread.tid))
                          .map((eventRef) => {
                            const eventIndex = eventRef.eventIndex;
                            const dur = eventRef.thread.dur[eventIndex];
                            return (
                              <li>
                                <div className="list-grid">
                                  <div>
                                    {eventRef.thread.getName(eventIndex)} {eventRef.thread.getTarget(eventIndex)}
                                  </div>
                                  <div>{format.durationUsec(dur)}</div>
                                </div>
                                <div
                                  className="list-percent"
                                  data-percent={`${(100 * (dur / this.props.model.getDurationMicros())).toFixed(0)}%`}
                                  style={{
                                    width: `${(100 * (dur / this.props.model.getDurationMicros())).toPrecision(3)}%`,
                                  }}></div>
                              </li>
                            );
                          })}
                      </ul>
                      {thread.length > this.state.eventPageSize * this.getNumPagesForThread(thread.tid) &&
                        !!this.state.eventPageSize && (
                          <div className="more" onClick={this.handleMoreEventsClicked.bind(this, thread.tid)}>
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
                    {allEventRefs.slice(0, allEventsPageSize).map((eventRef) => {
                      const eventIndex = eventRef.eventIndex;
                      const dur = eventRef.thread.dur[eventIndex];
                      return (
                        <li>
                          <div className="list-grid">
                            <div>{eventRef.thread.getName(eventIndex)}</div>
                            <div>{format.durationUsec(dur)}</div>
                          </div>
                          <div
                            className="list-percent"
                            data-percent={`${(100 * (dur / this.props.model.getDurationMicros())).toFixed(0)}%`}
                            style={{
                              width: `${(100 * (dur / this.props.model.getDurationMicros())).toPrecision(3)}%`,
                            }}></div>
                        </li>
                      );
                    })}
                  </ul>
                  {allEventRefs.length > allEventsPageSize && !!this.state.eventPageSize && (
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

function getProfileSizeBytes(profileFile?: build_event_stream.File): number | null {
  if (!profileFile?.uri) return null;
  const sizeBytesString = profileFile.uri.split("/").pop();
  if (!sizeBytesString) return null;
  const sizeBytes = Number(sizeBytesString);
  return Number.isFinite(sizeBytes) ? sizeBytes : null;
}

function isProfileTooLarge(profileFile?: build_event_stream.File) {
  const maxSizeBytes = Number(capabilities.config.timingProfileMaxSizeBytes || 0);
  const sizeBytes = getProfileSizeBytes(profileFile);
  return sizeBytes !== null && maxSizeBytes > 0 && sizeBytes > maxSizeBytes;
}

function getProfileDownloadHref(profileFile: build_event_stream.File | undefined, invocationId: string) {
  if (!profileFile?.uri) return "";
  return rpcService.getBytestreamUrl(profileFile.uri, invocationId, { filename: "timing_profile.gz" });
}
