import React from 'react';
import Long from 'long';
import moment from 'moment';
import rpcService from '../service/rpc_service'
import { TerminalComponent } from '../terminal/terminal'
import { invocation } from '../../proto/invocation_ts_proto';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';
import { command_line } from '../../proto/command_line_ts_proto';

interface State {
  invocations: invocation.Invocation[],

  progress: build_event_stream.Progress[],
  targets: build_event_stream.BuildEvent[],
  succeeded: build_event_stream.BuildEvent[],
  failed: build_event_stream.BuildEvent[],
  files: build_event_stream.NamedSetOfFiles[],
  finished: build_event_stream.BuildFinished,
  aborted: build_event_stream.Aborted,
  toolLogs: build_event_stream.BuildToolLogs,
  workspaceStatus: build_event_stream.WorkspaceStatus,
  configuration: build_event_stream.Configuration,
  workspaceConfig: build_event_stream.WorkspaceConfig,
  optionsParsed: build_event_stream.OptionsParsed,
  unstructuredCommandLine: build_event_stream.UnstructuredCommandLine,
  structuredCommandLine: command_line.CommandLine[],
  started: build_event_stream.BuildStarted,
  expanded: build_event_stream.BuildEvent,
  buildInfoMap: Map<string, string>,

  configuredMap: Map<string, invocation.InvocationEvent>,
  completedMap: Map<string, invocation.InvocationEvent>,

  progressLimit: number,
  succeededLimit: number,
  failedLimit: number,
  invocationLimit: number,
  artifactLimit: number,
}

interface Props {
  invocationId: string,
  hash: string
}

const defaultPageSize = 10;

export default class InvocationComponent extends React.Component {
  state: State = {
    invocations: [],

    progress: [],
    targets: [],
    succeeded: [],
    failed: [],
    files: [],
    started: null,
    finished: null,
    aborted: null,
    toolLogs: null,
    workspaceStatus: null,
    configuration: null,
    workspaceConfig: null,
    optionsParsed: null,
    unstructuredCommandLine: null,
    structuredCommandLine: [],
    expanded: null,
    buildInfoMap: new Map<string, string>(),

    configuredMap: new Map<string, build_event_stream.BuildEvent>(),
    completedMap: new Map<string, build_event_stream.BuildEvent>(),

    progressLimit: defaultPageSize,
    succeededLimit: defaultPageSize,
    failedLimit: defaultPageSize,
    invocationLimit: defaultPageSize,
    artifactLimit: defaultPageSize,
  };

  props: Props;

  componentWillMount() {
    // TODO(siggisim): Move moment configuration elsewhere
    moment.relativeTimeThreshold('ss', 0);

    let request = new invocation.GetInvocationRequest();
    request.query = new invocation.InvocationQuery();
    request.query.invocationId = this.props.invocationId;
    rpcService.service.getInvocation(request).then((response) => {
      console.log(response);
      this.state.invocations = response.invocation as invocation.Invocation[];
      for (let invocation of response.invocation) {
        for (let event of invocation.event) {
          let buildEvent = event.buildEvent;
          if (buildEvent.progress) this.state.progress.push(buildEvent.progress as build_event_stream.Progress);
          if (buildEvent.namedSetOfFiles) this.state.files.push(buildEvent.namedSetOfFiles as build_event_stream.NamedSetOfFiles);
          if (buildEvent.configured) this.state.targets.push(buildEvent as build_event_stream.BuildEvent);
          if (buildEvent.configured) {
            this.state.configuredMap.set(buildEvent.id.targetConfigured.label, event as invocation.InvocationEvent);
          }
          if (buildEvent.completed) {
            this.state.completedMap.set(buildEvent.id.targetCompleted.label, event as invocation.InvocationEvent);
          }
          if (buildEvent.completed && buildEvent.completed.success) {
            this.state.succeeded.push(buildEvent as build_event_stream.BuildEvent);
          }
          if (buildEvent.completed && !buildEvent.completed.success) {
            this.state.failed.push(buildEvent as build_event_stream.BuildEvent);
          }
          if (buildEvent.started) this.state.started = buildEvent.started as build_event_stream.BuildStarted;
          if (buildEvent.expanded) this.state.expanded = buildEvent as build_event_stream.BuildEvent;
          if (buildEvent.finished) this.state.finished = buildEvent.finished as build_event_stream.BuildFinished;
          if (buildEvent.aborted) this.state.aborted = buildEvent.aborted as build_event_stream.Aborted;
          if (buildEvent.buildToolLogs) this.state.toolLogs = buildEvent.buildToolLogs as build_event_stream.BuildToolLogs;
          if (buildEvent.workspaceStatus) this.state.workspaceStatus = buildEvent.workspaceStatus as build_event_stream.WorkspaceStatus;
          if (buildEvent.configuration) this.state.configuration = buildEvent.configuration as build_event_stream.Configuration;
          if (buildEvent.workspaceInfo) this.state.workspaceConfig = buildEvent.workspaceInfo as build_event_stream.WorkspaceConfig;
          if (buildEvent.optionsParsed) this.state.optionsParsed = buildEvent.optionsParsed as build_event_stream.OptionsParsed;
          if (buildEvent.unstructuredCommandLine) this.state.unstructuredCommandLine = buildEvent.unstructuredCommandLine as build_event_stream.UnstructuredCommandLine;
          if (buildEvent.structuredCommandLine) this.state.structuredCommandLine.push(buildEvent.structuredCommandLine as command_line.CommandLine);
        }
      }
      this.updateState();
    });

    this.updateState();
  }

  updateState() {
    for (let item of this.state.workspaceStatus?.item || []) {
      this.state.buildInfoMap.set(item.key, item.value);
    }
    for (let log of this.state.toolLogs?.log || []) {
      this.state.buildInfoMap.set(log.name, new TextDecoder().decode(log.contents));
    }
    this.setState(this.state);
  }

  getUser() {
    let username = this.state.buildInfoMap.get('BUILD_USER');
    if (!username) {
      return "Unknown user";
    }
    return username[0].toUpperCase() + username.slice(1);
  }

  getHost() {
    return this.state.buildInfoMap.get('BUILD_HOST') || "Unknown host";
  }

  getCommand() {
    return this.state.started?.command || "build"
  }

  getTool() {
    return `bazel ${this.state.started?.buildToolVersion} ` + this.state.started?.command || "build"
  }

  getPattern() {
    return this.state.expanded?.id.pattern.pattern.join(", ");
  }

  getStartTimeNumber() {
    return typeof this.state.started?.startTimeMillis == "number" ? this.state.started?.startTimeMillis : this.state.started?.startTimeMillis.toNumber();
  }

  getStartDate() {
    return moment(this.getStartTimeNumber()).format('MMMM Do, YYYY');
  }

  getStartTime() {
    return moment(this.getStartTimeNumber()).format('h:mm:ss a');
  }

  timeSinceStart() {
    return moment(this.getStartTimeNumber()).fromNow(true);
  }

  getHumanReadableDuration() {
    let elapsedTime = +this.state.buildInfoMap.get('elapsed time');
    if (elapsedTime < 1) {
      return "under a second";
    }
    return moment.duration(elapsedTime, "seconds").humanize() || "Unknown";
  }

  getDuractionSeconds() {
    return `${this.state.buildInfoMap.get('elapsed time')} seconds`
  }

  getTiming() {
    if (!this.state.finished && this.state.started) {
      return this.timeSinceStart();
    }
    return this.getHumanReadableDuration()
  }

  getStatus() {
    if (!this.state.started) {
      return "Not started"
    }
    if (!this.state.finished) {
      return "Building"
    }
    return this.state.finished.exitCode.code == 0 ? "Succeeded" : "Failed";
  }

  getStatusIcon() {
    if (!this.state.started) {
      return <img className="icon" src="/image/help-circle.svg" />
    }
    if (!this.state.finished) {
      return <img className="icon" src="/image/play-circle.svg" />
    }
    return this.state.finished.exitCode.code == 0 ? <img className="icon" src="/image/check-circle.svg" /> : <img className="icon" src="/image/x-circle.svg" />;

  }

  getDuration(completionTime: any, beginTime: any) {
    let nanos = ((+completionTime.nanos) - (+beginTime.nanos)) / 1000000000
    return `${(+completionTime.seconds) - (+beginTime.seconds) + nanos}`;
  }

  handleMoreFailedClicked() {
    this.setState({ ...this.state, failedLimit: this.state.failedLimit ? undefined : defaultPageSize })
  }

  handleMoreSucceededClicked() {
    this.setState({ ...this.state, succeededLimit: this.state.succeededLimit ? undefined : defaultPageSize })
  }

  handleMoreProgressClicked() {
    this.setState({ ...this.state, progressLimit: this.state.progressLimit ? undefined : defaultPageSize })
  }

  handleMoreInvocationClicked() {
    this.setState({ ...this.state, invocationLimit: this.state.invocationLimit ? undefined : defaultPageSize })
  }

  handleMoreArtifactClicked() {
    this.setState({ ...this.state, artifactLimit: this.state.artifactLimit ? undefined : defaultPageSize })
  }

  handleArtifactClicked(outputUri: string) {
    window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
  }

  render() {
    return (
      <div>
        <div className="shelf">
          <div className="container">
            <div className="invocation">Invocation {this.props.invocationId}</div>
            <div className="titles">
              <div className="title">{this.getUser()}'s {this.getCommand()} {this.getPattern()}</div>
              <div className="subtitle">{this.getStartDate()} at {this.getStartTime()}</div>
            </div>
            <div className="details">
              <div className="detail">{this.getStatusIcon()} {this.getStatus()}</div>
              <div className="detail"><img className="icon" src="/image/clock-regular.svg" /> {this.getTiming()}</div>
              <div className="detail"><img className="icon" src="/image/user-regular.svg" />
                {this.getUser()}</div>
              <div className="detail"><img className="icon" src="/image/hard-drive-regular.svg" />
                {this.getHost()}</div>
              <div className="detail"><img className="icon" src="/image/tool-regular.svg" />
                {this.getTool()}</div>
              <div className="detail"><img className="icon" src="/image/zap-regular.svg" />
                {this.getPattern()}</div>
              <div className="detail"><img className="icon" src="/image/target-regular.svg" />
                {this.state.targets.length} {this.state.targets.length == 1 ? "target" : "targets"}
              </div>
            </div>
          </div>
        </div>
        <div className="container">
          <div className="tabs">
            <a href="#" className={`tab ${this.props.hash == '' && 'selected'}`}>
              ALL
            </a>
            <a href="#log" className={`tab ${this.props.hash == '#log' && 'selected'}`}>
              BUILD LOGS
            </a>
            <a href="#targets" className={`tab ${this.props.hash == '#targets' && 'selected'}`}>
              TARGETS
            </a>
            <a href="#details" className={`tab ${this.props.hash == '#details' && 'selected'}`}>
              INVOCATION DETAILS
            </a>
            <a href="#artifacts" className={`tab ${this.props.hash == '#artifacts' && 'selected'}`}>
              ARTIFACTS
          </a>
            <a href="#raw" className={`tab ${this.props.hash == '#raw' && 'selected'}`}>
              RAW
          </a>
          </div>

          {(!this.props.hash || this.props.hash == "#log") &&
            <div className={`card dark ${this.props.hash == "#log" ? "expanded" : ""}`}>
              <img className="icon" src="/image/log-circle-light.svg" />
              <div className="content">
                <div className="title">Build logs </div>
                <div className="details">
                  <TerminalComponent value={this.state.progress/*.slice(!this.props.hash && -1 * this.state.progressLimit || undefined)*/.map(progress => {
                    return (progress.stdout + progress.stderr);
                  }).filter(output => output && output.length > 0).join("")} />
                </div>
                {/* {!this.props.hash && this.state.progress.length > defaultPageSize && !!this.state.progressLimit &&
                  <div className="more" onClick={this.handleMoreProgressClicked.bind(this)}>See more build logs</div>}
                {!this.props.hash && this.state.progress.length > defaultPageSize && !this.state.progressLimit &&
                  <div className="more" onClick={this.handleMoreProgressClicked.bind(this)}>See less build logs</div>} */}
              </div>
            </div>}


          {(!this.props.hash || this.props.hash == "#log") &&
            this.state.aborted?.description &&
            <div className="card">
              <img className="icon" src="/image/alert-circle.svg" />
              <div className="content">
                <div className="title">Error</div>
                <div className="details">
                  {this.state.aborted.description}
                </div>
              </div>
            </div>}

          {(!this.props.hash || this.props.hash == "#targets") &&
            !!this.state.failed.length &&
            <div className="card">
              <img className="icon" src="/image/x-circle.svg" />
              <div className="content">
                <div className="title">{this.state.failed.length} {this.state.failed.length == 1 ? "target" : "targets"} failed</div>
                <div className="details">
                  {this.state.failed.slice(0, !this.props.hash && this.state.failedLimit || undefined).map(failed =>
                    <div className="list-grid">
                      <div>{failed.id.targetCompleted.label}</div>
                      <div>{this.state.configuredMap.get(failed.id.targetCompleted.label).buildEvent.configured.targetKind}</div>
                      <div>{this.getDuration(
                              this.state.completedMap.get(failed.id.targetCompleted.label).eventTime, 
                              this.state.configuredMap.get(failed.id.targetCompleted.label).eventTime)} seconds</div>
                    </div>
                  )}
                </div>
                {!this.props.hash && this.state.failed.length > defaultPageSize && !!this.state.failedLimit &&
                  <div className="more" onClick={this.handleMoreFailedClicked.bind(this)}>See more failing targets</div>}
                {!this.props.hash && this.state.failed.length > defaultPageSize && !this.state.failedLimit &&
                  <div className="more" onClick={this.handleMoreFailedClicked.bind(this)}>See less failing targets</div>}
              </div>
            </div>}

          {(!this.props.hash || this.props.hash == "#targets") &&
            !!this.state.succeeded.length &&
            <div className="card">
              <img className="icon" src="/image/check-circle.svg" />
              <div className="content">
                <div className="title">{this.state.succeeded.length} {this.state.succeeded.length == 1 ? "target" : "targets"} passed</div>
                <div className="details">
                  {this.state.succeeded.slice(0, !this.props.hash && this.state.succeededLimit || undefined).map(succeeded =>
                    <div className="list-grid">
                      <div>{succeeded.id.targetCompleted.label}</div>
                      <div>{this.state.configuredMap.get(succeeded.id.targetCompleted.label).buildEvent.configured.targetKind}</div>
                      <div>{this.getDuration(
                              this.state.completedMap.get(succeeded.id.targetCompleted.label).eventTime, 
                              this.state.configuredMap.get(succeeded.id.targetCompleted.label).eventTime)} seconds</div>
                    </div>
                  )}
                </div>
                {!this.props.hash && this.state.succeeded.length > defaultPageSize && !!this.state.succeededLimit &&
                  <div className="more" onClick={this.handleMoreSucceededClicked.bind(this)}>See more passing targets</div>}
                {!this.props.hash && this.state.succeeded.length > defaultPageSize && !this.state.succeededLimit &&
                  <div className="more" onClick={this.handleMoreSucceededClicked.bind(this)}>See less passing targets</div>}
              </div>
            </div>}

          {(!this.props.hash || this.props.hash == "#details") &&
            <div className="card">
              <img className="icon" src="/image/info.svg" />
              <div className="content">
                <div className="title">Invocation details</div>
                <div className="details">
                  {this.state.structuredCommandLine
                      .filter(commandLine => commandLine.commandLineLabel && commandLine.commandLineLabel.length)
                      .slice(0, !this.props.hash && this.state.invocationLimit ? 1 : undefined)
                      .map(commandLine => 
                    <div className="invocation-command-line">
                      <div className="invocation-command-line-title">{commandLine.commandLineLabel}</div>
                      {commandLine.sections
                        // .slice(0, !this.props.hash && this.state.invocationLimit ? 2 : undefined)
                        .flatMap(section =>
                        <div className="invocation-section">
                          <div className="invocation-section-title">
                          {section.sectionLabel}
                          </div>
                          <div>
                          {section.chunkList?.chunk.map(chunk => <div className="invocation-chunk">{chunk}</div>) || []}
                          {section.optionList?.option.map(option => <div>
                              <span className="invocation-option-dash">--</span>
                              <span className="invocation-option-name">{option.optionName}</span>
                              <span className="invocation-option-equal">=</span>
                              <span className="invocation-option-value">{option.optionValue}</span> {/*option.effectTags.map(tag => JSON.stringify(tag)).join(", ") */}</div>) || []}
                          </div>
                        </div>
                      )}
                      </div>)}
                  </div>
                {!this.props.hash && !!this.state.invocationLimit &&
                  <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>See more invocation details</div>}
                {!this.props.hash && !this.state.invocationLimit &&
                  <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>See less invocation details</div>}
              </div>
            </div>}

          {(!this.props.hash || this.props.hash == "#artifacts") &&
            <div className="card artifacts">
              <img className="icon" src="/image/arrow-down-circle.svg" />
              <div className="content">
                <div className="title">Artifacts</div>
                <div className="details">
                  {this.state.succeeded.flatMap(completed => completed.completed.importantOutput.map(output => 
                  <div className="list-grid" onClick={this.handleArtifactClicked.bind(this, output.uri)}>
                    <div>{output.name}</div>
                    <div>{completed.id.targetCompleted.label}</div>
                  </div>))
                    .slice(0, !this.props.hash && this.state.artifactLimit || undefined)}
                  {this.state.succeeded.flatMap(completed => completed.completed.importantOutput).length == 0 && <span>No artifacts</span>}
                </div>
                {!this.props.hash && this.state.succeeded.flatMap(completed => completed.completed.importantOutput).length > defaultPageSize && !!this.state.artifactLimit &&
                  <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See more artifacts</div>}
                {!this.props.hash && this.state.succeeded.flatMap(completed => completed.completed.importantOutput).length > defaultPageSize && !this.state.artifactLimit && <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See less artifacts</div>}
              </div>
            </div>
          }

          {(this.props.hash == "#raw") &&
            <div className="card">
              <img className="icon" src="/image/log-circle.svg" />
              <div className="content">
                <div className="title">Raw logs</div>
                <div className="details code">
                  {this.state.invocations.flatMap(invocation => JSON.stringify(invocation.toJSON(), null, 4))}
                </div>
                {!this.props.hash && this.state.succeeded.flatMap(completed => completed.completed.importantOutput).length > defaultPageSize && !!this.state.artifactLimit &&
                  <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See more artifacts</div>}
                {!this.props.hash && this.state.succeeded.flatMap(completed => completed.completed.importantOutput).length > defaultPageSize && !this.state.artifactLimit && <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See less artifacts</div>}
              </div>
            </div>
          }
        </div>
      </div>
    );
  }
}
