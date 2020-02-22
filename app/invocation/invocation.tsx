import React from 'react';
import Long from 'long';
import moment from 'moment';
import rpcService from '../service/rpc_service'
import { invocation } from '../../proto/invocation_ts_proto';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';
import { command_line } from '../../proto/command_line_ts_proto';

interface State {
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
  structuredCommandLine: command_line.CommandLine,
  started: build_event_stream.BuildStarted,
  expanded: build_event_stream.BuildEvent,
  buildInfoMap: Map<string, string>,

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

const defaultPageSize = 5;

export default class InvocationComponent extends React.Component {
  state: State = {
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
    structuredCommandLine: null,
    expanded: null,
    buildInfoMap: new Map<string, string>(),

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
      for (let invocation of response.invocation) {
        for (let buildEvent of invocation.buildEvent) {
          if (buildEvent.progress) this.state.progress.push(buildEvent.progress as build_event_stream.Progress);
          if (buildEvent.namedSetOfFiles) this.state.files.push(buildEvent.namedSetOfFiles as build_event_stream.NamedSetOfFiles);
          if (buildEvent.configured) this.state.targets.push(buildEvent as build_event_stream.BuildEvent);
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
          if (buildEvent.structuredCommandLine?.commandLineLabel == "original") this.state.structuredCommandLine = buildEvent.structuredCommandLine as command_line.CommandLine;
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

  getSampleData(): State {
    return {
      progress: [{
        stdout: `bazel build --config=bb app
  INFO: Streaming build results to: http://localhost:8080/#/invocation/e45e186f-042e-47b5-aec1-b3d91e188711
  INFO: Analyzed target //app:app (0 packages loaded, 0 targets configured).
  INFO: Found 1 target...
  Target //app:app up-to-date:
    bazel-bin/app/app.d.ts
  INFO: Elapsed time: 0.268s, Critical Path: 0.00s
  INFO: 0 processes.
  INFO: Streaming build results to: http://localhost:8080/#/invocation/e45e186f-042e-47b5-aec1-b3d91e188711
  INFO: Build completed successfully, 1 total action
  siggi:buildbuddy siggi$ bazel build --config=bb app
  INFO: Streaming build results to: http://localhost:8080/#/invocation/368d4ef5-b510-486b-a650-804d80715bd7
  INFO: Analyzed target //app:app (0 packages loaded, 0 targets configured).
  INFO: Found 1 target...
  Target //app:app up-to-date:
    bazel-bin/app/app.d.ts
  INFO: Elapsed time: 0.429s, Critical Path: 0.20s
  INFO: 1 process: 1 worker.
  INFO: Streaming build results to: http://localhost:8080/#/invocation/368d4ef5-b510-486b-a650-804d80715bd7
  INFO: Build completed successfully, 3 total actions`, stderr: "", toJSON: null
      }],
      files: [],
      succeeded: [],
      failed: [],
      targets: [],
      finished: { overallSuccess: true, exitCode: { name: "SUCCESS", code: 0 }, finishTimeMillis: Long.fromString("1582062129168"), toJSON: null },
      toolLogs: { log: [{ name: "elapsed time", contents: new TextEncoder().encode("1.61500") }], toJSON: null },
      workspaceStatus: { item: [{ key: "BUILD_USER", value: "siggi" }], toJSON: null },
      configuration: null,
      workspaceConfig: null,
      optionsParsed: null,
      unstructuredCommandLine: null,
      structuredCommandLine: null,
      aborted: null,
      started: { startTimeMillis: Long.fromString("1581977983847"), uuid: "", buildToolVersion: "2.0.0", optionsDescription: "", command: "", workingDirectory: "", serverPid: null, workspaceDirectory: "", toJSON: null },
      expanded: null,
      buildInfoMap: new Map<string, string>(),
      progressLimit: defaultPageSize,
      succeededLimit: defaultPageSize,
      failedLimit: defaultPageSize,
      invocationLimit: defaultPageSize,
      artifactLimit: defaultPageSize,
    };
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

  getPattern() {
    return this.state.expanded?.id.pattern.pattern.join(", ");
  }

  getStartDate() {
    return moment(this.state.started?.startTimeMillis.toNumber()).format('MMMM Do, YYYY');
  }

  getStartTime() {
    return moment(this.state.started?.startTimeMillis.toNumber()).format('h:mm:ss a');
  }

  timeSinceStart() {
    return moment(this.state.started?.startTimeMillis.toNumber()).fromNow(true);
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
                {this.getCommand()}</div>
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
            <a href="#targets" className={`tab ${this.props.hash == '#targets' && 'selected'}`}>
              TARGETS
            </a>
            <a href="#log" className={`tab ${this.props.hash == '#log' && 'selected'}`}>
              BUILD LOGS
            </a>
            <a href="#details" className={`tab ${this.props.hash == '#details' && 'selected'}`}>
              INVOCATION DETAILS
            </a>
            <a href="#artifacts" className={`tab ${this.props.hash == '#artifacts' && 'selected'}`}>
              ARTIFACTS
          </a>
          </div>

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
                    <div>{failed.id.targetCompleted.label}</div>
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
                    <div>{succeeded.id.targetCompleted.label}</div>
                  )}
                </div>
                {!this.props.hash && this.state.succeeded.length > defaultPageSize && !!this.state.succeededLimit &&
                  <div className="more" onClick={this.handleMoreSucceededClicked.bind(this)}>See more passing targets</div>}
                {!this.props.hash && this.state.succeeded.length > defaultPageSize && !this.state.succeededLimit &&
                  <div className="more" onClick={this.handleMoreSucceededClicked.bind(this)}>See less passing targets</div>}
              </div>
            </div>}

          {(!this.props.hash || this.props.hash == "#log") &&
            <div className="card">
              <img className="icon" src="/image/log-circle.svg" />
              <div className="content">
                <div className="title">Build log</div>
                <div className="details">
                  {this.state.progress.slice(!this.props.hash && -1 * this.state.progressLimit || undefined).map(progress => {
                    let ansiRegex = /[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g;
                    return (progress.stdout + progress.stderr).replace(ansiRegex, "").replace(/\n\n/g, "\n");
                  })}
                </div>
                {!this.props.hash && this.state.progress.length > defaultPageSize && !!this.state.progressLimit &&
                  <div className="more" onClick={this.handleMoreProgressClicked.bind(this)}>See more build logs</div>}
                {!this.props.hash && this.state.progress.length > defaultPageSize && !this.state.progressLimit &&
                  <div className="more" onClick={this.handleMoreProgressClicked.bind(this)}>See less build logs</div>}
              </div>
            </div>}

          {(!this.props.hash || this.props.hash == "#details") &&
            <div className="card">
              <img className="icon" src="/image/info.svg" />
              <div className="content">
                <div className="title">Invocation details</div>
                <div className="details">
                  {this.state.structuredCommandLine?.sections.flatMap(section =>
                    section.optionList?.option.map(option => <div>{option.combinedForm}</div>) || []
                  ).slice(0, !this.props.hash && this.state.invocationLimit || undefined)}
                </div>
                {!this.props.hash && this.state.structuredCommandLine?.sections.flatMap(section => section.optionList?.option).length > defaultPageSize && !!this.state.invocationLimit &&
                  <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>See more invocation details</div>}
                {!this.props.hash && this.state.structuredCommandLine?.sections.flatMap(section => section.optionList?.option).length > defaultPageSize && !this.state.invocationLimit &&
                  <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>See less invocation details</div>}
              </div>
            </div>}

          {(!this.props.hash || this.props.hash == "#artifacts") &&
            <div className="card">
              <img className="icon" src="/image/arrow-down-circle.svg" />
              <div className="content">
                <div className="title">Artifacts</div>
                <div className="details">
                  {this.state.succeeded.flatMap(completed => completed.completed.importantOutput.map(
                    output => <div>{output.name}</div>)).slice(0, !this.props.hash && this.state.artifactLimit || undefined)}
                  {this.state.succeeded.flatMap(completed => completed.completed.importantOutput).length == 0 && <span>No artifacts</span>}
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
