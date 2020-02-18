import React from 'react';
import Long from 'long';
import moment from 'moment';
import rpcService from 'buildbuddy/app/service/rpc_service'
import { invocation } from 'buildbuddy/proto/invocation_ts_proto';
import { build_event_stream } from 'buildbuddy/proto/build_event_stream_ts_proto';
import { command_line } from 'buildbuddy/proto/command_line_ts_proto';

interface State {
  progress: build_event_stream.Progress[],
  finished: build_event_stream.BuildFinished,
  toolLogs: build_event_stream.BuildToolLogs,
  workspaceStatus: build_event_stream.WorkspaceStatus,
  configuration: build_event_stream.Configuration,
  workspaceConfig: build_event_stream.WorkspaceConfig,
  optionsParsed: build_event_stream.OptionsParsed,
  unstructuredCommandLine: build_event_stream.UnstructuredCommandLine,
  structuredCommandLine: command_line.CommandLine,
  started: build_event_stream.BuildStarted,
  buildInfoMap: Map<string, string>,
}

interface Props {
  invocationId: string
}

export default class InvocationComponent extends React.Component {
  state: State = {
    progress: [],
    finished: null,
    toolLogs: null,
    workspaceStatus: null,
    configuration: null,
    workspaceConfig: null,
    optionsParsed: null,
    unstructuredCommandLine: null,
    structuredCommandLine: null,
    started: null,
    buildInfoMap: new Map<string, string>(),
  };

  props: Props;

  componentWillMount() {
    // TODO(siggisim): Move moment configuration elsewhere
    moment.relativeTimeThreshold('ss', 1);

    let request = new invocation.GetInvocationRequest();
    request.query = new invocation.InvocationQuery();
    request.query.invocationId = this.props.invocationId;
    rpcService.service.getInvocation(request).then((response) => {
      for (let invocation of response.invocation) {
        for (let buildEvent of invocation.buildEvent) {
          if (buildEvent.progress) this.state.progress.push(buildEvent.progress as build_event_stream.Progress);
          if (buildEvent.finished) this.state.finished = buildEvent.finished as build_event_stream.BuildFinished;
          if (buildEvent.workspaceStatus) this.state.workspaceStatus = buildEvent.workspaceStatus as build_event_stream.WorkspaceStatus;
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
      finished: { overallSuccess: true, exitCode: { name: "SUCCESS", code: 0 }, finishTimeMillis: Long.fromString("1582062129168"), toJSON: null },
      toolLogs: { log: [{ name: "elapsed time", contents: new TextEncoder().encode("1.61500") }], toJSON: null },
      workspaceStatus: { item: [{ key: "BUILD_USER", value: "siggi" }], toJSON: null },
      configuration: null,
      workspaceConfig: null,
      optionsParsed: null,
      unstructuredCommandLine: null,
      structuredCommandLine: null,
      started: { startTimeMillis: Long.fromString("1581977983847"), uuid: "", buildToolVersion: "2.0.0", optionsDescription: "", command: "", workingDirectory: "", serverPid: null, workspaceDirectory: "", toJSON: null },
      buildInfoMap: new Map<string, string>(),
    };
  }

  getUser() {
    return this.state.buildInfoMap.get('BUILD_USER') || "Unknown user";
  }

  getStartDate() {
    return moment(this.state.started?.startTimeMillis.toNumber()).format('MMMM Do, YYYY [at] h:mm:ss a');
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

  getStatus() {
    if (!this.state.started) {
      return "Not started"
    }
    if (!this.state.finished) {
      return "Building"
    }
    return this.state.finished.exitCode.code == 0 ? "Succeeded" : "Failed";
  }

  render() {
    return (
      <div>
        <div className="shelf">
          <div className="container">
            <div className="titles">
              <div className="title">{this.getUser()}'s build</div>
              <div className="subtitle">{this.getStartDate()}</div>
            </div>
            <div className="details">
              <b>{this.getStatus()}</b>
              {this.state.finished &&
                <span> in <b title={this.getDuractionSeconds()}>{this.getHumanReadableDuration()}</b></span>}
              {!this.state.finished && this.state.started &&
                <span> for <b>{this.timeSinceStart()}</b></span>}
              <br />
              <b>329</b> targets  &middot;  <b>317</b> passed  &middot;  <b>12</b> failed <br />
            </div>
          </div>
        </div>
        <div className="container">

          <div className="card">
            <img className="icon" src="/image/log-circle.svg" />
            <div className="content">
              <div className="title">Build log</div>
              <div className="details">
                {this.state.progress.map(progress => progress.stdout + "\n" + progress.stderr)}
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/x-circle.svg" />
            <div className="content">
              <div className="title">12 targets failed</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/check-circle.svg" />
            <div className="content">
              <div className="title">317 targets passed</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/info.svg" />
            <div className="content">
              <div className="title">Invocation details</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>

          <div className="card">
            <img className="icon" src="/image/arrow-down-circle.svg" />
            <div className="content">
              <div className="title">Artifacts</div>
              <div className="details">
              </div>
              <div className="more">See more</div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
