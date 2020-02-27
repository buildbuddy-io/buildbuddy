import React from 'react';
import moment from 'moment';
import { invocation } from '../../proto/invocation_ts_proto';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';
import { command_line } from '../../proto/command_line_ts_proto';

export default class InvocationModel {
  invocations: invocation.Invocation[] = [];

  progress = new Array<build_event_stream.Progress>();
  targets: build_event_stream.BuildEvent[] = [];
  succeeded: build_event_stream.BuildEvent[] = [];
  broken: build_event_stream.BuildEvent[] = [];
  failed: build_event_stream.BuildEvent[] = [];
  flaky: build_event_stream.BuildEvent[] = [];
  files: build_event_stream.NamedSetOfFiles[] = [];
  structuredCommandLine: command_line.CommandLine[] = [];
  finished: build_event_stream.BuildFinished;
  aborted: build_event_stream.BuildEvent;
  toolLogs: build_event_stream.BuildToolLogs;
  workspaceStatus: build_event_stream.WorkspaceStatus;
  configuration: build_event_stream.Configuration;
  workspaceConfig: build_event_stream.WorkspaceConfig;
  optionsParsed: build_event_stream.OptionsParsed;
  unstructuredCommandLine: build_event_stream.UnstructuredCommandLine;
  started: build_event_stream.BuildStarted;
  expanded: build_event_stream.BuildEvent;
  buildMetrics: build_event_stream.BuildMetrics;

  workspaceStatusMap = new Map<string, string>();
  toolLogMap = new Map<string, string>();
  clientEnvMap = new Map<string, string>();
  configuredMap = new Map<string, invocation.InvocationEvent>();
  completedMap = new Map<string, invocation.InvocationEvent>();
  testResultMap: Map<string, invocation.InvocationEvent> = new Map<string, invocation.InvocationEvent>();

  static modelFromInvocations(invocations: invocation.Invocation[]) {
    let model = new InvocationModel();
    model.invocations = invocations as invocation.Invocation[];
    for (let invocation of invocations) {
      for (let event of invocation.event) {
        let buildEvent = event.buildEvent;
        if (buildEvent.progress) model.progress.push(buildEvent.progress as build_event_stream.Progress);
        if (buildEvent.namedSetOfFiles) model.files.push(buildEvent.namedSetOfFiles as build_event_stream.NamedSetOfFiles);
        if (buildEvent.configured) model.targets.push(buildEvent as build_event_stream.BuildEvent);
        if (buildEvent.configured) {
          model.configuredMap.set(buildEvent.id.targetConfigured.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.completed) {
          model.completedMap.set(buildEvent.id.targetCompleted.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.testResult) {
          model.testResultMap.set(buildEvent.id.testResult.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.started) model.started = buildEvent.started as build_event_stream.BuildStarted;
        if (buildEvent.expanded) model.expanded = buildEvent as build_event_stream.BuildEvent;
        if (buildEvent.finished) model.finished = buildEvent.finished as build_event_stream.BuildFinished;
        if (buildEvent.aborted) model.aborted = buildEvent as build_event_stream.BuildEvent;
        if (buildEvent.buildToolLogs) model.toolLogs = buildEvent.buildToolLogs as build_event_stream.BuildToolLogs;
        if (buildEvent.workspaceStatus) model.workspaceStatus = buildEvent.workspaceStatus as build_event_stream.WorkspaceStatus;
        if (buildEvent.configuration) model.configuration = buildEvent.configuration as build_event_stream.Configuration;
        if (buildEvent.workspaceInfo) model.workspaceConfig = buildEvent.workspaceInfo as build_event_stream.WorkspaceConfig;
        if (buildEvent.optionsParsed) model.optionsParsed = buildEvent.optionsParsed as build_event_stream.OptionsParsed;
        if (buildEvent.buildMetrics) model.buildMetrics = buildEvent.buildMetrics as build_event_stream.BuildMetrics;
        if (buildEvent.unstructuredCommandLine) model.unstructuredCommandLine = buildEvent.unstructuredCommandLine as build_event_stream.UnstructuredCommandLine;
        if (buildEvent.structuredCommandLine) model.structuredCommandLine.push(buildEvent.structuredCommandLine as command_line.CommandLine);
      }
    }

    for (let label of model.completedMap.keys()) {
      let buildEvent = model.completedMap.get(label)?.buildEvent;
      let testResult = model.testResultMap.get(label)?.buildEvent.testResult;
      if (!buildEvent.completed.success || (testResult && testResult.status != build_event_stream.TestStatus.PASSED)) {
        model.failed.push(buildEvent as build_event_stream.BuildEvent);
      } else {
        model.succeeded.push(buildEvent as build_event_stream.BuildEvent);
      }
    }

    for (let item of model.workspaceStatus?.item || []) {
      model.workspaceStatusMap.set(item.key, item.value);
    }
    for (let log of model.toolLogs?.log || []) {
      model.toolLogMap.set(log.name, new TextDecoder().decode(log.contents));
    }
    for (let commandLine of model.structuredCommandLine || []) {
      for (let section of commandLine.sections || []) {
        for (let option of section.optionList?.option || []) {
          if (option.optionName == "client_env") {
            let pair = option.optionValue.split("=");
            if (pair.length == 2) {
              model.clientEnvMap.set(pair[0], pair[1]);
            }
          }
        }
      }
    }
    return model;
  }

  getUser() {
    let username = this.workspaceStatusMap.get('BUILD_USER') || this.clientEnvMap.get('USER');
    if (!username) {
      return "Unknown user";
    }
    return username[0].toUpperCase() + username.slice(1);
  }

  getHost() {
    return this.workspaceStatusMap.get('BUILD_HOST') || "Unknown host";
  }

  getCommand() {
    return this.started?.command || "build"
  }

  getTool() {
    return `bazel v${this.started?.buildToolVersion} ` + this.started?.command || "build"
  }

  getPattern() {
    return this.expanded?.id.pattern.pattern.join(", ") || this.aborted?.id.pattern.pattern.join(", ");
  }

  getStartTimeNumber() {
    return typeof this.started?.startTimeMillis == "number" ? this.started?.startTimeMillis : this.started?.startTimeMillis.toNumber();
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
    let elapsedTime = +this.toolLogMap.get('elapsed time');
    if (elapsedTime < 1) {
      return "under a second";
    }
    return moment.duration(elapsedTime, "seconds").humanize() || "Unknown";
  }

  getDuractionSeconds() {
    return `${this.toolLogMap.get('elapsed time')} seconds`
  }

  getTiming() {
    if (!this.finished && this.started) {
      return this.timeSinceStart();
    }
    return this.getHumanReadableDuration()
  }

  getStatus() {
    if (!this.started) {
      return "Not started"
    }
    if (!this.finished) {
      return "Building"
    }
    return this.finished.exitCode.code == 0 ? "Succeeded" : "Failed";
  }

  getStatusIcon() {
    if (!this.started) {
      return <img className="icon" src="/image/help-circle.svg" />
    }
    if (!this.finished) {
      return <img className="icon" src="/image/play-circle.svg" />
    }
    return this.finished.exitCode.code == 0 ? <img className="icon" src="/image/check-circle.svg" /> : <img className="icon" src="/image/x-circle.svg" />;
  }

  getCPU() {
    return this.configuration?.makeVariable.TARGET_CPU || "Unknown CPU";
  }

  getMode() {
    return this.configuration?.makeVariable.COMPILATION_MODE || "Unknown mode";
  }

  getDuration(completionTime: any, beginTime: any) {
    let nanos = ((+completionTime.nanos) - (+beginTime.nanos)) / 1000000000
    return `${((+completionTime.seconds) - (+beginTime.seconds) + nanos).toFixed(3)}`;
  }

  getTestResultLog(label: string) {
    let testResult = this.testResultMap.get(label)?.buildEvent.testResult;
    if (!testResult || !testResult.testActionOutput.length) {
      return
    }
    return testResult.testActionOutput[0].uri;
  }

  getRuntime(label: string) {
    let testResult = this.testResultMap.get(label)?.buildEvent.testResult;
    if (testResult) {
      return +testResult.testAttemptDurationMillis / 1000;
    }
    return this.getDuration(
      this.completedMap.get(label).eventTime,
      this.configuredMap.get(label).eventTime);
  }

  getTestSize(testSize: build_event_stream.TestSize) {
    switch (testSize) {
      case build_event_stream.TestSize.SMALL:
        return " - Small";
      case build_event_stream.TestSize.MEDIUM:
        return " - Medium";
      case build_event_stream.TestSize.LARGE:
        return " - Large";
      case build_event_stream.TestSize.ENORMOUS:
        return " - Enormous";
    }
    return "";
  }
}