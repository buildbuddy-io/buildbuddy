import React from 'react';
import moment from 'moment';
import { invocation } from '../../proto/invocation_ts_proto';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';
import { command_line } from '../../proto/command_line_ts_proto';
import format from '../format/format';

export default class InvocationModel {
  invocations: invocation.Invocation[] = [];

  consoleBuffer: string;
  targets: build_event_stream.BuildEvent[] = [];
  succeeded: build_event_stream.BuildEvent[] = [];
  failed: build_event_stream.BuildEvent[] = [];
  succeededTest: build_event_stream.BuildEvent[] = [];
  failedTest: build_event_stream.BuildEvent[] = [];
  brokenTest: build_event_stream.BuildEvent[] = [];
  flakyTest: build_event_stream.BuildEvent[] = [];
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
  buildToolLogs: build_event_stream.BuildToolLogs;

  workspaceStatusMap = new Map<string, string>();
  toolLogMap = new Map<string, string>();
  clientEnvMap = new Map<string, string>();
  configuredMap = new Map<string, invocation.InvocationEvent>();
  completedMap = new Map<string, invocation.InvocationEvent>();
  testResultMap: Map<string, invocation.InvocationEvent[]> = new Map<string, invocation.InvocationEvent[]>();
  testSummaryMap: Map<string, invocation.InvocationEvent> = new Map<string, invocation.InvocationEvent>();

  static modelFromInvocations(invocations: invocation.Invocation[]) {
    let model = new InvocationModel();
    model.invocations = invocations as invocation.Invocation[];
    for (let invocation of invocations) {
      if (invocation.consoleBuffer) model.consoleBuffer = invocation.consoleBuffer
      for (let cl of invocation.structuredCommandLine) {
        model.structuredCommandLine.push(cl as command_line.CommandLine)
      }

      for (let event of invocation.event) {
        let buildEvent = event.buildEvent;
        if (buildEvent.namedSetOfFiles) model.files.push(buildEvent.namedSetOfFiles as build_event_stream.NamedSetOfFiles);
        if (buildEvent.configured) model.targets.push(buildEvent as build_event_stream.BuildEvent);
        if (buildEvent.configured) {
          model.configuredMap.set(buildEvent.id.targetConfigured.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.completed) {
          model.completedMap.set(buildEvent.id.targetCompleted.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.testResult) {
          let results = model.testResultMap.get(buildEvent.id.testResult.label) || [];
          results.push(event as invocation.InvocationEvent);
          model.testResultMap.set(buildEvent.id.testResult.label, results);
        }
        if (buildEvent.testSummary) {
          model.testSummaryMap.set(buildEvent.id.testSummary.label, event as invocation.InvocationEvent);
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
        if (buildEvent.buildToolLogs) model.buildToolLogs = buildEvent.buildToolLogs as build_event_stream.BuildToolLogs;
        if (buildEvent.unstructuredCommandLine) model.unstructuredCommandLine = buildEvent.unstructuredCommandLine as build_event_stream.UnstructuredCommandLine;
      }
    }

    for (let label of model.completedMap.keys()) {
      let buildEvent = model.completedMap.get(label)?.buildEvent;
      let testResult = model.testSummaryMap.get(label)?.buildEvent.testSummary;
      if (testResult && testResult.overallStatus == build_event_stream.TestStatus.FLAKY) {
        model.flakyTest.push(buildEvent as build_event_stream.BuildEvent);
      } else if (testResult && testResult.overallStatus == build_event_stream.TestStatus.FAILED_TO_BUILD) {
        model.brokenTest.push(buildEvent as build_event_stream.BuildEvent);
      } else if (testResult && testResult.overallStatus == build_event_stream.TestStatus.PASSED) {
        model.succeededTest.push(buildEvent as build_event_stream.BuildEvent);
      } else if (testResult) {
        model.failedTest.push(buildEvent as build_event_stream.BuildEvent);
      }

      if (buildEvent.completed.success) {
        model.succeeded.push(buildEvent as build_event_stream.BuildEvent);
      } else {
        model.failed.push(buildEvent as build_event_stream.BuildEvent);
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

  getUser(posessive: boolean) {
    let username = this.workspaceStatusMap.get('BUILD_USER') || this.clientEnvMap.get('USER');
    if (username == "<REDACTED>") {
      return "Loading";
    }

    if (!username) {
      return posessive ? "Unknown user" : "Unknown user";
    }
    return posessive ? `${username}'s` : username;
  }

  getId() {
    return this.invocations.find(() => true)?.invocationId;
  }

  getHost() {
    return this.workspaceStatusMap.get('BUILD_HOST') || "Unknown host";
  }

  getGithubUser() {
    return this.clientEnvMap.get('GITHUB_ACTOR');
  }

  getGithubRepo() {
    return this.clientEnvMap.get('GITHUB_REPOSITORY');
  }

  getGithubSHA() {
    return this.clientEnvMap.get('GITHUB_SHA');
  }

  getGithubRun() {
    return this.clientEnvMap.get('GITHUB_RUN_ID');
  }

  getGKEProject() {
    return this.clientEnvMap.get('GKE_PROJECT')
  }

  getGKECluster() {
    return this.clientEnvMap.get('GKE_CLUSTER')
  }

  getCommand() {
    return this.started?.command || "build"
  }

  getTool() {
    return `bazel v${this.started?.buildToolVersion} ` + this.started?.command || "build"
  }

  getPattern() {
    return this.getAllPatterns(3);
  }

  getAllPatterns(patternLimit?: number) {
    let patterns = this.expanded?.id.pattern.pattern || this.aborted?.id.pattern.pattern || [];
    if (patternLimit && patterns.length > patternLimit) {
      return `${patterns.slice(0, patternLimit).join(", ")} and ${patterns.length - 3} more`;
    }
    return patterns.join(", ");
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
    return format.durationSec(elapsedTime);
  }

  getDurationSeconds() {
    return `${this.toolLogMap.get('elapsed time')} seconds`
  }

  getDurationMicros() {
    return +this.toolLogMap.get('elapsed time') * 1000000;
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
      return "Running..."
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
    if (!completionTime || !beginTime) return "";
    let nanos = ((+completionTime.nanos) - (+beginTime.nanos)) / 1000000000
    return `${((+completionTime.seconds) - (+beginTime.seconds) + nanos).toFixed(3)}`;
  }

  getRuntime(label: string) {
    let testResult = this.testSummaryMap.get(label)?.buildEvent.testSummary;
    if (testResult) {
      return +testResult.totalRunDurationMillis / 1000 + " seconds";
    }
    return this.getDuration(
      this.completedMap.get(label)?.eventTime,
      this.configuredMap.get(label)?.eventTime) + " seconds";
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