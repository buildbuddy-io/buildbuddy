import { HelpCircle, PlayCircle, XCircle, CheckCircle } from "lucide-react";
import moment from "moment";
import React from "react";
import { api_key } from "../../proto/api_key_ts_proto";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { cache } from "../../proto/cache_ts_proto";
import { command_line } from "../../proto/command_line_ts_proto";
import { grp } from "../../proto/group_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import { IconType } from "../favicon/favicon";
import format from "../format/format";
import { formatDate } from "../format/format";
import { durationToMillisWithFallback, timestampToDateWithFallback } from "../util/proto";

export const CI_RUNNER_ROLE = "CI_RUNNER";
export const HOSTED_BAZEL_ROLE = "HOSTED_BAZEL";

export const InvocationStatus = invocation.Invocation.InvocationStatus;

export default class InvocationModel {
  invocations: invocation.Invocation[] = [];
  cacheStats: cache.CacheStats[] = [];
  scoreCard: cache.IScoreCard;

  targets: build_event_stream.BuildEvent[] = [];
  succeeded: build_event_stream.BuildEvent[] = [];
  failed: build_event_stream.BuildEvent[] = [];
  skipped: build_event_stream.BuildEvent[] = [];
  fetchEventURLs: string[] = [];
  succeededTest: build_event_stream.BuildEvent[] = [];
  failedTest: build_event_stream.BuildEvent[] = [];
  brokenTest: build_event_stream.BuildEvent[] = [];
  flakyTest: build_event_stream.BuildEvent[] = [];
  timeoutTest: build_event_stream.BuildEvent[] = [];
  structuredCommandLine: command_line.CommandLine[] = [];
  finished: build_event_stream.BuildFinished;
  aborted: build_event_stream.BuildEvent;
  toolLogs: build_event_stream.BuildToolLogs;
  workflowConfigured: build_event_stream.WorkflowConfigured;
  childInvocationsConfigured: build_event_stream.ChildInvocationsConfigured;
  childInvocationCompletedByInvocationId = new Map<
    string,
    build_event_stream.IChildInvocationCompleted | build_event_stream.IWorkflowCommandCompleted
  >();
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
  optionsMap = new Map<string, string>();
  clientEnvMap = new Map<string, string>();
  buildMetadataMap = new Map<string, string>();
  configuredMap = new Map<string, invocation.InvocationEvent>();
  completedMap = new Map<string, invocation.InvocationEvent>();
  skippedMap = new Map<string, invocation.InvocationEvent>();
  testResultMap: Map<string, invocation.InvocationEvent[]> = new Map<string, invocation.InvocationEvent[]>();
  testSummaryMap: Map<string, invocation.InvocationEvent> = new Map<string, invocation.InvocationEvent>();
  actionMap: Map<string, invocation.InvocationEvent[]> = new Map<string, invocation.InvocationEvent[]>();
  rootCauseTargetLabels: Set<String>;

  private fileSetIDToFilesMap: Map<string, build_event_stream.IFile[]> = new Map();

  static modelFromInvocations(invocations: invocation.Invocation[]) {
    let model = new InvocationModel();
    model.invocations = invocations as invocation.Invocation[];
    model.cacheStats = invocations
      .map((invocation) => invocation.cacheStats)
      .filter((cacheStat) => !!cacheStat) as cache.CacheStats[];

    for (let invocation of invocations) {
      if (invocation.scoreCard) model.scoreCard = invocation.scoreCard;
      for (let cl of invocation.structuredCommandLine) {
        model.structuredCommandLine.push(cl as command_line.CommandLine);
      }

      for (let event of invocation.event) {
        let buildEvent = event.buildEvent;
        if (buildEvent.namedSetOfFiles) {
          model.fileSetIDToFilesMap.set(buildEvent.id.namedSet.id, buildEvent.namedSetOfFiles.files);
        }
        if (buildEvent.configured) model.targets.push(buildEvent as build_event_stream.BuildEvent);
        if (buildEvent.configured) {
          model.configuredMap.set(buildEvent.id.targetConfigured.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.completed) {
          model.completedMap.set(buildEvent.id.targetCompleted.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.fetch) {
          model.fetchEventURLs.push(buildEvent.id.fetch.url);
        }
        if (buildEvent.testResult) {
          let results = model.testResultMap.get(buildEvent.id.testResult.label) || [];
          results.push(event as invocation.InvocationEvent);
          model.testResultMap.set(buildEvent.id.testResult.label, results);
        }
        if (buildEvent.action) {
          let results = model.actionMap.get(buildEvent.id.actionCompleted.label) || [];
          results.push(event as invocation.InvocationEvent);
          model.actionMap.set(buildEvent.id.actionCompleted.label, results);
        }
        if (buildEvent.testSummary) {
          model.testSummaryMap.set(buildEvent.id.testSummary.label, event as invocation.InvocationEvent);
        }
        if (buildEvent.started) model.started = buildEvent.started as build_event_stream.BuildStarted;
        if (buildEvent.expanded) model.expanded = buildEvent as build_event_stream.BuildEvent;
        if (buildEvent.finished) model.finished = buildEvent.finished as build_event_stream.BuildFinished;
        if (buildEvent.aborted && buildEvent.aborted.reason == build_event_stream.Aborted.AbortReason.SKIPPED) {
          model.skipped.push(buildEvent as build_event_stream.BuildEvent);
          model.skippedMap.set(buildEvent.id.targetCompleted.label, event as invocation.InvocationEvent);
        } else if (buildEvent.aborted) {
          model.aborted = buildEvent as build_event_stream.BuildEvent;
        }
        if (buildEvent.buildToolLogs) model.toolLogs = buildEvent.buildToolLogs as build_event_stream.BuildToolLogs;
        if (buildEvent.workspaceStatus) {
          model.workspaceStatus = buildEvent.workspaceStatus as build_event_stream.WorkspaceStatus;
        }
        if (buildEvent.workflowConfigured) {
          model.workflowConfigured = buildEvent.workflowConfigured as build_event_stream.WorkflowConfigured;
        }
        if (buildEvent.childInvocationsConfigured) {
          model.childInvocationsConfigured = buildEvent.childInvocationsConfigured as build_event_stream.ChildInvocationsConfigured;
        }
        if (buildEvent.workflowCommandCompleted) {
          model.childInvocationCompletedByInvocationId.set(
            buildEvent.id.workflowCommandCompleted.invocationId,
            buildEvent.workflowCommandCompleted
          );
        }
        if (buildEvent.childInvocationCompleted) {
          model.childInvocationCompletedByInvocationId.set(
            buildEvent.id.childInvocationCompleted.invocationId,
            buildEvent.childInvocationCompleted
          );
        }
        if (buildEvent.configuration && buildEvent?.id?.configuration?.id != "none") {
          model.configuration = buildEvent.configuration as build_event_stream.Configuration;
        }
        if (buildEvent.workspaceInfo) {
          model.workspaceConfig = buildEvent.workspaceInfo as build_event_stream.WorkspaceConfig;
        }
        if (buildEvent.optionsParsed) {
          model.optionsParsed = buildEvent.optionsParsed as build_event_stream.OptionsParsed;
        }
        if (buildEvent.buildMetrics) {
          model.buildMetrics = buildEvent.buildMetrics as build_event_stream.BuildMetrics;
        }
        if (buildEvent.buildToolLogs) {
          model.buildToolLogs = buildEvent.buildToolLogs as build_event_stream.BuildToolLogs;
        }
        if (buildEvent.unstructuredCommandLine) {
          model.unstructuredCommandLine = buildEvent.unstructuredCommandLine as build_event_stream.UnstructuredCommandLine;
        }
      }
    }
    model.rootCauseTargetLabels = new Set(
      [...model.completedMap.values()]
        .filter((e) => !e.buildEvent.completed.success)
        .map((e) => e.buildEvent.children.filter((child) => child.actionCompleted?.label))
        .flat()
        .map((child) => child.actionCompleted.label)
    );
    for (let label of model.completedMap.keys()) {
      let buildEvent = model.completedMap.get(label)?.buildEvent;
      let testResult = model.testSummaryMap.get(label)?.buildEvent.testSummary;
      if (testResult && testResult.overallStatus == build_event_stream.TestStatus.FLAKY) {
        model.flakyTest.push(buildEvent as build_event_stream.BuildEvent);
      } else if (testResult && testResult.overallStatus == build_event_stream.TestStatus.FAILED_TO_BUILD) {
        model.brokenTest.push(buildEvent as build_event_stream.BuildEvent);
      } else if (testResult && testResult.overallStatus == build_event_stream.TestStatus.PASSED) {
        model.succeededTest.push(buildEvent as build_event_stream.BuildEvent);
      } else if (testResult && testResult.overallStatus == build_event_stream.TestStatus.TIMEOUT) {
        model.timeoutTest.push(buildEvent as build_event_stream.BuildEvent);
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
      model.toolLogMap.set(log.name, new TextDecoder().decode(log.contents || new Uint8Array()));
    }
    for (let commandLine of model.structuredCommandLine || []) {
      for (let section of commandLine.sections || []) {
        for (let option of section.optionList?.option || []) {
          model.optionsMap.set(option.optionName, option.optionValue);
          if (option.optionName == "client_env") {
            let pair = option.optionValue.split("=");
            if (pair.length == 2) {
              model.clientEnvMap.set(pair[0], pair[1]);
            }
          }
          if (option.optionName == "build_metadata") {
            let parts = option.optionValue.split("=");
            if (parts.length >= 2) {
              let key = parts.shift();
              model.buildMetadataMap.set(key, parts.join("="));
            }
          }
        }
      }
    }
    return model;
  }

  getUser(possessive: boolean) {
    let invocationUser = this.invocations.find(() => true)?.user;
    if (invocationUser) {
      return possessive ? `${invocationUser}'s` : invocationUser;
    }

    let username = this.workspaceStatusMap.get("BUILD_USER") || this.clientEnvMap.get("USER");
    if (username == "<REDACTED>") {
      return "Loading";
    }

    if (!username) {
      return possessive ? "Unknown user's" : "Unknown user";
    }
    return possessive ? `${username}'s` : username;
  }

  /**
   * Returns the group which owns the invocation.
   *
   * If no groups are provided or if the group is not found, null is returned.
   */
  findOwnerGroup(groups: grp.Group[] | undefined) {
    if (this.invocations.length > 1) {
      console.error("findOwnerGroup may only be called from single-invocation views.");
      return null;
    }

    if (!groups?.length) return null;

    const invocation = this.invocations[0];
    if (!invocation) return null;

    return groups.find((group) => group.id === invocation.acl.groupId) || null;
  }

  isAnonymousInvocation(): boolean {
    return this.invocations.find(() => true)?.acl?.groupId === "";
  }

  hasCacheWriteCapability(): boolean {
    return this.invocations
      .find(() => true)
      ?.createdWithCapabilities?.some(
        (existingCapability) =>
          existingCapability == api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY ||
          existingCapability == api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY
      );
  }

  getId() {
    return this.invocations.find(() => true)?.invocationId;
  }

  getHost() {
    return this.invocations.find(() => true)?.host || this.workspaceStatusMap.get("BUILD_HOST") || "Unknown host";
  }

  booleanCommandLineOption(name: string, defaultValue = false): boolean {
    const rawVal = this.optionsMap.get(name);
    if (rawVal === undefined) return defaultValue;
    return rawVal !== "0";
  }

  stringCommandLineOption(name: string, defaultValue = ""): string {
    const rawVal = this.optionsMap.get(name);
    if (rawVal === undefined) return defaultValue;
    return rawVal;
  }

  isCacheCompressionEnabled() {
    return this.optionsMap.get("experimental_remote_cache_compression") === "1";
  }

  getCache() {
    if (!this.optionsMap.get("remote_cache") && !this.optionsMap.get("remote_executor")) {
      return "Cache off";
    }
    if (this.optionsMap.get("remote_upload_local_results") == "0") {
      return "Log upload on";
    }
    if (this.optionsMap.get("remote_download_outputs") == "toplevel") {
      return "Top-level cache";
    }
    if (this.optionsMap.get("remote_download_outputs") == "minimal") {
      return "Minimal cache";
    }
    return "Cache on";
  }

  getRemoteInstanceName() {
    return this.optionsMap.get("remote_instance_name") ?? "";
  }

  getIsRBEEnabled() {
    return Boolean(this.stringCommandLineOption("remote_executor"));
  }

  getRBE() {
    return this.getIsRBEEnabled() ? "Remote execution on" : "Remote execution off";
  }

  getFetchURLs() {
    return this.fetchEventURLs;
  }

  getRepo() {
    return this.getGithubRepo();
  }

  getCommit() {
    return this.getGithubSHA();
  }

  getBranchName() {
    return this.getGithubBranch();
  }

  getGithubUser() {
    return this.clientEnvMap.get("GITHUB_ACTOR");
  }

  getBuildkiteUrl() {
    if (this.clientEnvMap.get("BUILDKITE_BUILD_URL") && this.clientEnvMap.get("BUILDKITE_JOB_ID")) {
      return `${this.clientEnvMap.get("BUILDKITE_BUILD_URL")}#${this.clientEnvMap.get("BUILDKITE_JOB_ID")}`;
    }

    return this.clientEnvMap.get("BUILDKITE_BUILD_URL");
  }

  getGithubRepo() {
    return this.invocations.find(() => true)?.repoUrl || "";
  }

  getGithubSHA() {
    return this.invocations.find(() => true)?.commitSha || "";
  }

  getGithubRef() {
    return this.clientEnvMap.get("GITHUB_REF");
  }

  getGithubBranch() {
    return this.invocations.find(() => true)?.branchName || "";
  }

  getGithubRun() {
    return this.clientEnvMap.get("GITHUB_RUN_ID");
  }

  getGKEProject() {
    return this.clientEnvMap.get("GKE_PROJECT");
  }

  getGKECluster() {
    return this.clientEnvMap.get("GKE_CLUSTER");
  }

  getCommand() {
    return this.started?.command || "build";
  }

  getRole(): string {
    return this.invocations.find(() => true).role;
  }

  isWorkflowInvocation() {
    return this.getRole() === CI_RUNNER_ROLE;
  }

  isHostedBazelInvocation() {
    return this.getRole() === HOSTED_BAZEL_ROLE;
  }

  isBazelInvocation() {
    return !this.isWorkflowInvocation() && !this.isHostedBazelInvocation();
  }

  getTool() {
    if (this.isWorkflowInvocation()) {
      return "BuildBuddy workflow runner";
    }
    if (this.isHostedBazelInvocation()) {
      return "BuildBuddy hosted bazel";
    }
    return `bazel v${this.started?.buildToolVersion} ` + this.started?.command || "build";
  }

  getPattern() {
    return this.getAllPatterns(3);
  }

  getAllPatterns(patternLimit?: number) {
    let patterns =
      this.invocations.find(() => true).pattern ||
      this.expanded?.id?.pattern?.pattern ||
      this.aborted?.id?.pattern?.pattern ||
      [];
    if (patternLimit && patterns.length > patternLimit) {
      return `${patterns.slice(0, patternLimit).join(", ")} and ${patterns.length - 3} more`;
    }
    return patterns.join(", ");
  }

  getStartTimeDate(): Date {
    return timestampToDateWithFallback(this.started?.startTime, this.started?.startTimeMillis);
  }

  getEndTimeDate(): Date {
    let durationMillis = this.getDurationMicros() * 1000;
    return new Date(this.getStartTimeDate().getTime() + durationMillis);
  }

  getFormattedStartedDate() {
    return formatDate(this.getStartTimeDate());
  }

  timeSinceStart() {
    return moment(this.getStartTimeDate()).fromNow(true);
  }

  getDurationMicros() {
    if (!this.finished && this.started) {
      return Math.max(0, new Date().getTime() - this.getStartTimeDate().getTime()) * 1000;
    }
    if (this.toolLogMap.has("elapsed time")) {
      return +this.toolLogMap.get("elapsed time") * 1000000;
    }
    return +this.invocations.find(() => true)?.durationUsec;
  }

  getDurationSeconds() {
    let durationMicros = this.getDurationMicros();
    return `${durationMicros / 1000000} seconds`;
  }

  getHumanReadableDuration() {
    let durationMicros = this.getDurationMicros();
    return format.durationUsec(durationMicros);
  }

  getTiming() {
    let invocationStatus = this.invocations.find(() => true)?.invocationStatus;
    if (invocationStatus == invocation.Invocation.InvocationStatus.DISCONNECTED_INVOCATION_STATUS) {
      return "disconnected";
    }
    if (!this.finished && this.started) {
      return this.timeSinceStart();
    }
    return this.getHumanReadableDuration();
  }

  getStatus() {
    const invocation = this.invocations[0];
    if (!invocation) return "";

    switch (invocation.invocationStatus) {
      case InvocationStatus.COMPLETE_INVOCATION_STATUS:
        return invocation.success ? "Succeeded" : "Failed";
      case InvocationStatus.PARTIAL_INVOCATION_STATUS:
        return "In progress...";
      case InvocationStatus.DISCONNECTED_INVOCATION_STATUS:
        return "Disconnected";
      default:
        return "";
    }
  }

  getStatusClass() {
    const invocation = this.invocations[0];
    if (!invocation) return "";

    switch (invocation.invocationStatus) {
      case InvocationStatus.COMPLETE_INVOCATION_STATUS:
        return invocation.success ? "success" : "failure";
      case InvocationStatus.PARTIAL_INVOCATION_STATUS:
        return "in-progress";
      case InvocationStatus.DISCONNECTED_INVOCATION_STATUS:
        return "disconnected";
      default:
        return "";
    }
  }

  isComplete() {
    return this.invocations[0]?.invocationStatus === InvocationStatus.COMPLETE_INVOCATION_STATUS;
  }

  isInProgress() {
    return this.invocations[0]?.invocationStatus === InvocationStatus.PARTIAL_INVOCATION_STATUS;
  }

  getFaviconType() {
    let invocationStatus = this.invocations.find(() => true)?.invocationStatus;
    if (invocationStatus == invocation.Invocation.InvocationStatus.DISCONNECTED_INVOCATION_STATUS) {
      return IconType.Unknown;
    }
    if (!this.started) {
      return IconType.Unknown;
    }
    if (!this.finished) {
      return IconType.InProgress;
    }
    return this.finished.exitCode.code == 0 ? IconType.Success : IconType.Failure;
  }

  getStatusIcon() {
    let invocationStatus = this.invocations.find(() => true)?.invocationStatus;
    if (invocationStatus == invocation.Invocation.InvocationStatus.DISCONNECTED_INVOCATION_STATUS) {
      return <HelpCircle className="icon" />;
    }
    if (!this.started) {
      return <HelpCircle className="icon" />;
    }
    if (!this.finished) {
      return <PlayCircle className="icon blue" />;
    }
    return this.finished.exitCode.code == 0 ? <CheckCircle className="icon green" /> : <XCircle className="icon red" />;
  }

  getCPU() {
    return this.configuration?.makeVariable.TARGET_CPU || "Unknown CPU";
  }

  getMode() {
    return this.configuration?.makeVariable.COMPILATION_MODE || "Unknown mode";
  }

  getDuration(completionTime: any, beginTime: any) {
    if (!completionTime || !beginTime) return "0";
    let nanos = (+completionTime.nanos - +beginTime.nanos) / 1000000000;
    return `${(+completionTime.seconds - +beginTime.seconds + nanos).toFixed(3)}`;
  }

  getRuntime(label: string) {
    let testResult = this.testSummaryMap.get(label)?.buildEvent.testSummary;
    if (testResult) {
      let durationMillis = durationToMillisWithFallback(testResult.totalRunDuration, testResult.totalRunDurationMillis);
      return (durationMillis / 1000).toFixed(3) + " seconds";
    }
    return (
      this.getDuration(this.completedMap.get(label)?.eventTime, this.configuredMap.get(label)?.eventTime) + " seconds"
    );
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

  getLinks() {
    let links = this.buildMetadataMap?.get("BUILDBUDDY_LINKS")?.split(",");
    return (
      links
        ?.map((link) => link?.match(/\[(?<linkText>.*)\]\((?<linkUrl>.*)\)/)?.groups)
        ?.filter((link) => link?.linkUrl?.startsWith("http://") || link?.linkUrl?.startsWith("https://")) || []
    );
  }

  getFiles(event: build_event_stream.IBuildEvent): build_event_stream.IFile[] {
    if (!event?.completed) {
      return [];
    }
    if (event.completed.directoryOutput?.length) {
      return event.completed.directoryOutput || [];
    }
    return (
      event.completed.outputGroup
        ?.flatMap((group) => group.fileSets)
        .flatMap((set) => this.fileSetIDToFilesMap.get(set.id) || []) || []
    );
  }

  isQuery() {
    return this.getCommand() == "query";
  }

  hasChunkedEventLogs(): boolean {
    return this.invocations[0]?.hasChunkedEventLogs || false;
  }
}
