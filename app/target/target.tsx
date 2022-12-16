import React from "react";

import TargetTestLogCardComponent from "./target_test_log_card";
import TargetTestDocumentCardComponent from "./target_test_document_card";
import TargetArtifactsCardComponent from "./target_artifacts_card";
import ActionCardComponent from "./action_card";
import router from "../router/router";
import format from "../format/format";
import { User } from "../auth/auth_service";
import { Hash, Target, Box, SkipForward, CheckCircle, XCircle, HelpCircle, Clock, Copy, History } from "lucide-react";
import { invocation } from "../../proto/invocation_ts_proto";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { copyToClipboard } from "../util/clipboard";
import alert_service from "../alert/alert_service";
import { timestampToDateWithFallback } from "../util/proto";
import { OutlinedLinkButton } from "../components/button/link_button";

interface Props {
  invocationId: string;
  user?: User;
  repo?: string;
  targetLabel: string;
  hash: string;

  files: build_event_stream.IFile[];
  configuredEvent: invocation.InvocationEvent;
  completedEvent: invocation.InvocationEvent;
  skippedEvent: invocation.InvocationEvent;
  testResultEvents: invocation.InvocationEvent[];
  testSummaryEvent: invocation.InvocationEvent;
  actionEvents: invocation.InvocationEvent[];
  dark: boolean;
}

export default class TargetComponent extends React.Component<Props> {
  componentWillMount() {
    document.title = `Target ${this.props.invocationId} | BuildBuddy`;
  }

  handleOrganizationClicked() {
    router.navigateHome();
  }

  handleInvocationClicked() {
    router.navigateToInvocation(this.props.invocationId);
  }

  renderStatusIcon(status: build_event_stream.TestStatus): JSX.Element {
    if (this.props?.skippedEvent) {
      return <SkipForward className="icon purple" />;
    }

    if (!this.props?.testSummaryEvent) {
      return this.props?.completedEvent?.buildEvent?.completed?.success ? (
        <CheckCircle className="icon green" />
      ) : (
        <XCircle className="icon red" />
      );
    }

    switch (status) {
      case build_event_stream.TestStatus.PASSED:
        return <CheckCircle className="icon green" />;
      case build_event_stream.TestStatus.FLAKY:
        return <HelpCircle className="icon orange" />;
      case build_event_stream.TestStatus.TIMEOUT:
        return <Clock className="icon" />;
      default:
        return <XCircle className="icon red" />;
    }
  }

  getStatusTitle(status: build_event_stream.TestStatus) {
    if (this.props?.skippedEvent) {
      return "Skipped";
    }

    if (!this.props?.testSummaryEvent) {
      return this.props?.completedEvent?.buildEvent?.completed?.success ? "Success" : "Failure";
    }

    switch (status) {
      case build_event_stream.TestStatus.PASSED:
        return "Passed";
      case build_event_stream.TestStatus.FLAKY:
        return "Flaky";
      case build_event_stream.TestStatus.TIMEOUT:
        return "Timeout";
      case build_event_stream.TestStatus.FAILED:
        return "Failed";
      case build_event_stream.TestStatus.INCOMPLETE:
        return "Incomplete";
      case build_event_stream.TestStatus.REMOTE_FAILURE:
        return "Remote failure";
      case build_event_stream.TestStatus.FAILED_TO_BUILD:
        return "Failed to build";
      case build_event_stream.TestStatus.TOOL_HALTED_BEFORE_TESTING:
        return "Halted before testing";
      default:
        return "Unknown";
    }
  }

  getStatusClass(status: build_event_stream.TestStatus) {
    switch (status) {
      case build_event_stream.TestStatus.PASSED:
        return "test-passed";
      case build_event_stream.TestStatus.FLAKY:
        return "test-flaky";
      case build_event_stream.TestStatus.TIMEOUT:
      case build_event_stream.TestStatus.FAILED:
      case build_event_stream.TestStatus.REMOTE_FAILURE:
      case build_event_stream.TestStatus.FAILED_TO_BUILD:
        return "test-failed";
      case build_event_stream.TestStatus.INCOMPLETE:
        return "test-error";
      default:
        return "test-error";
    }
  }

  getTestSize(size: build_event_stream.TestSize) {
    switch (size) {
      case build_event_stream.TestSize.SMALL:
        return "Small test";
      case build_event_stream.TestSize.MEDIUM:
        return "Medium test";
      case build_event_stream.TestSize.LARGE:
        return "Large test";
      case build_event_stream.TestSize.ENORMOUS:
        return "Enormous test";
    }
  }

  resultSort(a: invocation.InvocationEvent, b: invocation.InvocationEvent) {
    let statusDiff = b.buildEvent.testResult.status - a.buildEvent.testResult.status;
    if (statusDiff != 0) {
      return statusDiff;
    }
    let shardDiff = a.buildEvent.id.testResult.shard - b.buildEvent.id.testResult.shard;
    if (shardDiff != 0) {
      return shardDiff;
    }
    let runDiff = a.buildEvent.id.testResult.run - b.buildEvent.id.testResult.run;
    if (runDiff != 0) {
      return runDiff;
    }
    return a.buildEvent.id.testResult.attempt - b.buildEvent.id.testResult.attempt;
  }

  actionSort(a: invocation.InvocationEvent, b: invocation.InvocationEvent) {
    return b.buildEvent.action.exitCode - a.buildEvent.action.exitCode;
  }

  getTime() {
    const testSummary = this.props?.testSummaryEvent?.buildEvent?.testSummary;
    if (testSummary?.lastStopTime || testSummary?.lastStopTimeMillis) {
      const lastStopDate = timestampToDateWithFallback(testSummary?.lastStopTime, testSummary?.lastStopTimeMillis);
      return format.formatDate(lastStopDate);
    }
    if (this.props?.completedEvent?.eventTime?.seconds) {
      return format.formatTimestampMillis(+this.props?.completedEvent?.eventTime.seconds * 1000);
    }
    return format.formatTimestampMillis(+this.props?.configuredEvent?.eventTime?.seconds * 1000);
  }

  handleCopyClicked(label: string) {
    copyToClipboard(label);
    alert_service.success("Label copied to clipboard!");
  }

  getTargetHistoryURL() {
    // Test history doesn't work without a repo selected.
    if (!this.props.repo) return "";

    const search = new URLSearchParams({
      filter: this.props.targetLabel,
      repo: this.props.repo,
    });
    return `/tests/?${search}`;
  }

  generateRunName(testResult: build_event_stream.BuildEventId.ITestResultId) {
    return `Run ${testResult.run} (Attempt ${testResult.attempt}, Shard ${testResult.shard})`;
  }

  render() {
    let historyURL = this.getTargetHistoryURL();
    let resultEvents = this.props.testResultEvents?.sort(this.resultSort) || [];
    let actionEvents = this.props.actionEvents?.sort(this.actionSort) || [];
    return (
      <div className="target-page">
        <div className="shelf">
          <div className="container">
            <div className="breadcrumbs">
              {this.props.user && (
                <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">
                  {this.props.user?.selectedGroupName()}
                </span>
              )}
              {this.props.user && (
                <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">
                  Builds
                </span>
              )}
              <span onClick={this.handleInvocationClicked.bind(this)} className="clickable">
                Invocation {this.props.invocationId}
              </span>
              <span>Target {this.props.targetLabel}</span>
            </div>
            <div className="titles">
              <div className="title">
                {this.props.targetLabel}{" "}
                <Copy className="copy-icon" onClick={this.handleCopyClicked.bind(this, this.props.targetLabel)} />
              </div>
              <div className="subtitle">{this.getTime()}</div>
              {historyURL && (
                <OutlinedLinkButton href={historyURL} className="target-history-button">
                  <History className="icon" />
                  <span>Target history</span>
                </OutlinedLinkButton>
              )}
            </div>
            <div className="details">
              <div className="detail">
                {this.renderStatusIcon(this.props?.testSummaryEvent?.buildEvent?.testSummary?.overallStatus)}
                {this.getStatusTitle(this.props?.testSummaryEvent?.buildEvent?.testSummary?.overallStatus)}
              </div>

              {this.props?.testSummaryEvent && (
                <div className="detail">
                  <Hash className="icon" />
                  {this.props?.testSummaryEvent?.buildEvent?.testSummary?.totalRunCount} total runs
                </div>
              )}
              <div className="detail">
                <Target className="icon" />
                {this.props?.configuredEvent?.buildEvent?.configured.targetKind ||
                  this.props.actionEvents?.map((action) => action?.buildEvent?.action?.type).join(",")}
              </div>
              {this.props?.configuredEvent?.buildEvent?.configured.testSize > 0 && (
                <div className="detail">
                  <Box className="icon" />
                  {this.getTestSize(this.props?.configuredEvent?.buildEvent?.configured.testSize)}
                </div>
              )}
            </div>
          </div>
        </div>
        <div className="container nopadding-dense">
          {resultEvents.length > 1 && (
            <div className={`runs ${resultEvents.length > 9 && "run-grid"}`}>
              {resultEvents.map((result, index) => (
                <a
                  href={`#${index + 1}`}
                  title={this.generateRunName(result.buildEvent.id.testResult)}
                  className={`run ${this.getStatusClass(result.buildEvent.testResult.status)} ${
                    (this.props.hash || "#1") == `#${index + 1}` ? "selected" : ""
                  }`}>
                  Run {result.buildEvent.id.testResult.run} (Attempt {result.buildEvent.id.testResult.attempt}, Shard{" "}
                  {result.buildEvent.id.testResult.shard})
                </a>
              ))}
            </div>
          )}
          {resultEvents
            .filter((value, index) => `#${index + 1}` == (this.props.hash || "#1"))
            .map((result) => (
              <span>
                <TargetTestDocumentCardComponent
                  dark={this.props.dark}
                  invocationId={this.props.invocationId}
                  testResult={result}
                />
                <TargetTestLogCardComponent
                  dark={this.props.dark}
                  invocationId={this.props.invocationId}
                  testResult={result}
                />
              </span>
            ))}
          {actionEvents.map((action) => (
            <ActionCardComponent dark={this.props.dark} invocationId={this.props.invocationId} action={action} />
          ))}
          {this.props.files && (
            <TargetArtifactsCardComponent
              name={"Target outputs"}
              invocationId={this.props.invocationId}
              files={this.props.files as build_event_stream.File[]}
            />
          )}
          {resultEvents
            .filter(
              (value, index) =>
                `#${index + 1}` == (this.props.hash || "#1") && value.buildEvent?.testResult?.testActionOutput
            )
            .map((result) => (
              <div>
                <TargetArtifactsCardComponent
                  name={this.generateRunName(result.buildEvent.id.testResult)}
                  invocationId={this.props.invocationId}
                  files={result.buildEvent?.testResult?.testActionOutput as build_event_stream.File[]}
                />
              </div>
            ))}
        </div>
      </div>
    );
  }
}
