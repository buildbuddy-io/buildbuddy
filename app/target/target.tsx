import React from 'react';

import TargetTestLogCardComponent from './target_test_log_card';
import TargetTestDocumentCardComponent from './target_test_document_card';
import TargetArtifactsCardComponent from './target_artifacts_card';
import router from '../router/router';
import format from '../format/format';
import { User } from '../auth/auth_service';

import { invocation } from '../../proto/invocation_ts_proto';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';

interface Props {
  invocationId: string;
  user?: User;
  targetLabel: string;
  hash: string;

  configuredEvent: invocation.InvocationEvent;
  completedEvent: invocation.InvocationEvent;
  testResultEvents: invocation.InvocationEvent[];
  testSummaryEvent: invocation.InvocationEvent;
}

export default class TargetComponent extends React.Component {

  props: Props;

  componentWillMount() {
    document.title = `Target ${this.props.invocationId} | Buildbuddy`;
  }

  handleOrganizationClicked() {
    router.navigateHome();
  }

  handleInvocationClicked() {
    router.navigateToInvocation(this.props.invocationId);
  }

  getStatusIcon(status: build_event_stream.TestStatus) {
    switch (status) {
      case build_event_stream.TestStatus.PASSED:
        return "/image/check-circle.svg";
      case build_event_stream.TestStatus.FLAKY:
        return "/image/help-circle.svg";
      default:
        return "/image/x-circle.svg";
    }
  }

  getStatusTitle(status: build_event_stream.TestStatus) {
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
      case build_event_stream.TestStatus.FAILED:
        return "test-failed";
      case build_event_stream.TestStatus.INCOMPLETE:
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
    let statusDiff = b.buildEvent.testResult.status - a.buildEvent.testResult.status
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

  render() {
    let resultEvents = this.props.testResultEvents?.sort(this.resultSort) || [];
    return (
      <div>
        <div className="shelf">
          <div className="container history-overview">
            <div className="breadcrumbs">
              {this.props.user && <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">{this.props.user?.selectedGroupName()}</span>}
              {this.props.user && <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">Builds</span>}
              <span onClick={this.handleInvocationClicked.bind(this)} className="clickable">Invocation {this.props.invocationId}</span>
              <span>Target {this.props.targetLabel}</span>
            </div>
            <div className="titles">
              <div className="title">
                {this.props.targetLabel}
              </div>
              <div className="subtitle">
                {this.props?.testSummaryEvent?.buildEvent?.testSummary?.lastStopTimeMillis ? format.formatTimestampMillis(this.props?.testSummaryEvent?.buildEvent?.testSummary?.lastStopTimeMillis) : format.formatTimestampMillis(+this.props?.completedEvent?.eventTime.seconds * 1000)}
              </div>
            </div>
            <div className="details">
              {this.props?.testSummaryEvent && <div className="detail">
                <img className="icon" src={this.getStatusIcon(this.props?.testSummaryEvent?.buildEvent?.testSummary?.overallStatus)} />
                {this.getStatusTitle(this.props?.testSummaryEvent?.buildEvent?.testSummary?.overallStatus)}
              </div>}
              {!this.props?.testSummaryEvent && <div className="detail">
                <img className="icon" src={this.props?.completedEvent?.buildEvent?.completed?.success ? "/image/check-circle.svg" : "/image/x-circle.svg"} />
                {this.props?.completedEvent?.buildEvent?.completed?.success ? "Succeeded" : "Failed"}
              </div>}
              {this.props?.testSummaryEvent && <div className="detail">
                <img className="icon" src="/image/hash.svg" />
                {this.props?.testSummaryEvent?.buildEvent?.testSummary?.totalRunCount} total runs
              </div>}
              <div className="detail">
                <img className="icon" src="/image/target-regular.svg" />
                {this.props?.configuredEvent?.buildEvent?.configured.targetKind}
              </div>
              {this.props?.configuredEvent?.buildEvent?.configured.testSize > 0 && <div className="detail">
                <img className="icon" src="/image/box-regular.svg" />
                {this.getTestSize(this.props?.configuredEvent?.buildEvent?.configured.testSize)}
              </div>}
            </div>
          </div>
        </div>
        <div className="container">
          {resultEvents.length > 1 && <div className={`runs ${resultEvents.length > 9 && "run-grid"}`}>
            {resultEvents.map((result, index) =>
              <a href={`#${index + 1}`}
                title={`Run ${result.buildEvent.id.testResult.run} (Attempt ${result.buildEvent.id.testResult.attempt}, Shard ${result.buildEvent.id.testResult.shard})`}
                className={`run ${this.getStatusClass(result.buildEvent.testResult.status)} ${((this.props.hash || "#1") == `#${index + 1}`) && 'selected'}`}>
                Run {result.buildEvent.id.testResult.run} (Attempt {result.buildEvent.id.testResult.attempt}, Shard {result.buildEvent.id.testResult.shard})
              </a>)}
          </div>}
          {resultEvents.filter((value, index) => `#${index + 1}` == (this.props.hash || '#1')).map((result) =>
            <span>
              <TargetTestDocumentCardComponent invocationId={this.props.invocationId} testResult={result} />
              <TargetTestLogCardComponent invocationId={this.props.invocationId} testResult={result} />
            </span>
          )}
          <TargetArtifactsCardComponent invocationId={this.props.invocationId} files={this.props?.completedEvent?.buildEvent?.completed.importantOutput as build_event_stream.File[]} />
        </div>
      </div>
    );
  }
}
