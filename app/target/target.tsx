import React from 'react';

import TargetTestResultCardComponent from './target_test_result_card';
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

  render() {
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
                {this.props?.testSummaryEvent?.buildEvent?.testSummary?.lastStopTimeMillis ? format.formatTimestampMillis(this.props?.testSummaryEvent?.buildEvent?.testSummary?.lastStopTimeMillis) : format.formatTimestampMillis(+this.props?.completedEvent?.eventTime.seconds * 1000) }
              </div>
            </div>
            <div className="details">
              {this.props?.testSummaryEvent && <div className="detail">
                <img className="icon" src={this.getStatusIcon(this.props?.testSummaryEvent?.buildEvent?.testSummary?.overallStatus)} />
                {this.getStatusTitle(this.props?.testSummaryEvent?.buildEvent?.testSummary?.overallStatus)}
              </div>}
              {!this.props?.testSummaryEvent && <div className="detail">
                <img className="icon" src={this.props?.completedEvent?.buildEvent?.completed?.success ? "/image/check-circle.svg": "/image/x-circle.svg"} />
                {this.props?.completedEvent?.buildEvent?.completed?.success ? "Succeeded": "Failed"}
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
          {this.props.testResultEvents?.sort((a, b) => b.buildEvent.testResult.status - a.buildEvent.testResult.status).map((result) => 
            <TargetTestResultCardComponent testResult={result} />
          )}
          <TargetArtifactsCardComponent files={this.props?.completedEvent?.buildEvent?.completed.importantOutput as build_event_stream.File[]}/>
        </div>
      </div>
    );
  }
}
