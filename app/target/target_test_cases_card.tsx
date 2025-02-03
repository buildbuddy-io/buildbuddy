import React from "react";
import format from "../format/format";
import { AlertCircle, XCircle, PlayCircle, CheckCircle } from "lucide-react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { durationToMillisWithFallback } from "../util/proto";
import TargetTestSuiteComponent from "./target_test_suite";

interface Props {
  buildEvent?: build_event_stream.BuildEvent;
  testSuite: Element;
  tagName?: string;
  dark?: boolean;
}

export default class TargetTestCasesCardComponent extends React.Component<Props> {
  getStatusTitle() {
    switch (this.props.tagName) {
      case "failure":
        return "failed";
      case "error":
        return "errored";
      case "skipped":
        return "skipped";
      default:
        return "passed";
    }
  }

  renderStatusIcon() {
    switch (this.props.tagName) {
      case "failure":
        return <XCircle className="icon red" />;
      case "error":
        return <AlertCircle className="icon black" />;
      case "skipped":
        return <PlayCircle className="icon" />;
      default:
        return <CheckCircle className="icon green" />;
    }
  }

  getCardClass() {
    switch (this.props.tagName) {
      case "failure":
        return "card-failure";
      case "error":
        return "card-broken";
      case "skipped":
        return "card-neutral";
      default:
        return "card-success";
    }
  }

  render() {
    let testCases = Array.from(this.props.testSuite.getElementsByTagName("testcase")).filter((testCase) => {
      let isSuccessCard = this.props.tagName === undefined;
      let hasMatchingChildren =
        this.props.tagName !== undefined && testCase.getElementsByTagName(this.props.tagName).length > 0;
      let hasFailureErrorSkippedChildren =
        testCase.getElementsByTagName("failure").length > 0 ||
        testCase.getElementsByTagName("error").length > 0 ||
        testCase.getElementsByTagName("skipped").length > 0;
      return (isSuccessCard && !hasFailureErrorSkippedChildren) || (!isSuccessCard && hasMatchingChildren);
    });
    return (
      testCases.length > 0 && (
        <div className={`card artifacts ${this.getCardClass()}`}>
          {this.renderStatusIcon()}
          <div className="content">
            <div className="title">{this.props.testSuite.getAttribute("name")}</div>
            <div className="test-subtitle">
              {testCases.length} {testCases.length == 1 ? "test" : "tests"} {this.getStatusTitle()} in{" "}
              {this.props.testSuite.getAttribute("time")
                ? `${this.props.testSuite.getAttribute("time")} s`
                : format.durationMillis(
                    durationToMillisWithFallback(
                      this.props.buildEvent?.testResult?.testAttemptDuration,
                      this.props.buildEvent?.testResult?.testAttemptDurationMillis || 0
                    )
                  )}
            </div>
            <TargetTestSuiteComponent testCases={testCases} dark={this.props.dark ?? false}></TargetTestSuiteComponent>
          </div>
        </div>
      )
    );
  }
}
