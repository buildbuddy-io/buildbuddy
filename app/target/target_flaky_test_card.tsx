import React from "react";
import format from "../format/format";
import { AlertCircle, XCircle, HelpCircle } from "lucide-react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { durationToMillisWithFallback } from "../util/proto";
import moment from "moment";
import router from "../router/router";
import Link from "../components/link/link";
import TargetTestSuiteComponent from "./target_test_suite";

interface Props {
  invocationId: string;
  invocationStartTimeUsec: number;
  target: string;
  buildEvent: build_event_stream.BuildEvent;
  testSuite: Element;
  status: string;
  dark: boolean;
}

export default class TargetFlakyTestCardComponent extends React.Component<Props> {
  getStatusTitle() {
    switch (this.props.status) {
      case "error":
        return "Errored";
      case "flaky":
        return "Flaky";
      default:
        return "Failed";
    }
  }

  renderStatusIcon() {
    switch (this.props.status) {
      case "error":
        return <AlertCircle className="icon black" />;
      case "flaky":
        return <HelpCircle className="icon orange" />;
      default:
        return <XCircle className="icon red" />;
    }
  }

  getCardClass() {
    switch (this.props.status) {
      case "flaky":
        return "card-flaky";
      case "error":
        return "card-broken";
      default:
        return "card-failure";
    }
  }

  render() {
    let testCases = Array.from(this.props.testSuite.getElementsByTagName("testcase")).filter(
      (testCase) => testCase.getElementsByTagName("failure").length > 0
    );

    return (
      testCases.length > 0 && (
        <div className={`card artifacts ${this.getCardClass()}`}>
          {this.renderStatusIcon()}
          <div className="content">
            <Link
              href={
                router.getInvocationUrl(this.props.invocationId) + "?target=" + encodeURIComponent(this.props.target)
              }>
              <div className="title">
                {this.getStatusTitle()}: {this.props.testSuite.getAttribute("name")}
              </div>
              <div className="test-subtitle">
                {testCases.length} {testCases.length == 1 ? "test" : "tests"} failed in{" "}
                {this.props.testSuite.getAttribute("time")
                  ? `${this.props.testSuite.getAttribute("time")} s`
                  : format.durationMillis(
                      durationToMillisWithFallback(
                        this.props.buildEvent?.testResult?.testAttemptDuration,
                        this.props.buildEvent?.testResult?.testAttemptDurationMillis || 0
                      )
                    )}{" "}
                <span title={format.formatTimestampUsec(this.props.invocationStartTimeUsec)}>
                  ({moment(this.props.invocationStartTimeUsec / 1000).fromNow()})
                </span>
              </div>
            </Link>
            <TargetTestSuiteComponent testCases={testCases} dark={this.props.dark}></TargetTestSuiteComponent>
          </div>
        </div>
      )
    );
  }
}
