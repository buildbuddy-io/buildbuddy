import React from "react";
import format from "../format/format";
import { AlertCircle, XCircle } from "lucide-react";
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
  dark: boolean;
}

type Status = "failure" | "error";

export default class TargetFlakyTestCardComponent extends React.Component<Props> {
  getStatusText(status: Status) {
    switch (status) {
      case "error":
        return "Errored";
      default:
        return "Failed";
    }
  }

  renderStatusIcon(status: Status) {
    switch (status) {
      case "error":
        return <AlertCircle className="icon black" />;
      default:
        return <XCircle className="icon red" />;
    }
  }

  getCardClass(status: Status) {
    switch (status) {
      case "error":
        return "card-broken";
      default:
        return "card-failure";
    }
  }

  renderCard(status: Status) {
    let testCases = Array.from(this.props.testSuite.getElementsByTagName("testcase")).filter(
      (testCase) => testCase.getElementsByTagName(status).length > 0
    );
    if (testCases.length === 0) {
      return <></>;
    }
    return (
      <div className={`card artifacts ${this.getCardClass(status)}`}>
        {this.renderStatusIcon(status)}
        <div className="content">
          <Link
            href={
              router.getInvocationUrl(this.props.invocationId) + "?target=" + encodeURIComponent(this.props.target)
            }>
            <div className="title">
              {this.getStatusText(status)}: {this.props.testSuite.getAttribute("name")}
            </div>
            <div className="test-subtitle">
              {testCases.length} {testCases.length == 1 ? "test" : "tests"} {this.getStatusText(status).toLowerCase()}{" "}
              in{" "}
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
    );
  }

  render() {
    return (
      <>
        {this.renderCard("failure")}
        {this.renderCard("error")}
      </>
    );
  }
}
