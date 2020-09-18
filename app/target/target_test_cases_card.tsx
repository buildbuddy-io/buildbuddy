import React from "react";
import format from "../format/format";

import { invocation } from "../../proto/invocation_ts_proto";

interface Props {
  testResult: invocation.InvocationEvent;
  testSuite: Element;
  tagName?: string;
}

export default class TargetTestCasesCardComponent extends React.Component {
  props: Props;

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

  getStatusIcon() {
    switch (this.props.tagName) {
      case "failure":
        return "/image/x-circle.svg";
      case "error":
        return "/image/alert-circle-regular.svg";
      case "skipped":
        return "/image/skipped-circle.svg";
      default:
        return "/image/check-circle.svg";
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
    let testCases = Array.from(this.props.testSuite.getElementsByTagName("testcase")).filter(
      (testCase) =>
        (!this.props.tagName && testCase.children.length == 0) ||
        testCase.getElementsByTagName(this.props.tagName).length > 0
    );
    return (
      testCases.length > 0 && (
        <div className={`card artifacts ${this.getCardClass()}`}>
          <img className="icon" src={this.getStatusIcon()} />
          <div className="content">
            <div className="title">{this.props.testSuite.getAttribute("name")}</div>
            <div className="test-subtitle">
              {testCases.length} {testCases.length == 1 ? "test" : "tests"} {this.getStatusTitle()} in{" "}
              {format.durationMillis(this.props.testResult.buildEvent.testResult.testAttemptDurationMillis)}
            </div>
            <div className="test-document">
              <div className="test-suite">
                <div className="test-cases">
                  {testCases.map((testCase) => (
                    <div className="test-case-container">
                      <div className="test-case">
                        <div className="test-case-name">
                          {testCase.getAttribute("classname") && (
                            <span className="test-class">{testCase.getAttribute("classname")}.</span>
                          )}
                          {testCase.getAttribute("name")}
                        </div>
                        <div className="test-case-time">{testCase.getAttribute("time")} s</div>
                      </div>
                      {Array.from(testCase.children).map((child) => (
                        <div className="test-case-info">
                          <div className="test-case-message">
                            {child.getAttribute("message")} {child.getAttribute("type")}
                          </div>
                          <div className="test-case-contents">
                            {child.innerHTML.replace("<![CDATA[", "").replace("--]]>", "")}
                          </div>
                        </div>
                      ))}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      )
    );
  }
}
