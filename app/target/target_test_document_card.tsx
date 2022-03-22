import React from "react";

import { invocation } from "../../proto/invocation_ts_proto";
import rpcService from "../service/rpc_service";
import TargetTestCasesCardComponent from "./target_test_cases_card";
import TargetLogCardComponent from "./target_log_card";

interface Props {
  testResult: invocation.InvocationEvent;
  invocationId: string;
  dark: boolean;
}

interface State {
  testLog: string;
  testDocument: XMLDocument;
  cacheEnabled: boolean;
}

export default class TargetTestDocumentCardComponent extends React.Component<Props> {
  state: State = {
    testLog: "",
    testDocument: null,
    cacheEnabled: true,
  };

  componentDidMount() {
    this.fetchTestXML();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.testResult !== prevProps.testResult) {
      this.fetchTestXML();
    }
  }

  fetchTestXML() {
    let testXMLUrl = this.props.testResult.buildEvent.testResult.testActionOutput.find(
      (log: any) => log.name == "test.xml"
    )?.uri;

    if (!testXMLUrl) {
      this.setState({ testDocument: null });
      return;
    }

    if (!testXMLUrl.startsWith("bytestream://")) {
      this.setState({ testDocument: null, cacheEnabled: false });
      return;
    }

    rpcService
      .fetchBytestreamFile(testXMLUrl, this.props.invocationId)
      .then((contents: string) => {
        let parser = new DOMParser();
        let xmlDoc = parser.parseFromString(contents, "text/xml");
        this.setState({ testDocument: xmlDoc });
      })
      .catch(() => {
        this.setState({
          testDocument: null,
          testLog: "Error loading bytestream test.xml!",
        });
      });
  }

  render() {
    return (
      <span>
        {this.state.testDocument &&
          this.state.cacheEnabled &&
          Array.from(this.state.testDocument.getElementsByTagName("testsuite"))
            .filter((testSuite) => testSuite.getElementsByTagName("testcase").length > 0)
            .sort((a, b) => +(b.getAttribute("failures") || 0) - +(a.getAttribute("failures") || 0))
            .map((testSuite) => (
              <div>
                <div className="stat-cards">
                  {+testSuite.getAttribute("failures") > 0 && (
                    <div className="card card-failure">
                      <div className="stat">{testSuite.getAttribute("failures") || 0}</div>
                      <div className="stat-label">failed</div>
                    </div>
                  )}
                  {+testSuite.getAttribute("errors") > 0 && (
                    <div className="card card-broken">
                      <div className="stat">{testSuite.getAttribute("errors") || 0}</div>
                      <div className="stat-label">errors</div>
                    </div>
                  )}
                  <div className="card card-success">
                    <div className="stat">
                      {+testSuite.getAttribute("tests") -
                        +testSuite.getAttribute("failures") -
                        +testSuite.getAttribute("errors") -
                        +testSuite.getAttribute("skipped") || 0}
                    </div>
                    <div className="stat-label">passed</div>
                  </div>
                  {+testSuite.getAttribute("skipped") > 0 && (
                    <div className="card card-neutral">
                      <div className="stat">{testSuite.getAttribute("skipped") || 0}</div>
                      <div className="stat-label">skipped</div>
                    </div>
                  )}
                  <div className="card">
                    <div className="stat">Run {this.props.testResult.buildEvent.id.testResult.run}</div>
                    <div className="stat-label">
                      (Attempt {this.props.testResult.buildEvent.id.testResult.attempt}, Shard{" "}
                      {this.props.testResult.buildEvent.id.testResult.shard})
                    </div>
                  </div>
                </div>
                <TargetTestCasesCardComponent
                  testResult={this.props.testResult}
                  testSuite={testSuite}
                  tagName="error"
                />
                <TargetTestCasesCardComponent
                  testResult={this.props.testResult}
                  testSuite={testSuite}
                  tagName="failure"
                />
                <TargetTestCasesCardComponent testResult={this.props.testResult} testSuite={testSuite} />
                <TargetTestCasesCardComponent
                  testResult={this.props.testResult}
                  testSuite={testSuite}
                  tagName="skipped"
                />

                {Array.from(testSuite.children)
                  .filter(
                    (child) =>
                      (child.tagName == "system-out" || child.tagName == "system-err") && child.innerHTML.length > 0
                  )
                  .map((child) => (
                    <TargetLogCardComponent
                      contents={child.textContent}
                      title={`${testSuite.getAttribute("name")} ${child.tagName}`}
                      dark={this.props.dark}
                    />
                  ))}
              </div>
            ))}
      </span>
    );
  }
}
