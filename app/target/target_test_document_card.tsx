import React from "react";

import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import rpcService from "../service/rpc_service";
import TargetTestCasesCardComponent from "./target_test_cases_card";
import TargetLogCardComponent from "./target_log_card";

interface Props {
  buildEvent?: build_event_stream.BuildEvent;
  invocationId: string;
  dark: boolean;
}

interface State {
  testLog: string;
  testDocument?: XMLDocument;
  cacheEnabled: boolean;
}

export default class TargetTestDocumentCardComponent extends React.Component<Props> {
  state: State = {
    testLog: "",
    cacheEnabled: true,
  };

  componentDidMount() {
    this.fetchTestXML();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.buildEvent !== prevProps.buildEvent) {
      this.fetchTestXML();
    }
  }

  fetchTestXML() {
    let testXMLUrl = this.props.buildEvent?.testResult?.testActionOutput.find((log: any) => log.name == "test.xml")
      ?.uri;

    if (!testXMLUrl) {
      this.setState({ testDocument: undefined });
      return;
    }

    if (!testXMLUrl.startsWith("bytestream://")) {
      this.setState({ testDocument: undefined, cacheEnabled: false });
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
          testDocument: undefined,
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
                  {+(testSuite.getAttribute("failures") ?? 0) > 0 && (
                    <div className="card card-failure">
                      <div className="stat">{testSuite.getAttribute("failures") || 0}</div>
                      <div className="stat-label">failed</div>
                    </div>
                  )}
                  {+(testSuite.getAttribute("errors") ?? 0) > 0 && (
                    <div className="card card-broken">
                      <div className="stat">{testSuite.getAttribute("errors") || 0}</div>
                      <div className="stat-label">errors</div>
                    </div>
                  )}
                  <div className="card card-success">
                    <div className="stat">
                      {+(testSuite.getAttribute("tests") ?? 0) -
                        +(testSuite.getAttribute("failures") ?? 0) -
                        +(testSuite.getAttribute("errors") ?? 0) -
                        +(testSuite.getAttribute("skipped") ?? 0) || 0}
                    </div>
                    <div className="stat-label">passed</div>
                  </div>
                  {+(testSuite.getAttribute("skipped") ?? 0) > 0 && (
                    <div className="card card-neutral">
                      <div className="stat">{testSuite.getAttribute("skipped") || 0}</div>
                      <div className="stat-label">skipped</div>
                    </div>
                  )}
                  {this.props.buildEvent?.id?.testResult && (
                    <div className="card">
                      <div className="stat">Run {this.props.buildEvent.id.testResult.run}</div>
                      <div className="stat-label">
                        (Attempt {this.props.buildEvent.id.testResult.attempt}, Shard{" "}
                        {this.props.buildEvent.id.testResult.shard})
                      </div>
                    </div>
                  )}
                </div>
                <TargetTestCasesCardComponent
                  buildEvent={this.props.buildEvent}
                  testSuite={testSuite}
                  tagName="error"
                  dark={this.props.dark}
                />
                <TargetTestCasesCardComponent
                  buildEvent={this.props.buildEvent}
                  testSuite={testSuite}
                  tagName="failure"
                  dark={this.props.dark}
                />
                <TargetTestCasesCardComponent
                  buildEvent={this.props.buildEvent}
                  testSuite={testSuite}
                  dark={this.props.dark}
                />
                <TargetTestCasesCardComponent
                  buildEvent={this.props.buildEvent}
                  testSuite={testSuite}
                  tagName="skipped"
                  dark={this.props.dark}
                />

                {Array.from(testSuite.children)
                  .filter(
                    (child) =>
                      (child.tagName == "system-out" || child.tagName == "system-err") && child.innerHTML.length > 0
                  )
                  .map((child) => (
                    <TargetLogCardComponent
                      contents={reconstructEscapeSequences(child.textContent ?? "")}
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

/**
 * Attempts to reconstruct some types of ANSI escape sequences that may have
 * been lost due to Bazel stripping them out:
 * https://github.com/bazelbuild/bazel/blob/2a2def82ba5f86d96ba49d4ae9dc7878f9329214/tools/test/generate-xml.sh#L31-L36
 *
 * Specifically, Bazel replaces things like "\x1b[33m" with "?[33m" because
 * "\x1b" is not a valid UTF-8 byte. We attempt to restore the lost "\x1b" byte
 * here, because we are capable of rendering it.
 */
function reconstructEscapeSequences(text: string): string {
  return text.replaceAll(/\?\[((;|\d)*)m/g, "\x1b[$1m");
}
