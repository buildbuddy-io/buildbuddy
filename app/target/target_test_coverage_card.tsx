import { Percent } from "lucide-react";
import React from "react";

import { invocation } from "../../proto/invocation_ts_proto";
import format from "../format/format";
import InvocationModel from "../invocation/invocation_model";
import rpcService from "../service/rpc_service";
import { percentageColor } from "../util/color";
import { parseLcov } from "../util/lcov";

interface Props {
  testResult: invocation.InvocationEvent;
  model: InvocationModel;
}

interface State {
  lcov: any[];
}

export default class TargetTestCoverageCardComponent extends React.Component<Props> {
  state: State = {
    lcov: [],
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
    let testXMLUrl = this.props.testResult.buildEvent?.testResult?.testActionOutput.find(
      (log: any) => log.name == "test.lcov"
    )?.uri;

    if (!testXMLUrl) {
      this.setState({ lcov: null });
      return;
    }

    if (!testXMLUrl.startsWith("bytestream://")) {
      this.setState({ lcov: null });
      return;
    }

    rpcService
      .fetchBytestreamFile(testXMLUrl, this.props.model.getId())
      .then((contents: string) => {
        this.setState({ lcov: parseLcov(contents) });
      })
      .catch(() => {
        this.setState({
          lcov: null,
        });
      });
  }

  render() {
    let testXMLUrl = this.props.testResult.buildEvent?.testResult?.testActionOutput.find(
      (log: any) => log.name == "test.lcov"
    )?.uri;

    let error = undefined;
    if (!testXMLUrl) {
      error = (
        <>
          To see test coverage data, run{" "}
          <span className="inline-code">
            bazel coverage {this.props.testResult?.buildEvent?.id?.testResult?.label || ""}
          </span>
        </>
      );
    }

    if (testXMLUrl && !testXMLUrl.startsWith("bytestream://")) {
      error = <>To see test coverage data, enable remote caching.</>;
    }

    let repoPath = "";
    if (this.props.model.getRepo()?.includes("github.com")) {
      repoPath = `/code/${format.formatGitUrl(this.props.model.getRepo())}/`;
    }

    return (
      <div className="card">
        <Percent className="icon purple" />
        <div className="content">
          <div className="title">Test coverage</div>
          <div className="details">
            {error}
            {!error &&
              this.state.lcov &&
              this.state.lcov.length > 0 &&
              this.state.lcov
                .filter((record) => Boolean(record.sourceFile))
                .map((record) => {
                  const percent = (record.numLinesHit * 1.0) / record.numLinesFound;
                  return (
                    <div className="coverage-record">
                      <a
                        href={
                          repoPath
                            ? `${repoPath}${
                                record.sourceFile
                              }?lcov=${testXMLUrl}&invocation_id=${this.props.model.getId()}&commit=${this.props.model.getCommit()}`
                            : "#"
                        }>
                        <span className="coverage-source">{record.sourceFile}</span>:{" "}
                        <span className="coverage-percent" style={{ color: percentageColor(percent) }}>
                          {format.percent(percent)}%
                        </span>{" "}
                        <span className="coverage-details">
                          ({record.numLinesHit} hits / {record.numLinesFound} lines)
                        </span>
                      </a>
                    </div>
                  );
                })}
          </div>
        </div>
      </div>
    );
  }
}
