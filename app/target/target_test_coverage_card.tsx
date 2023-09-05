import { Percent } from "lucide-react";
import React from "react";

import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import format from "../format/format";
import rpcService from "../service/rpc_service";
import { percentageColor } from "../util/color";
import { parseLcov } from "../util/lcov";

interface Props {
  invocationId: string;
  repo: string;
  commit: string;
  buildEvent?: build_event_stream.BuildEvent;
}

interface State {
  lcov: any[];
}

export default class TargetTestCoverageCardComponent extends React.Component<Props> {
  state: State = {
    lcov: [],
  };

  componentDidMount() {
    this.fetchTestCoverage();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.buildEvent !== prevProps.buildEvent) {
      this.fetchTestCoverage();
    }
  }

  fetchTestCoverage() {
    let testCoverageUrl = this.props.buildEvent?.testResult?.testActionOutput.find(
      (log: any) => log.name == "test.lcov"
    )?.uri;

    if (!testCoverageUrl || !this.props.invocationId) {
      this.setState({ lcov: null });
      return;
    }

    if (!testCoverageUrl.startsWith("bytestream://")) {
      this.setState({ lcov: null });
      return;
    }

    const invocationId = this.props.invocationId;
    if (!invocationId) {
      this.setState({ lcov: null });
      return;
    }

    rpcService
      .fetchBytestreamFile(testCoverageUrl, invocationId)
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
    let testCoverageUrl = this.props.buildEvent?.testResult?.testActionOutput.find(
      (log: any) => log.name == "test.lcov"
    )?.uri;

    let error = undefined;
    if (!testCoverageUrl) {
      error = (
        <>
          To see test coverage data, run{" "}
          <span className="inline-code">bazel coverage {this.props.buildEvent?.id?.testResult?.label || ""}</span>
        </>
      );
    }

    if (testCoverageUrl && !testCoverageUrl.startsWith("bytestream://")) {
      error = <>To see test coverage data, enable remote caching.</>;
    }

    let repoPath = "";
    if (this.props.repo?.includes("github.com")) {
      repoPath = `/code/${format.formatGitUrl(this.props.repo)}/`;
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
                            ? `${repoPath}${record.sourceFile}?lcov=${testCoverageUrl}&invocation_id=${this.props.invocationId}&commit=${this.props.commit}`
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
