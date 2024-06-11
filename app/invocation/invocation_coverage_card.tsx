import React from "react";
import SetupCodeComponent from "../docs/setup_code";
import rpcService from "../service/rpc_service";
import InvocationModel from "./invocation_model";
import Button from "../components/button/button";
import { Clock, ListChecks } from "lucide-react";
import errorService from "../errors/error_service";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { parseLcov } from "../util/lcov";
import format from "../format/format";
import { percentageColor } from "../util/color";

interface Props {
  model: InvocationModel;
}

interface State {
  report: any[] | null;
  loading: boolean;
}

export default class InvocationCoverageCardComponent extends React.Component<Props, State> {
  state: State = {
    report: null,
    loading: true,
  };

  componentDidMount() {
    this.fetchProfile();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.fetchProfile();
    }
  }

  getReportFile(): build_event_stream.File | undefined {
    return this.props.model.buildToolLogs?.log.find(
      (log: build_event_stream.File) => log.name == "coverage_report.lcov" && log.uri
    );
  }

  isCoverageEnabled() {
    return Boolean(this.getReportFile()?.uri?.startsWith("bytestream://"));
  }

  fetchProfile() {
    if (!this.isCoverageEnabled()) {
      this.setState({ loading: false });
    }

    // Already fetched
    if (this.state.report) return;

    let profileFile = this.getReportFile();
    if (!profileFile?.uri) return;

    this.setState({ loading: true });

    rpcService
      .fetchBytestreamFile(profileFile.uri, this.props.model.getInvocationId(), "text")
      .then((response) => {
        this.setState({ report: parseLcov(response) });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  downloadReport() {
    let profileFile = this.getReportFile();
    if (!profileFile?.uri) {
      return;
    }

    try {
      rpcService.downloadBytestreamFile("coverage_report.lcov", profileFile.uri, this.props.model.getInvocationId());
    } catch {
      console.error("Error downloading bytestream coverage report");
    }
  }

  renderEmptyState() {
    if (this.state.loading) {
      return (
        <div>
          <div className="loading" />
        </div>
      );
    }

    if (!this.props.model.buildToolLogs) {
      return <>Build is in progress...</>;
    }

    // Note: This profile file should be present even if remote cache is disabled,
    // so enabling remote cache won't fix a missing profile. Show a special message
    // for this case.
    if (!this.getReportFile()) {
      return (
        <>
          Could not find coverage report. This might be because Bazel was invoked without the --combined_report=lcov
          flag.
        </>
      );
    }

    return (
      <>
        <p>Coverage isn't enabled for this invocation. To enable coverage you must add gRPC remote caching.</p>
        <SetupCodeComponent
          requireCacheEnabled
          instructionsHeader={
            <p>
              To enable remote caching, check <b>Enable cache</b> below, update your <b>.bazelrc</b> accordingly, and
              re-run your invocation:
            </p>
          }
        />
      </>
    );
  }

  render() {
    if (!this.state.report) {
      return (
        <div className="card">
          <ListChecks className="icon" />
          <div className="content">
            <div className="header">
              <div className="title">Coverage</div>
            </div>
            <div className="empty-state">{this.renderEmptyState()}</div>
          </div>
        </div>
      );
    }

    let testCoverageUrl = this.getReportFile()?.uri;

    let repoPath = "";
    if (this.props.model.getRepo()?.includes("github.com")) {
      repoPath = `/code/${format.formatGitUrl(this.props.model.getRepo())}/`;
    }

    return (
      <>
        <div className="card">
          <ListChecks className="icon" />
          <div className="content">
            <div className="header">
              <div className="title">Coverage</div>
              {Boolean(this.getReportFile()?.uri) && (
                <div className="button">
                  <Button className="download-gz-file" onClick={this.downloadReport.bind(this)}>
                    Download coverage report
                  </Button>
                </div>
              )}
            </div>

            <div className="details">
              {this.state.report &&
                this.state.report.length > 0 &&
                this.state.report
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
                                }?lcov=${testCoverageUrl}&invocation_id=${this.props.model.getInvocationId()}&commit=${this.props.model.getCommit()}`
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
      </>
    );
  }
}
