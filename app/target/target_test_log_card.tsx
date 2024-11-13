import React from "react";
import format from "../format/format";
import SetupCodeComponent from "../docs/setup_code";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { TerminalComponent } from "../terminal/terminal";
import rpcService from "../service/rpc_service";
import { CheckCircle, Clock, HelpCircle, PauseCircle, XCircle } from "lucide-react";
import { durationToMillisWithFallback } from "../util/proto";
import router from "../router/router";
import moment from "moment";
import Link from "../components/link/link";

interface Props {
  buildEvent?: build_event_stream.BuildEvent;
  invocationId: string;
  dark: boolean;
}

interface State {
  testLog: string;
  cacheEnabled: boolean;
  loading: boolean;
}

function getStatusClass(status?: build_event_stream.TestStatus) {
  switch (status) {
    case build_event_stream.TestStatus.PASSED:
      return "card-success";
    case build_event_stream.TestStatus.FLAKY:
      return "card-flaky";
    case build_event_stream.TestStatus.TIMEOUT:
      return "card-timeout";
    default:
      return "card-failure";
  }
}

function getStatusIcon(status?: build_event_stream.TestStatus) {
  switch (status) {
    case build_event_stream.TestStatus.PASSED:
      return <CheckCircle className="icon green" />;
    case build_event_stream.TestStatus.FLAKY:
      return <HelpCircle className="icon orange" />;
    case build_event_stream.TestStatus.TIMEOUT:
      return <Clock className="icon" />;
    default:
      return <XCircle className="icon red" />;
  }
}

function getStatusTitle(status?: build_event_stream.TestStatus) {
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

export default class TargetTestLogCardComponent extends React.Component<Props, State> {
  state: State = {
    testLog: "",
    cacheEnabled: true,
    loading: false,
  };

  componentDidMount() {
    this.fetchTestLog();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.buildEvent !== prevProps.buildEvent) {
      this.fetchTestLog();
    }
  }

  fetchTestLog() {
    let testLogUrl = this.props.buildEvent?.testResult?.testActionOutput.find((log: any) => log.name == "test.log")
      ?.uri;

    if (!testLogUrl) {
      return;
    }

    if (!testLogUrl.startsWith("bytestream://")) {
      this.setState({ cacheEnabled: false });
      return;
    }

    this.setState({ loading: true });
    rpcService
      .fetchBytestreamFile(testLogUrl, this.props.invocationId)
      .then((contents: string) => {
        this.setState({ testLog: contents, loading: false });
      })
      .catch(() => {
        this.setState({
          loading: false,
          testLog: "Error loading bytestream test.log!",
        });
      });
  }

  render() {
    const title = <div className="title">Test log</div>;
    const strategy = this.props.buildEvent?.testResult?.executionInfo?.strategy;
    return (
      <>
        <div className={`card ${getStatusClass(this.props.buildEvent?.testResult?.status)}`}>
          {getStatusIcon(this.props.buildEvent?.testResult?.status)}
          <div className="content">
            <div className="title">
              {getStatusTitle(this.props.buildEvent?.testResult?.status)} in{" "}
              {format.durationMillis(
                durationToMillisWithFallback(
                  this.props.buildEvent?.testResult?.testAttemptDuration,
                  this.props.buildEvent?.testResult?.testAttemptDurationMillis ?? 0
                )
              )}
              {strategy && <> ({strategy})</>}
            </div>
            <div className="subtitle">
              On Shard {this.props.buildEvent?.id?.testResult?.shard ?? 0} (Run{" "}
              {this.props.buildEvent?.id?.testResult?.run ?? 0}, Attempt{" "}
              {this.props.buildEvent?.id?.testResult?.attempt ?? 0})
            </div>
          </div>
        </div>
        <div
          className={`card ${this.state.cacheEnabled && (this.props.dark ? "dark" : "light-terminal")} ${getStatusClass(
            this.props.buildEvent?.testResult?.status
          )}`}>
          <PauseCircle className={`icon rotate-90 ${this.props.dark ? "white" : ""}`} />
          <div className="content">
            {!this.state.cacheEnabled && (
              <div className="empty-state">
                {title}
                Test log uploading isn't enabled for this invocation.
                <br />
                <br />
                To enable test log uploading you must add GRPC remote caching. You can do so by checking{" "}
                <b>Enable cache</b> below, updating your <b>.bazelrc</b> accordingly, and re-running your invocation:
                <SetupCodeComponent />
              </div>
            )}
            {this.state.cacheEnabled && (
              <div className="test-log">
                <TerminalComponent
                  title={title}
                  value={this.state.testLog || "Empty log"}
                  loading={this.state.loading}
                  lightTheme={!this.props.dark}
                />
              </div>
            )}
          </div>
        </div>
      </>
    );
  }
}

interface FlakyCardProps {
  invocationId: string;
  invocationStartTimeUsec: number;
  target: string;
  testResult: build_event_stream.TestResult;
  logContents: string;
  dark: boolean;
}

export class FlakyTargetSampleLogCardComponent extends React.Component<FlakyCardProps> {
  render() {
    const status = this.props.testResult.status;
    const logs = this.props.logContents ?? "Failed to load failure logs.";

    return (
      <div className={`card artifacts ${getStatusClass(status)}`}>
        {getStatusIcon(status)}
        <div className="content">
          <Link
            href={
              router.getInvocationUrl(this.props.invocationId) + "?target=" + encodeURIComponent(this.props.target)
            }>
            <div className="title">
              {getStatusTitle(status)}: {this.props.target}
            </div>
            <div className="test-subtitle">
              Failed to load test.xml, showing raw logs. Total test duration:{" "}
              {format.durationMillis(
                durationToMillisWithFallback(
                  this.props.testResult.testAttemptDuration,
                  this.props.testResult.testAttemptDurationMillis
                )
              )}{" "}
              <span title={format.formatTimestampUsec(this.props.invocationStartTimeUsec)}>
                ({moment(this.props.invocationStartTimeUsec / 1000).fromNow()})
              </span>
            </div>
          </Link>
          <div className="test-log">
            <TerminalComponent title="" value={logs} lightTheme={!this.props.dark} />
          </div>
        </div>
      </div>
    );
  }
}
