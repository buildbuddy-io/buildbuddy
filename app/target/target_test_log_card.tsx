import React from "react";
import format from "../format/format";
import SetupCodeComponent from "../docs/setup_code";
import { invocation } from "../../proto/invocation_ts_proto";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { TerminalComponent } from "../terminal/terminal";
import rpcService from "../service/rpc_service";
import { PauseCircle } from "lucide-react";

interface Props {
  testResult: invocation.InvocationEvent;
  invocationId: string;
  dark: boolean;
}

interface State {
  testLog: string;
  cacheEnabled: boolean;
  loading: boolean;
}

export default class TargetTestLogCardComponent extends React.Component {
  props: Props;

  state: State = {
    testLog: "",
    cacheEnabled: true,
    loading: false,
  };

  componentDidMount() {
    this.fetchTestLog();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.testResult !== prevProps.testResult) {
      this.fetchTestLog();
    }
  }

  fetchTestLog() {
    let testLogUrl = this.props.testResult.buildEvent.testResult.testActionOutput.find(
      (log: any) => log.name == "test.log"
    )?.uri;

    if (!testLogUrl) {
      return;
    }

    if (!testLogUrl.startsWith("bytestream://")) {
      this.setState({ ...this.state, cacheEnabled: false });
      return;
    }

    this.setState({ ...this.state, loading: true });
    rpcService
      .fetchBytestreamFile(testLogUrl, this.props.invocationId)
      .then((contents: string) => {
        this.setState({ ...this.state, testLog: contents, loading: false });
      })
      .catch(() => {
        this.setState({
          ...this.state,
          loading: false,
          testLog: "Error loading bytestream test.log!",
        });
      });
  }

  getStatusTitle(status: build_event_stream.TestStatus) {
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

  getStatusClass(status: build_event_stream.TestStatus) {
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
  render() {
    const title = <div className="title">Test log</div>;
    const subtitle = (
      <div className="test-subtitle">
        {this.getStatusTitle(this.props.testResult.buildEvent.testResult.status)} in{" "}
        {format.durationMillis(this.props.testResult.buildEvent.testResult.testAttemptDurationMillis)} on Shard{" "}
        {this.props.testResult.buildEvent.id.testResult.shard} (Run {this.props.testResult.buildEvent.id.testResult.run}
        , Attempt {this.props.testResult.buildEvent.id.testResult.attempt})
      </div>
    );

    return (
      <div
        className={`card ${
          this.state.cacheEnabled && (this.props.dark ? "dark" : "light-terminal")
        } ${this.getStatusClass(this.props.testResult.buildEvent.testResult.status)}`}>
        <PauseCircle className={`icon rotate-90 ${this.props.dark ? "white" : ""}`} />
        <div className="content">
          {!this.state.cacheEnabled && (
            <div className="empty-state">
              {title}
              {subtitle}
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
                subtitle={subtitle}
                value={this.state.testLog || "Empty log"}
                loading={this.state.loading}
                lightTheme={!this.props.dark}
              />
            </div>
          )}
        </div>
      </div>
    );
  }
}
