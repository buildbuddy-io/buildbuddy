import React from "react";
import format from "../format/format";
import SetupCodeComponent from "../docs/setup_code";
import { invocation } from "../../proto/invocation_ts_proto";
import { TerminalComponent } from "../terminal/terminal";
import rpcService from "../service/rpc_service";
import { PauseCircle, PlayCircle } from "lucide-react";

interface Props {
  action: invocation.InvocationEvent;
  invocationId: string;
  dark: boolean;
}

interface State {
  stdErr: string;
  stdOut: string;
  cacheEnabled: boolean;
  loadingStdout: boolean;
  loadingStderr: boolean;
}

export default class ActionCardComponent extends React.Component<Props, State> {
  state: State = {
    stdErr: "",
    stdOut: "",
    cacheEnabled: true,
    loadingStdout: true,
    loadingStderr: true,
  };

  componentDidMount() {
    console.log(this.props.action);
    this.fetchStdErr();
    this.fetchStdOut();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.action !== prevProps.action) {
      this.fetchStdErr();
      this.fetchStdOut();
    }
  }

  fetchStdErr() {
    let logUrl = this.props.action?.buildEvent?.action?.stderr?.uri;

    if (!logUrl) {
      return;
    }

    if (!logUrl.startsWith("bytestream://")) {
      this.setState({ cacheEnabled: false });
      return;
    }

    this.setState({ loadingStderr: true });
    rpcService
      .fetchBytestreamFile(logUrl, this.props.invocationId)
      .then((contents: string) => {
        this.setState({
          stdErr: contents,
          loadingStderr: false,
        });
      })
      .catch(() => {
        this.setState({
          stdErr: "Error loading bytestream stderr!",
        });
      });
  }

  fetchStdOut() {
    let logUrl = this.props.action?.buildEvent?.action?.stdout?.uri;

    if (!logUrl) {
      return;
    }

    if (!logUrl.startsWith("bytestream://")) {
      this.setState({ cacheEnabled: false });
      return;
    }

    this.setState({ loadingStdout: true });
    rpcService
      .fetchBytestreamFile(logUrl, this.props.invocationId)
      .then((contents: string) => {
        this.setState({
          stdOut: contents,
          loadingStdout: false,
        });
      })
      .catch(() => {
        this.setState({
          stdErr: "Error loading bytestream stdout!",
        });
      });
  }

  getStatusTitle(status: boolean) {
    return status ? "Passed" : "Failed";
  }

  render() {
    const title = <div className="title">Error Log</div>;
    const action = this.props.action?.buildEvent?.action;
    return (
      <div className="target-action-card">
        {action?.stderr?.uri && (
          <div
            className={`card ${this.state.cacheEnabled && (this.props.dark ? "dark" : "light-terminal")} ${
              action?.success ? "card-success" : "card-failure"
            }`}>
            <PauseCircle className={`icon rotate-90 ${this.props.dark ? "white" : ""}`} />
            <div className="content">
              {!this.state.cacheEnabled && (
                <>
                  {title}
                  <div className="empty-state">
                    Log uploading isn't enabled for this invocation. To enable action log uploading you must add gRPC
                    remote caching.
                    <SetupCodeComponent
                      requireCacheEnabled
                      instructionsHeader={
                        <p>
                          To enable remote cache, check <b>Enable cache</b> below, update your <b>.bazelrc</b>{" "}
                          accordingly, and re-run your invocation:
                        </p>
                      }
                    />
                  </div>
                </>
              )}
              {this.state.cacheEnabled && (
                <TerminalComponent
                  title={title}
                  loading={this.state.loadingStderr}
                  value={this.state.stdErr || "Empty log"}
                  lightTheme={!this.props.dark}
                />
              )}
            </div>
          </div>
        )}

        {action?.stdout?.uri && (
          <div className={`card ${this.state.cacheEnabled && (this.props.dark ? "dark" : "light-terminal")}`}>
            <PauseCircle className={`icon rotate-90 ${this.props.dark ? "white" : ""}`} />
            <div className="content">
              {!this.state.cacheEnabled && (
                <>
                  <div className="title">Log</div>
                  <div className="empty-state">
                    Log uploading isn't enabled for this invocation. To enable action log uploading, you must add gRPC
                    remote caching.
                    <SetupCodeComponent
                      requireCacheEnabled
                      instructionsHeader={
                        <p>
                          To enable remote caching, check <b>Enable cache</b> below, update your <b>.bazelrc</b>{" "}
                          accordingly, and re-run your invocation:
                        </p>
                      }
                    />
                  </div>
                </>
              )}
              {this.state.cacheEnabled && (
                <TerminalComponent
                  title={<div className="title">Log</div>}
                  loading={this.state.loadingStdout}
                  value={this.state.stdOut || "Empty log"}
                  lightTheme={!this.props.dark}
                />
              )}
            </div>
          </div>
        )}

        {(action?.failureDetail?.message?.length ?? 0) > 0 && (
          <div className={`card ${this.props.dark ? "dark" : "light-terminal"}`}>
            <PauseCircle className={`icon rotate-90 ${this.props.dark ? "white" : ""}`} />
            <div className="content">
              <TerminalComponent
                title={<div className="title">Failure Message</div>}
                value={action?.failureDetail?.message}
                lightTheme={!this.props.dark}
              />
            </div>
          </div>
        )}

        <div className={`card ${action?.success ? "card-success" : "card-failure"}`}>
          <PlayCircle className="icon" />
          <div className="content">
            <div className="title">{action?.label}</div>
            <div className="test-subtitle">
              {action?.type} command exited with code {action?.exitCode}
            </div>
            <div>
              {action?.commandLine.map((commandLineArg) => (
                <div className="command-line-arg">{commandLineArg}</div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
