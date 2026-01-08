import { PlayCircle } from "lucide-react";
import React from "react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import { TerminalComponent } from "../terminal/terminal";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  dark: boolean;
}

interface State {
  stdout?: string;
  stderr?: string;
  loading: boolean;
}

export default class InvocationRunOutputCardComponent extends React.Component<Props, State> {
  state: State = { loading: true };

  private inFlightFetch: CancelablePromise | null = null;

  componentDidMount() {
    this.fetchRunOutput();
  }

  componentDidUpdate(prevProps: Props): void {
    if (prevProps.model.getInvocationId() !== this.props.model.getInvocationId()) {
      this.fetchRunOutput();
    }
  }

  fetchRunOutput() {
    this.inFlightFetch?.cancel();

    const ro = this.props.model.runOutput as build_event_stream.RunOutput | undefined;
    if (!ro) {
      this.setState({ stdout: "", stderr: "", loading: false });
      return;
    }

    const promises: Promise<any>[] = [];
    let stdout: string | undefined = undefined;
    let stderr: string | undefined = undefined;

    if (ro.stdout?.contents?.length) {
      stdout = new TextDecoder().decode(ro.stdout.contents);
    } else if (ro.stdout?.uri) {
      promises.push(
        rpcService
          .fetchBytestreamFile(ro.stdout.uri, this.props.model.getInvocationId(), "text")
          .then((s) => (stdout = s))
      );
    }

    if (ro.stderr?.contents?.length) {
      stderr = new TextDecoder().decode(ro.stderr.contents);
    } else if (ro.stderr?.uri) {
      promises.push(
        rpcService
          .fetchBytestreamFile(ro.stderr.uri, this.props.model.getInvocationId(), "text")
          .then((s) => (stderr = s))
      );
    }

    this.setState({ loading: promises.length > 0 });
    this.inFlightFetch = new CancelablePromise(Promise.all(promises));
    this.inFlightFetch
      .then(() => this.setState({ stdout: stdout ?? "", stderr: stderr ?? "", loading: false }))
      .catch(() => this.setState({ stdout: stdout ?? "", stderr: stderr ?? "", loading: false }));
  }

  render() {
    const combined =
      (this.state.stdout ?? "") +
      ((this.state.stdout && this.state.stderr) ? "\n" : "") +
      (this.state.stderr ?? "");

    return (
      <div className={`card build-logs-card ${this.props.dark ? "dark" : "light-terminal"}`}>
        <PlayCircle className={`icon ${this.props.dark ? "white" : ""}`} />
        <div className="content">
          <div className="details">
            <TerminalComponent
              title={<div className="title">Run output</div>}
              loading={this.state.loading}
              value={combined}
              lightTheme={!this.props.dark}
              debugId={"run-output"}
            />
          </div>
        </div>
      </div>
    );
  }
}

