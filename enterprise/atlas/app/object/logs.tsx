import Long from "long";
import { RefreshCw } from "lucide-react";
import React from "react";
import { OutlinedButton } from "../../../../app/components/button/button";
import Checkbox from "../../../../app/components/checkbox/checkbox";
import Select, { Option } from "../../../../app/components/select/select";
import { BuildBuddyError } from "../../../../app/util/errors";
import { atlas } from "../../../../proto/atlas_ts_proto";
import rpcService, { ServerStream } from "../lib/rpc_service";

interface Props {
  pod: atlas.Entry;
}

interface State {
  container: string;
  tailLines: string;
  follow: boolean;
  previous: boolean;
  text: string;
  errorMessage?: string;
  streaming: boolean;
}

const TAIL_OPTIONS = ["200", "1000", "5000"];

/**
 * A pod's logs, streamed from the StreamLogs RPC. With follow on, the stream
 * stays open and new output is appended as it arrives.
 */
export default class LogsComponent extends React.Component<Props, State> {
  state: State = {
    container: this.props.pod.containers[0] ?? "",
    tailLines: TAIL_OPTIONS[0],
    follow: false,
    previous: false,
    text: "",
    streaming: false,
  };
  private stream?: ServerStream<atlas.StreamLogsResponse>;
  private decoder = new TextDecoder();
  private pane = React.createRef<HTMLPreElement>();

  componentDidMount() {
    this.start();
  }

  componentDidUpdate(_: Props, prevState: State) {
    if (
      prevState.container !== this.state.container ||
      prevState.tailLines !== this.state.tailLines ||
      prevState.follow !== this.state.follow ||
      prevState.previous !== this.state.previous
    ) {
      this.start();
    }
  }

  componentWillUnmount() {
    this.stop();
  }

  private stop() {
    this.stream?.cancel();
    this.stream = undefined;
  }

  private start() {
    this.stop();
    this.decoder = new TextDecoder();
    this.setState({ text: "", errorMessage: undefined, streaming: true });
    const pod = this.props.pod;
    this.stream = rpcService.service.streamLogs(
      new atlas.StreamLogsRequest({
        cluster: pod.cluster,
        namespace: pod.namespace,
        name: pod.name,
        container: this.state.container,
        tailLines: Long.fromNumber(Number(this.state.tailLines)),
        follow: this.state.follow,
        previous: this.state.previous,
      }),
      {
        next: (chunk) => this.append(this.decoder.decode(chunk.data, { stream: true })),
        error: (e) => this.setState({ errorMessage: BuildBuddyError.parse(e).description, streaming: false }),
        complete: () => this.setState({ streaming: false }),
      }
    );
  }

  private append(text: string) {
    // Stick to the bottom only if the reader is already there.
    const pane = this.pane.current;
    const pinned = !pane || pane.scrollHeight - pane.scrollTop - pane.clientHeight < 40;
    this.setState(
      (state) => ({ text: state.text + text }),
      () => {
        if (pinned && this.pane.current) this.pane.current.scrollTop = this.pane.current.scrollHeight;
      }
    );
  }

  render() {
    const containers = this.props.pod.containers;
    const { text, errorMessage, streaming } = this.state;
    return (
      <div className="atlas-section">
        <h3>Logs</h3>
        <div className="atlas-card">
          <div className="atlas-logbar">
            {containers.length > 1 ? (
              <Select value={this.state.container} onChange={(e) => this.setState({ container: e.target.value })}>
                {containers.map((c) => (
                  <Option key={c} value={c}>
                    {c}
                  </Option>
                ))}
              </Select>
            ) : (
              <span className="atlas-muted atlas-small">{containers[0] ?? ""}</span>
            )}
            <Select value={this.state.tailLines} onChange={(e) => this.setState({ tailLines: e.target.value })}>
              {TAIL_OPTIONS.map((n) => (
                <Option key={n} value={n}>
                  {n} lines
                </Option>
              ))}
            </Select>
            <label>
              <Checkbox checked={this.state.follow} onChange={(e) => this.setState({ follow: e.target.checked })} />{" "}
              follow
            </label>
            <label>
              <Checkbox checked={this.state.previous} onChange={(e) => this.setState({ previous: e.target.checked })} />{" "}
              previous run
            </label>
            <OutlinedButton className="atlas-small-button" onClick={() => this.start()}>
              <RefreshCw className="icon" /> reload
            </OutlinedButton>
          </div>
          <pre ref={this.pane} className="atlas-logpane">
            {errorMessage ?? (text || (streaming ? "…" : "(no output)"))}
          </pre>
        </div>
      </div>
    );
  }
}
