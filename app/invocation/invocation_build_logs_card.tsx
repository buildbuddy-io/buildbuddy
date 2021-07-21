import React from "react";
import { eventlog } from "../../proto/eventlog_ts_proto";
import errorService from "../errors/error_service";
import rpcService from "../service/rpc_service";
import { TerminalComponent } from "../terminal/terminal";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  expanded: boolean;
  dark: boolean;
}

interface State {
  consoleBuffer?: string;
}

const POLL_TAIL_INTERVAL_MS = 3_000;
// How many lines to request from the server on each chunk request.
const MIN_LINES = 100_000;

export default class BuildLogsCardComponent extends React.Component<Props, State> {
  state: State = {
    consoleBuffer: "",
  };

  private pollTailTimeout: number | null = null;

  componentDidMount() {
    if (this.props.model.hasChunkedEventLogs()) {
      this.fetchTail();
    }
  }

  componentWillUnmount() {
    window.clearTimeout(this.pollTailTimeout);
  }

  private fetchTail(chunkId = "") {
    rpcService.service
      .getEventLogChunk(
        new eventlog.GetEventLogChunkRequest({
          invocationId: this.props.model.getId(),
          chunkId,
          minLines: MIN_LINES,
        })
      )
      .then((response) => {
        const consoleBuffer = this.state.consoleBuffer + String.fromCharCode(...response.chunk?.buffer);
        this.setState({ consoleBuffer });

        // Empty next chunk ID means there are no more chunks to fetch.
        if (!response.nextChunkId) return;

        // Unchanged next chunk ID means the chunk has not yet been written
        // yet and that we should poll for it.
        if (response.nextChunkId === chunkId) {
          this.pollTailTimeout = window.setTimeout(() => this.fetchTail(chunkId), POLL_TAIL_INTERVAL_MS);
          return;
        }

        // New next chunk ID means we successfully fetched the requested
        // chunk, and more may be available. Try fetching it immediately.
        this.fetchTail(response.nextChunkId);
      })
      .catch((e) => errorService.handleError(e));
  }

  private getConsoleBuffer() {
    if (!this.props.model.hasChunkedEventLogs()) {
      return this.props.model.consoleBuffer || "";
    }

    return this.state.consoleBuffer;
  }

  render() {
    return (
      <div className={`card ${this.props.dark ? "dark" : "light-terminal"} ${this.props.expanded ? "expanded" : ""}`}>
        <img className="icon" src={this.props.dark ? "/image/log-circle-light.svg" : "/image/log-circle.svg"} />
        <div className="content">
          <div className="title">Build logs </div>
          <div className="details">
            <TerminalComponent value={this.getConsoleBuffer()} />
          </div>
        </div>
      </div>
    );
  }
}
