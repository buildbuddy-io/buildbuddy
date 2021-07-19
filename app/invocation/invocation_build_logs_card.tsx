import React from "react";
import { eventlog } from "../../proto/eventlog_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import capabilities from "../capabilities/capabilities";
import errorService from "../errors/error_service";
import rpcService from "../service/rpc_service";
import { TerminalComponent } from "../terminal/terminal";
import { BuildBuddyError } from "../util/errors";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  expanded: boolean;
}

interface State {
  consoleBuffer?: string;
  nextChunkId?: string;
  fetchedFirstChunk?: boolean;
}

const POLL_TAIL_INTERVAL_MS = 3_000;
// How many lines to request from the server on each chunk request.
const MIN_LINES = 100_000;

export default class BuildLogsCardComponent extends React.Component<Props, State> {
  state: State = {
    consoleBuffer: "",
    nextChunkId: "",
    fetchedFirstChunk: false,
  };

  private pollTailTimeout: number | null = null;

  componentDidMount() {
    if (this.shouldFetchFromEventLog()) {
      this.fetchTail(/*isFirstRequest=*/ true);
    }
  }

  componentWillUnmount() {
    window.clearTimeout(this.pollTailTimeout);
  }

  private shouldFetchFromEventLog() {
    return capabilities.chunkedEventLogs && Boolean(this.props.model.invocations[0]?.lastChunkId);
  }

  private fetchTail(isFirstRequest = false) {
    let rpcError: BuildBuddyError;
    let nextChunkId = "";
    const wasCompleteBeforeMakingRequest = this.props.model.isComplete();

    rpcService.service
      .getEventLogChunk(
        new eventlog.GetEventLogChunkRequest({
          invocationId: this.props.model.getId(),
          chunkId: this.state.nextChunkId,
          minLines: MIN_LINES,
          readBackward: isFirstRequest,
        })
      )
      .then((response) => {
        let consoleBuffer = this.state.consoleBuffer;
        for (const line of response.chunk?.lines || []) {
          consoleBuffer += String.fromCharCode(...line) + "\n";
        }
        nextChunkId = response.nextChunkId;
        this.setState({ consoleBuffer, nextChunkId });
      })
      .catch((e) => {
        rpcError = BuildBuddyError.parse(e);
      })
      .finally(() => {
        // NotFound / OutOfRange errors just mean the next chunk has not yet been written
        // and that we should continue polling.
        if (rpcError?.code === "NotFound" || rpcError?.code === "OutOfRange") {
          // If the tail chunk of a completed invocation was not found, no new
          // chunks should be written, so stop polling.
          // Note: this relies on the server not writing any new chunks after an
          // invocation is marked complete.
          if (wasCompleteBeforeMakingRequest) return;

          // Wait some time since new chunks are unlikely to be written since we last made
          // our request.
          this.pollTailTimeout = window.setTimeout(() => this.fetchTail(), POLL_TAIL_INTERVAL_MS);
          return;
        }

        // Other error codes indicate something is wrong and that we should stop fetching.
        if (rpcError) {
          errorService.handleError(rpcError);
          return;
        }

        // There won't be a next chunk ID if we've reached the upper limit.
        // This should rarely happen (if ever) but check for it just to be safe.
        if (!nextChunkId) return;

        // At this point, we successfully fetched a chunk. Immediately request the next
        // chunk to avoid unnecessarily delaying the logs from being displayed in case
        // more chunks are already available.
        this.fetchTail();
      });
  }

  private getConsoleBuffer() {
    if (!this.shouldFetchFromEventLog()) {
      return this.props.model.consoleBuffer || "";
    }

    return this.state.consoleBuffer;
  }

  render() {
    return (
      <div className={`card dark ${this.props.expanded ? "expanded" : ""}`}>
        <img className="icon" src="/image/log-circle-light.svg" />
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
