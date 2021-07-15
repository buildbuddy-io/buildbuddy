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

const POLL_TAIL_INTERVAL_MS = 1000;
const MAX_INITIAL_LINES = 10000;

const InvocationStatus = invocation.Invocation.InvocationStatus;

export default class BuildLogsCardComponent extends React.Component<Props, State> {
  state: State = {
    consoleBuffer: "",
    nextChunkId: "",
    fetchedFirstChunk: false,
  };

  private pollTailTimeout: number | null = null;

  constructor(props: Props) {
    super(props);

    const invocation = props.model.invocations[0];
    if (!invocation) {
      console.error("BuildLogsCard: invocation model is missing invocation");
    }
  }

  componentDidMount() {
    if (this.isLogStreamingEnabled()) {
      this.pollTail();
    }
  }

  componentWillUnmount() {
    window.clearTimeout(this.pollTailTimeout);
  }

  private isLogStreamingEnabled() {
    return capabilities.chunkedEventLogs && this.props.model.invocations[0]?.lastChunkId;
  }

  private isInvocationComplete() {
    const invocation = this.props.model.invocations[0];
    return invocation?.invocationStatus === InvocationStatus.COMPLETE_INVOCATION_STATUS;
  }

  private pollTail() {
    const invocation = this.props.model.invocations[0];
    let rpcError: BuildBuddyError | null = null;
    let nextChunkId = "";
    const wasCompleteBeforeMakingRequest = this.isInvocationComplete();

    rpcService.service
      .getEventLogChunk(
        new eventlog.GetEventLogChunkRequest({
          invocationId: invocation.invocationId,
          chunkId: this.state.nextChunkId,
          // For the first request, fetch a large amount of lines.
          // Subsequent requests will just
          minLines: this.state.fetchedFirstChunk ? 0 : MAX_INITIAL_LINES,
          readBackward: !this.state.fetchedFirstChunk,
        })
      )
      .then((response) => {
        console.log(response);
        if (response.chunk?.lines) {
          let consoleBuffer = this.state.consoleBuffer;
          for (const line of response.chunk.lines) {
            consoleBuffer += String.fromCharCode(...line) + "\n";
          }
          this.setState({ consoleBuffer });
        }
        nextChunkId = response.nextChunkId;
        this.setState({ nextChunkId });
      })
      .catch((e) => {
        rpcError = BuildBuddyError.parse(e);
        console.warn({ rpcError });
      })
      .finally(() => {
        if (!this.state.fetchedFirstChunk) {
          this.setState({ fetchedFirstChunk: true });
        }

        // NotFound / OutOfRange errors just mean the next chunk has not yet been written
        // and that we should continue polling.
        if (rpcError?.code === "NotFound" || rpcError?.code === "OutOfRange") {
          // If we failed to fetch any new chunks on a completed invocation, that
          // means there are no new chunks to be written, so stop polling.
          // Note: this relies on the server not writing any new chunks after an
          // invocation is marked complete.
          if (wasCompleteBeforeMakingRequest) return;

          // Wait some time since new chunks are unlikely to be written since we last made
          // our request.
          window.setTimeout(() => this.pollTail(), POLL_TAIL_INTERVAL_MS);
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

        // At this point, we successfully fetched a chunk and the invocation is either
        // still in progress, or completed while we were making our last request.
        // Greedily fetch the next chunk.
        this.pollTail();
      });
  }

  private getConsoleBuffer() {
    if (!this.isLogStreamingEnabled()) {
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
