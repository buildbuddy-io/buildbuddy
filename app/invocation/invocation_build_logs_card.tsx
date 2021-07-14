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
}

const POLL_TAIL_INTERVAL_MS = 1000;

const InvocationStatus = invocation.Invocation.InvocationStatus;

export default class BuildLogsCardComponent extends React.Component<Props, State> {
  state: State = {
    consoleBuffer: "",
    nextChunkId: "",
  };

  private pollTailTimeout: number | null = null;

  constructor(props: Props) {
    super(props);

    const invocation = props.model.invocations[0];
    if (invocation) {
      this.state.nextChunkId = invocation.lastChunkId;
    } else {
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
    return capabilities.logStreaming && this.props.model.invocations[0]?.lastChunkId;
  }

  private isInvocationComplete() {
    const invocation = this.props.model.invocations[0];
    return invocation?.invocationStatus === InvocationStatus.COMPLETE_INVOCATION_STATUS;
  }

  private pollTail() {
    let rpcError: BuildBuddyError | null = null;
    let nextChunkId = "";
    const wasCompleteBeforeMakingRequest = this.isInvocationComplete();

    rpcService.service
      .getEventLogChunk(
        new eventlog.GetEventLogChunkRequest({
          invocationId: this.props.model.invocations[0]?.invocationId,
          chunkId: this.state.nextChunkId,
        })
      )
      .then((response) => {
        if (response.chunk?.buffer) {
          this.setState({ consoleBuffer: this.state.consoleBuffer + String.fromCharCode(...response.chunk.buffer) });
        }
        nextChunkId = response.nextChunkId;
        this.setState({ nextChunkId });
      })
      .catch((e) => {
        rpcError = BuildBuddyError.parse(e);
      })
      .finally(() => {
        // NotFound errors just mean the next chunk has not yet been written
        // and that we should continue polling.
        if (rpcError && rpcError.code !== "NotFound") {
          errorService.handleError(rpcError);
          return;
        }

        // There won't be a next chunk ID if we've reached the upper limit.
        // This should rarely happen (if ever) but check for it just to be safe.
        if (!nextChunkId) return;

        // If the invocation is complete, do one more poll in case it completed
        // while we were making the RPC above and there are still more chunks
        // to fetch.
        if (this.isInvocationComplete() && !wasCompleteBeforeMakingRequest && nextChunkId) {
          this.pollTail();
          return;
        }

        window.setTimeout(() => this.pollTail(), POLL_TAIL_INTERVAL_MS);
      });
  }

  private getConsoleBuffer() {
    if (this.isLogStreamingEnabled()) {
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
