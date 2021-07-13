import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { eventlog } from "../../proto/eventlog_ts_proto";
import rpcService from "../service/rpc_service";
import errorService from "../errors/error_service";
import { BuildBuddyError } from "../util/errors";
import Log from "./log";
import FAKE_FETCHER from "./fake_fetcher";

export type InvocationLogProps = {
  invocationId: string;
};

type State = {
  lines: string[] | null;
  previousChunkId?: string;
  nextChunkId?: string;
};

export default class InvocationLog extends React.Component<InvocationLogProps, State> {
  state: State = {
    lines: null,
  };

  componentDidMount() {
    // setInterval(() => {
    //   rpcService.service
    //     .getInvocation(
    //       new invocation.GetInvocationRequest({
    //         lookup: new invocation.InvocationLookup({
    //           invocationId: this.props.invocationId,
    //         }),
    //       })
    //     )
    //     .then((response: any) => console.log(response));
    // }, 5000);
    // rpcService.service
    //   .getEventLogChunk(
    //     new eventlog.GetEventLogChunkRequest({
    //       invocationId: this.props.invocationId,
    //     })
    //   )
    //   .then((response: eventlog.GetEventLogChunkResponse) => {
    //     console.log(response);
    //     this.setState({
    //       lines: String.fromCharCode(...(response.chunk?.buffer || [])).split("\n"),
    //       previousChunkId: response.previousChunkId,
    //       nextChunkId: response.nextChunkId,
    //     });
    //   })
    //   .catch((error: any) => {
    //     errorService.handleError(error);
    //   });
  }

  async fetchPreviousChunk(): Promise<string[]> {
    if (!this.state.previousChunkId) return [];

    return rpcService.service
      .getEventLogChunk(
        new eventlog.GetEventLogChunkRequest({
          invocationId: this.props.invocationId,
          chunkId: this.state.previousChunkId,
        })
      )
      .then((response: eventlog.GetEventLogChunkResponse) => {
        console.log("Fetched previous chunk: ", response);
        this.setState({ previousChunkId: response.previousChunkId });
        return String.fromCharCode(...(response.chunk?.buffer || [])).split("\n");
      })
      .catch((error: any) => {
        errorService.handleError(error);
      }) as Promise<string[]>;
  }

  async fetchNextChunk(): Promise<string[]> {
    try {
      const response = await rpcService.service.getEventLogChunk(
        new eventlog.GetEventLogChunkRequest({
          invocationId: this.props.invocationId,
          chunkId: this.state.nextChunkId,
        })
      );
      console.log("Fetched next chunk: ", response);
      this.setState({ nextChunkId: response.nextChunkId });
      return String.fromCharCode(...(response.chunk?.buffer || [])).split("\n");
    } catch (e) {
      const error = BuildBuddyError.parse(e);
      if (error.code === "NotFound") return [];
      errorService.handleError(error);
      return [];
    }
  }

  hasMoreContents() {
    // TODO: Return whether the invocation is done
    return true;
  }

  render() {
    // if (!this.state.lines) return <div className="loading" />;

    return (
      <div
        style={{
          height: 300,
          overflow: "hidden",
          margin: 32,
          borderRadius: 4,
          boxShadow: "0 1px 3px rgba(0, 0, 0, 0.12)",
        }}>
        <Log
          dark
          initialLines={FAKE_FETCHER.initialLines()}
          fetcher={FAKE_FETCHER}
          tailPollIntervalMs={100}
          // initialLines={this.state.lines}
          // fetcher={this}
        />
      </div>
    );
  }
}
