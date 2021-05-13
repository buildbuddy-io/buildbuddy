import pako from "pako";
import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { build } from "../../proto/remote_execution_ts_proto";
import rpcService from "../service/rpc_service";

interface Props {
  model: InvocationModel;
  inProgress: boolean;
  search: string;
}

interface State {
  loading: boolean;
  contents: ArrayBuffer;
  action_array: Uint8Array;
  action_obj: build.bazel.remote.execution.v2.Action;
  error: string;
}

export default class ActionCardComponent extends React.Component {
  props: Props;

  state: State = {
    loading: false,
    contents: new ArrayBuffer(8),
    action_array: new Uint8Array(),
    action_obj: null,
    error: "",
  };

  fetchAction() {
    let actionFile = "bytestream://localhost:1987/blobs/" + this.props.search.substring(1);
    console.log(actionFile);
    rpcService
      .fetchBytestreamFile(actionFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        let temp_array = Uint8Array.from(action_buff);
        this.setState({
          ...this.state,
          contents: action_buff,
          action_array: temp_array,
          action_obj: build.bazel.remote.execution.v2.Action.decode(temp_array),
          loading: true,
        });
      })
      .catch(() => {
        console.error("Error loading bytestream action profile!");
        this.setState({
          ...this.state,
          error: "Error loading action profile. Make sure your cache is correctly configured.",
        });
      });
  }

  render() {
    return (
      <div className="card">
        <img className="icon" src="/image/link.svg" />
        <div className="content">
          <div className="title" onClick={this.fetchAction.bind(this)}>
            {"bytestream://localhost:1987/blobs/" + this.props.search}
          </div>
          {this.state.loading && <div>{build.bazel.remote.execution.v2.Action.verify(this.state.action_obj)}</div>}
        </div>
      </div>
    );
  }
}
