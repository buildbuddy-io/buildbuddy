import pako from "pako";
import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { build } from "../../proto/remote_execution_ts_proto";
import rpcService from "../service/rpc_service";

interface Props {
  model: InvocationModel;
  search: string;
}

interface State {
  loading: boolean;
  contents: ArrayBuffer;
  action_array: Uint8Array;
  action_obj: build.bazel.remote.execution.v2.Action;
  command_obj: build.bazel.remote.execution.v2.Command;
  error: string;
}

export default class ActionCardComponent extends React.Component {
  props: Props;

  state: State = {
    loading: false,
    contents: new ArrayBuffer(8),
    action_array: new Uint8Array(),
    action_obj: null,
    command_obj: null,
    error: "",
  };

  componentDidMount() {
    this.fetchAction();
    this.fetchCommand();
  }

  fetchAction() {
    let actionFile = "bytestream://localhost:1987/blobs/" + this.props.search.substring(14);
    console.log(this.props.search);
    console.log(actionFile);
    rpcService
      .fetchBytestreamFile(actionFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        let temp_array = Uint8Array.from(action_buff);
        console.log(temp_array);
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
  

  fetchCommand() {
    let commandFile = "bytestream://localhost:1987/blobs/" + this.state.action_obj.commandDigest.hash + "/" + this.state.action_obj.commandDigest.sizeBytes;
    console.log(commandFile);
    rpcService
      .fetchBytestreamFile(commandFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        let temp_array = Uint8Array.from(action_buff);
        this.setState({
          ...this.state,
          command_obj: build.bazel.remote.execution.v2.Command.decode(temp_array),
          loading: true,
        });
      })
      .catch(() => {
        console.error("Error loading bytestream command profile!");
        this.setState({
          ...this.state,
          error: "Error loading command profile. Make sure your cache is correctly configured.",
        });
      });
  }

  render() {
    return (
      <div className="card">
        <img className="icon" src="/image/filter.svg"/>
        <div className="content">
          <div className="title">
            {"bytestream://localhost:1987/blobs/" + this.props.search}
          </div>
          {this.state.action_obj && (
            <div>
              <div>{build.bazel.remote.execution.v2.Action.verify(this.state.action_obj)}</div>
              <div>{this.state.action_obj.outputNodeProperties.map((outputNodeProperty) => (
                  <div className="output-node">{outputNodeProperty}</div>
                ))}
              </div>
              <div>{this.state.action_obj.toJSON()}</div>
              <div>{this.state.action_obj.timeout}</div>
              <div>{this.state.action_obj.commandDigest}</div>
              <div>
              </div>
            </div>
          )}
          
        </div>
      </div>
    );
  }
}
