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
  contents?: ArrayBuffer;
  action_array?: Uint8Array;
  action?: build.bazel.remote.execution.v2.Action;
  command?: build.bazel.remote.execution.v2.Command;
  error?: string;
}

export default class ActionCardComponent extends React.Component<Props, State> {
  state: State = {};
  componentDidMount() {
    this.fetchAction();
  }

  fetchAction() {
    let actionFile = "bytestream://localhost:1987/blobs/" + this.props.search.substring(14);
    console.log(this.props.search);
    console.log(actionFile);
    rpcService
      .fetchBytestreamFile(actionFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        console.log(action_buff);
        let temp_array = new Uint8Array(action_buff);
        console.log(temp_array);
        let temp_action = build.bazel.remote.execution.v2.Action.decode(temp_array);
        this.setState({
          ...this.state,
          contents: action_buff,
          action_array: temp_array,
          action: temp_action,
        });
        console.log(JSON.stringify(this.state.action, null, 2));
        this.fetchCommand(temp_action);
      })
      .catch(() => {
        console.error("Error loading bytestream action profile!");
        this.setState({
          ...this.state,
          error: "Error loading action profile. Make sure your cache is correctly configured.",
        });
      });
  }

  fetchCommand(action: build.bazel.remote.execution.v2.Action) {
    let commandFile =
      "bytestream://localhost:1987/blobs/" + action.commandDigest.hash + "/" + action.commandDigest.sizeBytes;
    console.log(commandFile);
    rpcService
      .fetchBytestreamFile(commandFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        let temp_array = new Uint8Array(action_buff);
        this.setState({
          ...this.state,
          command: build.bazel.remote.execution.v2.Command.decode(temp_array),
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
        <img className="icon" src="/image/filter.svg" />
        <div className="content">
          <div className="title">{"bytestream://localhost:1987/blobs/" + this.props.search}</div>
          {this.state.action && (
            <div>
              <div>{build.bazel.remote.execution.v2.Action.verify(this.state.action)}</div>
              <pre>
                <code>{JSON.stringify(this.state.action, null, 2)}</code>
              </pre>
              <div>
                {this.state.action.outputNodeProperties.map((outputNodeProperty) => (
                  <div className="output-node">{outputNodeProperty}</div>
                ))}
              </div>
              <div>Timeout:</div>
              <div>Command Digest Hash/Size</div>
            </div>
          )}

          {this.state.command && (
            <div>
              <div>{build.bazel.remote.execution.v2.Command.verify(this.state.command)}</div>
              <pre>
                <code>{JSON.stringify(this.state.command, null, 2)}</code>
              </pre>
              <div>
                {this.state.command.arguments.map((argument) => (
                  <div className="command-argument">{argument}</div>
                ))}
              </div>
              <div>
                {this.state.command.environmentVariables.map((variable) => (
                  <div className="command-variable">{variable.name}</div>
                ))}
              </div>
              <div>
                {this.state.command.outputDirectories.map((directory) => (
                  <div className="command-output-dir">{directory}</div>
                ))}
              </div>
              <div>
                {this.state.command.outputFiles.map((file) => (
                  <div className="command-output-file">{file}</div>
                ))}
              </div>
              <div></div>
            </div>
          )}
        </div>
      </div>
    );
  }
}
