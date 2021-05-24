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
    // TODO: Replace localhost:1987 with the remote cache address and prefix the path with the instance name
    let actionFile = "bytestream://localhost:1987/blobs/" + this.props.search.substring(14);
    rpcService
      .fetchBytestreamFile(actionFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        let temp_array = new Uint8Array(action_buff);
        let temp_action = build.bazel.remote.execution.v2.Action.decode(temp_array);
        this.setState({
          ...this.state,
          contents: action_buff,
          action_array: temp_array,
          action: temp_action,
        });
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

  displayOutputNodeProps() {
    if (this.state.action.outputNodeProperties.length != 0) {
      return (
        <div>
          {this.state.action.outputNodeProperties.map((outputNodeProperty) => (
            <div className="output-node">{outputNodeProperty}</div>
          ))}
        </div>
      );
    } else {
      return <div>None found.</div>;
    }
  }

  fetchCommand(action: build.bazel.remote.execution.v2.Action) {
    // TODO: Replace localhost:1987 with the remote cache address and prefix the path with the instance name
    let commandFile =
      "bytestream://localhost:1987/blobs/" + action.commandDigest.hash + "/" + action.commandDigest.sizeBytes;
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

  displayList(list: string[]) {
    if (list.length > 0) {
      return <div className="action-list">{list.map((argument) => <div>{argument}</div> || [])}</div>;
    } else {
      return <div>None found.</div>;
    }
  }

  render() {
    return (
      <div>
        <div className="card">
          <img className="icon" src="/image/info.svg" />
          <div className="content">
            <div className="title">Action details </div>
            <div className="details">
              {this.state.action && (
                <div>
                  <div className="action-section">
                    <div className="action-property-title">Hash/Size: </div>
                    <div>{this.props.search.substring(14)}</div>
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Output Node Properties: </div>
                    {this.displayOutputNodeProps()}
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Do Not Cache: </div>
                    <div>{this.state.action.doNotCache ? "True" : "False"}</div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
        <div className="card">
          <img className="icon" src="/image/info.svg" />
          <div className="content">
            <div className="title">Command details</div>
            <div className="details">
              {this.state.command && (
                <div>
                  <div className="action-section">
                    <div className="action-property-title">Arguments:</div>
                    {this.displayList(this.state.command.arguments)}
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Environment Variables:</div>
                    <div className="action-list">
                      {this.state.command.environmentVariables.map(
                        (variable) =>
                          (
                            <div>
                              <span className="env-name">{variable.name}</span>
                              <span className="env-value">={variable.value}</span>
                            </div>
                          ) || []
                      )}
                    </div>
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Output Directories:</div>
                    {this.displayList(this.state.command.outputDirectories)}
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Output Files:</div>
                    {this.displayList(this.state.command.outputFiles)}
                  </div>
                  <div></div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
