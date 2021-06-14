import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { build } from "../../proto/remote_execution_ts_proto";
import rpcService from "../service/rpc_service";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
}

interface State {
  contents?: ArrayBuffer;
  action?: build.bazel.remote.execution.v2.Action;
  actionResult?: build.bazel.remote.execution.v2.ActionResult;
  command?: build.bazel.remote.execution.v2.Command;
  error?: string;
}

export default class ActionCardComponent extends React.Component<Props, State> {
  state: State = {};
  componentDidMount() {
    this.fetchAction();
    this.fetchActionResult();
  }

  fetchAction() {
    let actionFile = "bytestream://" + this.getCacheAddress() + "/blobs/" + this.props.search.get("actionDigest");
    rpcService
      .fetchBytestreamFile(actionFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        let tempAction = build.bazel.remote.execution.v2.Action.decode(new Uint8Array(action_buff));
        this.setState({
          ...this.state,
          contents: action_buff,
          action: tempAction,
        });
        this.fetchCommand(tempAction);
      })
      .catch(() => {
        console.error("Error loading bytestream action profile!");
        this.setState({
          ...this.state,
          error: "Error loading action profile. Make sure your cache is correctly configured.",
        });
      });
  }

  fetchActionResult() {
    let actionResultFile =
      "actioncache://" + this.getCacheAddress() + "/blobs/ac/" + this.props.search.get("actionDigest");
    rpcService
      .fetchBytestreamFile(actionResultFile, this.props.model.getId(), "arraybuffer")
      .then((action_buff: any) => {
        let temp_array = new Uint8Array(action_buff);
        this.setState({
          ...this.state,
          actionResult: build.bazel.remote.execution.v2.ActionResult.decode(temp_array),
        });
      })
      .catch(() => {
        console.error("Error loading action result!");
        this.setState({
          ...this.state,
          error: "Error loading command profile. Make sure your cache is correctly configured.",
        });
      });
  }

  fetchCommand(action: build.bazel.remote.execution.v2.Action) {
    let commandFile =
      "bytestream://" +
      this.getCacheAddress() +
      "/blobs/" +
      action.commandDigest.hash +
      "/" +
      action.commandDigest.sizeBytes;
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
    if (list.length == 0) return <div>None found</div>;
    return (
      <div className="action-list">
        {list.map((argument) => (
          <div>{argument}</div>
        ))}
      </div>
    );
  }

  getCacheAddress() {
    let address = this.props.model.optionsMap.get("remote_executor").replace("grpc://", "");
    address = address.replace("grpcs://", "");
    if (this.props.model.optionsMap.get("remote_cache")) {
      address = this.props.model.optionsMap.get("remote_cache").replace("grpc://", "");
      address = address.replace("grpcs://", "");
    }
    if (this.props.model.optionsMap.get("remote_instance_name")) {
      address = address + "/" + this.props.model.optionsMap.get("remote_instance_name");
    }
    return address;
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
                    <div className="action-property-title">Hash/Size</div>
                    <div>{this.props.search.get("actionDigest")} bytes</div>
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Cacheable</div>
                    <div>{!this.state.action.doNotCache ? "True" : "False"}</div>
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Input Root Hash/Size</div>
                    <span>
                      {this.state.action.inputRootDigest.hash}/{this.state.action.inputRootDigest.sizeBytes} bytes
                    </span>
                  </div>
                  <div className="action-section">
                    <div
                      title="List of required supported NodeProperty [build.bazel.remote.execution.v2.NodeProperty] keys."
                      className="action-property-title">
                      Output Node Properties
                    </div>
                    {this.state.action.outputNodeProperties.length ? (
                      <div>
                        {this.state.action.outputNodeProperties.map((outputNodeProperty) => (
                          <div className="output-node">{outputNodeProperty}</div>
                        ))}
                      </div>
                    ) : (
                      <div>Default</div>
                    )}
                  </div>
                </div>
              )}
              <div className="action-line">
                <div className="action-title">Command details</div>
                {this.state.command && (
                  <div>
                    <div className="action-section">
                      <div className="action-property-title">Arguments</div>
                      {this.displayList(this.state.command.arguments)}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Environment Variables</div>
                      <div className="action-list">
                        {this.state.command.environmentVariables.map((variable) => (
                          <div>
                            <span className="prop-name">{variable.name}</span>
                            <span className="prop-value">={variable.value}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
              <div className="action-line">
                <div className="action-title">Result details</div>
                {this.state.actionResult && (
                  <div>
                    <div className="action-section">
                      <div className="action-property-title">Exit Code</div>
                      <div>{this.state.actionResult.exitCode}</div>
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Execution Metadata</div>
                      {this.state.actionResult.executionMetadata ? (
                        <div className="action-list">
                          <div className="metadata-title">Worker</div>
                          <div className="metadata-detail">{this.state.actionResult.executionMetadata.worker} </div>
                          <div className="metadata-title">Executor ID</div>
                          <div className="metadata-detail">{this.state.actionResult.executionMetadata.executorId}</div>
                          <div className="metadata-title">Timeline</div>
                          <div className="metadata-detail">
                            Queued @ {format.formatTimestamp(this.state.actionResult.executionMetadata.queuedTimestamp)}
                          </div>
                          <div className="metadata-detail">
                            Worker Started @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.workerStartTimestamp)}
                          </div>
                          <div className="metadata-detail">
                            Input Fetching Started @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.inputFetchStartTimestamp)}
                          </div>
                          <div className="metadata-detail">
                            Input Fetching Completed @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.inputFetchCompletedTimestamp
                            )}
                          </div>
                          <div className="metadata-detail">
                            Execution Started @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.executionStartTimestamp)}
                          </div>
                          <div className="metadata-detail">
                            Execution Completed @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.executionCompletedTimestamp
                            )}
                          </div>
                          <div className="metadata-detail">
                            Output Upload Started @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.outputUploadStartTimestamp
                            )}
                          </div>
                          <div className="metadata-detail">
                            Output Upload Completed @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.outputUploadCompletedTimestamp
                            )}
                          </div>
                          <div className="metadata-info">
                            Worker Completed @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.workerCompletedTimestamp)}
                          </div>
                        </div>
                      ) : (
                        <div>None found</div>
                      )}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Output Files</div>
                      {this.state.actionResult.outputFiles ? (
                        <div className="action-list">
                          {this.state.actionResult.outputFiles.map((file) => (
                            <div>
                              <span className="prop-value">{file.path}</span>
                              {file.isExecutable && <span className="detail"> (executable)</span>}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div>None found</div>
                      )}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Output Directories</div>
                      {this.state.actionResult.outputDirectories.length ? (
                        <div className="action-list">
                          {this.state.actionResult.outputDirectories.map((dir) => (
                            <div>
                              <span className="prop-value">{dir.path}</span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div>None</div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
