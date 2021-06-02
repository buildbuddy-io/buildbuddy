import pako from "pako";
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
  }

  fetchAction() {
    // TODO: Replace localhost:1987 with the remote cache address and prefix the path with the instance name
    let actionFile = "bytestream://localhost:1987/blobs/" + this.props.search.get("actionDigest");
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
    let actionResultFile = "actioncache://localhost:1987/blobs/ac/" + this.props.search.get("actionDigest");
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
    if (list.length == 0) return <div>None found.</div>;
    return (
      <div className="action-list">
        {list.map((argument) => (
          <div>{argument}</div>
        ))}
      </div>
    );
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
                    <div>{this.props.search.get("actionDigest")} bytes</div>
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Output Node Properties: </div>
                    {this.state.action.outputNodeProperties.length ? (
                      <div>
                        {this.state.action.outputNodeProperties.map((outputNodeProperty) => (
                          <div className="output-node">{outputNodeProperty}</div>
                        ))}
                      </div>
                    ) : (
                      <div>None found.</div>
                    )}
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Do Not Cache: </div>
                    <div>{this.state.action.doNotCache ? "True" : "False"}</div>
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Input Root Hash/Size:</div>
                    <span>
                      {this.state.action.inputRootDigest.hash}/{this.state.action.inputRootDigest.sizeBytes} bytes
                    </span>
                  </div>
                </div>
              )}
              <div className="action-line">
                <div className="action-title">Command details</div>
                {this.state.command && (
                  <div>
                    <div className="action-section">
                      <div className="action-property-title">Arguments:</div>
                      {this.displayList(this.state.command.arguments)}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Environment Variables:</div>
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
                      <div className="action-property-title">Exit Code: </div>
                      <div>{this.state.actionResult.exitCode}</div>
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Execution Metadata:</div>
                      {this.state.actionResult.executionMetadata ? (
                        <div className="action-list">
                          <div className="metadata-title">Worker</div>
                          <div className="metadata-detail">{this.state.actionResult.executionMetadata.worker} </div>
                          <div className="metadata-title">Executor ID</div>
                          <div className="metadata-detail">{this.state.actionResult.executionMetadata.executorId}</div>
                          <div className="metadata-title">Timeline</div>
                          <div className="metadata-detail">
                            Queued @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.queuedTimestamp.seconds,
                              this.state.actionResult.executionMetadata.queuedTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-detail">
                            Worker Started @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.workerStartTimestamp.seconds,
                              this.state.actionResult.executionMetadata.workerStartTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-detail">
                            Input Fetching Started @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.inputFetchStartTimestamp.seconds,
                              this.state.actionResult.executionMetadata.inputFetchStartTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-detail">
                            Input Fetching Completed @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.inputFetchCompletedTimestamp.seconds,
                              this.state.actionResult.executionMetadata.inputFetchCompletedTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-detail">
                            Execution Started @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.executionStartTimestamp.seconds,
                              this.state.actionResult.executionMetadata.executionStartTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-detail">
                            Execution Completed @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.executionCompletedTimestamp.seconds,
                              this.state.actionResult.executionMetadata.executionCompletedTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-detail">
                            Output Upload Started @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.outputUploadStartTimestamp.seconds,
                              this.state.actionResult.executionMetadata.outputUploadStartTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-detail">
                            Output Upload Completed @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.outputUploadCompletedTimestamp.seconds,
                              this.state.actionResult.executionMetadata.outputUploadCompletedTimestamp.nanos
                            )}
                          </div>
                          <div className="metadata-info">
                            Worker Completed @{" "}
                            {format.formatTimestampSecondsNanos(
                              this.state.actionResult.executionMetadata.workerCompletedTimestamp.seconds,
                              this.state.actionResult.executionMetadata.workerCompletedTimestamp.nanos
                            )}
                          </div>
                        </div>
                      ) : (
                        <div>None found.</div>
                      )}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Output Files:</div>
                      {this.state.actionResult.outputFiles ? (
                        <div className="action-list">
                          {this.state.actionResult.outputFiles.map((file) => (
                            <div>
                              <span className="prop-name">PATH=</span>
                              <span className="prop-value">{file.path}</span>
                              {file.isExecutable && <span className="detail"> (executable)</span>}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div>None found.</div>
                      )}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Output Directories:</div>
                      {this.state.actionResult.outputDirectories.length ? (
                        <div className="action-list">
                          {this.state.actionResult.outputDirectories.map((dir) => (
                            <div>
                              <span className="prop-name">PATH=</span>
                              <span className="prop-value">{dir.path}</span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div>None found.</div>
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
