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

export default class InvocationActionCardComponent extends React.Component<Props, State> {
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

  private renderTimeline(i: number) {
    const metadata = this.state.actionResult.executionMetadata;

    type TimelineEvent = { name: string; color: string; timestamp: any };
    const events: TimelineEvent[] = [
      {
        name: "Queued",
        color: "#607D8B",
        timestamp: metadata.queuedTimestamp,
      },
      {
        name: "Initializing",
        color: "#673AB7",
        timestamp: metadata.workerStartTimestamp,
      },
      {
        name: "Downloading inputs",
        color: "#FF6F00",
        timestamp: metadata.inputFetchStartTimestamp,
      },
      {
        name: "Preparing runner",
        color: "#673AB7",
        timestamp: metadata.inputFetchCompletedTimestamp,
      },
      {
        name: "Executing",
        color: "#1E88E5",
        timestamp: metadata.executionStartTimestamp,
      },
      {
        name: "Preparing for upload",
        color: "#673AB7",
        timestamp: metadata.executionCompletedTimestamp,
      },
      {
        name: "Uploading outputs",
        color: "#FF6F00",
        timestamp: metadata.outputUploadStartTimestamp,
      },
      // End marker -- not actually rendered.
      {
        name: "Upload complete",
        color: "",
        timestamp: metadata.outputUploadCompletedTimestamp,
      },
    ];

    // Make sure that we've actually received the metadata. This will not be sent
    // until the action is completed.
    // The metadata should include all timestamps, so either all timestamps should
    // be null or none of them should be null, but check all of them for good measure.
    for (const event of events) {
      if (!event.timestamp) return null;
    }

    if (!events[i].color) return null;

    const totalDuration = durationSeconds(events[0].timestamp, events[events.length - 1].timestamp);
    const next = events[i + 1];
    const duration = durationSeconds(events[i].timestamp, next.timestamp);
    const weight = duration / totalDuration;

    return (
      <div>
        <div className="metadata-detail">
          {events[i].name} @ {format.formatTimestamp(events[i].timestamp)}
        </div>
        <div className="action-timeline">
          <div
            className="timeline-event"
            title={`${events[i].name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
            style={{ flex: `${weight} 0 0`, backgroundColor: events[i].color }}></div>
          <div
            className="timeline-event-label"
            title={`${events[i].name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
            style={{ flex: `${1 - weight} 0 0`, backgroundColor: `rgba(0, 0, 0, .1)` }}>
            ({format.compactDurationSec(duration)}, {(weight * 100).toFixed(0)}%)
          </div>
        </div>
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
              {this.state.actionResult && (
                <div className="action-line">
                  <div className="action-title">Result details</div>
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
                            Queuing @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.queuedTimestamp)}
                            {this.renderTimeline(0)}
                          </div>
                          <div className="metadata-detail">
                            Initializing @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.workerStartTimestamp)}
                            {this.renderTimeline(1)}
                          </div>
                          <div className="metadata-detail">
                            Downloading inputs @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.inputFetchStartTimestamp)}
                            {this.renderTimeline(2)}
                          </div>
                          <div className="metadata-detail">
                            Preparing runner @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.inputFetchCompletedTimestamp
                            )}
                            {this.renderTimeline(3)}
                          </div>
                          <div className="metadata-detail">
                            Executing @{" "}
                            {format.formatTimestamp(this.state.actionResult.executionMetadata.executionStartTimestamp)}
                            {this.renderTimeline(4)}
                          </div>
                          <div className="metadata-detail">
                            Preparing for upload @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.executionCompletedTimestamp
                            )}
                            {this.renderTimeline(5)}
                          </div>
                          <div className="metadata-detail">
                            Uploading outputs @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.outputUploadStartTimestamp
                            )}
                            {this.renderTimeline(6)}
                          </div>
                          <div className="metadata-detail">
                            Worker completed @{" "}
                            {format.formatTimestamp(
                              this.state.actionResult.executionMetadata.outputUploadCompletedTimestamp
                            )}
                            {this.renderTimeline(7)}
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
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}

function durationSeconds(t1: any, t2: any): number {
  return timestampToUnixSeconds(t2) - timestampToUnixSeconds(t1);
}

function timestampToUnixSeconds(timestamp: any): number {
  return timestamp.seconds + timestamp.nanos / 1e9;
}
