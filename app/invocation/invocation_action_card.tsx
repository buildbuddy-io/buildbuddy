import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { build } from "../../proto/remote_execution_ts_proto";
import InputFileComponent from "./invocation_input_file_node";
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
  inputRoot?: build.bazel.remote.execution.v2.Directory;
  treeShaToExpanded?: Map<string, boolean>;
  treeShaToChildrenMap?: Map<string, (any | any)[]>;
}

export default class InvocationActionCardComponent extends React.Component<Props, State> {
  state: State = {
    treeShaToExpanded: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, (any | any)[]>(),
  };
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

  fetchInputRoot(rootDigest: build.bazel.remote.execution.v2.IDigest) {
    let inputRootFile =
      "bytestream://" + this.getCacheAddress() + "/blobs/" + rootDigest.hash + "/" + rootDigest.sizeBytes;
    console.log(inputRootFile);
    rpcService
      .fetchBytestreamFile(inputRootFile, this.props.model.getId(), "arraybuffer")
      .then((root_buff: any) => {
        let tempRoot = build.bazel.remote.execution.v2.Directory.decode(new Uint8Array(root_buff));
        this.setState({
          ...this.state,
          inputRoot: tempRoot,
        });
      })
      .catch(() => {
        console.error("Error loading bytestream inputRoot profile!");
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
        this.fetchInputRoot(action.inputRootDigest);
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

  private renderTimeline() {
    const metadata = this.state.actionResult.executionMetadata;

    type TimelineEvent = { name: string; color: string; timestamp: any } | { timestamp: any };
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
      { timestamp: metadata.outputUploadCompletedTimestamp },
    ];

    // Make sure that we've actually received the metadata. This will not be sent
    // until the action is completed.
    // The metadata should include all timestamps, so either all timestamps should
    // be null or none of them should be null, but check all of them for good measure.
    for (const event of events) {
      if (!event.timestamp) return null;
    }

    const totalDuration = durationSeconds(events[0].timestamp, events[events.length - 1].timestamp);

    return (
      <div className="action-timeline">
        {events.map((event, i) => {
          // Don't render the end marker.
          if (!("name" in event)) return null;

          const next = events[i + 1];
          const duration = durationSeconds(event.timestamp, next.timestamp);
          const weight = duration / totalDuration;
          return (
            <div
              className="timeline-event"
              title={`${event.name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
              style={{ flex: `${weight} 0 0`, backgroundColor: event.color }}>
              <div className="timeline-event-label">
                <span className="event-name">{event.name}</span> ({format.compactDurationSec(duration)},{" "}
                {(weight * 100).toFixed(0)}%)
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  handleFileClicked(node: any) {
    console.log(node);
    console.log(build.bazel.remote.execution.v2.FileNode.verify(node));
    console.log(node.hasOwnProperty("isExecutable"));
    let digest_string = node.digest.hash + "/" + node.digest.sizeBytes;
    let dirFile = "bytestream://" + this.getCacheAddress() + "/blobs/" + digest_string;
    if (!node.hasOwnProperty("isExecutable")) {
      if (this.state.treeShaToExpanded.get(digest_string)) {
        this.state.treeShaToExpanded.set(digest_string, false);
        this.setState({ treeShaToExpanded: this.state.treeShaToExpanded });
        return;
      }
      console.log(dirFile);
      rpcService
        .fetchBytestreamFile(dirFile, this.props.model.getId(), "arraybuffer")
        .then((dir_buff: any) => {
          let tempDir = build.bazel.remote.execution.v2.Directory.decode(new Uint8Array(dir_buff));
          console.log(dir_buff);
          this.state.treeShaToExpanded.set(digest_string, true);
          let contents: (any | any)[] = [];
          this.state.treeShaToChildrenMap.set(
            digest_string,
            contents.concat(tempDir.directories).concat(tempDir.files)
          );
          this.setState({
            treeShaToChildrenMap: this.state.treeShaToChildrenMap,
            treeShaToExpanded: this.state.treeShaToExpanded,
          });
        })
        .catch(() => {
          console.error("Error loading bytestream file profile!");
          this.setState({
            ...this.state,
            error: "Error loading action profile. Make sure your cache is correctly configured.",
          });
        });
      return;
    } else {
      try {
        rpcService.downloadBytestreamFile(node.name, dirFile, this.props.model.getId());
      } catch {
        console.error("Error downloading bytestream timing profile");
      }
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
                  <div className="action-section">
                    <div
                      title="List of required supported NodeProperty [build.bazel.remote.execution.v2.NodeProperty] keys."
                      className="action-property-title">
                      Input File Tree
                    </div>
                    <div className="input-file-tree">
                      {this.state.inputRoot &&
                        this.state.inputRoot.directories.map((node: any) => (
                          <InputFileComponent
                            node={node}
                            treeShaToExpanded={this.state.treeShaToExpanded}
                            treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                            handleFileClicked={this.handleFileClicked.bind(this)}
                          />
                        ))}
                    </div>
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
                          {this.renderTimeline()}
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
                          <div className="metadata-detail">
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
