import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { Download, Info } from "lucide-react";
import { build } from "../../proto/remote_execution_ts_proto";
import InputNodeComponent, { InputNode } from "./invocation_action_input_node";
import rpcService from "../service/rpc_service";
import DigestComponent, { parseDigest } from "../components/digest/digest";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
}

interface State {
  action?: build.bazel.remote.execution.v2.Action;
  actionResult?: build.bazel.remote.execution.v2.ActionResult;
  command?: build.bazel.remote.execution.v2.Command;
  error?: string;
  inputRoot?: build.bazel.remote.execution.v2.Directory;
  inputDirs?: InputNode[];
  treeShaToExpanded?: Map<string, boolean>;
  treeShaToChildrenMap?: Map<string, InputNode[]>;
}

export default class InvocationActionCardComponent extends React.Component<Props, State> {
  state: State = {
    treeShaToExpanded: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, InputNode[]>(),
    inputDirs: [],
  };

  componentDidMount() {
    this.fetchAction();
    this.fetchActionResult();
  }

  fetchAction() {
    const digest = parseDigest(this.props.search.get("actionDigest"));
    const actionUrl = `bytestream://${this.getCacheAddress()}/blobs/${digest.hash}/${digest.sizeBytes ?? 1}`;
    rpcService
      .fetchBytestreamFile(actionUrl, this.props.model.getId(), "arraybuffer")
      .then((buffer: any) => {
        let action = build.bazel.remote.execution.v2.Action.decode(new Uint8Array(buffer));
        this.setState({
          action: action,
        });
        this.fetchCommand(action);
        this.fetchInputRoot(action.inputRootDigest);
      })
      .catch((e) => console.error("Failed to fetch action:", e));
  }

  fetchInputRoot(rootDigest: build.bazel.remote.execution.v2.IDigest) {
    let inputRootFile =
      "bytestream://" + this.getCacheAddress() + "/blobs/" + rootDigest.hash + "/" + rootDigest.sizeBytes;
    rpcService
      .fetchBytestreamFile(inputRootFile, this.props.model.getId(), "arraybuffer")
      .then((buffer: any) => {
        let tempRoot = build.bazel.remote.execution.v2.Directory.decode(new Uint8Array(buffer));
        let inputDirs: InputNode[] = tempRoot.directories.map(
          (node) =>
            ({
              obj: node,
              type: "dir",
            } as InputNode)
        );
        this.setState({
          inputRoot: tempRoot,
          inputDirs: inputDirs,
        });
      })
      .catch((e) => console.error("Failed to fetch input root:", e));
  }

  fetchActionResult() {
    const digest = parseDigest(this.props.search.get("actionDigest"));
    const actionResultUrl = `actioncache://${this.getCacheAddress()}/blobs/ac/${digest.hash}/${digest.sizeBytes ?? 1}`;
    rpcService
      .fetchBytestreamFile(actionResultUrl, this.props.model.getId(), "arraybuffer")
      .then((buffer: any) => {
        this.setState({
          actionResult: build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer)),
        });
      })
      .catch((e) => console.error("Failed to fetch action result:", e));
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
      .then((buffer: any) => {
        this.setState({
          command: build.bazel.remote.execution.v2.Command.decode(new Uint8Array(buffer)),
        });
      })
      .catch((e) => console.error("Failed to fetch command:", e));
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

  handleOutputFileClicked(file: build.bazel.remote.execution.v2.IOutputFile) {
    rpcService.downloadBytestreamFile(
      file.path,
      "bytestream://" + this.getCacheAddress() + "/blobs/" + file.digest.hash + "/" + file.digest.sizeBytes,
      this.props.model.getId()
    );
  }

  getCacheAddress() {
    let address = (this.props.model.optionsMap.get("remote_executor") || "").replace("grpc://", "");
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

  private renderTimelines() {
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

    const totalDuration = durationSeconds(events[0].timestamp, events[events.length - 1].timestamp);

    return (
      <div>
        {events.map((event, i) => {
          // Don't render the end marker.
          if (!event.color) return null;

          const next = events[i + 1];
          const duration = durationSeconds(event.timestamp, next.timestamp);
          const weight = duration / totalDuration;
          return (
            <div>
              <div className="metadata-detail">
                <span className="label">
                  {event.name} @ {format.formatTimestamp(event.timestamp)}
                </span>
                <span className="bar-description">
                  {format.compactDurationSec(duration)} ({(weight * 100).toFixed(1)}%)
                </span>
              </div>
              <div className="action-timeline">
                <div
                  className="timeline-event"
                  title={`${event.name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
                  style={{ flex: `${weight} 0 0`, backgroundColor: event.color }}></div>
                <div
                  className="timeline-event-gray"
                  title={`${event.name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
                  style={{ flex: `${1 - weight} 0 0`, backgroundColor: `rgba(0, 0, 0, .1)` }}></div>
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  handleFileClicked(node: InputNode) {
    let digestString = node.obj.digest.hash + "/" + node.obj.digest.sizeBytes;
    let dirUrl = "bytestream://" + this.getCacheAddress() + "/blobs/" + digestString;

    if (this.state.treeShaToExpanded.get(digestString)) {
      this.state.treeShaToExpanded.set(digestString, false);
      this.forceUpdate();
      return;
    }
    if (node.type == "file") {
      rpcService.downloadBytestreamFile(node.obj.name, dirUrl, this.props.model.getId());
      return;
    }
    rpcService
      .fetchBytestreamFile(dirUrl, this.props.model.getId(), "arraybuffer")
      .then((buffer: any) => {
        let dir = build.bazel.remote.execution.v2.Directory.decode(new Uint8Array(buffer));
        this.state.treeShaToExpanded.set(digestString, true);
        let dirs: InputNode[] = dir.directories.map(
          (child) =>
            ({
              obj: child,
              type: "dir",
            } as InputNode)
        );
        let files: InputNode[] = dir.files.map(
          (child) =>
            ({
              obj: child,
              type: "file",
            } as InputNode)
        );
        this.state.treeShaToChildrenMap.set(digestString, dirs.concat(files));
        this.forceUpdate();
      })
      .catch((e) => console.log(e));
    return;
  }

  render() {
    const digest = parseDigest(this.props.search.get("actionDigest"));
    return (
      <div className="invocation-action-card">
        <div className="card">
          <Info className="icon purple" />
          <div className="content">
            <div className="title">Action details </div>
            <div className="details">
              {this.state.action ? (
                <div>
                  <div className="action-section">
                    <div className="action-property-title">Digest</div>
                    <DigestComponent digest={digest} expanded={true} />
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Cacheable</div>
                    <div>{!this.state.action.doNotCache ? "True" : "False"}</div>
                  </div>
                  <div className="action-section">
                    <div className="action-property-title">Input root digest</div>
                    <div>
                      <DigestComponent digest={this.state.action.inputRootDigest} expanded={true} />
                    </div>
                  </div>
                  <div className="action-section">
                    <div
                      title="List of required supported NodeProperty [build.bazel.remote.execution.v2.NodeProperty] keys."
                      className="action-property-title">
                      Output node properties
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
                    <div className="action-property-title">Input files</div>
                    {this.state.inputDirs.length ? (
                      <div className="input-tree">
                        {this.state.inputDirs.map((node) => (
                          <InputNodeComponent
                            node={node}
                            treeShaToExpanded={this.state.treeShaToExpanded}
                            treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                            handleFileClicked={this.handleFileClicked.bind(this)}
                          />
                        ))}
                      </div>
                    ) : (
                      <div>None found</div>
                    )}
                  </div>
                </div>
              ) : (
                <div>No action details were found.</div>
              )}
              <div className="action-line">
                <div className="action-title">Command details</div>
                {this.state.command ? (
                  <div>
                    <div className="action-section">
                      <div className="action-property-title">Arguments</div>
                      {this.displayList(this.state.command.arguments)}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Environment variables</div>
                      <div className="action-list">
                        {this.state.command.environmentVariables.map((variable) => (
                          <div>
                            <span className="prop-name">{variable.name}</span>
                            <span className="prop-value">={variable.value}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Platform properties</div>
                      <div className="action-list">
                        {this.state.command.platform.properties.map((property) => (
                          <div>
                            <span className="prop-name">{property.name}</span>
                            <span className="prop-value">={property.value}</span>
                          </div>
                        ))}
                        {!this.state.command.platform.properties.length && <div>(Default)</div>}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div>No command details were found.</div>
                )}
              </div>
              <div className="action-line">
                <div className="action-title">Result details</div>
                {this.state.actionResult ? (
                  <div>
                    <div className="action-section">
                      <div className="action-property-title">Exit code</div>
                      <div>{this.state.actionResult.exitCode}</div>
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Execution metadata</div>
                      {this.state.actionResult.executionMetadata ? (
                        <div className="action-list">
                          <div className="metadata-title">Worker</div>
                          <div className="metadata-detail">{this.state.actionResult.executionMetadata.worker} </div>
                          <div className="metadata-title">Executor ID</div>
                          <div className="metadata-detail">{this.state.actionResult.executionMetadata.executorId}</div>
                          <div className="metadata-title">Timeline</div>
                          {this.renderTimelines()}
                        </div>
                      ) : (
                        <div>None found</div>
                      )}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Output files</div>
                      {this.state.actionResult.outputFiles ? (
                        <div className="action-list">
                          {this.state.actionResult.outputFiles.map((file) => (
                            <div
                              className="file-name clickable"
                              onClick={this.handleOutputFileClicked.bind(this, file)}>
                              <span>
                                <Download className="icon file-icon" />
                              </span>
                              <span className="prop-link">{file.path}</span>
                              {file.isExecutable && <span className="detail"> (executable)</span>}
                              <DigestComponent digest={file.digest} />
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div>None found</div>
                      )}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Output directories</div>
                      {this.state.actionResult.outputDirectories.length ? (
                        <div className="action-list">
                          {this.state.actionResult.outputDirectories.map((dir) => (
                            <div>
                              <span>{dir.path}</span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div>None</div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div>No action result details found.</div>
                )}
              </div>
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
