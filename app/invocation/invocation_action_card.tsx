import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { Download, Info, HelpCircle } from "lucide-react";
import { build } from "../../proto/remote_execution_ts_proto";
import { resource } from "../../proto/resource_ts_proto";
import InputNodeComponent, { InputNode } from "./invocation_action_input_node";
import rpcService from "../service/rpc_service";
import DigestComponent, { parseDigest } from "../components/digest/digest";
import { TextLink } from "../components/link/link";
import TerminalComponent from "../terminal/terminal";
import UserPreferences from "../preferences/preferences";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  preferences: UserPreferences;
}

interface State {
  action?: build.bazel.remote.execution.v2.Action;
  loadingAction: boolean;
  actionResult?: build.bazel.remote.execution.v2.ActionResult;
  treeShaToTotalSizeMap: Map<string, Number>;
  command?: build.bazel.remote.execution.v2.Command;
  error?: string;
  inputRoot?: build.bazel.remote.execution.v2.Directory;
  inputDirs: InputNode[];
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, InputNode[]>;
  stderr?: string;
  stdout?: string;
}

export default class InvocationActionCardComponent extends React.Component<Props, State> {
  state: State = {
    treeShaToExpanded: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, InputNode[]>(),
    treeShaToTotalSizeMap: new Map<string, Number>(),
    inputDirs: [],
    loadingAction: true,
  };

  componentDidMount() {
    this.fetchAction();
    this.fetchActionResult();
  }

  fetchAction() {
    this.setState({ loadingAction: true });
    const digest = parseDigest(this.props.search.get("actionDigest") ?? "");
    const digestType = this.props.model.getDigestFunctionDir();
    const actionUrl = `bytestream://${this.getCacheAddress()}/blobs/${digestType}${digest.hash}/${
      digest.sizeBytes ?? 1
    }`;
    rpcService
      .fetchBytestreamFile(actionUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer: any) => {
        let action = build.bazel.remote.execution.v2.Action.decode(new Uint8Array(buffer));
        this.setState({
          action: action,
        });
        this.fetchCommand(action);
        this.fetchInputRoot(action.inputRootDigest ?? build.bazel.remote.execution.v2.Digest.create({}));
        this.fetchDirectorySizes(action.inputRootDigest ?? build.bazel.remote.execution.v2.Digest.create({}));
      })
      .catch((e) => console.error("Failed to fetch action:", e))
      .finally(() => this.setState({ loadingAction: false }));
  }

  fetchDirectorySizes(rootDigest: build.bazel.remote.execution.v2.Digest) {
    const remoteInstanceName = this.props.model.optionsMap.get("remote_instance_name") || undefined;
    rpcService.service
      .getTreeDirectorySizes({
        rootDigest: rootDigest,
        instanceName: remoteInstanceName,
        /* TODO(jdhollen): pass back the DigestFunction used for the invocation. */
      })
      .then((r) => {
        const sizes = new Map<string, Number>();
        r.sizes.forEach((v) => {
          sizes.set(v.digest, +v.totalSize);
        });
        this.setState({ treeShaToTotalSizeMap: sizes });
      })
      .catch((e) => {
        this.setState({ treeShaToTotalSizeMap: new Map<string, Number>() });
      });
  }

  fetchInputRoot(rootDigest: build.bazel.remote.execution.v2.IDigest) {
    let inputRootFile =
      "bytestream://" +
      this.getCacheAddress() +
      "/blobs/" +
      this.props.model.getDigestFunctionDir() +
      rootDigest.hash +
      "/" +
      rootDigest.sizeBytes;
    rpcService
      .fetchBytestreamFile(inputRootFile, this.props.model.getInvocationId(), "arraybuffer")
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
    let digestParam = this.props.search.get("actionResultDigest");
    if (digestParam == null) {
      digestParam = this.props.search.get("actionDigest");
    }
    const digestType = this.props.model.getDigestFunctionDir();
    const digest = parseDigest(digestParam ?? "");
    const actionResultUrl = `actioncache://${this.getCacheAddress()}/blobs/ac/${digestType}${digest.hash}/${
      digest.sizeBytes ?? 1
    }`;
    rpcService
      .fetchBytestreamFile(actionResultUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer: any) => {
        const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
        this.setState({
          actionResult: actionResult,
        });
        this.fetchStdoutAndStderr(actionResult);
      })
      .catch((e) => console.error("Failed to fetch action result:", e));
  }

  fetchStdoutAndStderr(actionResult: build.bazel.remote.execution.v2.ActionResult) {
    let stdoutUrl =
      "bytestream://" +
      this.getCacheAddress() +
      "/blobs/" +
      this.props.model.getDigestFunctionDir() +
      actionResult.stdoutDigest?.hash +
      "/" +
      actionResult.stdoutDigest?.sizeBytes;

    rpcService
      .fetchBytestreamFile(stdoutUrl, this.props.model.getInvocationId())
      .then((content: string) => {
        this.setState({
          stdout: content,
        });
      })
      .catch((e) => console.error("Failed to fetch stdout:", e));

    let stderrUrl =
      "bytestream://" +
      this.getCacheAddress() +
      "/blobs/" +
      this.props.model.getDigestFunctionDir() +
      actionResult.stderrDigest?.hash +
      "/" +
      actionResult.stderrDigest?.sizeBytes;
    rpcService
      .fetchBytestreamFile(stderrUrl, this.props.model.getInvocationId())
      .then((content: string) => {
        this.setState({
          stderr: content,
        });
      })
      .catch((e) => console.error("Failed to fetch stderr:", e));
  }

  fetchCommand(action: build.bazel.remote.execution.v2.Action) {
    let commandFile =
      "bytestream://" +
      this.getCacheAddress() +
      "/blobs/" +
      this.props.model.getDigestFunctionDir() +
      action.commandDigest?.hash +
      "/" +
      action.commandDigest?.sizeBytes;
    rpcService
      .fetchBytestreamFile(commandFile, this.props.model.getInvocationId(), "arraybuffer")
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

  handleOutputFileClicked(file: build.bazel.remote.execution.v2.OutputFile) {
    rpcService.downloadBytestreamFile(
      file.path,
      "bytestream://" +
        this.getCacheAddress() +
        "/blobs/" +
        this.props.model.getDigestFunctionDir() +
        file.digest?.hash +
        "/" +
        file.digest?.sizeBytes,
      this.props.model.getInvocationId()
    );
  }

  getCacheAddress() {
    const orderedOptions = ["remote_cache", "remote_executor", "cache_backend", "rbe_backend"];
    let address = "";
    for (const optionName of orderedOptions) {
      const option = this.props.model.optionsMap.get(optionName);
      if (!option) continue;

      address = option.replace("grpc://", "").replace("grpcs://", "");
      break;
    }
    if (this.props.model.optionsMap.get("remote_instance_name")) {
      address = address + "/" + this.props.model.optionsMap.get("remote_instance_name");
    }
    return address;
  }

  private renderTimelines() {
    const metadata = this.state.actionResult?.executionMetadata;

    type TimelineEvent = { name: string; color: string; timestamp: any };
    const events: TimelineEvent[] = [
      {
        name: "Queued",
        color: "#607D8B",
        timestamp: metadata?.queuedTimestamp,
      },
      {
        name: "Initializing",
        color: "#673AB7",
        timestamp: metadata?.workerStartTimestamp,
      },
      {
        name: "Downloading inputs",
        color: "#FF6F00",
        timestamp: metadata?.inputFetchStartTimestamp,
      },
      {
        name: "Preparing runner",
        color: "#673AB7",
        timestamp: metadata?.inputFetchCompletedTimestamp,
      },
      {
        name: "Executing",
        color: "#1E88E5",
        timestamp: metadata?.executionStartTimestamp,
      },
      {
        name: "Preparing for upload",
        color: "#673AB7",
        timestamp: metadata?.executionCompletedTimestamp,
      },
      {
        name: "Uploading outputs",
        color: "#FF6F00",
        timestamp: metadata?.outputUploadStartTimestamp,
      },
      // End marker -- not actually rendered.
      {
        name: "Upload complete",
        color: "",
        timestamp: metadata?.outputUploadCompletedTimestamp,
      },
    ];

    const filteredEvents = events.filter((event) => event && event.timestamp);
    if (filteredEvents.length == 0) {
      return null;
    }

    const totalDuration = durationSeconds(
      filteredEvents[0].timestamp,
      filteredEvents[filteredEvents.length - 1].timestamp
    );

    return (
      <div>
        {filteredEvents.map((event, i) => {
          // Don't render the end marker.
          if (i == filteredEvents.length - 1) return null;

          const next = filteredEvents[i + 1];
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
    let digestString = node.obj.digest?.hash + "/" + node.obj.digest?.sizeBytes;
    let dirUrl =
      "bytestream://" + this.getCacheAddress() + "/blobs/" + this.props.model.getDigestFunctionDir() + digestString;

    if (this.state.treeShaToExpanded.get(digestString)) {
      this.state.treeShaToExpanded.set(digestString, false);
      this.forceUpdate();
      return;
    }
    if (node.type == "file") {
      rpcService.downloadBytestreamFile(node.obj.name, dirUrl, this.props.model.getInvocationId());
      return;
    }
    rpcService
      .fetchBytestreamFile(dirUrl, this.props.model.getInvocationId(), "arraybuffer")
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

  private renderNotFoundDetails({ result = false }) {
    const hasRemoteUploadLocalResults = this.props.model.booleanCommandLineOption("remote_upload_local_results");
    const hasRemoteExecutor = Boolean(this.props.model.stringCommandLineOption("remote_executor"));
    const hasRemoteCache = Boolean(this.props.model.booleanCommandLineOption("remote_cache"));

    if (result && !hasRemoteCache && !hasRemoteExecutor) {
      return (
        <p>
          No result details found. Action results are available for builds with{" "}
          <TextLink href="/docs/setup">full cache or remote execution</TextLink> enabled.
        </p>
      );
    }
    if (result && !hasRemoteExecutor && !hasRemoteUploadLocalResults) {
      return (
        <p>
          No result details found. Action results for locally executed actions are available for builds with{" "}
          <TextLink href="/docs/setup">full cache</TextLink> enabled (
          <span className="inline-code">--remote_upload_local_results</span>).
        </p>
      );
    }
    return (
      <>
        {result && <p>Action result not found. The result may have expired from cache, or it may not be cacheable.</p>}
        {!result && <p>Action not found. The action may have expired from cache.</p>}
        {!hasRemoteExecutor &&
          !this.props.model.isAnonymousInvocation() &&
          !this.props.model.hasCacheWriteCapability() && (
            <p>This could also be because the invocation was authenticated with a read-only API key.</p>
          )}
      </>
    );
  }

  render() {
    const digest = parseDigest(this.props.search.get("actionDigest") ?? "");

    return (
      <div className="invocation-action-card">
        {this.state.loadingAction && (
          <div className="card">
            <div className="loading" />
          </div>
        )}
        {!this.state.loadingAction && (
          <div className="card">
            <Info className="icon purple" />
            <div className="content">
              <div className="title">Action details</div>
              {this.state.action ? (
                <div className="details">
                  <div>
                    <div className="action-section">
                      <div className="action-property-title">Digest</div>
                      <DigestComponent digest={digest} expanded={true} />
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Cacheable</div>
                      <div>{!this.state.action.doNotCache ? "True" : "False"}</div>
                    </div>
                    {this.state.action.inputRootDigest && (
                      <div className="action-section">
                        <div className="action-property-title">Input root digest</div>
                        <div>
                          <DigestComponent digest={this.state.action.inputRootDigest} expanded={true} />
                        </div>
                      </div>
                    )}
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
                              treeShaToTotalSizeMap={this.state.treeShaToTotalSizeMap}
                              handleFileClicked={this.handleFileClicked.bind(this)}
                            />
                          ))}
                        </div>
                      ) : (
                        <div>None found</div>
                      )}
                    </div>
                  </div>
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
                            {this.state.command?.platform?.properties.map((property) => (
                              <div>
                                <span className="prop-name">{property.name}</span>
                                <span className="prop-value">={property.value}</span>
                              </div>
                            ))}
                            {!this.state.command?.platform?.properties.length && <div>(Default)</div>}
                          </div>
                        </div>
                      </div>
                    ) : (
                      <div>No command details were found.</div>
                    )}
                  </div>
                </div>
              ) : (
                <div className="details">{this.renderNotFoundDetails({ result: false })}</div>
              )}
              <div>
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
                            <div className="metadata-title">Executor Host ID</div>
                            <div className="metadata-detail">{this.state.actionResult.executionMetadata.worker} </div>
                            <div className="metadata-title">Executor ID</div>
                            <div className="metadata-detail">
                              {this.state.actionResult.executionMetadata.executorId}
                            </div>
                            {this.state.actionResult.executionMetadata.usageStats && (
                              <>
                                <div className="metadata-title">Resource usage</div>
                                <div>
                                  <div>
                                    Peak memory:{" "}
                                    {format.bytes(this.state.actionResult.executionMetadata.usageStats.peakMemoryBytes)}
                                  </div>
                                  <div>MilliCPU: {computeMilliCpu(this.state.actionResult)}</div>
                                  {this.state.actionResult.executionMetadata.usageStats.peakFileSystemUsage?.map(
                                    (fs) => (
                                      <div>
                                        Peak disk usage: {fs.target} ({fs.fstype}): {format.bytes(fs.usedBytes)} of{" "}
                                        {format.bytes(fs.totalBytes)}
                                      </div>
                                    )
                                  )}
                                </div>
                              </>
                            )}
                            {this.state.actionResult.executionMetadata.estimatedTaskSize && (
                              <>
                                <div className="metadata-title">Estimated resource usage</div>
                                <div>
                                  <div>
                                    Peak memory:{" "}
                                    {format.bytes(
                                      this.state.actionResult.executionMetadata.estimatedTaskSize.estimatedMemoryBytes
                                    )}
                                  </div>
                                  <div>
                                    MilliCPU:{" "}
                                    {this.state.actionResult.executionMetadata.estimatedTaskSize.estimatedMilliCpu}
                                  </div>
                                </div>
                              </>
                            )}
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
                                {file.digest && <DigestComponent digest={file.digest} />}
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
                      <div className="action-section">
                        <div className="action-property-title">Stderr</div>
                        <div>
                          {this.state.stderr ? (
                            <TerminalComponent
                              value={this.state.stderr}
                              lightTheme={this.props.preferences.lightTerminalEnabled}
                            />
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                      </div>
                      <div className="action-section">
                        <div className="action-property-title">Stdout</div>
                        <div>
                          {this.state.stdout ? (
                            <TerminalComponent
                              value={this.state.stdout}
                              lightTheme={this.props.preferences.lightTerminalEnabled}
                            />
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div>{this.renderNotFoundDetails({ result: true })}</div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  }
}

function computeMilliCpu(result: build.bazel.remote.execution.v2.ActionResult): number {
  const metadata = result?.executionMetadata;
  if (!metadata) return 0;
  const usage = metadata.usageStats;
  if (!usage) return 0;

  const execDurationSeconds = durationSeconds(metadata.executionStartTimestamp, metadata.executionCompletedTimestamp);
  const cpuMillis = Number(usage.cpuNanos) / 1e6;
  return Math.floor(cpuMillis / execDurationSeconds);
}

function durationSeconds(t1: any, t2: any): number {
  return timestampToUnixSeconds(t2) - timestampToUnixSeconds(t1);
}

function timestampToUnixSeconds(timestamp: any): number {
  return timestamp.seconds + timestamp.nanos / 1e9;
}
