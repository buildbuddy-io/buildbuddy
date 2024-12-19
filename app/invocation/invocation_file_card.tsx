import React from "react";
import InvocationModel from "./invocation_model";
import Select, { Option } from "../components/select/select";
import rpcService from "../service/rpc_service";
import { OutlinedButton } from "../components/button/button";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { tools } from "../../proto/spawn_ts_proto";
import format from "../format/format";
import error_service from "../errors/error_service";
import * as varint from "varint";
import { Download, FileIcon, Folder, FolderOpen } from "lucide-react";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";
import Long from "long";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  filter: string;
}

interface State {
  loading: boolean;
  sort: string;
  direction: "asc" | "desc";
  fileLimit: number;
  directoryLimit: number;
  log: tools.protos.ExecLogEntry[] | undefined;
  directoryToFileLimit: Map<string, number>;
}

const pageSize = 100;
export default class InvocationFileCardComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
    sort: "size",
    direction: "desc",
    fileLimit: pageSize,
    directoryLimit: pageSize,
    log: undefined,
    directoryToFileLimit: new Map<string, number>(),
  };

  timeoutRef?: number;

  componentDidMount() {
    this.fetchLog();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.fetchLog();
    }
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
  }

  getExecutionLogFile(): build_event_stream.File | undefined {
    return this.props.model.buildToolLogs?.log.find(
      (log: build_event_stream.File) =>
        (log.name == "execution.log" || log.name == "execution_log.binpb.zst") &&
        log.uri &&
        Boolean(log.uri.startsWith("bytestream://"))
    );
  }

  fetchLog() {
    if (!this.getExecutionLogFile()) {
      this.setState({ loading: false });
    }

    // Already fetched
    if (this.state.log) return;

    let logFile = this.getExecutionLogFile();
    if (!logFile?.uri) return;

    const init = {
      // Set the stored encoding header to prevent the server from double-compressing.
      headers: { "X-Stored-Encoding-Hint": "zstd" },
    };

    this.setState({ loading: true });
    rpcService
      .fetchBytestreamFile(logFile.uri, this.props.model.getInvocationId(), "arraybuffer", { init })
      .then(async (body) => {
        if (body === null) throw new Error("response body is null");
        let entries: tools.protos.ExecLogEntry[] = [];
        let byteArray = new Uint8Array(body);
        for (var offset = 0; offset < body.byteLength; ) {
          let length = varint.decode(byteArray, offset);
          let bytes = varint.decode.bytes || 0;
          offset += bytes;
          entries.push(tools.protos.ExecLogEntry.decode(byteArray.subarray(offset, offset + length)));
          offset += length;
        }
        console.log(entries);
        return entries;
      })
      .then((log) => this.setState({ log: log }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  downloadLog() {
    let profileFile = this.getExecutionLogFile();
    if (!profileFile?.uri) {
      return;
    }

    try {
      rpcService.downloadBytestreamFile("execution_log.binpb.zst", profileFile.uri, this.props.model.getInvocationId());
    } catch {
      console.error("Error downloading execution log");
    }
  }

  compareFiles(a: tools.protos.ExecLogEntry, b: tools.protos.ExecLogEntry): number {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    switch (this.state.sort) {
      case "size":
        return +(first?.file?.digest?.sizeBytes || 0) - +(second?.file?.digest?.sizeBytes || 0);
      default:
        return (first.file?.path || "").localeCompare(second.file?.path || "");
    }
  }

  compareDirectories(a: tools.protos.ExecLogEntry, b: tools.protos.ExecLogEntry): number {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    switch (this.state.sort) {
      case "size":
        return +this.sumFileSizes(first?.directory?.files) - +this.sumFileSizes(second?.directory?.files);
      default:
        return (first.directory?.path || "").localeCompare(second.directory?.path || "");
    }
  }

  sumFileSizes(files: Array<tools.protos.ExecLogEntry.File> | undefined) {
    return (
      (files || []).map((f) => f.digest?.sizeBytes || 0).reduce((a, b) => new Long(+a + +b), Long.ZERO) || Long.ZERO
    );
  }

  hashFileHashes(files: Array<tools.protos.ExecLogEntry.File> | undefined) {
    // Just do something simple here that gives us a unique string that changes when one file changes.
    return (files || [])
      .map((f) => f.digest?.hash || "")
      .reduce((a, b) => {
        var hash = "";
        for (var i = 0; i < Math.max(a.length, b.length); i++) {
          let char = (a.charCodeAt(i) + b.charCodeAt(i)) % 36;
          hash += String.fromCharCode(char < 26 ? char + 97 : char + 22);
        }
        return hash;
      }, "");
  }

  handleSortDirectionChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({
      direction: event.target.value as "asc" | "desc",
    });
  }

  handleSortChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({
      sort: event.target.value,
    });
  }

  handleMoreFilesClicked() {
    this.setState({ fileLimit: this.state.fileLimit + pageSize });
  }

  handleAllFilesClicked() {
    this.setState({ fileLimit: Number.MAX_SAFE_INTEGER });
  }

  handleMoreDirectoriesClicked() {
    this.setState({ directoryLimit: this.state.directoryLimit + pageSize });
  }

  handleAllDirectoriesClicked() {
    this.setState({ directoryLimit: Number.MAX_SAFE_INTEGER });
  }

  getFileLink(file: tools.protos.ExecLogEntry.File | null | undefined) {
    if (!file?.digest) {
      return undefined;
    }

    return `/code/buildbuddy-io/buildbuddy/?bytestream_url=${encodeURIComponent(
      this.props.model.getBytestreamURL(file.digest)
    )}&invocation_id=${this.props.model.getInvocationId()}&filename=${file.path}`;
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.log?.length) {
      return (
        <div className="invocation-execution-empty-state">
          No files for this invocation{this.props.model.isInProgress() && <span> yet</span>}.
        </div>
      );
    }

    const files = this.state.log
      .filter((l) => {
        if (l.type != "file") {
          return false;
        }
        if (
          this.props.filter != "" &&
          !l.file?.digest?.hash.toLowerCase().includes(this.props.filter.toLowerCase()) &&
          !l.file?.path.toLowerCase().includes(this.props.filter.toLowerCase())
        ) {
          return false;
        }

        return true;
      })
      .sort(this.compareFiles.bind(this));

    const directories = this.state.log
      .filter((l) => {
        if (l.type != "directory") {
          return false;
        }
        if (
          this.props.filter != "" &&
          !l.directory?.files.find((f) =>
            (l.directory?.path + "/" + f.path).toLowerCase().includes(this.props.filter.toLowerCase())
          ) &&
          !l.directory?.files.find((f) => f.digest?.hash.includes(this.props.filter)) &&
          !l.directory?.path.toLowerCase().includes(this.props.filter.toLowerCase())
        ) {
          return false;
        }

        return true;
      })
      .sort(this.compareDirectories.bind(this));

    return (
      <div>
        <div className={`card expanded`}>
          <div className="content">
            <div className="invocation-content-header">
              <div className="title">
                Files ({format.formatWithCommas(files.length)}){" "}
                <Download className="download-exec-log-button" onClick={() => this.downloadLog()} />
              </div>
              <div className="invocation-sort-controls">
                <span className="invocation-sort-title">Sort by</span>
                <Select onChange={this.handleSortChange.bind(this)} value={this.state.sort}>
                  <Option value="path">File path</Option>
                  <Option value="size">File size</Option>
                </Select>
                <span className="group-container">
                  <div>
                    <input
                      id="direction-asc"
                      checked={this.state.direction == "asc"}
                      onChange={this.handleSortDirectionChange.bind(this)}
                      value="asc"
                      name="execLogDirection"
                      type="radio"
                    />
                    <label htmlFor="direction-asc">Asc</label>
                  </div>
                  <div>
                    <input
                      id="direction-desc"
                      checked={this.state.direction == "desc"}
                      onChange={this.handleSortDirectionChange.bind(this)}
                      value="desc"
                      name="execLogDirection"
                      type="radio"
                    />
                    <label htmlFor="direction-desc">Desc</label>
                  </div>
                </span>
              </div>
            </div>
            <div>
              <div className="invocation-execution-table">
                {files.slice(0, this.state.fileLimit).map((file) => (
                  <Link key={file.id} className="invocation-execution-row" href={this.getFileLink(file?.file)}>
                    <div className="invocation-execution-row-image">
                      <FileIcon className="icon" />
                    </div>
                    <div>
                      <div className="invocation-execution-row-header">
                        {file.file?.digest && (
                          <div>
                            <DigestComponent digest={file.file.digest} expanded={true} />
                          </div>
                        )}
                      </div>
                      <div className="invocation-execution-row-header-status">{file.file?.path}</div>
                    </div>
                  </Link>
                ))}
              </div>
              {files.length > this.state.fileLimit && (
                <div className="more-buttons">
                  <OutlinedButton onClick={this.handleMoreFilesClicked.bind(this)}>See more files</OutlinedButton>
                  <OutlinedButton onClick={this.handleAllFilesClicked.bind(this)}>See all files</OutlinedButton>
                </div>
              )}
            </div>
          </div>
        </div>
        <div className={`card expanded`}>
          <div className="content">
            <div className="invocation-content-header">
              <div className="title">
                Directories ({format.formatWithCommas(directories.length)}){" "}
                <Download className="download-exec-log-button" onClick={() => this.downloadLog()} />
              </div>
              <div className="invocation-sort-controls">
                <span className="invocation-sort-title">Sort by</span>
                <Select onChange={this.handleSortChange.bind(this)} value={this.state.sort}>
                  <Option value="path">File path</Option>
                  <Option value="size">File size</Option>
                </Select>
                <span className="group-container">
                  <div>
                    <input
                      id="direction-asc-dir"
                      checked={this.state.direction == "asc"}
                      onChange={this.handleSortDirectionChange.bind(this)}
                      value="asc"
                      name="dirDirection"
                      type="radio"
                    />
                    <label htmlFor="direction-asc-dir">Asc</label>
                  </div>
                  <div>
                    <input
                      id="direction-desc-dir"
                      checked={this.state.direction == "desc"}
                      onChange={this.handleSortDirectionChange.bind(this)}
                      value="desc"
                      name="dirDirection"
                      type="radio"
                    />
                    <label htmlFor="direction-desc-dir">Desc</label>
                  </div>
                </span>
              </div>
            </div>
            <div>
              <div className="invocation-execution-table">
                {directories.slice(0, this.state.directoryLimit).map((entry) => {
                  let path = entry.directory?.path || "";
                  let fileLimit = this.state.directoryToFileLimit.get(path) || 0;
                  return (
                    <>
                      <div
                        className="invocation-execution-row clickable"
                        onClick={() => {
                          this.state.directoryToFileLimit.set(path, !fileLimit ? pageSize : 0);
                          this.forceUpdate();
                        }}>
                        <div className="invocation-execution-row-image">
                          {fileLimit ? <FolderOpen className="icon" /> : <Folder className="icon" />}
                        </div>
                        <div>
                          <div className="invocation-execution-row-header">
                            <div>
                              <DigestComponent
                                digest={{
                                  sizeBytes: this.sumFileSizes(entry.directory?.files),
                                  hash: this.hashFileHashes(entry.directory?.files),
                                }}
                                expanded={true}
                              />
                            </div>
                          </div>
                          <div className="invocation-execution-row-header-status">{path}</div>
                        </div>
                      </div>
                      {(Boolean(fileLimit) || this.props.filter) &&
                        entry.directory?.files
                          .filter(
                            (e) =>
                              (path + "/" + e.path).toLowerCase().includes(this.props.filter) ||
                              e.digest?.hash.includes(this.props.filter)
                          )
                          .sort((a, b) =>
                            this.compareFiles(
                              new tools.protos.ExecLogEntry({ file: a }),
                              new tools.protos.ExecLogEntry({ file: b })
                            )
                          )
                          .slice(0, Math.max(fileLimit, this.props.filter ? pageSize : 0))
                          .map((file) => (
                            <Link
                              key={file.path}
                              className="invocation-execution-row nested"
                              href={this.getFileLink(file)}>
                              <div className="invocation-execution-row-image">
                                <FileIcon className="icon" />
                              </div>
                              <div>
                                <div className="invocation-execution-row-header">
                                  {file?.digest && (
                                    <div>
                                      <DigestComponent digest={file.digest} expanded={true} />
                                    </div>
                                  )}
                                </div>
                                <div className="invocation-execution-row-header-status">{file?.path}</div>
                              </div>
                            </Link>
                          ))}
                      {Boolean(fileLimit) && (entry.directory?.files.length || 0) > fileLimit && (
                        <div className="more-buttons nested">
                          <OutlinedButton
                            onClick={() => {
                              this.state.directoryToFileLimit.set(path, fileLimit + pageSize);
                              this.forceUpdate();
                            }}>
                            See more files
                          </OutlinedButton>
                          <OutlinedButton
                            onClick={() => {
                              this.state.directoryToFileLimit.set(path, Number.MAX_SAFE_INTEGER);
                              this.forceUpdate();
                            }}>
                            See all files
                          </OutlinedButton>
                        </div>
                      )}
                    </>
                  );
                })}
              </div>
              {directories.length > this.state.directoryLimit && (
                <div className="more-buttons">
                  <OutlinedButton onClick={this.handleMoreDirectoriesClicked.bind(this)}>
                    See more directories
                  </OutlinedButton>
                  <OutlinedButton onClick={this.handleAllDirectoriesClicked.bind(this)}>
                    See all directories
                  </OutlinedButton>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
