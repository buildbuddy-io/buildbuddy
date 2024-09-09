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
import { Download, FileIcon } from "lucide-react";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  filter: string;
}

interface State {
  loading: boolean;
  sort: string;
  direction: "asc" | "desc";
  limit: number;
  log: tools.protos.ExecLogEntry[] | undefined;
}

export default class InvocationExecLogCardComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
    sort: "size",
    direction: "desc",
    limit: 100,
    log: undefined,
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

  sort(a: tools.protos.ExecLogEntry, b: tools.protos.ExecLogEntry): number {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    switch (this.state.sort) {
      case "size":
        return +(first?.file?.digest?.sizeBytes || 0) - +(second?.file?.digest?.sizeBytes || 0);
      default:
        return (first.file?.path || "").localeCompare(second.file?.path || "");
    }
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

  handleMoreClicked() {
    this.setState({ limit: this.state.limit + 100 });
  }

  handleAllClicked() {
    this.setState({ limit: Number.MAX_SAFE_INTEGER });
  }

  getFileLink(entry: tools.protos.ExecLogEntry) {
    if (!entry.file?.digest) {
      return undefined;
    }

    return `/code/buildbuddy-io/buildbuddy/?bytestream_url=${encodeURIComponent(
      this.props.model.getBytestreamURL(entry.file.digest)
    )}&invocation_id=${this.props.model.getInvocationId()}&filename=${entry.file.path}`;
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
      .sort(this.sort.bind(this));

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
                {files.slice(0, this.state.limit).map((file) => (
                  <Link key={file.id} className="invocation-execution-row" href={this.getFileLink(file)}>
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
              {files.length > this.state.limit && (
                <div className="more-buttons">
                  <OutlinedButton onClick={this.handleMoreClicked.bind(this)}>See more files</OutlinedButton>
                  <OutlinedButton onClick={this.handleAllClicked.bind(this)}>See all files</OutlinedButton>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
