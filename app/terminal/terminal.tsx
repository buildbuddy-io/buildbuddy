import { WrapText, Download } from "lucide-react";
import React from "react";
import { LazyLog } from "react-lazylog";
import { eventlog } from "../../proto/eventlog_ts_proto";
import rpcService from "../service/rpc_service";

export interface TerminalProps {
  value?: string;
  lightTheme?: boolean;
  invocationId?: string;
}

interface TerminalState {
  wrap: boolean;
}

const WRAP_LOCAL_STORAGE_KEY = "terminal-wrap";
const WRAP_LOCAL_STORAGE_VALUE = "wrap";
const ANSI_STYLES_REGEX = /\x1b\[[\d;]+?m/g;

export default class TerminalComponent extends React.Component<TerminalProps, TerminalState> {
  state = { wrap: false };

  terminalRef = React.createRef<HTMLDivElement>();

  componentDidMount() {
    this.setState({ wrap: localStorage.getItem(WRAP_LOCAL_STORAGE_KEY) === WRAP_LOCAL_STORAGE_VALUE });
  }

  render() {
    return (
      <div className={`terminal-container ${this.props.lightTheme ? "light-terminal" : ""}`}>
        <div className="terminal-actions">
          <button
            title="Wrap"
            onClick={this.handleWrapClicked.bind(this)}
            className={`terminal-action ${this.state.wrap ? "active" : ""}`}>
            <WrapText className="icon white" />
          </button>
          <button title="Download" onClick={this.handleDownloadClicked.bind(this)} className="terminal-action">
            <Download className="icon white" />
          </button>
        </div>
        <div className="terminal" ref={this.terminalRef}>
          <LazyLog
            selectableLines={true}
            caseInsensitive={true}
            rowHeight={20}
            enableSearch={true}
            lineClassName="terminal-line"
            follow={true}
            // Ensure a trailing blank line to prevent the horizontal scrollbar
            // from covering up the last line of logs.
            text={(this.wrapText(this.props.value) || "No build logs...") + "\n\n"}
            // This spread works around lightTheme not being included in @types/react-lazylog
            // (it's a custom prop we added in our fork).
            {...{ lightTheme: this.props.lightTheme }}
          />
        </div>
      </div>
    );
  }

  wrapText(text: string): string {
    if (!this.state.wrap || !this.terminalRef.current) {
      return text;
    }
    let width = Math.floor(this.terminalRef.current?.getBoundingClientRect().width / 8.5);
    return text.replace(new RegExp(`(?![^\\n]{1,${width}}$)([^\\n]{1,${width}})\\s`, "g"), "$1\n");
  }

  handleWrapClicked() {
    let shouldWrap = !this.state.wrap;
    localStorage.setItem(WRAP_LOCAL_STORAGE_KEY, shouldWrap ? WRAP_LOCAL_STORAGE_VALUE : undefined);
    this.setState({ wrap: shouldWrap });
  }

  handleDownloadClicked() {
    if (typeof this.props.invocationId === "string") {
      rpcService.service
        .getEventLogChunk(
          new eventlog.GetEventLogChunkRequest({
            invocationId: this.props.invocationId,
            minLines: 2147483647, // int32 max value; this is the maximum number of lines we can request.
          })
        )
        .then((response: eventlog.GetEventLogChunkResponse) => {
          var element = document.createElement("a");
          let fullLog = new TextDecoder().decode(response.buffer || new Uint8Array());
          const unstyledLogs = fullLog.replace(ANSI_STYLES_REGEX, "");
          element.setAttribute("href", "data:text/plain;charset=utf-8," + encodeURIComponent(unstyledLogs));
          element.setAttribute("download", "build_logs.txt");
          element.style.display = "none";
          document.body.appendChild(element);
          element.click();
          document.body.removeChild(element);
        });
      return;
    }
    var element = document.createElement("a");
    const unstyledLogs = this.props.value.replace(ANSI_STYLES_REGEX, "");
    element.setAttribute("href", "data:text/plain;charset=utf-8," + encodeURIComponent(unstyledLogs));
    element.setAttribute("download", "build_logs.txt");
    element.style.display = "none";
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  }
}

export { TerminalComponent };
