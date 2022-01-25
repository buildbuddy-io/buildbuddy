import { WrapText, Download } from "lucide-react";
import React from "react";
import { LazyLog } from "react-lazylog";
import errorService from "../errors/error_service";
import Spinner from "../components/spinner/spinner";

export interface TerminalProps {
  value?: string;
  loading?: boolean;

  title?: JSX.Element;

  lightTheme?: boolean;
  fullLogsFetcher?: () => Promise<string>;
}

interface TerminalState {
  wrap: boolean;
  isLoadingFullLog: boolean;
}

const WRAP_LOCAL_STORAGE_KEY = "terminal-wrap";
const WRAP_LOCAL_STORAGE_VALUE = "wrap";
const ANSI_STYLES_REGEX = /\x1b\[[\d;]+?m/g;

export default class TerminalComponent extends React.Component<TerminalProps, TerminalState> {
  state = { wrap: false, isLoadingFullLog: false };

  terminalRef = React.createRef<HTMLDivElement>();

  componentDidMount() {
    this.setState({ wrap: localStorage.getItem(WRAP_LOCAL_STORAGE_KEY) === WRAP_LOCAL_STORAGE_VALUE });
  }

  render() {
    return (
      <div className={`terminal-container ${this.props.lightTheme ? "light-terminal" : ""}`}>
        <div className="terminal-content">
          <div className="terminal-top-bar">
            {this.props.title && <div className="terminal-titles">{this.props.title}</div>}
            <div className="terminal-actions">
              <button
                title="Wrap"
                onClick={this.handleWrapClicked.bind(this)}
                className={`terminal-action ${this.state.wrap ? "active" : ""}`}>
                <WrapText className="icon white" />
              </button>
              {this.state.isLoadingFullLog ? (
                <Spinner />
              ) : (
                <button title="Download" onClick={this.handleDownloadClicked.bind(this)} className="terminal-action">
                  <Download className="icon white" />
                </button>
              )}
            </div>
          </div>
          <div className="terminal" ref={this.terminalRef}>
            {this.props.loading ? (
              <div className={`loading ${this.props.lightTheme ? "" : "loading-dark-terminal"}`} />
            ) : (
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
            )}
          </div>
        </div>
      </div>
    );
  }

  wrapText(text: string): string {
    if (!this.state.wrap || !this.terminalRef.current || !text) {
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
    const serveLog = (log: string) => {
      const element = document.createElement("a");
      const unstyledLogs = log.replace(ANSI_STYLES_REGEX, "");
      element.setAttribute("href", "data:text/plain;charset=utf-8," + encodeURIComponent(unstyledLogs));
      element.setAttribute("download", "build_logs.txt");
      element.style.display = "none";
      document.body.appendChild(element);
      element.click();
      element.remove();
    };
    if (this.props.fullLogsFetcher) {
      this.setState({ isLoadingFullLog: true });
      this.props
        .fullLogsFetcher()
        .then(serveLog)
        .catch((e) => errorService.handleError(e))
        .finally(() => {
          this.setState({ isLoadingFullLog: false });
        });
      return;
    }
    serveLog(this.props.value);
  }
}

export { TerminalComponent };
