import React from "react";
import { LazyLog } from "react-lazylog";

export interface TerminalProps {
  value?: string;
  lightTheme?: boolean;
}

interface TerminalState {
  wrap: boolean;
}

const wrapLocalStorageKey = "terminal-wrap";
const wrapLocalStorageValue = "wrap";

export default class TerminalComponent extends React.Component<TerminalProps, TerminalState> {
  state = { wrap: false };

  terminalRef = React.createRef<HTMLDivElement>();

  componentDidMount() {
    this.setState({ wrap: localStorage.getItem(wrapLocalStorageKey) === wrapLocalStorageValue });
  }

  render() {
    return (
      <>
        <div className="terminal-actions">
          <button
            title="Wrap"
            onClick={this.handleWrapClicked.bind(this)}
            className={`terminal-action ${this.state.wrap ? "active" : ""}`}>
            <img src="/image/wrap-white.svg" />
          </button>
          <button title="Download" onClick={this.handleDownloadClicked.bind(this)} className="terminal-action">
            <img src="/image/download-white.svg" />
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
      </>
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
    localStorage.setItem(wrapLocalStorageKey, shouldWrap ? wrapLocalStorageValue : undefined);
    this.setState({ wrap: shouldWrap });
  }

  handleDownloadClicked() {
    var element = document.createElement("a");
    element.setAttribute("href", "data:text/plain;charset=utf-8," + encodeURIComponent(this.props.value));
    element.setAttribute("download", "build_logs.txt");
    element.style.display = "none";
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  }
}

export { TerminalComponent };
