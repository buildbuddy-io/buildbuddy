import React from "react";
import { LazyLog } from "react-lazylog";

export interface TerminalProps {
  value?: string;
}

export default class TerminalComponent extends React.Component<TerminalProps> {
  render() {
    return (
      <div className="terminal">
        <LazyLog
          selectableLines={true}
          caseInsensitive={true}
          rowHeight={20}
          lineClassName="terminal-line"
          follow={true}
          text={this.props.value || "No build logs..."}
        />
      </div>
    );
  }
}

export { TerminalComponent };
