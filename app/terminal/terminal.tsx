import React from "react";
import { LazyLog } from "react-lazylog";

export interface TerminalProps {
  value?: string;
  lightTheme?: boolean;
}

export default class TerminalComponent extends React.Component<TerminalProps> {
  render() {
    return (
      <div className="terminal">
        <LazyLog
          selectableLines={true}
          caseInsensitive={true}
          rowHeight={20}
          enableSearch={true}
          lineClassName="terminal-line"
          follow={true}
          // Ensure a trailing blank line to prevent the horizontal scrollbar
          // from covering up the last line of logs.
          text={(this.props.value || "No build logs...") + "\n\n"}
          // This spread works around lightTheme not being included in @types/react-lazylog
          // (it's a custom prop we added in our fork).
          {...{ lightTheme: this.props.lightTheme }}
        />
      </div>
    );
  }
}

export { TerminalComponent };
