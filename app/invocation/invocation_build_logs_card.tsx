import { PauseCircle } from "lucide-react";
import React from "react";
import { TerminalComponent } from "../terminal/terminal";

interface Props {
  value: string;
  loading: boolean;
  expanded: boolean;
  dark: boolean;
  fullLogsFetcher: () => Promise<string>;
}

export default class BuildLogsCardComponent extends React.Component<Props> {
  render() {
    return (
      <div
        className={`card build-logs-card ${this.props.dark ? "dark" : "light-terminal"} ${
          this.props.expanded ? "expanded" : ""
        }`}>
        <PauseCircle className={`icon rotate-90 ${this.props.dark ? "white" : ""}`} />
        <div className="content">
          <div className="details">
            <TerminalComponent
              title={<div className="title">Build logs</div>}
              loading={this.props.loading}
              value={this.props.value}
              lightTheme={!this.props.dark}
              fullLogsFetcher={this.props.fullLogsFetcher}
            />
          </div>
        </div>
      </div>
    );
  }
}
