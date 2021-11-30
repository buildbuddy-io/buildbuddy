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
          <div className="title">Build logs </div>
          <div className="details">
            {this.props.loading ? (
              // "terminal" class makes sure the loading container's size
              // is the same as the terminal that will take its place.
              <div className="terminal">
                <div className={`loading ${this.props.dark ? "loading-dark" : ""}`} />
              </div>
            ) : (
              <TerminalComponent
                value={this.props.value}
                lightTheme={!this.props.dark}
                fullLogsFetcher={this.props.fullLogsFetcher}
              />
            )}
          </div>
        </div>
      </div>
    );
  }
}
