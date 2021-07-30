import React from "react";
import { TerminalComponent } from "../terminal/terminal";

interface Props {
  value: string;
  loading: boolean;
  expanded: boolean;
  dark: boolean;
}

export default class BuildLogsCardComponent extends React.Component<Props> {
  render() {
    return (
      <div className={`card ${this.props.dark ? "dark" : "light-terminal"} ${this.props.expanded ? "expanded" : ""}`}>
        <img className="icon" src={this.props.dark ? "/image/log-circle-light.svg" : "/image/log-circle.svg"} />
        <div className="content">
          <div className="title">Build logs </div>
          <div className="details">
            {this.props.loading ? (
              // "terminal" class makes sure the loading container's size
              // is the same as the terminal that will take its place.
              <div className="terminal">
                <div className="loading" />
              </div>
            ) : (
              <TerminalComponent value={this.props.value} lightTheme={!this.props.dark} />
            )}
          </div>
        </div>
      </div>
    );
  }
}
