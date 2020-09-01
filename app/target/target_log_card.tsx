import React from "react";

import { invocation } from "../../proto/invocation_ts_proto";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { TerminalComponent } from "../terminal/terminal";
import rpcService from "../service/rpc_service";

interface Props {
  title: string;
  subtitle: string;
  contents: string;
}

export default class TargetLogCardComponent extends React.Component {
  props: Props;

  render() {
    return (
      <div className={`card dark`}>
        <img className="icon" src="/image/log-circle-light.svg" />
        <div className="content">
          <div className="title">{this.props.title}</div>
          <div className="test-subtitle">{this.props.subtitle}</div>
          {this.props.contents && (
            <div className="test-log">
              <TerminalComponent value={this.props.contents} />
            </div>
          )}
        </div>
      </div>
    );
  }
}
