import React from 'react';
import format from '../format/format'
import SetupCodeComponent from '../docs/setup_code'

import { invocation } from '../../proto/invocation_ts_proto';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';
import { TerminalComponent } from '../terminal/terminal'
import rpcService from '../service/rpc_service';


interface Props {
  action: invocation.InvocationEvent,
  invocationId: string,
}

interface State {
  stdErr: string;
  stdOut: string;
  cacheEnabled: boolean;
}

export default class TargetTestLogCardComponent extends React.Component {
  props: Props;

  state: State = {
    stdErr: '',
    stdOut: '',
    cacheEnabled: true
  }

  componentDidMount() {
    console.log(this.props.action);
    this.fetchStdErr();
    this.fetchStdOut();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.action !== prevProps.action) {
      this.fetchStdErr();
      this.fetchStdOut();
    }
  }

  fetchStdErr() {
    let logUrl = this.props.action?.buildEvent?.action?.stderr?.uri;

    if (!logUrl) {
      return;
    }

    if (!logUrl.startsWith("bytestream://")) {
      this.setState({ ...this.state, cacheEnabled: false });
      return;
    }

    rpcService.fetchBytestreamFile(logUrl, this.props.invocationId).then((contents: string) => {
      this.setState({ ...this.state, stdErr: contents });
    }).catch(() => {
      this.setState({ ...this.state, testLog: "Error loading bytestream stderr!" });
    });
  }

  fetchStdOut() {
    let logUrl = this.props.action?.buildEvent?.action?.stdout?.uri;

    if (!logUrl) {
      return;
    }

    if (!logUrl.startsWith("bytestream://")) {
      this.setState({ ...this.state, cacheEnabled: false });
      return;
    }

    rpcService.fetchBytestreamFile(logUrl, this.props.invocationId).then((contents: string) => {
      this.setState({ ...this.state, stdout: contents });
    }).catch(() => {
      this.setState({ ...this.state, testLog: "Error loading bytestream stdout!" });
    });
  }

  getStatusTitle(status: boolean) {
    return status ? "Passed" : "Failed";
  }

  render() {
    return <div>
      {this.props.action?.buildEvent?.action?.stderr?.uri && <div className={`card ${this.state.cacheEnabled && "dark"} ${this.props.action.buildEvent.action.success ? "card-success" : "card-failure"}`}>
        <img className="icon" src="/image/log-circle-light.svg" />
        <div className="content">
          <div className="title">Error Log</div>
          <div className="test-subtitle">{this.getStatusTitle(this.props.action.buildEvent.action.success)}</div>
          {!this.state.cacheEnabled &&
            <div className="empty-state">
              Log uploading isn't enabled for this invocation.<br /><br />
            To enable action log uploading you must add GRPC remote caching. You can do so by checking <b>Enable cache</b> below, updating your <b>.bazelrc</b> accordingly, and re-running your invocation:
            <SetupCodeComponent />
            </div>}
          {this.state.cacheEnabled && this.state.stdErr && <div className="error-log"><TerminalComponent value={this.state.stdErr} /></div>}
          {this.state.cacheEnabled && !this.state.stdErr && <span><br />Loading...</span>}
        </div>
      </div>}

      {this.props.action?.buildEvent?.action?.stdout?.uri && <div className={`card ${this.state.cacheEnabled && "dark"}`}>
        <img className="icon" src="/image/log-circle-light.svg" />
        <div className="content">
          <div className="title">Log</div>
          {!this.state.cacheEnabled &&
            <div className="empty-state">
              Log uploading isn't enabled for this invocation.<br /><br />
            To enable action log uploading you must add GRPC remote caching. You can do so by checking <b>Enable cache</b> below, updating your <b>.bazelrc</b> accordingly, and re-running your invocation:
            <SetupCodeComponent />
            </div>}
          {this.state.cacheEnabled && this.state.stdOut && <div className="error-log"><TerminalComponent value={this.state.stdOut} /></div>}
          {this.state.cacheEnabled && !this.state.stdOut && <span><br />Loading...</span>}
        </div>
      </div>}

      <div className={`card ${this.props.action.buildEvent.action.success ? "card-success" : "card-failure"}`}>
        <img className="icon" src="/image/play-circle.svg" />
        <div className="content">
          <div className="title">Executed command</div>
          <div>
            {this.props.action?.buildEvent?.action?.commandLine.map(commandLineArg => <div className="command-line-arg">
              {commandLineArg}
            </div>)}
          </div>
        </div>
      </div>
    </div>
  }
}
