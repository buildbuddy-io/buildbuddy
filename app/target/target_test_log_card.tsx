import React from 'react';
import format from '../format/format'
import CacheCodeComponent from '../docs/cache_code'

import { invocation } from '../../proto/invocation_ts_proto';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';
import { TerminalComponent } from '../terminal/terminal'
import rpcService from '../service/rpc_service';


interface Props {
  testResult: invocation.InvocationEvent,
  invocationId: string,
}

interface State {
  testLog: string;
  cacheEnabled: boolean;
}

export default class TargetTestLogCardComponent extends React.Component {
  props: Props;

  state: State = {
    testLog: '',
    cacheEnabled: true
  }

  componentDidMount() {
    this.fetchTestLog();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.testResult !== prevProps.testResult) {
      this.fetchTestLog();
    }
  }

  fetchTestLog() {
    let testLogUrl = this.props.testResult.buildEvent.testResult.testActionOutput.find((log: any) => log.name == "test.log")?.uri;

    if (!testLogUrl) {
      return;
    }

    if (!testLogUrl.startsWith("bytestream://")) {
      this.setState({ ...this.state, cacheEnabled: false });
      return;
    }

    rpcService.fetchBytestreamFile(testLogUrl, this.props.invocationId).then((contents: string) => {
      this.setState({ ...this.state, testLog: contents });
    }).catch(() => {
      this.setState({ ...this.state, testLog: "Error loading bytestream test.log!" });
    });
  }

  getStatusTitle(status: build_event_stream.TestStatus) {
    switch (status) {
      case build_event_stream.TestStatus.PASSED:
        return "Passed";
      case build_event_stream.TestStatus.FLAKY:
        return "Flaky";
      case build_event_stream.TestStatus.TIMEOUT:
        return "Timeout";
      case build_event_stream.TestStatus.FAILED:
        return "Failed";
      case build_event_stream.TestStatus.INCOMPLETE:
        return "Incomplete";
      case build_event_stream.TestStatus.REMOTE_FAILURE:
        return "Remote failure";
      case build_event_stream.TestStatus.FAILED_TO_BUILD:
        return "Failed to build";
      case build_event_stream.TestStatus.TOOL_HALTED_BEFORE_TESTING:
        return "Halted before testing";
      default:
        return "Unknown";
    }
  }

  render() {
    return <div className={`card ${this.state.cacheEnabled && "dark"} ${this.props.testResult.buildEvent.testResult.status == build_event_stream.TestStatus.PASSED ? "card-success" : "card-failure"}`}>
      <img className="icon" src="/image/log-circle-light.svg" />
      <div className="content">
        <div className="title">Test log</div>
        <div className="test-subtitle">{this.getStatusTitle(this.props.testResult.buildEvent.testResult.status)} in {format.durationMillis(this.props.testResult.buildEvent.testResult.testAttemptDurationMillis)} on Shard {this.props.testResult.buildEvent.id.testResult.shard} (Run {this.props.testResult.buildEvent.id.testResult.run}, Attempt {this.props.testResult.buildEvent.id.testResult.attempt})</div>
        {!this.state.cacheEnabled &&
          <div className="empty-state">
            Test log uploading isn't enabled for this invocation.<br /><br />
            To enable test log uploading you must add GRPC remote caching. You can do so by adding the following line to your <b>.bazelrc</b> and re-running your invocation:
            <CacheCodeComponent />
          </div>}
        {this.state.cacheEnabled && this.state.testLog && <div className="test-log"><TerminalComponent value={this.state.testLog} /></div>}
        {this.state.cacheEnabled && !this.state.testLog && <span><br />Loading...</span>}
      </div>
    </div>
  }
}
