import React from 'react';
import InvocationModel from './invocation_model';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';

interface Props {
  model: InvocationModel,
  limitResults: boolean,
  iconPath: string,
  buildEvents: build_event_stream.BuildEvent[]
  pastVerb: string,
  presentVerb: string,
}

interface State {
  limit: number
}

const defaultPageSize = 10;

export default class TargetsCardComponent extends React.Component {
  props: Props;

  state: State = {
    limit: defaultPageSize
  }

  handleMoreClicked() {
    this.setState({ ...this.state, limit: this.state.limit ? undefined : defaultPageSize })
  }

  handleTargetClicked(label: string) {
    let log = this.props.model.getTestResultLog(label);
    if (!log) return;
    if (log.startsWith("file://")) {
      window.prompt("Copy test log path to clipboard: Cmd+C, Enter", log);
    } else {
      window.open(log, '_blank');
    }
  }

  render() {
    return <div className="card">
      <img className="icon" src={this.props.iconPath} />
      <div className="content">
        <div className="title">
          {this.props.buildEvents.length} {this.props.buildEvents.length == 1 ? "target" : "targets"} {this.props.pastVerb}
        </div>
        <div className="details">
          {this.props.buildEvents.slice(0, this.props.limitResults && this.state.limit || undefined).map(target =>
            <div className="list-grid" onClick={this.handleTargetClicked.bind(this, target.id.targetCompleted.label)}>
              <div title={`${this.props.model.configuredMap.get(target.id.targetCompleted.label).buildEvent.configured.targetKind} ${this.props.model.getTestSize(this.props.model.configuredMap.get(target.id.targetCompleted.label).buildEvent.configured.testSize)}`}
                className={`${this.props.model.getTestResultLog(target.id.targetCompleted.label) ? 'clickable target' : 'target'}`}>
                <img className="target-status-icon" src={this.props.iconPath} /> {target.id.targetCompleted.label}
              </div>
              <div>{this.props.model.getRuntime(target.id.targetCompleted.label)} seconds</div>
            </div>
          )}
        </div>
        {this.props.limitResults && this.props.buildEvents.length > defaultPageSize && !!this.state.limit &&
          <div className="more" onClick={this.handleMoreClicked.bind(this)}>See more {this.props.presentVerb} targets</div>}
        {this.props.limitResults && this.props.buildEvents.length > defaultPageSize && !this.state.limit &&
          <div className="more" onClick={this.handleMoreClicked.bind(this)}>See less {this.props.presentVerb} targets</div>}
      </div>
    </div>
  }
}
