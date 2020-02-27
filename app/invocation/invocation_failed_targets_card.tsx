import React from 'react';
import InvocationModel from './invocation_model'

interface Props {
  model: InvocationModel,
  limitResults: boolean,
}

interface State {
  limit: number
}

const defaultPageSize = 10;

export default class SucceededTargetsCardComponent extends React.Component {
  props: Props;

  state: State = {
    limit: defaultPageSize
  }

  handleMoreFailedClicked() {
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
      <img className="icon" src="/image/x-circle.svg" />
      <div className="content">
        <div className="title">
          {this.props.model.failed.length} {this.props.model.failed.length == 1 ? "target" : "targets"} failed
        </div>
        <div className="details">
          {this.props.model.failed.slice(0, this.props.limitResults && this.state.limit || undefined).map(failed =>
            <div className="list-grid" onClick={this.handleTargetClicked.bind(this, failed.id.targetCompleted.label)}>
              <div title={`${this.props.model.configuredMap.get(failed.id.targetCompleted.label).buildEvent.configured.targetKind} ${this.props.model.getTestSize(this.props.model.configuredMap.get(failed.id.targetCompleted.label).buildEvent.configured.testSize)}`}
                className={`${this.props.model.getTestResultLog(failed.id.targetCompleted.label) ? 'clickable target' : 'target'}`}>
                <img className="target-status-icon" src="/image/x-circle.svg" /> {failed.id.targetCompleted.label}
              </div>
              <div>{this.props.model.getRuntime(failed.id.targetCompleted.label)} seconds</div>
            </div>
          )}
        </div>
        {this.props.limitResults && this.props.model.failed.length > defaultPageSize && !!this.state.limit &&
          <div className="more" onClick={this.handleMoreFailedClicked.bind(this)}>See more failing targets</div>}
        {this.props.limitResults && this.props.model.failed.length > defaultPageSize && !this.state.limit &&
          <div className="more" onClick={this.handleMoreFailedClicked.bind(this)}>See less failing targets</div>}
      </div>
    </div>
  }
}
