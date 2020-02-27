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

  handleMoreSucceededClicked() {
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
      <img className="icon" src="/image/check-circle.svg" />
      <div className="content">
        <div className="title">
          {this.props.model.succeeded.length} {this.props.model.succeeded.length == 1 ? "target" : "targets"} passed
        </div>
        <div className="details">
          {this.props.model.succeeded.slice(0, this.props.limitResults && this.state.limit || undefined).map(succeeded =>
            <div className="list-grid" onClick={this.handleTargetClicked.bind(this, succeeded.id.targetCompleted.label)}>
              <div title={`${this.props.model.configuredMap.get(succeeded.id.targetCompleted.label).buildEvent.configured.targetKind} ${this.props.model.getTestSize(this.props.model.configuredMap.get(succeeded.id.targetCompleted.label).buildEvent.configured.testSize)}`}
                className={`${this.props.model.getTestResultLog(succeeded.id.targetCompleted.label) ? 'clickable target' : 'target'}`}>
                <img className="target-status-icon" src="/image/check-circle.svg" /> {succeeded.id.targetCompleted.label}
              </div>
              <div>{this.props.model.getRuntime(succeeded.id.targetCompleted.label)} seconds</div>
            </div>
          )}
        </div>
        {this.props.limitResults && this.props.model.succeeded.length > defaultPageSize && !!this.state.limit &&
          <div className="more" onClick={this.handleMoreSucceededClicked.bind(this)}>See more passing targets</div>}
        {this.props.limitResults && this.props.model.succeeded.length > defaultPageSize && !this.state.limit &&
          <div className="more" onClick={this.handleMoreSucceededClicked.bind(this)}>See less passing targets</div>}
      </div>
    </div>
  }
}
