import React from 'react';
import InvocationModel from './invocation_model';
import router from '../router/router';
import { build_event_stream } from '../../proto/build_event_stream_ts_proto';

interface Props {
  model: InvocationModel,
  iconPath: string,
  buildEvents: build_event_stream.BuildEvent[]
  pastVerb: string,
  presentVerb: string,
  pageSize: number,
  filter: string,
  className: string,
}

interface State {
  numPages: number
}

export default class TargetsCardComponent extends React.Component {
  props: Props;

  state: State = {
    numPages: 1
  }

  handleMoreClicked() {
    this.setState({ ...this.state, numPages: this.state.numPages + 1 })
  }

  handleTargetClicked(label: string) {
    router.navigateToQueryParam("target", label);
  }

  render() {
    let events = this.props.buildEvents
      .filter(target => !this.props.filter || target.id.targetCompleted.label.toLowerCase().includes(this.props.filter.toLowerCase()));

    return <div className={`card ${this.props.className}`}>
      <img className="icon" src={this.props.iconPath} />
      <div className="content">
        <div className="title">
          {events.length}{this.props.filter ? " matching" : ""} {this.props.pastVerb}
        </div>
        <div className="details">
          {
            events.slice(0, this.props.pageSize && (this.state.numPages * this.props.pageSize) || undefined)
              .map(target =>
                <div className="list-grid" onClick={this.handleTargetClicked.bind(this, target.id.targetCompleted.label)}>
                  <div title={`${this.props.model.configuredMap.get(target.id.targetCompleted.label)?.buildEvent.configured.targetKind} ${this.props.model.getTestSize(this.props.model.configuredMap.get(target.id.targetCompleted.label)?.buildEvent.configured.testSize)}`}
                    className="clickable target">
                    <img className="target-status-icon" src={this.props.iconPath} /> {target.id.targetCompleted.label}
                  </div>
                  <div>{this.props.model.getRuntime(target.id.targetCompleted.label)}</div>
                </div>
              )}
        </div>
        {this.props.pageSize && events.length > (this.props.pageSize * this.state.numPages) && !!this.state.numPages &&
          <div className="more" onClick={this.handleMoreClicked.bind(this)}>See more {this.props.presentVerb}</div>}
      </div>
    </div>
  }
}
