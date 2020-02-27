import React from 'react';
import InvocationModel from '../invocation_model'

interface Props {
  model: InvocationModel,
  invocationId: string
}
export default class DenseInvocationOverviewComponent extends React.Component {
  props: Props;

  render() {
    return <div className="container">
      <div className="dense-invocation">
        <div className="dense-invocation-title">Invocation</div>
        <div className="dense-invocation-invocation-id">{this.props.invocationId} ({this.props.model.getStartDate()}, {this.props.model.getStartTime()})</div>
      </div>
      <div className="dense-invocation-status-bar">
        <div>
          {this.props.model.targets.length} {this.props.model.targets.length == 1 ? "target" : "targets"} evaluated on&nbsp;
          {this.props.model.getStartDate()} at {this.props.model.getStartTime()} for <span title={this.props.model.getDuractionSeconds()}>{this.props.model.getTiming()}</span>
        </div>
        <div>
          Evaluation started by {this.props.model.getUser()}
        </div>
      </div>
      <div className="dense-invocation-overview-grid">
        <div className="dense-invocation-overview-grid-chunk">
          <div className="dense-invocation-overview-grid-title">
            Invocation
          </div>
          <div className="dense-invocation-overview-grid-value">
            {this.props.model.getStatusIcon()}
            {this.props.model.getStatus()}
          </div>
        </div>
        <div className="dense-invocation-overview-grid-chunk">
          <div className="dense-invocation-overview-grid-title">
            Targets affected
          </div>
          <div className="dense-invocation-overview-grid-value">
            {this.props.model.targets.length}
          </div>
        </div>
        <div className="dense-invocation-overview-grid-chunk">
          <div className="dense-invocation-overview-grid-title">
            Broken
          </div>
          <div className="dense-invocation-overview-grid-value">
            {this.props.model.broken.length}
          </div>
        </div>
        <div className="dense-invocation-overview-grid-chunk">
          <div className="dense-invocation-overview-grid-title">
            Failed
          </div>
          <div className="dense-invocation-overview-grid-value">
            {this.props.model.failed.length}
          </div>
        </div>
        <div className="dense-invocation-overview-grid-chunk">
          <div className="dense-invocation-overview-grid-title">
            Flaky
          </div>
          <div className="dense-invocation-overview-grid-value">
            {this.props.model.flaky.length}
          </div>
        </div>
        <div className="dense-invocation-overview-grid-chunk">
          <div className="dense-invocation-overview-grid-title">
            Successful
          </div>
          <div className="dense-invocation-overview-grid-value">
            {this.props.model.succeeded.length}
          </div>
        </div>
      </div>
    </div>
  }
}
