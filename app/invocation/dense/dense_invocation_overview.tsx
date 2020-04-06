import React from 'react';
import InvocationModel from '../invocation_model'
import router from '../../router/router';

interface Props {
  model: InvocationModel,
  invocationId: string
}
export default class DenseInvocationOverviewComponent extends React.Component {
  props: Props;

  handleUserClicked() {
    router.navigateToUserHistory(this.props.model.getUser());
  }

  handleHostClicked() {
    router.navigateToHostHistory(this.props.model.getHost());
  }

  render() {
    return <div className="container">
      <div className="dense-invocation">
        <div className="dense-invocation-title">Invocation</div>
        <div className="dense-invocation-invocation-id">{this.props.invocationId} ({this.props.model.getStartDate()}, {this.props.model.getStartTime()})</div>
      </div>
      <div className={this.props.model.getStatus() == "Succeeded" ? `dense-invocation-status-bar succeeded` : `dense-invocation-status-bar failed`}>
        <div>
          {this.props.model.targets.length} {this.props.model.targets.length == 1 ? "target" : "targets"} evaluated on&nbsp;
          {this.props.model.getStartDate()} at {this.props.model.getStartTime()} for <span title={this.props.model.getDurationSeconds()}>{this.props.model.getTiming()}</span>
        </div>
        <div>
          Evaluation started by <b onClick={this.handleUserClicked.bind(this)} className="clickable">{this.props.model.getUser()}</b> on <b onClick={this.handleHostClicked.bind(this)} className="clickable">{this.props.model.getHost()}</b>
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
