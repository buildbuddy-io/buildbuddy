import React from "react";
import { User } from "../../auth/auth_service";
import router from "../../router/router";
import InvocationModel from "../invocation_model";
import InvocationCompareButton from "../invocation_compare_button";
import InvocationShareButton from "../invocation_share_button";
import InvocationMenuComponent from "../invocation_menu";

interface Props {
  user?: User;
  model: InvocationModel;
  invocationId: string;
}
export default class DenseInvocationOverviewComponent extends React.Component {
  props: Props;

  handleUserClicked() {
    router.navigateToUserHistory(this.props.model.getUser(false));
  }

  handleHostClicked() {
    router.navigateToHostHistory(this.props.model.getHost());
  }

  render() {
    const isBazelInvocation = this.props.model.isBazelInvocation();

    return (
      <div className="container nopadding-dense">
        <div className="dense-invocation">
          <div>
            <div className="dense-invocation-title">Invocation</div>
            <div className="dense-invocation-invocation-id">
              {this.props.invocationId} ({this.props.model.getStartDate()}, {this.props.model.getStartTime()})
            </div>
          </div>
          <div className="invocation-top-right-buttons">
            {isBazelInvocation && <InvocationCompareButton invocationId={this.props.invocationId} />}
            <InvocationShareButton
              user={this.props.user}
              model={this.props.model}
              invocationId={this.props.invocationId}
            />
            <InvocationMenuComponent
              user={this.props.user}
              model={this.props.model}
              invocationId={this.props.invocationId}
            />
          </div>
        </div>
        <div
          className={`dense-invocation-status-bar ${this.props.model.getStatusClass()} ${
            !isBazelInvocation && "dense-ci-run-status-bar"
          }`}>
          <div className="dense-invocation-status-bar-left">
            {!isBazelInvocation && <div className="ci-run-status-icon">{this.props.model.getStatusIcon()}</div>}
            <div>
              {isBazelInvocation ? (
                <>
                  {this.props.model.targets.length} {this.props.model.targets.length == 1 ? "target" : "targets"}&nbsp;
                </>
              ) : (
                <>
                  <span className="ci-run-status-text">{this.props.model.getStatus()}</span> CI run{" "}
                </>
              )}
              on&nbsp;
              {this.props.model.getStartDate()} at {this.props.model.getStartTime()} for{" "}
              <span title={this.props.model.getDurationSeconds()}>{this.props.model.getTiming()}</span>
            </div>
          </div>
          <div>
            Evaluation started by{" "}
            <b onClick={this.handleUserClicked.bind(this)} className="clickable">
              {this.props.model.getUser(false)}
            </b>{" "}
            on{" "}
            <b onClick={this.handleHostClicked.bind(this)} className="clickable">
              {this.props.model.getHost()}
            </b>
          </div>
        </div>
        {isBazelInvocation && (
          <div className="dense-invocation-overview-grid">
            <div className="dense-invocation-overview-grid-chunk">
              <div className="dense-invocation-overview-grid-title">Invocation</div>
              <div className="dense-invocation-overview-grid-value">
                {this.props.model.getStatusIcon()}
                {this.props.model.getStatus()}
              </div>
            </div>
            <div className="dense-invocation-overview-grid-chunk">
              <div className="dense-invocation-overview-grid-title">Targets affected</div>
              <div className="dense-invocation-overview-grid-value">{this.props.model.targets.length}</div>
            </div>
            <div className="dense-invocation-overview-grid-chunk">
              <div className="dense-invocation-overview-grid-title">Broken</div>
              <div className="dense-invocation-overview-grid-value">{this.props.model.brokenTest.length}</div>
            </div>
            <div className="dense-invocation-overview-grid-chunk">
              <div className="dense-invocation-overview-grid-title">Failed</div>
              <div className="dense-invocation-overview-grid-value">
                {this.props.model.failed.length + this.props.model.failedTest.length}
              </div>
            </div>
            <div className="dense-invocation-overview-grid-chunk">
              <div className="dense-invocation-overview-grid-title">Flaky</div>
              <div className="dense-invocation-overview-grid-value">{this.props.model.flakyTest.length}</div>
            </div>
            <div className="dense-invocation-overview-grid-chunk">
              <div className="dense-invocation-overview-grid-title">Successful</div>
              <div className="dense-invocation-overview-grid-value">
                {this.props.model.succeeded.length - this.props.model.failedTest.length}
              </div>
            </div>
          </div>
        )}
      </div>
    );
  }
}
