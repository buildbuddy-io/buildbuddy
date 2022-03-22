import React from "react";
import { User } from "../../auth/auth_service";
import router from "../../router/router";
import InvocationButtons from "../invocation_buttons";
import InvocationModel from "../invocation_model";

interface Props {
  user?: User;
  model: InvocationModel;
  invocationId: string;
}
export default class DenseInvocationOverviewComponent extends React.Component<Props> {
  handleUserClicked() {
    router.navigateToUserHistory(this.props.model.getUser(false));
  }

  handleHostClicked() {
    router.navigateToHostHistory(this.props.model.getHost());
  }

  render() {
    const isBazelInvocation = this.props.model.isBazelInvocation();

    return (
      <div className={`container nopadding-dense ${isBazelInvocation ? "" : "workflow-invocation"}`}>
        <div className="dense-invocation">
          <div>
            <div className="dense-invocation-title">
              {isBazelInvocation ? <>Invocation</> : <>Workflow invocation</>}
            </div>
            <div className="dense-invocation-invocation-id">
              {this.props.invocationId} ({this.props.model.getFormattedStartedDate()})
            </div>
          </div>
          <InvocationButtons invocationId={this.props.invocationId} model={this.props.model} user={this.props.user} />
        </div>
        <div className={`dense-invocation-status-bar ${this.props.model.getStatusClass()}`}>
          <div>
            <div>
              {isBazelInvocation ? (
                <>
                  {this.props.model.targets.length} {this.props.model.targets.length == 1 ? "target" : "targets"}
                </>
              ) : (
                <>1 action</>
              )}{" "}
              on {this.props.model.getFormattedStartedDate()} for{" "}
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
        <div className="dense-invocation-overview-grid">
          <div className="dense-invocation-overview-grid-chunk">
            <div className="dense-invocation-overview-grid-title">Invocation</div>
            <div className="dense-invocation-overview-grid-value">
              {this.props.model.getStatusIcon()}
              {this.props.model.getStatus()}
            </div>
          </div>
          {isBazelInvocation && (
            <>
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
            </>
          )}
        </div>
      </div>
    );
  }
}
