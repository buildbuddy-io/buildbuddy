import React from "react";
import { User } from "../../auth/auth_service";
import router from "../../router/router";
import InvocationButtons from "../invocation_buttons";
import InvocationModel from "../invocation_model";

interface Props {
  user?: User;
  model: InvocationModel;
}
export default class DenseInvocationOverviewComponent extends React.Component<Props> {
  handleUserClicked() {
    router.navigateToUserHistory(this.props.model.getUser());
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
              {this.props.model.getInvocationId()} ({this.props.model.getFormattedStartedDate()})
            </div>
          </div>
          <InvocationButtons model={this.props.model} user={this.props.user} />
        </div>
        <div className={`dense-invocation-status-bar ${this.props.model.getStatusClass()}`}>
          <div>
            <div>
              {isBazelInvocation ? (
                <>
                  {this.props.model.getTargetConfiguredCount()}{" "}
                  {this.props.model.getTargetConfiguredCount() == 1 ? "target" : "targets"}
                </>
              ) : (
                <>1 action</>
              )}{" "}
              on {this.props.model.getFormattedStartedDate()} for{" "}
              <span title={this.props.model.getDurationSeconds()}>{this.props.model.getTiming()}</span>
            </div>
          </div>
          {this.props.model.getUser() || this.props.model.getHost() ? (
            <div>
              Evaluation started{" "}
              {this.props.model.getUser() ? (
                <>
                  by{" "}
                  <b onClick={this.handleUserClicked.bind(this)} className="clickable">
                    {this.props.model.getUser()}
                  </b>{" "}
                </>
              ) : null}
              {this.props.model.getHost() ? (
                <>
                  on{" "}
                  <b onClick={this.handleHostClicked.bind(this)} className="clickable">
                    {this.props.model.getHost()}
                  </b>
                </>
              ) : null}
            </div>
          ) : null}
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
                <div className="dense-invocation-overview-grid-value">
                  {this.props.model.getTargetConfiguredCount()}
                </div>
              </div>
              <div className="dense-invocation-overview-grid-chunk">
                <div className="dense-invocation-overview-grid-title">Broken</div>
                <div className="dense-invocation-overview-grid-value">{this.props.model.getFailedToBuildCount()}</div>
              </div>
              <div className="dense-invocation-overview-grid-chunk">
                <div className="dense-invocation-overview-grid-title">Failed</div>
                <div className="dense-invocation-overview-grid-value">{this.props.model.getFailedCount()}</div>
              </div>
              <div className="dense-invocation-overview-grid-chunk">
                <div className="dense-invocation-overview-grid-title">Flaky</div>
                <div className="dense-invocation-overview-grid-value">{this.props.model.getFlakyCount()}</div>
              </div>
              <div className="dense-invocation-overview-grid-chunk">
                <div className="dense-invocation-overview-grid-title">Successful</div>
                <div className="dense-invocation-overview-grid-value">{this.props.model.getBuiltCount()}</div>
              </div>
            </>
          )}
        </div>
      </div>
    );
  }
}
