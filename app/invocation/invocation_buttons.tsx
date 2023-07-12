import React from "react";
import { User } from "../auth/auth_service";
import InvocationCancelButton from "./invocation_cancel_button";
import InvocationCompareButton from "./invocation_compare_button";
import InvocationMenuComponent from "./invocation_menu";
import InvocationModel from "./invocation_model";
import InvocationShareButton from "./invocation_share_button";
import WorkflowRerunButton from "./workflow_rerun_button";
import SuggestionButton from "./suggestion_button";

export interface InvocationButtonsProps {
  model: InvocationModel;
  user?: User;
}

export default class InvocationButtons extends React.Component<InvocationButtonsProps> {
  state = {
    askLoading: false,
  };

  private canRerunWorkflow() {
    if (!this.props.user?.groups.some((group) => group.id === this.props.model.invocation.acl?.groupId)) {
      return false;
    }

    const repoUrl = this.props.model.getRepo();
    // This repo URL comes from the GitHub API, so no need to worry about
    // ssh or other URL formats.
    return repoUrl.startsWith("https://github.com/");
  }

  render() {
    const showCancelButton =
      (this.props.model.isWorkflowInvocation() || this.props.model.isHostedBazelInvocation()) &&
      this.props.model.isInProgress();
    const showRerunButton = !showCancelButton && this.props.model.isWorkflowInvocation() && this.canRerunWorkflow();

    return (
      <div className="invocation-top-right-buttons">
        {showRerunButton && <WorkflowRerunButton model={this.props.model} />}
        {showCancelButton && <InvocationCancelButton invocationId={this.props.model.getInvocationId()} />}
        <InvocationCompareButton invocationId={this.props.model.getInvocationId()} />

        <SuggestionButton user={this.props.user} model={this.props.model} />
        <InvocationShareButton
          user={this.props.user}
          model={this.props.model}
          invocationId={this.props.model.getInvocationId()}
        />
        <InvocationMenuComponent user={this.props.user} model={this.props.model} />
      </div>
    );
  }
}
