import FilledButton from "../components/button/button";
import { Bot, Timer } from "lucide-react";
import React from "react";
import { User } from "../auth/auth_service";
import InvocationCancelButton from "./invocation_cancel_button";
import InvocationCompareButton from "./invocation_compare_button";
import InvocationMenuComponent from "./invocation_menu";
import InvocationModel from "./invocation_model";
import InvocationShareButton from "./invocation_share_button";
import WorkflowRerunButton from "./workflow_rerun_button";
import capabilities from "../capabilities/capabilities";

export interface InvocationButtonsProps {
  model: InvocationModel;
  invocationId: string;
  user?: User;
}

export default class InvocationButtons extends React.Component<InvocationButtonsProps> {
  state = {
    askLoading: false,
  };

  private canRerunWorkflow() {
    if (!this.props.user?.groups.some((group) => group.id === this.props.model.invocations[0]?.acl?.groupId)) {
      return false;
    }

    const repoUrl = this.props.model.getRepo();
    // This repo URL comes from the GitHub API, so no need to worry about
    // ssh or other URL formats.
    return repoUrl.startsWith("https://github.com/");
  }

  private onAskClicked() {
    this.setState({ askLoading: true });
    return this.props.model.fetchSuggestions().finally(() => this.setState({ askLoading: false }));
  }

  render() {
    const showCancelButton =
      (this.props.model.isWorkflowInvocation() || this.props.model.isHostedBazelInvocation()) &&
      this.props.model.isInProgress();
    const showRerunButton = !showCancelButton && this.props.model.isWorkflowInvocation() && this.canRerunWorkflow();

    return (
      <div className="invocation-top-right-buttons">
        {showRerunButton && <WorkflowRerunButton model={this.props.model} />}
        {showCancelButton && <InvocationCancelButton invocationId={this.props.invocationId} />}
        <InvocationCompareButton invocationId={this.props.invocationId} />

        {capabilities.config.botSuggestionsEnabled && this.props.model.getStatus() == "Failed" && (
          <FilledButton
            disabled={this.state.askLoading}
            className="invocation-ask-button"
            onClick={this.onAskClicked.bind(this)}>
            {this.state.askLoading ? <Timer className="icon white" /> : <Bot className="icon white" />}
            {this.state.askLoading ? "Thinking..." : "Ask Buddy"}
          </FilledButton>
        )}
        <InvocationShareButton user={this.props.user} model={this.props.model} invocationId={this.props.invocationId} />
        <InvocationMenuComponent
          user={this.props.user}
          model={this.props.model}
          invocationId={this.props.invocationId}
        />
      </div>
    );
  }
}
