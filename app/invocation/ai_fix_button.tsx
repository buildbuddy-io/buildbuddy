import React from "react";
import alertService from "../alert/alert_service";
import { User } from "../auth/user";
import capabilities from "../capabilities/capabilities";
import AIButton from "../components/button/ai_button";
import errorService from "../errors/error_service";
import { copyToClipboard } from "../util/clipboard";
import { RemoteRunnerAgent, supportsRemoteRun, triggerRemoteRun } from "../util/remote_runner";
import InvocationModel from "./invocation_model";

export interface AIFixButtonProps {
  model: InvocationModel;
  user: User | undefined;
}

export default class AIFixButton extends React.Component<AIFixButtonProps> {
  private getCommand(agent: RemoteRunnerAgent, push: boolean) {
    return `bb agent fix --agent=${agent}${push ? " --push" : ""} ${this.props.model.getInvocationId()}`;
  }

  private async fixWithAI(agent: RemoteRunnerAgent) {
    try {
      // `bb agent fix` doesn't require a checked out repo, so if the repo isn't
      // linked, skip the checkout rather than requiring workflows to be set up.
      const repoURL = this.props.model.getRepo();
      const canCheckout = Boolean(repoURL) && (await supportsRemoteRun(repoURL));
      await triggerRemoteRun(
        this.props.model,
        this.getCommand(agent, canCheckout),
        true,
        new Map<string, string>([
          ["EstimatedComputeUnits", "3"],
          ["env-secrets", agent === "claude" ? "ANTHROPIC_API_KEY" : "CODEX_API_KEY"],
        ]),
        canCheckout ? [] : ["--skip_auto_checkout=true"],
        "agent fix",
        agent
      );
    } catch (e) {
      errorService.handleError(e);
    }
  }

  private copyCommand(agent: RemoteRunnerAgent) {
    try {
      copyToClipboard(this.getCommand(agent, false));
      alertService.success("Copied command to clipboard");
    } catch (e) {
      errorService.handleError(e);
    }
  }

  render() {
    if (
      !capabilities.config.botSuggestionsEnabled ||
      !this.props.user ||
      !this.props.user.selectedGroup?.botSuggestionsEnabled ||
      !this.props.model.isFailed()
    ) {
      return <></>;
    }

    return (
      <AIButton
        label="Fix"
        loadingLabel="Starting..."
        onClick={this.fixWithAI.bind(this)}
        onCopyCommand={this.copyCommand.bind(this)}
        docsUrl="https://www.buildbuddy.io/docs/cli-commands#bb-agent"
      />
    );
  }
}
