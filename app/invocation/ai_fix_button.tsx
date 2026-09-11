import React from "react";
import alertService from "../alert/alert_service";
import { User } from "../auth/user";
import capabilities from "../capabilities/capabilities";
import AIButton from "../components/button/ai_button";
import Button from "../components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../components/dialog/dialog";
import { TextLink } from "../components/link/link";
import Modal from "../components/modal/modal";
import errorService from "../errors/error_service";
import { copyToClipboard } from "../util/clipboard";
import { RemoteRunnerAgent, supportsRemoteRun, triggerRemoteRun } from "../util/remote_runner";
import InvocationModel from "./invocation_model";
import LinkGithubRepoModal from "./link_github_repo_modal";

export interface AIFixButtonProps {
  model: InvocationModel;
  user: User | undefined;
}

type State = {
  isDialogOpen: boolean;
  isLinkRepoModalOpen: boolean;
};

export default class AIFixButton extends React.Component<AIFixButtonProps, State> {
  state: State = {
    isDialogOpen: false,
    isLinkRepoModalOpen: false,
  };

  private getCommand(agent: RemoteRunnerAgent) {
    return `bb agent fix --agent=${agent} ${this.props.model.getInvocationId()}`;
  }

  private async fixWithAI(agent: RemoteRunnerAgent) {
    try {
      const repoURL = this.props.model.getRepo();
      if (!repoURL) {
        alertService.error("A repo URL is required.");
        return;
      }
      if (!(await supportsRemoteRun(repoURL))) {
        this.setState({ isLinkRepoModalOpen: true });
        return;
      }
      await triggerRemoteRun(
        this.props.model,
        this.getCommand(agent),
        true,
        new Map<string, string>([
          ["EstimatedComputeUnits", "3"],
          ["env-secrets", agent === "claude" ? "ANTHROPIC_API_KEY" : "CODEX_API_KEY"],
        ]),
        [],
        "agent fix",
        agent
      );
    } catch (e) {
      errorService.handleError(e);
    }
  }

  private copyCommand(agent: RemoteRunnerAgent) {
    try {
      copyToClipboard(this.getCommand(agent));
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
      <>
        <AIButton
          label="Fix"
          loadingLabel="Starting..."
          onClick={this.fixWithAI.bind(this)}
          onCopyCommand={this.copyCommand.bind(this)}
          onInfoClick={() => this.setState({ isDialogOpen: true })}
        />
        <Modal isOpen={this.state.isDialogOpen} onRequestClose={() => this.setState({ isDialogOpen: false })}>
          <Dialog>
            <DialogHeader>
              <DialogTitle>Fix this build with AI</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <p>
                <span className="inline-code">bb agent fix</span> uses AI to reproduce the failure, edit the working
                tree with a fix, and verify the fix.
              </p>
              <p>
                <b>Run from the UI</b>
                <br />
                When triggered from the UI, the command will be run on a remote runner. The generated patch will be
                uploaded to the invocation under the 'Artifacts' tab. The runner requires an{" "}
                <TextLink href="/settings/org/secrets" target="_blank">
                  agent API-key stored as a BuildBuddy secret
                </TextLink>
                .
              </p>
              <p>
                <b>Run locally</b>
                <br />
                The adjacent menu has a button to copy the command to run locally. On your local machine, the command
                can use your existing agent sign-in.
              </p>
              <p>
                Relevant profile data is sent to the AI provider, and provider charges may apply.{" "}
                <TextLink href="https://www.buildbuddy.io/docs/cli-commands#bb-agent" target="_blank">
                  See the docs for more info.
                </TextLink>
              </p>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                <Button onClick={() => this.setState({ isDialogOpen: false })}>Done</Button>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
        <LinkGithubRepoModal
          isOpen={this.state.isLinkRepoModalOpen}
          onRequestClose={() => this.setState({ isLinkRepoModalOpen: false })}
        />
      </>
    );
  }
}
