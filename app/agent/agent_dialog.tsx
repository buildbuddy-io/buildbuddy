import React from "react";
import { git } from "../../proto/git_ts_proto";
import { github } from "../../proto/github_ts_proto";
import { runner } from "../../proto/runner_ts_proto";
import SimpleModalDialog from "../components/dialog/simple_modal_dialog";
import errorService from "../errors/error_service";
import rpcService from "../service/rpc_service";

interface State {
  isOpen: boolean;
  question: string;
  loading: boolean;
}

export default class AgentDialog extends React.Component<{}, State> {
  state: State = { isOpen: false, question: "", loading: false };

  open() {
    this.setState({ isOpen: true, question: "", loading: false });
  }

  private close() {
    if (this.state.loading) return;
    this.setState({ isOpen: false });
  }

  private async onSubmit() {
    const { question } = this.state;
    if (!question.trim()) return;

    this.setState({ loading: true });
    try {
      const reposRsp = await rpcService.service.getLinkedGitHubRepos(new github.GetLinkedReposRequest());
      const repoUrl = reposRsp.repos[0]?.repoUrl ?? "";

      const request = new runner.RunRequest({
        gitRepo: new git.GitRepo({ repoUrl }),
        steps: [new runner.Step({ run: 'tools/run_agent.sh -q "$AGENT_QUESTION"' })],
        env: { AGENT_QUESTION: question },
        remoteHeaders: ["x-buildbuddy-platform.secret-env-overrides=ANTHROPIC_API_KEY"],
        async: true,
        runRemotely: true,
      });

      const response = await rpcService.service.run(request);
      window.open(`/invocation/${response.invocationId}?queued=true`, "_blank");
      this.close();
    } catch (e) {
      errorService.handleError(e);
      this.setState({ loading: false });
    }
  }

  render() {
    return (
      <SimpleModalDialog
        title="Ask the agent"
        isOpen={this.state.isOpen}
        onRequestClose={this.close.bind(this)}
        onSubmit={this.onSubmit.bind(this)}
        submitLabel="Run"
        loading={this.state.loading}
        submitDisabled={!this.state.question.trim()}>
        <textarea
          autoFocus
          placeholder="Why did this build have such high cache download?"
          value={this.state.question}
          onChange={(e) => this.setState({ question: e.target.value })}
          rows={4}
          style={{ width: "100%", resize: "vertical" }}
        />
      </SimpleModalDialog>
    );
  }
}
