import { ExternalLink, GitPullRequest } from "lucide-react";
import React from "react";
import Button from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";
import InvocationModel from "../../../app/invocation/invocation_model";
import rpcService from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";

const PROPOSAL_MANIFEST_NAME = "ask-buildbuddy-proposal.json";
const PROPOSAL_PATCH_NAME = "ask-buildbuddy.patch";
const DOWNLOAD_DIRECTORY_NAME = "bb-download";

interface ProposalFile {
  content?: string;
  mode?: string;
  deleted?: boolean;
}

interface ProposalManifest {
  version: number;
  repository: string;
  baseCommit: string;
  baseBranch: string;
  patchArtifact: string;
  suggestedTitle: string;
  suggestedBody: string;
  files: Record<string, ProposalFile>;
}

interface Props {
  invocationId: string;
  model: InvocationModel;
}

interface State {
  proposal?: ProposalManifest;
  creatingPullRequest: boolean;
  pullRequestUrl?: string;
}

export default class AskBuildBuddyProposal extends React.Component<Props, State> {
  state: State = {
    creatingPullRequest: false,
  };

  private loading = false;

  componentDidMount() {
    this.loadProposal();
  }

  componentDidUpdate(previousProps: Props) {
    if (this.props.model !== previousProps.model) {
      this.loadProposal();
    }
  }

  private async loadProposal() {
    if (this.loading || this.state.proposal) return;
    this.loading = true;
    try {
      const response = await rpcService.service.getTarget({
        invocationId: this.props.invocationId,
        status: 0,
        filter: DOWNLOAD_DIRECTORY_NAME,
      });
      const artifacts = response.targetGroups.flatMap((group) => group.targets.flatMap((target) => target.files));
      const manifestArtifact = artifacts.find(
        (artifact) => artifact.name === `${DOWNLOAD_DIRECTORY_NAME}/${PROPOSAL_MANIFEST_NAME}`
      );
      const patchArtifact = artifacts.find(
        (artifact) => artifact.name === `${DOWNLOAD_DIRECTORY_NAME}/${PROPOSAL_PATCH_NAME}`
      );
      if (!manifestArtifact?.uri || !patchArtifact?.uri) return;

      const rawManifest = await rpcService.fetchBytestreamFile(manifestArtifact.uri, this.props.invocationId);
      const proposal = JSON.parse(rawManifest) as ProposalManifest;
      if (
        proposal.version !== 1 ||
        proposal.patchArtifact !== PROPOSAL_PATCH_NAME ||
        !proposal.repository ||
        !proposal.baseCommit ||
        !proposal.suggestedTitle ||
        !proposal.suggestedBody ||
        !proposal.files ||
        !Object.keys(proposal.files).length
      ) {
        console.error("Ignoring invalid Ask BuildBuddy proposal manifest.");
        return;
      }
      this.setState({ proposal });
    } catch (error) {
      // Artifacts may not exist until the runner command finishes. The parent
      // invocation component supplies a refreshed model as the run updates.
      console.debug("Ask BuildBuddy proposal is not available yet:", error);
    } finally {
      this.loading = false;
    }
  }

  private async createDraftPullRequest() {
    const proposal = this.state.proposal;
    if (!proposal) return;

    let files: github.GithubFileChange[];
    try {
      files = Object.entries(proposal.files).map(
        ([path, file]) =>
          new github.GithubFileChange({
            path,
            content: file.deleted ? undefined : decodeBase64(file.content || ""),
            mode: file.mode,
            deleted: file.deleted,
          })
      );
    } catch (error) {
      errorService.handleError(`Invalid pull request proposal: ${error}`);
      return;
    }

    this.setState({ creatingPullRequest: true });
    try {
      const response = await rpcService.service.createGithubInstallationDraftPull(
        new github.CreateGithubInstallationDraftPullRequest({
          repoUrl: proposal.repository,
          baseCommit: proposal.baseCommit,
          baseBranch: proposal.baseBranch,
          head: `buildbuddy/ask/${this.props.invocationId}`,
          title: proposal.suggestedTitle,
          body: proposal.suggestedBody,
          files,
        })
      );
      this.setState({ pullRequestUrl: response.url });
      window.location.href = response.url;
    } catch (error) {
      errorService.handleError(error);
    } finally {
      this.setState({ creatingPullRequest: false });
    }
  }

  render() {
    if (!this.state.proposal) return null;

    return (
      <div className="card ask-buildbuddy-proposal-card">
        <GitPullRequest />
        <div className="content">
          <div className="title">Proposed repository changes</div>
          <div className="details">
            Ask BuildBuddy produced a patch for {this.state.proposal.repository}. Review the patch in the Artifacts tab
            or create a draft pull request.
          </div>
          <div className="ask-buildbuddy-proposal-actions">
            {this.state.pullRequestUrl ? (
              <a className="base-button filled-button" href={this.state.pullRequestUrl} target="_blank">
                View draft PR <ExternalLink size={16} />
              </a>
            ) : (
              <Button disabled={this.state.creatingPullRequest} onClick={this.createDraftPullRequest.bind(this)}>
                {this.state.creatingPullRequest ? "Creating draft PR…" : "Create draft PR"}
              </Button>
            )}
          </div>
        </div>
      </div>
    );
  }
}

function decodeBase64(value: string): Uint8Array {
  const decoded = window.atob(value);
  return Uint8Array.from(decoded, (character) => character.charCodeAt(0));
}
