import DiffMatchPatch from "diff-match-patch";
import { XCircle } from "lucide-react";
import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import CheckboxButton from "../components/button/checkbox_button";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import { BuildBuddyError } from "../util/errors";
import { OutlinedLinkButton } from "../components/button/link_button";
import InvocationModel from "../invocation/invocation_model";

export interface CompareInvocationsComponentProps {
  user?: User;
  search: URLSearchParams;
  invocationAId: string;
  invocationBId: string;
}

type Status = "INIT" | "LOADING" | "LOADED" | "ERROR";

interface State {
  status?: Status;
  error?: string | null;

  modelA?: InvocationModel;
  modelB?: InvocationModel;

  showChangesOnly: boolean;
}

const INITIAL_STATE: State = {
  status: "INIT",
  error: null,
  showChangesOnly: false,
};

const FACETS = [
  {
    name: "Invocation ID",
    facet: (i?: InvocationModel) => i?.getInvocationId(),
    link: (i?: InvocationModel) => `/invocation/${i?.getInvocationId()}`,
  },
  { name: "Command", facet: (i?: InvocationModel) => i?.getCommand() },
  { name: "Pattern", facet: (i?: InvocationModel) => i?.getPattern() },
  { name: "Exit code", facet: (i?: InvocationModel) => i?.invocation.bazelExitCode.toLowerCase() },
  { name: "Start date", facet: (i?: InvocationModel) => i?.getFormattedStartedDate() },
  { name: "Duration", facet: (i?: InvocationModel) => i?.getHumanReadableDuration() },
  { name: "Host", facet: (i?: InvocationModel) => i?.getHost() },
  { name: "Tool", facet: (i?: InvocationModel) => i?.getTool() },
  { name: "Mode", facet: (i?: InvocationModel) => i?.getMode() },
  { name: "CPU", facet: (i?: InvocationModel) => i?.getCPU() },
  { name: "Cache", facet: (i?: InvocationModel) => i?.getCache() },
  { name: "Remote execution", facet: (i?: InvocationModel) => i?.getRBE() },
  {
    name: "Compression",
    facet: (i?: InvocationModel) => (i?.isCacheCompressionEnabled() ? "Enabled" : "Disabled"),
  },
  { name: "Digest function", facet: (i?: InvocationModel) => i?.optionsMap.get("digest_function")?.toLowerCase() },
  { name: "Pull request", facet: (i?: InvocationModel) => i?.getPullRequestNumber() },
  { name: "Instance name", facet: (i?: InvocationModel) => i?.getRemoteInstanceName() || "<default>" },
  { name: "Forked repo URL", facet: (i?: InvocationModel) => i?.getForkRepoURL() },
  { name: "Repo URL", facet: (i?: InvocationModel) => i?.getRepo() },
  { name: "Commit SHA", facet: (i?: InvocationModel) => i?.getCommit() },
  { name: "Branch", facet: (i?: InvocationModel) => i?.getBranchName() },
  { name: "Role", facet: (i?: InvocationModel) => i?.getRole() },
  { name: "Status", facet: (i?: InvocationModel) => i?.getStatus() },
  { name: "Tags", facet: (i?: InvocationModel) => i?.getTags().join("\n") },
  { name: "Fetch count", facet: (i?: InvocationModel) => i?.getFetchURLs().length },
  { name: "Explicit command line", facet: (i?: InvocationModel) => i?.optionsParsed?.explicitCmdLine.join("\n") },
  { name: "Full command line", facet: (i?: InvocationModel) => i?.optionsParsed?.cmdLine.join("\n") },
  {
    name: "Explicit startup options",
    facet: (i?: InvocationModel) => i?.optionsParsed?.explicitStartupOptions.join("\n"),
  },
  { name: "Full startup options", facet: (i?: InvocationModel) => i?.optionsParsed?.startupOptions.join("\n") },
  {
    name: "Invocation policy",
    facet: (i?: InvocationModel) => i?.optionsParsed?.invocationPolicy?.flagPolicies.join("\n"),
  },
  { name: "Attempt count", facet: (i?: InvocationModel) => i?.getAttempt() },
  { name: "Target count", facet: (i?: InvocationModel) => i?.getTargetConfiguredCount() },
  { name: "Success count", facet: (i?: InvocationModel) => i?.getBuiltCount() },
  { name: "Build failure count", facet: (i?: InvocationModel) => i?.getFailedToBuildCount() },
  { name: "Failure count", facet: (i?: InvocationModel) => i?.getFailedCount() },
  { name: "Flaky count", facet: (i?: InvocationModel) => i?.getFlakyCount() },
  { name: "Tool tag", facet: (i?: InvocationModel) => i?.getToolTag() },
  { name: "GKE Cluster", facet: (i?: InvocationModel) => i?.getGKECluster() },
  { name: "GKE Project", facet: (i?: InvocationModel) => i?.getGKEProject() },
  { name: "Buildkite URL", facet: (i?: InvocationModel) => i?.getBuildkiteUrl() },
  {
    name: "Cache writes",
    facet: (i?: InvocationModel) => (i?.hasCacheWriteCapability() ? "Allowed" : "Not allowed"),
  },
];
export default class CompareInvocationsComponent extends React.Component<CompareInvocationsComponentProps, State> {
  state: State = INITIAL_STATE;

  componentDidMount() {
    this.fetchInvocations();
  }

  componentDidUpdate(prevProps: CompareInvocationsComponentProps) {
    if (
      prevProps.user !== this.props.user ||
      prevProps.invocationAId !== this.props.invocationAId ||
      prevProps.invocationBId !== this.props.invocationBId
    ) {
      this.setState(INITIAL_STATE);
      this.fetchInvocations();
    }
  }

  private async fetchInvocations() {
    const { invocationAId, invocationBId } = this.props;

    let error: any;
    let invocationA: invocation.Invocation, invocationB: invocation.Invocation;
    try {
      [invocationA, invocationB] = await Promise.all([
        this.fetchInvocation(invocationAId),
        this.fetchInvocation(invocationBId),
      ]);
    } catch (e) {
      error = e;
    }
    // Don't tell the user about an error if they've already moved on anyway.
    if (invocationAId !== this.props.invocationAId || invocationBId !== this.props.invocationBId) {
      return;
    }

    if (error) {
      console.error(error);
      this.setState({ status: "ERROR", error: BuildBuddyError.parse(error).description });
      return;
    }

    this.setState({
      status: "LOADED",
      modelA: new InvocationModel(invocationA!),
      modelB: new InvocationModel(invocationB!),
      error: null,
    });
  }

  private async fetchInvocation(invocationId: string): Promise<invocation.Invocation> {
    const response = await rpcService.service.getInvocation(
      new invocation.GetInvocationRequest({
        lookup: new invocation.InvocationLookup({
          invocationId,
        }),
      })
    );
    return response.invocation[0];
  }

  private onClickShowChangesOnly() {
    this.setState({ showChangesOnly: !this.state.showChangesOnly });
  }

  render() {
    const { status, error } = this.state;

    if (status === "LOADING" || status === "INIT") {
      return <div className="loading" />;
    }

    if (status == "ERROR") {
      return (
        <div className="error-container">
          <XCircle className="icon red" />
          <div>{error}</div>
        </div>
      );
    }

    return (
      <div className="compare-invocations">
        <div className="shelf nopadding-dense">
          <header className="container header">
            <h2 className="title">Comparing invocations</h2>
            <CheckboxButton
              className="show-changes-only-button"
              onChange={this.onClickShowChangesOnly.bind(this)}
              checked={this.state.showChangesOnly}>
              Show changes only
            </CheckboxButton>
          </header>
        </div>
        <div className="compare-table">
          {FACETS.map((f) => {
            let facetA = f.facet(this.state.modelA);
            let facetB = f.facet(this.state.modelB);

            let different = facetA != facetB;

            if (!different && this.state.showChangesOnly) {
              return <></>;
            }

            if (!facetA && !facetB) {
              return <></>;
            }

            let diffs: DiffMatchPatch.Diff[] = [];
            if (different) {
              diffs = computeDiffs(`${facetA}`, `${facetB}`);
            }

            return (
              <div className={`compare-row ${different && "different"}`}>
                <div>{f.name}</div>
                <div className={`${f.link && "link"}`}>
                  <a target="_blank" href={f.link && f.link(this.state.modelA)}>
                    {different
                      ? diffs.map((d) => {
                          if (d[0] == -1) {
                            return <span className="difference-left">{d[1]}</span>;
                          }
                          if (d[0] == 0) {
                            return <>{d[1]}</>;
                          }
                          return <></>;
                        })
                      : facetA}
                  </a>
                </div>
                <div className={`${f.link && "link"}`}>
                  <a target="_blank" href={f.link && f.link(this.state.modelB)}>
                    {different
                      ? diffs.map((d) => {
                          if (d[0] == 1) {
                            return <span className="difference-right">{d[1]}</span>;
                          }
                          if (d[0] == 0) {
                            return <>{d[1]}</>;
                          }
                          return <></>;
                        })
                      : facetB}
                  </a>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  }
}

function InvocationIdTag({ prefix, id }: { prefix: string; id: string }) {
  const href = `/invocation/${id}`;
  return (
    <OutlinedLinkButton className="invocation-id-tag" href={href}>
      <div className="invocation-id-tag-prefix">{prefix}:</div> <div className="invocation-id-tag-id">{id}</div>
    </OutlinedLinkButton>
  );
}

const dmp = new DiffMatchPatch.diff_match_patch();

function computeDiffs(text1: string, text2: string) {
  let diffs = dmp.diff_main(text1, text2);
  dmp.diff_cleanupSemantic(diffs);
  return diffs;
}
