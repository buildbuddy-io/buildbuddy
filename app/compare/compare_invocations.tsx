import { GitCompare, XCircle } from "lucide-react";
import React from "react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import alert_service from "../alert/alert_service";
import { User } from "../auth/auth_service";
import Button from "../components/button/button";
import CheckboxButton from "../components/button/checkbox_button";
import InvocationModel from "../invocation/invocation_model";
import rpcService from "../service/rpc_service";
import { renderComparisonFacets } from "../util/diff";
import { BuildBuddyError } from "../util/errors";
import { triggerRemoteRun } from "../util/remote_runner";
import CompareExecutionLogFilesComponent from "./compare_execution_log_files";
import CompareExecutionLogSpawnsComponent from "./compare_execution_log_spawns";

export interface CompareInvocationsComponentProps {
  user?: User;
  tab: string;
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
  isRunningExplain: boolean;
}

const INITIAL_STATE: State = {
  status: "INIT",
  error: null,
  showChangesOnly: false,
  isRunningExplain: false,
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
  { name: "Pull request", facet: (i?: InvocationModel) => `${i?.getPullRequestNumber()}` },
  { name: "Instance name", facet: (i?: InvocationModel) => i?.getRemoteInstanceName() || "<default>" },
  { name: "Forked repo URL", facet: (i?: InvocationModel) => i?.getForkRepoURL() },
  { name: "Repo URL", facet: (i?: InvocationModel) => i?.getRepo() },
  { name: "Commit SHA", facet: (i?: InvocationModel) => i?.getCommit() },
  { name: "Branch", facet: (i?: InvocationModel) => i?.getBranchName() },
  { name: "Role", facet: (i?: InvocationModel) => i?.getRole() },
  { name: "Status", facet: (i?: InvocationModel) => i?.getStatus() },
  {
    name: "Tags",
    facet: (i?: InvocationModel) =>
      i
        ?.getTags()
        .map((t) => t.name)
        .join("\n"),
  },
  { name: "Fetch count", facet: (i?: InvocationModel) => `${i?.getFetchURLs().length}` },
  {
    name: "Explicit command line",
    facet: (i?: InvocationModel) => i?.getExplicitCommandLineOptions().join("\n"),
    type: "flag",
  },
  {
    name: "Full command line",
    facet: (i?: InvocationModel) => i?.getEffectiveCommandLineOptions().join("\n"),
    type: "flag",
  },
  {
    name: "Explicit startup options",
    facet: (i?: InvocationModel) => i?.getExplicitStartupOptions().join("\n"),
    type: "flag",
  },
  {
    name: "Full startup options",
    facet: (i?: InvocationModel) => i?.getEffectiveStartupOptions().join("\n"),
    type: "flag",
  },
  {
    name: "Invocation policy",
    facet: (i?: InvocationModel) => i?.optionsParsed?.invocationPolicy?.flagPolicies.join("\n"),
  },
  { name: "Attempt count", facet: (i?: InvocationModel) => `${i?.getAttempt()}` },
  { name: "Target count", facet: (i?: InvocationModel) => `${i?.getTargetConfiguredCount()}` },
  { name: "Success count", facet: (i?: InvocationModel) => `${i?.getBuiltCount()}` },
  { name: "Build failure count", facet: (i?: InvocationModel) => `${i?.getFailedToBuildCount()}` },
  { name: "Failure count", facet: (i?: InvocationModel) => `${i?.getFailedCount()}` },
  { name: "Flaky count", facet: (i?: InvocationModel) => `${i?.getFlakyCount()}` },
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

  private hasExecLog(invocation: InvocationModel): boolean {
    return Boolean(
      invocation.buildToolLogs?.log.some(
        (log: build_event_stream.File) =>
          log.name == "execution_log.binpb.zst" && log.uri && Boolean(log.uri.startsWith("bytestream://"))
      )
    );
  }

  private async onClickRunExplain() {
    const { modelA, modelB } = this.state;
    if (!modelA || !modelB) return;

    this.setState({ isRunningExplain: true });

    try {
      // Check if either invocation has a repo URL
      if (!this.hasExecLog(modelA) || !this.hasExecLog(modelB)) {
        alert_service.error(
          "Both invocations must have a compact execution log to run bb explain. Re-run the invocations with the `--execution_log_compact_file=` flag enabled."
        );
        return;
      }

      const command = `
curl -fsSL https://raw.githubusercontent.com/buildbuddy-io/buildbuddy/master/cli/install.sh  | bash
output=$(bb explain --old ${modelB.getInvocationId()} --new ${modelA.getInvocationId()} --target ${modelA.getCacheAddress()} --verbose)
if [ -z "$output" ]; then
    echo "There are no differences between the compact execution logs of the two invocations."
else
  printf "%s\\n" "$output"
fi
`;

      let platformProps = new Map([["EstimatedComputeUnits", "3"]]);
      triggerRemoteRun(modelA, command, false /*autoOpenChild*/, platformProps, ["--skip_auto_checkout=true"]);
    } catch (error) {
      console.error("Error running bb explain:", error);
      alert_service.error("Failed to run bb explain: " + error);
    } finally {
      this.setState({ isRunningExplain: false });
    }
  }

  render() {
    const { status, error } = this.state;

    if (status === "LOADING" || status === "INIT") {
      return <div className="loading" />;
    }

    if (status == "ERROR") {
      return (
        <div className="compare-invocations container">
          <div className="error-container">
            <XCircle className="icon red" />
            <div>{error}</div>
          </div>
        </div>
      );
    }

    return (
      <div className="compare-invocations">
        <div className="shelf nopadding-dense">
          <header className="container header">
            <h2 className="title">Comparing invocations</h2>
            <div className="header-buttons">
              <Button
                className="bb-explain-button"
                onClick={this.onClickRunExplain.bind(this)}
                disabled={this.state.isRunningExplain}>
                <GitCompare className="icon" />
                {this.state.isRunningExplain ? "Running..." : "Run bb explain"}
              </Button>
              {this.props.tab != "#file" && this.props.tab != "#spawn" && (
                <CheckboxButton
                  className="show-changes-only-button"
                  onChange={this.onClickShowChangesOnly.bind(this)}
                  checked={this.state.showChangesOnly}>
                  Show changes only
                </CheckboxButton>
              )}
            </div>
          </header>
          <div className="container">
            <div className="tabs">
              <a href="#" className={`tab ${!this.props.tab ? "selected" : ""}`}>
                Details
              </a>
              <a href="#flag" className={`tab ${this.props.tab == "#flag" ? "selected" : ""}`}>
                Flags
              </a>
              <a href="#file" className={`tab ${this.props.tab == "#file" ? "selected" : ""}`}>
                Files
              </a>
              <a href="#spawn" className={`tab ${this.props.tab == "#spawn" ? "selected" : ""}`}>
                Spawns
              </a>
            </div>
          </div>
        </div>
        <div className="compare-table">
          {renderComparisonFacets(FACETS, this.state.modelA, this.state.modelB, {
            showChangesOnly: this.state.showChangesOnly,
            filterType: this.props.tab?.substring(1),
          })}
        </div>
        {this.props.tab == "#file" && (
          <div className="container">
            {(!this.state.modelA?.getIsExecutionLogEnabled() || !this.state.modelB?.getIsExecutionLogEnabled()) && (
              <div>
                In order to compare files, both invocation must have the execution log enabled with the
                `--execution_log_compact_file=` flag.
              </div>
            )}
            {this.state.modelA?.getIsExecutionLogEnabled() && this.state.modelB?.getIsExecutionLogEnabled() && (
              <CompareExecutionLogFilesComponent
                modelA={this.state.modelA}
                modelB={this.state.modelB}
                search={this.props.search}
                filter={""}
              />
            )}
          </div>
        )}
        {this.props.tab == "#spawn" && (
          <div className="container">
            {(!this.state.modelA?.getIsExecutionLogEnabled() || !this.state.modelB?.getIsExecutionLogEnabled()) && (
              <div>
                In order to compare spawns, both invocation must have the execution log enabled with the
                `--execution_log_compact_file=` flag.
              </div>
            )}
            {this.state.modelA?.getIsExecutionLogEnabled() && this.state.modelB?.getIsExecutionLogEnabled() && (
              <CompareExecutionLogSpawnsComponent
                modelA={this.state.modelA}
                modelB={this.state.modelB}
                search={this.props.search}
                filter={""}
              />
            )}
          </div>
        )}
      </div>
    );
  }
}
