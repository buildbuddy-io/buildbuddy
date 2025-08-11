import { XCircle } from "lucide-react";
import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { User } from "../auth/auth_service";
import CheckboxButton from "../components/button/checkbox_button";
import InvocationModel from "../invocation/invocation_model";
import rpcService from "../service/rpc_service";
import { parseActionDigest } from "../util/cache";
import { renderComparisonFacets } from "../util/diff";
import { BuildBuddyError } from "../util/errors";

export interface CompareActionsComponentProps {
  user?: User;
  tab: string;
  search: URLSearchParams;
  invocationAId: string;
  invocationBId: string;
  actionADigest: string;
  actionBDigest: string;
}

type Status = "INIT" | "LOADING" | "LOADED" | "ERROR";

interface ActionDetails {
  invocationId: string;
  actionDigest: string;
  invocationModel?: InvocationModel;
  action?: build.bazel.remote.execution.v2.Action;
  command?: build.bazel.remote.execution.v2.Command;
}

interface State {
  status?: Status;
  error?: string | null;

  actionA?: ActionDetails;
  actionB?: ActionDetails;

  showChangesOnly: boolean;
}

const INITIAL_STATE: State = {
  status: "INIT",
  error: null,
  showChangesOnly: false,
};

const FACETS = [
  // Action Details
  {
    name: "Invocation ID",
    facet: (a?: ActionDetails) => a?.invocationId,
    link: (a?: ActionDetails) => `/invocation/${a?.invocationId}`,
  },
  {
    name: "Action Digest",
    facet: (a?: ActionDetails) => a?.actionDigest,
    link: (a?: ActionDetails) => `/invocation/${a?.invocationId}?actionDigest=${a?.actionDigest}#action`,
  },
  {
    name: "Input Root Digest",
    facet: (a?: ActionDetails) => a?.action?.inputRootDigest?.hash,
  },
  {
    name: "Cacheable",
    facet: (a?: ActionDetails) => (a?.action?.doNotCache ? "No" : "Yes"),
  },
  {
    name: "Timeout",
    facet: (a?: ActionDetails) => (a?.action?.timeout ? `${a.action.timeout.seconds}s` : "None"),
  },
  // Command Details
  {
    name: "Command",
    facet: (a?: ActionDetails) => a?.command?.arguments?.join(" "),
  },
  {
    name: "Environment variables",
    facet: (a?: ActionDetails) => a?.command?.environmentVariables?.map((e: any) => `${e.name}=${e.value}`).join(", "),
  },
  {
    name: "Platform properties",
    facet: (a?: ActionDetails) => a?.command?.platform?.properties?.map((p: any) => `${p.name}=${p.value}`).join(", "),
  },
];

// TODO: add support for diffing input root digests
// TODO: add support for diffing action results
// TODO: add support for diffing logs

export default class CompareActionsComponent extends React.Component<CompareActionsComponentProps, State> {
  state: State = INITIAL_STATE;

  componentDidMount() {
    this.fetchActions();
  }

  componentDidUpdate(prevProps: CompareActionsComponentProps) {
    if (
      prevProps.user !== this.props.user ||
      prevProps.invocationAId !== this.props.invocationAId ||
      prevProps.invocationBId !== this.props.invocationBId ||
      prevProps.actionADigest !== this.props.actionADigest ||
      prevProps.actionBDigest !== this.props.actionBDigest
    ) {
      this.setState(INITIAL_STATE);
      this.fetchActions();
    }
  }

  private async fetchActions() {
    const { invocationAId, invocationBId, actionADigest, actionBDigest } = this.props;

    this.setState({ status: "LOADING" });

    let error: any;
    let actionA: ActionDetails | undefined, actionB: ActionDetails | undefined;
    try {
      [actionA, actionB] = await Promise.all([
        this.fetchAction(invocationAId, actionADigest),
        this.fetchAction(invocationBId, actionBDigest),
      ]);
    } catch (e) {
      error = e;
    }

    // Don't tell the user about an error if they've already moved on anyway.
    if (
      invocationAId !== this.props.invocationAId ||
      invocationBId !== this.props.invocationBId ||
      actionADigest !== this.props.actionADigest ||
      actionBDigest !== this.props.actionBDigest
    ) {
      return;
    }

    if (error) {
      console.error(error);
      this.setState({ status: "ERROR", error: BuildBuddyError.parse(error).description });
      return;
    }

    this.setState({
      status: "LOADED",
      actionA,
      actionB,
      error: null,
    });
  }

  private async fetchAction(invocationId: string, actionDigestStr: string): Promise<ActionDetails> {
    const details: ActionDetails = {
      invocationId,
      actionDigest: actionDigestStr,
    };

    const digest = parseActionDigest(actionDigestStr);
    if (!digest) {
      throw new Error(`Invalid action digest: ${actionDigestStr}`);
    }

    try {
      const response = await rpcService.service.getInvocation(
        new invocation.GetInvocationRequest({
          lookup: new invocation.InvocationLookup({
            invocationId: invocationId,
          }),
        })
      );
      if (response.invocation && response.invocation.length > 0) {
        details.invocationModel = new InvocationModel(response.invocation[0]);
      }
    } catch (e) {
      console.error(`Failed to fetch invocation for ${invocationId}:`, e);
    }

    try {
      const actionUrl = details.invocationModel?.getBytestreamURL(digest);
      if (actionUrl) {
        const buffer = await rpcService.fetchBytestreamFile(actionUrl, invocationId, "arraybuffer");
        details.action = build.bazel.remote.execution.v2.Action.decode(new Uint8Array(buffer));
      }

      if (details.action?.commandDigest) {
        const commandUrl = details.invocationModel?.getBytestreamURL(details.action?.commandDigest);
        if (commandUrl) {
          const cmdBuffer = await rpcService.fetchBytestreamFile(commandUrl, invocationId, "arraybuffer");
          details.command = build.bazel.remote.execution.v2.Command.decode(new Uint8Array(cmdBuffer));
        }
      }
    } catch (e) {
      console.error(`Failed to fetch action details for ${actionDigestStr}:`, e);
    }

    return details;
  }

  private onClickShowChangesOnly() {
    this.setState({ showChangesOnly: !this.state.showChangesOnly });
  }

  render() {
    const { status, error } = this.state;

    if (status === "LOADING" || status === "INIT") {
      return <div className="loading" />;
    }

    if (status === "ERROR") {
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
            <h2 className="title">Comparing actions</h2>
            <CheckboxButton
              className="show-changes-only-button"
              onChange={this.onClickShowChangesOnly.bind(this)}
              checked={this.state.showChangesOnly}>
              Show changes only
            </CheckboxButton>
          </header>
          <div className="container">
            <div className="tabs">
              <a href="#" className={`tab ${!this.props.tab ? "selected" : ""}`}>
                Details
              </a>
            </div>
          </div>
        </div>

        <div className="compare-table">
          {renderComparisonFacets(FACETS, this.state.actionA, this.state.actionB, {
            showChangesOnly: this.state.showChangesOnly,
            filterType: this.props.tab?.substring(1),
          })}
        </div>
      </div>
    );
  }
}
