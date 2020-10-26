import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import { OutlinedButton } from "../components/button/button";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import { parseError } from "../util/errors";
import JsDiff from "diff";
import DiffChunk from "./diff_chunk";

export interface CompareInvocationsComponentProps {
  user?: User;
  invocationAId: string;
  invocationBId: string;
}

type Status = "INIT" | "LOADING" | "LOADED" | "ERROR";

type InvocationDiff = JsDiff.Change[];

interface State {
  status?: Status;
  error?: string | null;
  invocationA?: invocation.IInvocation | null;
  invocationB?: invocation.IInvocation | null;
  diff?: InvocationDiff | null;
  showChangesOnly: boolean;
}

const INITIAL_STATE: State = {
  status: "INIT",
  error: null,
  invocationA: null,
  invocationB: null,
  showChangesOnly: true,
};

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

    try {
      const [a, b] = await Promise.all([this.fetchInvocation(invocationAId), this.fetchInvocation(invocationBId)]);
      console.log("Comparing invocations", { a, b });
      this.setState({ status: "LOADED", invocationA: a, invocationB: b, diff: this.computeDiff(a, b) });
    } catch (e) {
      this.setState({ status: "ERROR", error: parseError(e).description });
    }
  }

  private computeDiff(invocationA: invocation.IInvocation, invocationB: invocation.IInvocation) {
    const aJson = JSON.stringify(invocationA, null, 2);
    const bJson = JSON.stringify(invocationB, null, 2);

    return JsDiff.diffLines(aJson, bJson);
  }

  private async fetchInvocation(invocationId: string) {
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
    if (this.state.status === "LOADING" || this.state.status === "INIT") {
      return <div className="loading" />;
    }
    if (this.state.status === "ERROR") {
      // TODO: Move this to a shared location.
      return (
        <div className="compare-invocations">
          <div className="container">
            <div className="error-container">
              <img src="/image/x-circle.svg" />
              <div>{this.state.error}</div>
            </div>
          </div>
        </div>
      );
    }

    // TODO: Display a structured diff
    return (
      <div className="compare-invocations">
        <div className="shelf nopadding-dense">
          <header className="container header">
            <h2 className="heading">Comparing invocations</h2>
            <div className="invocation-tags">
              <InvocationIdTag prefix="base" id={this.props.invocationAId} />
              <img className="compare-arrow" alt="comparing to" src="/image/arrow-left.svg" />
              <InvocationIdTag prefix="compare" id={this.props.invocationBId} />
            </div>
            <OutlinedButton className="show-changes-only-button">
              <label className="show-changes-only-label">
                <span>Show changes only </span>
                <input
                  type="checkbox"
                  checked={this.state.showChangesOnly}
                  onChange={this.onClickShowChangesOnly.bind(this)}
                />
              </label>
            </OutlinedButton>
          </header>
        </div>
        <div className="container">
          <pre>
            {this.state.diff.map((change: JsDiff.Change, index: number) => (
              <DiffChunk key={index} change={change} defaultExpanded={!this.state.showChangesOnly} />
            ))}
          </pre>
        </div>
      </div>
    );
  }
}

function InvocationIdTag({ prefix, id }: { prefix: string; id: string }) {
  const href = `/invocation/${id}`;
  return (
    <OutlinedButton className="invocation-id-tag">
      <a
        href={href}
        onClick={(e: React.MouseEvent) => {
          e.preventDefault();
          router.navigateTo(href);
        }}>
        <div className="invocation-id-tag-prefix">{prefix}:</div> <div className="invocation-id-tag-id">{id}</div>
      </a>
    </OutlinedButton>
  );
}
