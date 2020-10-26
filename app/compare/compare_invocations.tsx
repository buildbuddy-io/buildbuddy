import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import { OutlinedButton } from "../components/button/button";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import { parseError } from "../util/errors";
import JsDiff from "diff";
import DiffChunk from "./diff_chunk";
import { PreProcessingOptions, computeTextForDiff } from "./diff_preprocessing";
import CheckboxButton from "../components/button/checkbox_button";

export interface CompareInvocationsComponentProps {
  user?: User;
  search: URLSearchParams;
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

export default class CompareInvocationsComponent extends React.Component<CompareInvocationsComponentProps, State> {
  state: State = this.getInitialState();
  private getInitialState(): State {
    return {
      status: "INIT",
      error: null,
      invocationA: null,
      invocationB: null,
      showChangesOnly: true,
    };
  }

  private preProcessingOptions: PreProcessingOptions = this.getPreProcessingOptions();

  componentDidMount() {
    this.fetchInvocations();
  }

  componentDidUpdate(prevProps: CompareInvocationsComponentProps, prevState: State) {
    this.preProcessingOptions = this.getPreProcessingOptions();

    if (
      prevProps.user !== this.props.user ||
      prevProps.invocationAId !== this.props.invocationAId ||
      prevProps.invocationBId !== this.props.invocationBId
    ) {
      this.setState(this.getInitialState());
      this.fetchInvocations();
    } else if (prevProps.search !== this.props.search) {
      this.setState({ diff: this.computeDiff(this.state.invocationA, this.state.invocationB) });
    }
  }

  private getPreProcessingOptions(): PreProcessingOptions {
    const optionsParam = this.props.search?.get("options");
    return optionsParam
      ? JSON.parse(optionsParam)
      : {
          sortEvents: true,
          hideTimingData: true,
          hideConsoleOutput: true,
          hideInvocationIds: true,
          hideUuids: true,
        };
  }

  private async fetchInvocations() {
    const { invocationAId, invocationBId } = this.props;

    try {
      const [a, b] = await Promise.all([this.fetchInvocation(invocationAId), this.fetchInvocation(invocationBId)]);
      // TODO: Make sure invocation ID props haven't changed
      this.setState({ status: "LOADED", invocationA: a, invocationB: b, diff: this.computeDiff(a, b) });
    } catch (e) {
      console.error(e);
      this.setState({ status: "ERROR", error: parseError(e).description });
    }
  }

  private computeDiff(invocationA: invocation.IInvocation, invocationB: invocation.IInvocation) {
    return JsDiff.diffLines(
      computeTextForDiff(invocationA, this.preProcessingOptions),
      computeTextForDiff(invocationB, this.preProcessingOptions)
    );
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

  private onClickPreProcessingOption(e: React.ChangeEvent<HTMLInputElement>) {
    const { name, checked } = e.target;
    const preProcessingOptions = { ...this.preProcessingOptions, [name]: checked };
    router.replaceParams({ options: JSON.stringify(preProcessingOptions) });
  }

  private renderPreProcessingOption(optionKey: keyof PreProcessingOptions, label: string) {
    return (
      <CheckboxButton
        name={optionKey}
        checked={this.preProcessingOptions[optionKey]}
        onChange={this.onClickPreProcessingOption.bind(this)}>
        {label}
      </CheckboxButton>
    );
  }

  render() {
    const { status } = this.state;

    if (status === "LOADING" || status === "INIT") {
      return <div className="loading" />;
    }
    if (status === "ERROR") {
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
            <CheckboxButton
              className="show-changes-only-button"
              onChange={this.onClickShowChangesOnly.bind(this)}
              checked={this.state.showChangesOnly}>
              Show changes only
            </CheckboxButton>
          </header>
        </div>
        <div className="container denoising-options">
          <img alt="Comparison options" src="/image/sliders.svg" />
          {this.renderPreProcessingOption("sortEvents", "Sort events")}
          {this.renderPreProcessingOption("hideTimingData", "Hide timing data")}
          {this.renderPreProcessingOption("hideInvocationIds", "Hide invocation IDs")}
          {this.renderPreProcessingOption("hideConsoleOutput", "Hide console output")}
          {this.renderPreProcessingOption("hideUuids", "Hide Bazel-generated UUIDs")}
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
