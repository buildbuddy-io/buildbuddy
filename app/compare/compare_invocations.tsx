import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import { OutlinedButton } from "../components/button/button";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import { parseError } from "../util/errors";
import JsDiff from "diff";
import DiffChunk from "./diff_chunk";
import { PreProcessingOptions, prepareForDiff } from "./diff_preprocessing";
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

const INITIAL_STATE: State = {
  status: "INIT",
  error: null,
  invocationA: null,
  invocationB: null,
  showChangesOnly: true,
};

const DEFAULT_PREPROCESSING_OPTIONS: PreProcessingOptions = {
  sortEvents: true,
  hideTimingData: true,
  hideConsoleOutput: true,
  hideInvocationIds: true,
  hideUuids: true,
};

export default class CompareInvocationsComponent extends React.Component<CompareInvocationsComponentProps, State> {
  state: State = INITIAL_STATE;

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
      this.setState(INITIAL_STATE);
      this.fetchInvocations();
    } else if (prevProps.search !== this.props.search) {
      this.setState({ diff: this.computeDiff(this.state.invocationA, this.state.invocationB) });
    }
  }

  private getPreProcessingOptions(): PreProcessingOptions {
    return optionsFromSearch(this.props.search?.toString() || "");
  }

  private async fetchInvocations() {
    const { invocationAId, invocationBId } = this.props;

    let error: any;
    let invocationA: invocation.IInvocation, invocationB: invocation.IInvocation;
    try {
      [invocationA, invocationB] = await Promise.all([
        this.fetchInvocation(invocationAId),
        this.fetchInvocation(invocationBId),
      ]);
    } catch (e) {
      error = e;
    }
    if (invocationAId !== this.props.invocationAId || invocationBId !== this.props.invocationBId) {
      return;
    }

    if (error) {
      console.error(error);
      this.setState({ status: "ERROR", error: parseError(error).description });
      return;
    }
    this.setState({ status: "LOADED", invocationA, invocationB, diff: this.computeDiff(invocationA, invocationB) });
  }

  private computeDiff(invocationA: invocation.IInvocation, invocationB: invocation.IInvocation) {
    return JsDiff.diffLines(
      JSON.stringify(prepareForDiff(invocationA, this.preProcessingOptions), null, 2),
      JSON.stringify(prepareForDiff(invocationB, this.preProcessingOptions), null, 2)
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
    router.replaceParams(optionsToQueryParams(preProcessingOptions));
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
        <div className="container preprocessing-options">
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

function optionsToQueryParams(options: PreProcessingOptions): Record<string, string> {
  return Object.fromEntries(Object.entries(options).map(([key, value]) => [key, String(value)]));
}

function optionsFromSearch(search: string): PreProcessingOptions {
  const params = new URLSearchParams(search);
  return {
    ...DEFAULT_PREPROCESSING_OPTIONS,
    ...Object.fromEntries([...params.entries()].map(([key, value]) => [key, value === "true"])),
  };
}
