import DiffMatchPatch from "diff-match-patch";
import { ArrowLeft, Sliders, XCircle } from "lucide-react";
import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import { OutlinedButton } from "../components/button/button";
import CheckboxButton from "../components/button/checkbox_button";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import { BuildBuddyError } from "../util/errors";
import DiffChunk, { DiffChunkData } from "./diff_chunk";
import { prepareForDiff, PreProcessingOptions } from "./diff_preprocessing";

export interface CompareInvocationsComponentProps {
  user?: User;
  search: URLSearchParams;
  invocationAId: string;
  invocationBId: string;
}

type Status = "INIT" | "LOADING" | "LOADED" | "ERROR";

type Diff = DiffChunkData[];

interface State {
  status?: Status;
  error?: string | null;
  invocationA?: invocation.IInvocation | null;
  invocationB?: invocation.IInvocation | null;
  diff?: Diff | null;
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
  hideInvocationIds: true,
  hideUuids: true,
  hideProgress: true,
};

export default class CompareInvocationsComponent extends React.Component<CompareInvocationsComponentProps, State> {
  state: State = INITIAL_STATE;

  private preProcessingOptions: PreProcessingOptions = this.getPreProcessingOptions();

  componentDidMount() {
    this.fetchInvocations();
  }

  componentDidUpdate(prevProps: CompareInvocationsComponentProps) {
    this.preProcessingOptions = this.getPreProcessingOptions();

    if (
      prevProps.user !== this.props.user ||
      prevProps.invocationAId !== this.props.invocationAId ||
      prevProps.invocationBId !== this.props.invocationBId
    ) {
      this.setState(INITIAL_STATE);
      this.fetchInvocations();
    } else if (prevProps.search !== this.props.search) {
      this.computeDiff(this.state.invocationA, this.state.invocationB);
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
      this.setState({ status: "ERROR", error: BuildBuddyError.parse(error).description });
      return;
    }
    this.computeDiff(invocationA, invocationB);
  }

  private computeDiff(invocationA: invocation.IInvocation, invocationB: invocation.IInvocation) {
    if (this.state.error) return;

    const textA = JSON.stringify(prepareForDiff(invocationA, this.preProcessingOptions), null, 2);
    const textB = JSON.stringify(prepareForDiff(invocationB, this.preProcessingOptions), null, 2);

    const lineDiffs = computeLineDiffs(textA, textB);

    const diff = lineDiffs.map(([op, data]) => ({
      marker: op,
      lines: data.trimEnd().split("\n"),
    }));

    this.setState({
      status: "LOADED",
      invocationA,
      invocationB,
      diff,
      error: null,
    });
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
    const { status, error, diff } = this.state;

    if (status === "LOADING" || status === "INIT") {
      return <div className="loading" />;
    }

    // TODO: Display a structured diff
    return (
      <div className="compare-invocations">
        <div className="shelf nopadding-dense">
          <header className="container header">
            <h2 className="heading">Comparing invocations</h2>
            <div className="invocation-tags">
              <InvocationIdTag prefix="base" id={this.props.invocationAId} />
              <ArrowLeft className="compare-arrow" />
              <InvocationIdTag prefix="compare" id={this.props.invocationBId} />
            </div>
            {diff && (
              <CheckboxButton
                className="show-changes-only-button"
                onChange={this.onClickShowChangesOnly.bind(this)}
                checked={this.state.showChangesOnly}>
                Show changes only
              </CheckboxButton>
            )}
          </header>
        </div>
        {diff && (
          <div className="container preprocessing-options">
            <Sliders />
            <div className="preprocessing-options-list">
              {this.renderPreProcessingOption("sortEvents", "Sort events")}
              {this.renderPreProcessingOption("hideTimingData", "Hide timing data")}
              {this.renderPreProcessingOption("hideProgress", "Hide progress")}
              {this.renderPreProcessingOption("hideInvocationIds", "Hide invocation IDs")}
              {this.renderPreProcessingOption("hideUuids", "Hide Bazel-generated UUIDs")}
            </div>
          </div>
        )}
        <div className="container">
          {error && (
            <div className="error-container">
              <XCircle className="icon red" />
              <div>{error}</div>
            </div>
          )}
          {diff && (
            <pre className="diff-container">
              {diff.map((chunk: DiffChunkData, index: number) => (
                <DiffChunk key={index} chunk={chunk} defaultExpanded={!this.state.showChangesOnly} />
              ))}
            </pre>
          )}
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

const dmp = new DiffMatchPatch.diff_match_patch();

function computeLineDiffs(text1: string, text2: string) {
  const { chars1, chars2, lineArray } = dmp.diff_linesToChars_(text1, text2);
  const diffs = dmp.diff_main(chars1, chars2, false);
  dmp.diff_charsToLines_(diffs, lineArray);
  return diffs;
}
