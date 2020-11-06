import DiffMatchPatch from "diff-match-patch";
import React from "react";
import { OutlinedButton } from "../components/button/button";

export type DiffChunkProps = {
  change: DiffMatchPatch.Diff;
  defaultExpanded: boolean;
};

type State = {
  expanded: boolean;
};

const MIN_COLLAPSED_UNCHANGED_REGION_NUM_LINES = 16;
const MIN_COLLAPSED_CHANGED_REGION_NUM_LINES = 128;
// Number of lines to show before and after a long segment of identical lines.
const NUM_LINES_OF_CONTEXT = 4;

export default class DiffChunk extends React.Component<DiffChunkProps, State> {
  state: State = this.getInitialState();

  private getInitialState() {
    const [diffType] = this.props.change;
    return { expanded: diffType === DiffMatchPatch.DIFF_EQUAL && this.props.defaultExpanded };
  }

  componentDidUpdate(prevProps: DiffChunkProps) {
    if (prevProps.defaultExpanded !== this.props.defaultExpanded) {
      this.setState(this.getInitialState());
    }
  }

  private onExpand() {
    this.setState({ expanded: true });
  }

  private renderAdded(text: string) {
    return <ins className="added">{text}</ins>;
  }

  private renderRemoved(text: string) {
    return (
      <del className="removed">
        <span>{text}</span>
      </del>
    );
  }

  private renderUnchanged(text: string) {
    return <>{text}</>;
  }

  private renderChunk(
    renderText: (text: string) => React.ReactNode,
    collapsedLabel: string,
    expandButtonClass: string,
    minCollapsedRegionSize: number
  ) {
    const [_, text] = this.props.change;

    if (this.state.expanded) {
      return renderText(text);
    }

    const lines = text.split("\n");
    if (lines.length < NUM_LINES_OF_CONTEXT * 2 + minCollapsedRegionSize) {
      return renderText(text);
    }

    return (
      <>
        {renderText(lines.slice(0, NUM_LINES_OF_CONTEXT).join("\n"))}
        <OutlinedButton className={`diff-line collapsed ${expandButtonClass}`} onClick={this.onExpand.bind(this)}>
          <div className="maximize-icon-container">
            <img className="maximize-icon" src="/image/maximize-2.svg" />
          </div>
          <pre>
            Show {lines.length - NUM_LINES_OF_CONTEXT * 2} {collapsedLabel} lines
          </pre>
        </OutlinedButton>
        {renderText(lines.slice(lines.length - NUM_LINES_OF_CONTEXT, lines.length).join("\n"))}
      </>
    );
  }

  render() {
    const [diffType] = this.props.change;
    switch (diffType) {
      case DiffMatchPatch.DIFF_INSERT:
        return this.renderChunk(
          this.renderAdded.bind(this),
          /* collapsedLabel= */ "added",
          /* expandButtonClass= */ "added",
          MIN_COLLAPSED_CHANGED_REGION_NUM_LINES
        );
      case DiffMatchPatch.DIFF_DELETE:
        return this.renderChunk(
          this.renderRemoved.bind(this),
          /* collapsedLabel= */ "removed",
          /* expandButtonClass= */ "removed",
          MIN_COLLAPSED_CHANGED_REGION_NUM_LINES
        );
      case DiffMatchPatch.DIFF_EQUAL:
      default:
        return this.renderChunk(
          this.renderUnchanged.bind(this),
          /* collapsedLabel= */ "identical",
          /* expandButtonClass= */ "identical",
          MIN_COLLAPSED_UNCHANGED_REGION_NUM_LINES
        );
    }
  }
}
