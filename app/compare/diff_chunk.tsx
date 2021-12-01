import DiffMatchPatch from "diff-match-patch";
import { Maximize2 } from "lucide-react";
import React from "react";
import { OutlinedButton } from "../components/button/button";

export type DiffChunkData = { marker: number; lines: string[] };

export type DiffChunkProps = {
  chunk: DiffChunkData;
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
    return { expanded: this.props.chunk.marker === DiffMatchPatch.DIFF_EQUAL && this.props.defaultExpanded };
  }

  componentDidUpdate(prevProps: DiffChunkProps) {
    if (prevProps.defaultExpanded !== this.props.defaultExpanded) {
      this.setState(this.getInitialState());
    }
  }

  private onExpand() {
    this.setState({ expanded: true });
  }

  private renderAdded(lines: string[]) {
    return lines.map((line, i) => (
      <div className="diff-line added" key={i}>
        <div className="plus-minus-cell">+</div>
        <div>{line}</div>
      </div>
    ));
  }

  private renderRemoved(lines: string[]) {
    return lines.map((line, i) => (
      <div className="diff-line removed" key={i}>
        <div className="plus-minus-cell">-</div>
        <div>{line}</div>
      </div>
    ));
  }

  private renderUnchanged(lines: string[]) {
    return lines.map((line, i) => (
      <div className="diff-line" key={i}>
        <div className="plus-minus-cell">&nbsp;</div>
        <div>{line}</div>
      </div>
    ));
  }

  private renderChunk(
    renderLines: (lines: string[]) => React.ReactNode,
    collapsedLabel: string,
    expandButtonClass: string,
    minCollapsedRegionSize: number
  ) {
    const { lines } = this.props.chunk;

    if (this.state.expanded || lines.length < NUM_LINES_OF_CONTEXT * 2 + minCollapsedRegionSize) {
      return renderLines(lines);
    }

    return (
      <>
        {renderLines(lines.slice(0, NUM_LINES_OF_CONTEXT))}
        <OutlinedButton className={`diff-line collapsed ${expandButtonClass}`} onClick={this.onExpand.bind(this)}>
          <div className="maximize-icon-container">
            <Maximize2 className="maximize-icon" />
          </div>
          <pre>
            Show {lines.length - NUM_LINES_OF_CONTEXT * 2} {collapsedLabel} lines
          </pre>
        </OutlinedButton>
        {renderLines(lines.slice(lines.length - NUM_LINES_OF_CONTEXT, lines.length))}
      </>
    );
  }

  render() {
    const marker = this.props.chunk.marker;
    switch (marker) {
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
