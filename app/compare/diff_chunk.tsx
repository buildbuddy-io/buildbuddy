import DiffMatchPatch from "diff-match-patch";
import React from "react";
import { OutlinedButton } from "../components/button/button";

export type DiffChunkProps = {
  change: DiffMatchPatch.Diff;
  defaultExpanded: boolean;
};

type DiffChunkState = {
  expanded: boolean;
};

const MIN_COLLAPSED_UNCHANGED_REGION_SIZE = 16;
const MIN_COLLAPSED_CHANGED_REGION_SIZE = 128;
// Number of lines to show before and after a long segment of identical lines.
const NUM_LINES_OF_CONTEXT = 4;

export default class DiffChunk extends React.Component<DiffChunkProps, DiffChunkState> {
  state = { expanded: this.props.defaultExpanded };

  componentDidUpdate(prevProps: DiffChunkProps) {
    if (prevProps.defaultExpanded !== this.props.defaultExpanded) {
      this.setState({ expanded: this.props.defaultExpanded });
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
    const [_, data] = this.props.change;
    const text = data.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

    if (this.state.expanded) {
      return renderText(text);
    }

    const lines = text.trimEnd().split("\n");
    if (lines.length < NUM_LINES_OF_CONTEXT * 2 + minCollapsedRegionSize) {
      return renderText(text);
    }

    // Render some lines of context, then a collapsed region, then more context.

    const contextBefore = [];
    for (let i = 0; i < NUM_LINES_OF_CONTEXT; i++) {
      contextBefore.push(
        <>
          {renderText(lines[i])}
          <br />
        </>
      );
    }

    const contextAfter = [];
    for (let i = lines.length - NUM_LINES_OF_CONTEXT; i < lines.length - 1; i++) {
      contextAfter.push(
        <>
          {renderText(lines[i])}
          <br />
        </>
      );
    }
    contextAfter.push(lines[lines.length - 1]);
    if (text.endsWith("\n")) {
      contextAfter.push(<br />);
    }

    return (
      <>
        {contextBefore}
        <OutlinedButton className={`diff-line collapsed ${expandButtonClass}`} onClick={this.onExpand.bind(this)}>
          <div className="plus-minus-cell">
            <img className="maximize-icon" src="/image/maximize-2.svg" />
          </div>
          <pre>
            Show {lines.length - NUM_LINES_OF_CONTEXT * 2} {collapsedLabel} lines
          </pre>
        </OutlinedButton>
        {contextAfter}
      </>
    );
  }

  render() {
    const [op] = this.props.change;
    switch (op) {
      case +1:
        return this.renderChunk(
          this.renderAdded.bind(this),
          /* collapsedLabel= */ "added",
          /* expandButtonClass= */ "added",
          MIN_COLLAPSED_CHANGED_REGION_SIZE
        );
      case -1:
        return this.renderChunk(
          this.renderRemoved.bind(this),
          /* collapsedLabel= */ "removed",
          /* expandButtonClass= */ "removed",
          MIN_COLLAPSED_CHANGED_REGION_SIZE
        );
      case 0:
      default:
        return this.renderChunk(
          this.renderUnchanged.bind(this),
          /* collapsedLabel= */ "identical",
          /* expandButtonClass= */ "identical",
          MIN_COLLAPSED_UNCHANGED_REGION_SIZE
        );
    }
  }
}
