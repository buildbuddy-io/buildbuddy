import React from "react";
import { github } from "../../../proto/github_ts_proto";
import ReviewThreadComponent from "./review_thread";
import { CommentModel, ReviewModel, ThreadModel } from "./review_model";
import { ReviewController } from "./review_controller";

interface FileContentComponentProps {
  reviewModel: ReviewModel;
  disabled: boolean;
  handler: ReviewController;
  viewerLogin: string;
  owner: string;
  repo: string;
  pull: number;
  path: string;
  patch: string;
  commitSha: string;
}

interface DiffLineInfo {
  startLine: number;
  lineCount: number;
}

interface SourceLine {
  source?: string;
  lineNumber?: number;
}

interface DiffLinePair {
  left: SourceLine;
  right: SourceLine;
}

interface Hunk {
  header: string;
  lines: DiffLinePair[];
}

export default class FileContentComponent extends React.Component<FileContentComponentProps> {
  replyBodyTextRef: React.RefObject<HTMLTextAreaElement> = React.createRef();
  replyApprovalCheckRef: React.RefObject<HTMLInputElement> = React.createRef();

  renderThread(thread: ThreadModel) {
    const comments = thread.getComments();
    const draft = thread.getDraft();
    if (comments.length < 1 && !draft) {
      return <></>;
    }
    const firstComment = comments[0] ?? draft;
    const leftSide = firstComment.getSide() === github.CommentSide.LEFT_SIDE;

    return (
      <div className="thread-block">
        {!leftSide ? (
          <>
            <pre className="thread-line-number-space"> </pre>
            <div className="thread-empty-side"> </div>
          </>
        ) : undefined}
        <pre className="thread-line-number-space"> </pre>
        <div className="thread-container">
          <ReviewThreadComponent
            threadId={thread.getId()}
            reviewId={this.props.reviewModel.getDraftReviewId()}
            viewerLogin={this.props.reviewModel.getViewerLogin()}
            comments={comments}
            draftComment={draft}
            disabled={Boolean(this.props.disabled)}
            updating={Boolean(draft && draft.isSubmittedToGithub())}
            editing={Boolean(this.props.reviewModel.isCommentInProgress(draft?.getId()))}
            saving={/* TODO(jdhollen */ false}
            handler={this.props.handler}
            activeUsername={this.props.viewerLogin}></ReviewThreadComponent>
        </div>
        {leftSide ? (
          <>
            <pre className="thread-line-number-space"> </pre>
            <div className="thread-empty-side"> </div>
          </>
        ) : undefined}
      </div>
    );
  }

  renderComments(comments: CommentModel[]) {
    const threads: Map<String, ThreadModel> = ThreadModel.threadsFromComments(
      comments,
      this.props.reviewModel.getDraftReviewId()
    );
    const outs: JSX.Element[] = [];

    threads.forEach((t) => {
      outs.push(this.renderThread(t));
    });
    return <>{outs}</>;
  }

  renderDiffHunk(hunk: Hunk, commitSha: string) {
    const comments = this.props.reviewModel.getCommentsForFile(this.props.path, commitSha);

    return (
      <>
        <pre className="diff-header">{hunk.header}</pre>
        {hunk.lines.map((v) => {
          let leftClasses =
            "source-line left" +
            (v.right.source === undefined ? " new" : "") +
            (v.left.source === undefined ? " empty" : "");
          let rightClasses =
            "source-line right" +
            (v.left.source === undefined ? " new" : "") +
            (v.right.source === undefined ? " empty" : "");
          if (v.left.source !== undefined && v.right.source !== undefined && v.left.source !== v.right.source) {
            leftClasses += " modified";
            rightClasses += " modified";
          }
          const leftLineNumber = v.left.lineNumber;
          const rightLineNumber = v.right.lineNumber;
          return (
            <>
              <div className="source-line-pair">
                <pre className="source-line-number">{v.left.lineNumber ?? " "}</pre>
                <pre
                  className={leftClasses}
                  onClick={
                    leftLineNumber
                      ? () =>
                          this.props.handler.startComment(
                            github.CommentSide.LEFT_SIDE,
                            this.props.path,
                            commitSha,
                            leftLineNumber
                          )
                      : undefined
                  }>
                  {v.left.source ?? ""}
                </pre>
                <pre className="source-line-number">{v.right.lineNumber ?? " "}</pre>
                <pre
                  className={rightClasses}
                  onClick={
                    rightLineNumber
                      ? () =>
                          this.props.handler.startComment(
                            github.CommentSide.RIGHT_SIDE,
                            this.props.path,
                            commitSha,
                            rightLineNumber
                          )
                      : undefined
                  }>
                  {v.right.source ?? ""}
                </pre>
              </div>
              <div className="threads">
                {this.renderComments(
                  comments.filter((c) => {
                    return (
                      c.getLine() ===
                      (c.getSide() === github.CommentSide.LEFT_SIDE ? v.left.lineNumber : v.right.lineNumber)
                    );
                  })
                )}
              </div>
            </>
          );
        })}
      </>
    );
  }

  render(): JSX.Element[] {
    if (this.props.patch.length === 0) {
      return [<div>No diff info available (binary file?)</div>];
    }
    const out: JSX.Element[] = [];

    const patchLines = this.props.patch.split("\n");
    let currentIndex = 0;
    while (currentIndex < patchLines.length) {
      let hunk: Hunk;
      if (patchLines[currentIndex].startsWith("@@")) {
        [hunk, currentIndex] = readNextHunk(patchLines, currentIndex);
        // TODO(jdhollen): Need to check commit sha.
        out.push(this.renderDiffHunk(hunk, this.props.commitSha));
      }
    }

    return out;
  }
}

function getDiffLineInfo(input: string): DiffLineInfo {
  const breakdown = input.slice(1).split(",");
  return { startLine: Number(breakdown[0]) || 0, lineCount: Number(breakdown[1]) || 0 };
}

function readNextHunk(patchLines: string[], startIndex: number): [Hunk, number] {
  // Parse the hunk start line.
  const hunkSummary = patchLines[startIndex].split("@@");
  if (hunkSummary.length < 3) {
    return [{ header: "", lines: [] }, startIndex];
  }
  const diffLineInfo = hunkSummary[1].trim().split(" ");
  let leftInfo: DiffLineInfo | undefined, rightInfo: DiffLineInfo | undefined;
  for (let i = 0; i < diffLineInfo.length; i++) {
    const value = diffLineInfo[i];
    if (value.length < 4) {
      continue;
    }
    if (value[0] === "+") {
      rightInfo = getDiffLineInfo(value);
    } else if (value[0] === "-") {
      leftInfo = getDiffLineInfo(value);
    }
  }

  let leftLinesRead = 0;
  let rightLinesRead = 0;
  let currentLineOffset = 0;
  let currentIndex = startIndex + 1;

  let leftLines: SourceLine[] = [];
  let rightLines: SourceLine[] = [];
  while (
    (leftLinesRead < (leftInfo?.lineCount || 1) || rightLinesRead < (rightInfo?.lineCount || 1)) &&
    currentIndex < patchLines.length
  ) {
    let line = patchLines[currentIndex];
    if (line[0] === "+") {
      rightLines.push({ source: line.slice(1), lineNumber: (rightInfo?.startLine ?? 0) + rightLinesRead });
      rightLinesRead += 1;
      currentLineOffset += 1;
    } else if (line[0] === "-") {
      leftLines.push({ source: line.slice(1), lineNumber: (leftInfo?.startLine ?? 0) + leftLinesRead });
      leftLinesRead += 1;
      currentLineOffset -= 1;
    } else {
      rightLines.push({ source: line.slice(1), lineNumber: (rightInfo?.startLine ?? 0) + rightLinesRead });
      leftLines.push({ source: line.slice(1), lineNumber: (leftInfo?.startLine ?? 0) + leftLinesRead });
      leftLinesRead += 1;
      rightLinesRead += 1;
      const arrayToGrow = currentLineOffset < 0 ? rightLines : leftLines;
      for (let i = 0; i < Math.abs(currentLineOffset); i++) {
        arrayToGrow.push({});
      }
      currentLineOffset = 0;
    }
    currentIndex++;
  }
  const finalOffset = rightLines.length - leftLines.length;
  if (finalOffset !== 0) {
    const arrayToGrow = finalOffset < 0 ? rightLines : leftLines;
    for (let i = 0; i < Math.abs(finalOffset); i++) {
      arrayToGrow.push({});
    }
  }

  let output: DiffLinePair[] = [];
  for (let i = rightLines.length - 1; i >= 0; i--) {
    if (leftLines[i].source === undefined) {
      let j = i - 1;
      while (j >= 0) {
        if (leftLines[j].source !== undefined) {
          if (leftLines[j].source === rightLines[i].source) {
            leftLines[i] = leftLines[j];
            leftLines[j] = {};
          }
          break;
        }
        j--;
      }
    }
  }
  for (let i = 0; i < rightLines.length; i++) {
    output.push({ left: leftLines[i], right: rightLines[i] });
  }

  return [{ header: patchLines[startIndex], lines: output }, currentIndex];
}
