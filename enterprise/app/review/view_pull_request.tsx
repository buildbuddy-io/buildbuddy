import Long from "long";
import React from "react";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";
import format from "../../../app/format/format";
import rpc_service from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";
import { Github, MessageCircle } from "lucide-react";
import error_service from "../../../app/errors/error_service";
import ReviewThreadComponent from "./review_thread";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import CheckboxButton from "../../../app/components/button/checkbox_button";

const FAKE_ID_PREFIX = "bb-tmp/";

interface ViewPullRequestComponentProps {
  owner: string;
  repo: string;
  pull: number;
  path: string;
}

interface State {
  response?: github.GetGithubPullRequestDetailsResponse;
  displayedDiffs: string[];
  replyDialogOpen: boolean;
  draftReplyText: string;

  // This map contains draft comments that the user is *actively editing*.
  inProgressCommentsById: Set<string>;
  pendingRequest: boolean;
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

interface ThreadAndDraft {
  threadId: string;
  comments: github.Comment[];
  draft?: github.Comment;
}

let fakeReviewIdCounter = 0;
function newFakeId(): string {
  fakeReviewIdCounter++;
  return FAKE_ID_PREFIX + fakeReviewIdCounter;
}

export default class ViewPullRequestComponent extends React.Component<ViewPullRequestComponentProps, State> {
  replyBodyTextRef: React.RefObject<HTMLTextAreaElement> = React.createRef();
  replyApprovalCheckRef: React.RefObject<HTMLInputElement> = React.createRef();

  state: State = {
    displayedDiffs: [],
    replyDialogOpen: false,
    draftReplyText: "",
    inProgressCommentsById: new Set<string>(),
    pendingRequest: false,
  };

  componentWillMount() {
    document.title = `Change #${this.props.pull} in ${this.props.owner}/${this.props.repo} | BuildBuddy`;
    rpc_service.service
      .getGithubPullRequestDetails({
        owner: this.props.owner,
        repo: this.props.repo,
        pull: Long.fromInt(this.props.pull),
      })
      .then((r) => {
        if (!r.draftReviewId) {
          r.draftReviewId = newFakeId();
        }
        this.setState({ response: r });
      })
      .catch((e) => error_service.handleError(e));
  }

  renderSingleReviewer(reviewer: github.Reviewer) {
    return (
      <span className={"reviewer " + (reviewer.attention ? "strong " : "") + (reviewer.approved ? "approved" : "")}>
        {reviewer.login}
      </span>
    );
  }

  renderReviewers(reviewers: github.Reviewer[]) {
    reviewers.sort((a, b) => (a.login.toLowerCase() < b.login.toLowerCase() ? -1 : 1));
    return (
      <>
        {this.joinReactNodes(
          reviewers.map((r) => this.renderSingleReviewer(r)),
          ", "
        )}
      </>
    );
  }

  joinReactNodes(nodes: React.ReactNode[], joiner: React.ReactNode): React.ReactNode[] {
    const joined: React.ReactNode[] = [];
    for (let i = 0; i < nodes.length; i++) {
      joined.push(nodes[i]);
      // If the next element exists, append the joiner node.
      if (i + 1 < nodes.length) {
        joined.push(joiner);
      }
    }
    return joined;
  }

  renderFileHeader() {
    return (
      <tr className="file-list-header">
        <td></td>
        <td className="diff-file-name">File</td>
        <td>Comments</td>
        <td>Inline</td>
        <td>Delta</td>
        <td></td>
      </tr>
    );
  }

  renderDiffBar(additions: number, deletions: number, max: number) {
    // TODO(jdhollen): render cute little green/red diff stats.
    return "";
  }

  handleCreateComment(comment: github.Comment) {
    if (!this.state.response || this.state.pendingRequest) {
      return;
    }

    const req = new github.CreateGithubPullRequestCommentRequest({
      owner: this.props.owner,
      repo: this.props.repo,
      pullId: this.state.response.pullId,
      path: comment.path,
      body: comment.body,
      commitSha: comment.commitSha,
      line: comment.position?.startLine ?? comment.position?.endLine ?? undefined,
      side: comment.position?.side,
    });
    if (comment.threadId && !comment.threadId.startsWith(FAKE_ID_PREFIX)) {
      req.threadId = comment.threadId;
    }
    if (!this.state.response.draftReviewId.startsWith(FAKE_ID_PREFIX)) {
      req.reviewId = this.state.response.draftReviewId;
    }
    console.log(req);

    this.setState({ pendingRequest: true });
    rpc_service.service
      .createGithubPullRequestComment(req)
      .then((r) => {
        console.log(r);
        error_service.handleError("posted!");
        if (this.state.response) {
          const commentToUpdate = this.state.response.comments.find((c) => c.id === comment.id);
          if (commentToUpdate) {
            commentToUpdate.id = r.commentId;
            commentToUpdate.body = comment.body;
            commentToUpdate.reviewId = r.reviewId;
          } else {
            const anyComment = this.state.response.comments.find((c) => c.threadId === req.threadId);
            if (anyComment != undefined) {
              const newComment = new github.Comment(anyComment);
              newComment.createdAtUsec = Long.fromNumber(Date.now() * 1000);
              newComment.id = r.commentId;
              newComment.body = req.body;
              newComment.reviewId = r.reviewId;
              this.state.response.comments.push(newComment);
            }
          }
          this.state.response.draftReviewId = r.reviewId;
        }
        this.removeFromPendingAndSetState(comment.id);
      })
      .catch((e) => {
        error_service.handleError(e);
      })
      .finally(() => this.setState({ pendingRequest: false }));
  }

  handleUpdateComment(commentId: string, newBody: string) {
    if (!this.state.response || this.state.pendingRequest) {
      return;
    }
    const req = new github.UpdateGithubPullRequestCommentRequest({ commentId, newBody });
    console.log(req);

    this.setState({ pendingRequest: true });
    rpc_service.service
      .updateGithubPullRequestComment(req)
      .then((r) => {
        console.log(r);
        error_service.handleError("posted!");
        const comment = this.state.response?.comments.find((c) => c.id === commentId);
        if (comment) {
          comment.body = newBody;
        }
        this.removeFromPendingAndSetState(commentId);
      })
      .catch((e) => {
        error_service.handleError(e);
      })
      .finally(() => this.setState({ pendingRequest: false }));
  }

  handleDeleteComment(commentId: string) {
    if (!this.state.response || this.state.pendingRequest) {
      return;
    }
    const req = new github.DeleteGithubPullRequestCommentRequest({ commentId });
    console.log(req);

    this.setState({ pendingRequest: true });
    rpc_service.service
      .deleteGithubPullRequestComment(req)
      .then((r) => {
        console.log(r);
        error_service.handleError("posted!");
        if (this.state.response) {
          const commentIndex = this.state.response.comments.findIndex((c) => c.id === commentId);
          if (commentIndex >= 0) {
            this.state.response.comments.splice(commentIndex, 1);
          }
        }
        this.removeFromPendingAndSetState(commentId);
      })
      .catch((e) => {
        error_service.handleError(e);
      })
      .finally(() => this.setState({ pendingRequest: false }));
  }

  removeFromPendingAndSetState(commentId: string) {
    const newInProgress = new Set(this.state.inProgressCommentsById);
    newInProgress.delete(commentId);
    this.setState({ inProgressCommentsById: newInProgress });
  }

  handleStartReply(threadId: string) {
    if (this.state.pendingRequest || !this.state.response) {
      return;
    }

    const anyComment = this.state.response.comments.find((c) => c.threadId === threadId);
    if (anyComment === undefined) {
      return;
    }

    let draft: github.Comment | undefined = this.state.response.comments.find(
      (c) => c.reviewId === this.state.response!.draftReviewId && c.threadId === threadId
    );
    if (draft === undefined) {
      const reviewId = this.state.response.draftReviewId;
      draft = new github.Comment({
        id: newFakeId(),
        threadId,
        reviewId,
        parentCommentId: "",
        body: "",
        path: anyComment.path,
        commitSha: anyComment.commitSha,
        position: anyComment.position,
        commenter: new github.ReviewUser({ login: this.state.response.viewerLogin || "you" }),
        createdAtUsec: Long.fromNumber(Date.now() * 1000),
        isResolved: false,
      });
      this.state.response.comments.push(draft);
    }

    const newInProgress = new Set(this.state.inProgressCommentsById);
    newInProgress.add(draft.id);
    this.setState({ inProgressCommentsById: newInProgress });
  }

  handleCancelComment(id: string) {
    const commentIndex = this.findCommentIndexById(id);
    if (commentIndex < 0 || !this.state.response) {
      return;
    }

    const wasInProgress = this.state.inProgressCommentsById.delete(id);
    if (!wasInProgress) {
      return;
    }

    // If the comment is a brand new draft, just delete it.
    if (this.state.response.comments[commentIndex].id.startsWith(FAKE_ID_PREFIX)) {
      this.state.response.comments.splice(commentIndex, 1);
    }
    this.setState({ inProgressCommentsById: new Set(this.state.inProgressCommentsById) });
  }

  findCommentIndexById(commentId: string): number {
    return this.state.response?.comments.findIndex((c) => c.id === commentId) ?? -1;
  }

  renderThread(thread: ThreadAndDraft) {
    if (thread.comments.length < 1 && !thread.draft) {
      return <></>;
    }
    const firstComment = thread.comments[0] ?? thread.draft;
    const leftSide = firstComment.position?.side === github.CommentSide.LEFT_SIDE; // RIGHT_SIDE is default.

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
            threadId={thread.threadId}
            comments={thread.comments}
            draftComment={thread.draft}
            disabled={Boolean(this.state.pendingRequest)}
            updating={Boolean(thread.draft && !thread.draft.id.startsWith(FAKE_ID_PREFIX))}
            editing={Boolean(this.state.inProgressCommentsById.has(thread.draft?.id ?? "bogus-id-doesnt-exist"))}
            saving={/* TODO(jdhollen */ false}
            handler={this}
            activeUsername={this.state.response?.viewerLogin ?? ""}></ReviewThreadComponent>
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

  renderComments(comments: github.Comment[]) {
    if (comments.length > 0) {
    }
    if (!this.state.response) {
      return <></>;
    }
    const threads: Map<String, ThreadAndDraft> = new Map();
    comments.forEach((c) => {
      const thread = c.threadId;
      let threadAndDraft = threads.get(thread);
      if (!threadAndDraft) {
        threadAndDraft = {
          threadId: thread,
          comments: [],
        };
        threads.set(thread, threadAndDraft);
      }
      if (c.reviewId === this.state.response!.draftReviewId) {
        threadAndDraft.draft = c;
      } else {
        threadAndDraft.comments.push(c);
      }
    });
    const outs: JSX.Element[] = [];

    threads.forEach((comments) => {
      outs.push(this.renderThread(comments));
    });
    return <>{outs}</>;
  }

  startComment(side: github.CommentSide, path: string, commitSha: string, lineNumber: number) {
    if (this.state.pendingRequest || !this.state.response) {
      return;
    }
    const reviewId = this.state.response.draftReviewId;
    const draft = new github.Comment({
      id: newFakeId(),
      threadId: newFakeId(),
      reviewId,
      parentCommentId: "",
      body: "",
      // TODO(jdhollen): pass user back from github.
      commenter: new github.ReviewUser({ login: "you" }),
      path,
      commitSha,
      position: new github.CommentPosition({
        startLine: Long.fromNumber(lineNumber),
        endLine: Long.fromNumber(lineNumber),
        side,
      }),
      createdAtUsec: Long.fromNumber(Date.now() * 1000),
      isResolved: false,
    });
    this.state.response.comments.push(draft);
    const newInProgress = new Set(this.state.inProgressCommentsById);
    newInProgress.add(draft.id);
    this.setState({ inProgressCommentsById: newInProgress });
  }

  renderDiffHunk(hunk: Hunk, path: string, commitSha: string, comments: github.Comment[]) {
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
          return (
            <>
              <div className="source-line-pair">
                <pre className="source-line-number">{v.left.lineNumber ?? " "}</pre>
                <pre
                  className={leftClasses}
                  onClick={
                    v.left.lineNumber
                      ? this.startComment.bind(this, github.CommentSide.LEFT_SIDE, path, commitSha, v.left.lineNumber)
                      : undefined
                  }>
                  {v.left.source ?? ""}
                </pre>
                <pre className="source-line-number">{v.right.lineNumber ?? " "}</pre>
                <pre
                  className={rightClasses}
                  onClick={
                    v.right.lineNumber
                      ? this.startComment.bind(this, github.CommentSide.RIGHT_SIDE, path, commitSha, v.right.lineNumber)
                      : undefined
                  }>
                  {v.right.source ?? ""}
                </pre>
              </div>
              <div className="threads">
                {this.renderComments(
                  comments.filter((c) => {
                    return (
                      +(c.position?.startLine || c.position?.endLine || 0) ===
                      (c.position?.side === github.CommentSide.LEFT_SIDE ? v.left.lineNumber : v.right.lineNumber)
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

  getDiffLines(patch: string, path: string, commitSha: string, comments: github.Comment[]): JSX.Element[] {
    if (patch.length === 0) {
      return [<div>No diff info available (binary file?)</div>];
    }
    const out: JSX.Element[] = [];

    const patchLines = patch.split("\n");
    let currentIndex = 0;
    while (currentIndex < patchLines.length) {
      let hunk: Hunk;
      if (patchLines[currentIndex].startsWith("@@")) {
        [hunk, currentIndex] = readNextHunk(patchLines, currentIndex);
        out.push(this.renderDiffHunk(hunk, path, commitSha, comments));
      }
    }

    return out;
  }

  renderFileDiffs(patch: string, filename: string, commitSha: string) {
    // TODO(jdhollen): Need to check commit sha.
    const fileComments = this.state.response!.comments.filter((v) => v.path === filename);
    const out = this.getDiffLines(patch, filename, commitSha, fileComments);

    return (
      <tr className="file-list-diff">
        <td colSpan={6}>{out}</td>
      </tr>
    );
  }

  handleDiffClicked(name: string) {
    const newValue = [...this.state.displayedDiffs];
    const index = newValue.indexOf(name);
    if (index === -1) {
      newValue.push(name);
    } else {
      newValue.splice(index, 1);
    }
    this.setState({ displayedDiffs: newValue });
  }

  renderFileRow(file: github.FileSummary) {
    let expanded = this.state.displayedDiffs.indexOf(file.name) !== -1;
    return (
      <>
        <tr className="file-list-row" onClick={this.handleDiffClicked.bind(this, file.name)}>
          <td className="viewed">
            <input type="checkbox"></input>
          </td>
          <td className="diff-file-name">{file.name}</td>
          <td>{file.comments}</td>
          <td>{expanded ? "Hide" : "Diff"}</td>
          <td>{+file.additions + +file.deletions}</td>
          <td>{this.renderDiffBar(+file.additions, +file.deletions, 0)}</td>
        </tr>
        {expanded && this.renderFileDiffs(file.patch, file.name, file.commitSha)}
      </>
    );
  }

  statusToCssClass(s: github.ActionStatusState): string {
    switch (s) {
      case github.ActionStatusState.ACTION_STATUS_STATE_SUCCESS:
      case github.ActionStatusState.ACTION_STATUS_STATE_NEUTRAL:
        return "success";
      case github.ActionStatusState.ACTION_STATUS_STATE_FAILURE:
        return "failure";
      default:
        return "pending";
    }
  }

  renderAnalysisResults(statuses: github.ActionStatus[]) {
    const done = statuses
      .filter(
        (v) =>
          v.status === github.ActionStatusState.ACTION_STATUS_STATE_SUCCESS ||
          v.status === github.ActionStatusState.ACTION_STATUS_STATE_FAILURE ||
          v.status === github.ActionStatusState.ACTION_STATUS_STATE_NEUTRAL
      )
      .sort((a, b) =>
        a.status === b.status ? 0 : a.status === github.ActionStatusState.ACTION_STATUS_STATE_FAILURE ? -1 : 1
      )
      .map((v) => (
        <a href={v.url} target="_blank" className={"action-status " + this.statusToCssClass(v.status)}>
          {v.name}
        </a>
      ));
    const pending = statuses
      .filter(
        (v) =>
          v.status === github.ActionStatusState.ACTION_STATUS_STATE_PENDING ||
          v.status === github.ActionStatusState.ACTION_STATUS_STATE_UNKNOWN
      )
      .map((v) => (
        <a href={v.url} target="_blank" className={"action-status " + this.statusToCssClass(v.status)}>
          {v.name}
        </a>
      ));

    return (
      <>
        {pending.length > 0 && <div>Pending: {pending}</div>}
        {done.length > 0 && <div>Done: {done}</div>}
      </>
    );
  }

  getPrStatusClass(r?: github.GetGithubPullRequestDetailsResponse) {
    if (!r) {
      return "pending";
    }
    if (r.submitted) {
      return "submitted";
    } else if (r.mergeable) {
      return "approved";
    } else {
      return "pending";
    }
  }

  getPrStatusString(r: github.GetGithubPullRequestDetailsResponse) {
    if (r.submitted) {
      return "Merged";
    } else if (r.mergeable) {
      return "Ready to merge";
    } else {
      return "Pending";
    }
  }

  handleReviewReplyClick(approve: boolean) {
    if (!this.state.response) {
      return;
    }
    if (!approve) {
      this.showReplyDialog();
    } else {
      this.submitReview("", true);
    }
  }

  showReplyDialog() {
    this.setState({ replyDialogOpen: true });
  }

  handleCloseReplyDialog() {
    this.setState({ replyDialogOpen: false });
  }

  submitReview(body: string, approve: boolean) {
    if (!this.state.response) {
      return;
    }
    this.setState({ pendingRequest: true });
    const req = new github.SendGithubPullRequestReviewRequest({
      reviewId: this.state.response.draftReviewId.startsWith(FAKE_ID_PREFIX) ? "" : this.state.response.draftReviewId,
      pullRequestId: this.state.response.pullId,
      body,
      approve,
    });
    console.log(req);
    rpc_service.service
      .sendGithubPullRequestReview(req)
      .then((r) => {
        console.log(r);
        window.location.reload();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ pendingRequest: false }));
  }

  submit() {
    if (!this.state.response) {
      return;
    }
    this.setState({ pendingRequest: true });
    const req = new github.MergeGithubPullRequest({
      owner: this.state.response.owner,
      repo: this.state.response.repo,
      pullNumber: this.state.response.pull,
    });
    console.log(req);
    rpc_service.service
      .mergeGithubPull(req)
      .then((r) => {
        console.log(r);
        window.location.reload();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ pendingRequest: false }));
  }

  hasAnyDraftComments() {
    const response = this.state.response;
    if (!response) {
      return false;
    }
    return response.comments.filter((v) => v.reviewId === response.draftReviewId).length > 0;
  }

  replyNotReady() {
    return (this.replyBodyTextRef.current?.value ?? "").length === 0 && !this.hasAnyDraftComments();
  }

  userIsPrAuthor() {
    return Boolean(this.state.response?.viewerLogin && this.state.response.viewerLogin === this.state.response.author);
  }

  handleReplyTextChange(e: React.ChangeEvent<HTMLTextAreaElement>) {
    this.setState({ draftReplyText: e.target.value });
  }

  renderReplyModal() {
    if (!this.state.response) {
      return undefined;
    }
    const userIsPrAuthor = this.userIsPrAuthor();
    const draftReviewId = this.state.response.draftReviewId;
    const draftComments = draftReviewId ? this.state.response.comments.filter((v) => v.reviewId === draftReviewId) : [];
    return (
      <Modal isOpen={this.state.replyDialogOpen} onRequestClose={this.handleCloseReplyDialog.bind(this)}>
        <Dialog className="pr-view">
          <DialogHeader>
            <DialogTitle>Replying to change #{this.props.pull}</DialogTitle>
          </DialogHeader>
          <DialogBody>
            <textarea
              disabled={this.state.pendingRequest}
              ref={this.replyBodyTextRef}
              className="comment-input"
              onChange={this.handleReplyTextChange.bind(this)}
              defaultValue={""}></textarea>
            {!userIsPrAuthor && (
              <CheckboxButton checkboxRef={this.replyApprovalCheckRef} className="reply-modal-approve-button">
                Approve
              </CheckboxButton>
            )}
            {draftComments.map((c) => (
              <div className="reply-modal-thread-container">
                <ReviewThreadComponent
                  threadId={c.threadId}
                  comments={[]}
                  draftComment={c}
                  disabled={Boolean(this.state.pendingRequest)}
                  updating={!c.id.startsWith(FAKE_ID_PREFIX)}
                  editing={Boolean(this.state.inProgressCommentsById.has(c.id ?? "bogus-id-doesnt-exist"))}
                  saving={/* TODO(jdhollen */ false}
                  handler={this}
                  activeUsername={this.state.response?.viewerLogin ?? ""}></ReviewThreadComponent>
              </div>
            ))}
          </DialogBody>
          <DialogFooter>
            <DialogFooterButtons>
              <OutlinedButton
                disabled={false}
                onClick={() => {
                  this.handleCloseReplyDialog();
                }}>
                Cancel
              </OutlinedButton>
              <FilledButton
                disabled={this.state.pendingRequest || this.replyNotReady()}
                onClick={() => {
                  this.submitReview(
                    this.replyBodyTextRef.current?.value ?? "",
                    this.replyApprovalCheckRef.current?.checked ?? false
                  );
                }}>
                Send
              </FilledButton>
            </DialogFooterButtons>
          </DialogFooter>
        </Dialog>
      </Modal>
    );
  }

  render() {
    const userIsPrAuthor = this.userIsPrAuthor();

    return (
      <div className={"pr-view " + this.getPrStatusClass(this.state.response)}>
        {this.state.response !== undefined && (
          <>
            <div className="summary-section">
              <div className="review-header">
                <span className="review-title">
                  <span className="review-number">Change #{this.state.response.pull}&nbsp;</span>
                  <span className="review-details">
                    by <span className="review-author">{this.state.response.author}</span> in{" "}
                    <span className="review-repo">
                      {this.state.response.owner}/{this.state.response.repo}
                    </span>
                  </span>
                  <a href={this.state.response.githubUrl} className="review-gh-link">
                    <Github size="16" className="icon" />
                  </a>
                </span>
                <div className="review-actions">
                  {userIsPrAuthor && !this.state.response.submitted && (
                    <OutlinedButton disabled={!this.state.response.mergeable} onClick={() => this.submit()}>
                      Submit
                    </OutlinedButton>
                  )}
                  {!userIsPrAuthor && (
                    <OutlinedButton onClick={() => this.handleReviewReplyClick(true)}>Approve</OutlinedButton>
                  )}
                  <FilledButton onClick={() => this.handleReviewReplyClick(false)}>Reply</FilledButton>
                </div>
              </div>
              <div className="header-separator"></div>
              <div className="review-cell">
                <div className="attr-grid">
                  <div className="attr-label">Reviewers</div>
                  <div>{this.renderReviewers(this.state.response.reviewers)}</div>
                  <div className="attr-label">Issues</div>
                  <div></div>
                  <div className="attr-label">Mentions</div>
                  <div></div>
                  <div></div>
                </div>
              </div>
              <div className="review-cell">
                <div className="description">
                  {this.state.response.title}
                  <br />
                  <br />
                  {this.state.response.body}
                </div>
              </div>
              <div className="review-cell">
                <div className="attr-grid">
                  <div className="attr-label">Created</div>
                  <div>{format.formatTimestampUsec(this.state.response.createdAtUsec)}</div>
                  <div className="attr-label">Modified</div>
                  <div>{format.formatTimestampUsec(this.state.response.updatedAtUsec)}</div>
                  <div className="attr-label">Branch</div>
                  <div>{this.state.response.branch}</div>
                </div>
              </div>
              <div className="review-cell">
                <div className="attr-grid">
                  <div className="attr-label">Status</div>
                  <div>{this.getPrStatusString(this.state.response)}</div>
                  <div className="attr-label">Analysis</div>
                  <div>{this.renderAnalysisResults(this.state.response.actionStatuses)}</div>
                </div>
              </div>
              <div className="review-cell header">Files</div>
              <div className="review-cell header"></div>
            </div>
            <div className="file-section">
              <table>
                {this.renderFileHeader()}
                {this.state.response.files.map((f) => this.renderFileRow(f))}
              </table>
            </div>
            {this.renderReplyModal()}
          </>
        )}
      </div>
    );
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
