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
import error_service from "../../../app/errors/error_service";
import ReviewThreadComponent from "./review_thread";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import CheckboxButton from "../../../app/components/button/checkbox_button";
import { CommentModel, ReviewModel, FileModel } from "./review_model";
import Link from "../../../app/components/link/link";
import router from "../../../app/router/router";
import PullRequestHeaderComponent from "./pull_request_header";
import FileContentMonacoComponent from "./file_content_monaco";

interface ViewPullRequestComponentProps {
  owner: string;
  repo: string;
  pull: number;
  path: string;
}

interface State {
  reviewModel?: ReviewModel;
  displayedDiffs: string[];
  replyDialogOpen: boolean;
  draftReplyText: string;
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

export default class ViewPullRequestComponent extends React.Component<ViewPullRequestComponentProps, State> {
  replyBodyTextRef: React.RefObject<HTMLTextAreaElement> = React.createRef();
  replyApprovalCheckRef: React.RefObject<HTMLInputElement> = React.createRef();

  state: State = {
    displayedDiffs: [],
    replyDialogOpen: false,
    draftReplyText: "",
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
        console.log(r);
        this.setState({ reviewModel: ReviewModel.fromResponse(r) });
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

  renderReviewers(reviewers: readonly github.Reviewer[]) {
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

  handleCreateComment(comment: CommentModel) {
    if (!this.state.reviewModel || this.state.pendingRequest) {
      return;
    }

    const req = new github.CreateGithubPullRequestCommentRequest({
      owner: this.props.owner,
      repo: this.props.repo,
      pullId: this.state.reviewModel.getPullId(),
      path: comment.getPath(),
      body: comment.getBody(),
      commitSha: comment.getCommitSha(),
      line: Long.fromNumber(comment.getLine()),
      side: comment.getSide(),
    });
    if (comment.isThreadSavedToGithub()) {
      req.threadId = comment.getThreadId();
    }
    if (this.state.reviewModel.isReviewSavedToGithub()) {
      req.reviewId = this.state.reviewModel.getDraftReviewId();
    }
    console.log(req);

    this.setState({ pendingRequest: true });
    rpc_service.service
      .createGithubPullRequestComment(req)
      .then((r) => {
        console.log(r);
        error_service.handleError("posted!");
        if (this.state.reviewModel) {
          if (!r.comment) {
            // TODO(jdhollen): Refresh page? I dunno. This shouldn't happen.
            return;
          }
          const oldId = comment.getId();
          let newModel = this.state.reviewModel;
          if (r.comment) {
            const newComment: CommentModel = CommentModel.fromComment(r.comment);
            if (this.state.reviewModel.getComment(oldId)) {
              newModel = newModel.updateComment(oldId, newComment);
            } else {
              newModel = newModel.addComment(newComment);
            }
            newModel.setDraftReviewId(r.reviewId);
          }
          newModel.removeCommentFromPending(oldId);
          this.setState({ reviewModel: newModel });
        }
      })
      .catch((e) => {
        error_service.handleError(e);
      })
      .finally(() => this.setState({ pendingRequest: false }));
  }

  handleUpdateComment(commentId: string, newBody: string) {
    if (!this.state.reviewModel || this.state.pendingRequest) {
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
        if (this.state.reviewModel) {
          const comment = this.state.reviewModel.getComment(commentId);
          let newModel = this.state.reviewModel;
          if (comment) {
            newModel = newModel.updateComment(commentId, comment.updateBody(newBody));
          }
          newModel = newModel.removeCommentFromPending(commentId);
          this.setState({ reviewModel: newModel });
        }
      })
      .catch((e) => {
        error_service.handleError(e);
      })
      .finally(() => this.setState({ pendingRequest: false }));
  }

  handleDeleteComment(commentId: string) {
    if (!this.state.reviewModel || this.state.pendingRequest) {
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
        if (this.state.reviewModel) {
          let newModel = this.state.reviewModel.deleteComment(commentId).removeCommentFromPending(commentId);
          this.setState({ reviewModel: newModel });
        }
      })
      .catch((e) => {
        error_service.handleError(e);
      })
      .finally(() => this.setState({ pendingRequest: false }));
  }

  handleStartReply(threadId: string) {
    if (this.state.pendingRequest || !this.state.reviewModel) {
      return;
    }

    const threadComments = this.state.reviewModel.getCommentsForThread(threadId);
    if (threadComments.length === 0) {
      return;
    }

    let newModel = this.state.reviewModel;

    let draft: CommentModel | undefined = threadComments.find(
      (c) => c.getReviewId() === this.state.reviewModel!.getDraftReviewId()
    );
    if (draft === undefined) {
      draft = threadComments[0].createReply(
        this.state.reviewModel.getDraftReviewId(),
        this.state.reviewModel.getViewerLogin()
      );
      newModel = newModel.addComment(draft);
    }

    newModel = newModel.setCommentToPending(draft.getId());
    this.setState({ reviewModel: newModel });
  }

  handleCancelComment(id: string) {
    if (!this.state.reviewModel || !this.state.reviewModel.isCommentInProgress(id)) {
      return;
    }

    const comment = this.state.reviewModel.getComment(id);
    if (!comment) {
      return;
    }

    let newModel = this.state.reviewModel.removeCommentFromPending(id);
    // If the comment is a brand new draft, just delete it.
    if (!comment.isSubmittedToGithub()) {
      newModel = newModel.deleteComment(id);
    }

    this.setState({ reviewModel: newModel });
  }

  startComment(side: github.CommentSide, path: string, commitSha: string, lineNumber: number) {
    if (this.state.pendingRequest || !this.state.reviewModel) {
      return;
    }
    const reviewId = this.state.reviewModel.getDraftReviewId();
    const newComment = CommentModel.newComment(reviewId, path, commitSha, lineNumber, side);

    this.setState({
      reviewModel: this.state.reviewModel.addComment(newComment).setCommentToPending(newComment.getId()),
    });
  }

  renderFileDiffs(file: FileModel) {
    if (!this.state.reviewModel) {
      return <></>;
    }
    return (
      <tr className="file-list-diff">
        <td colSpan={6}>
          <FileContentMonacoComponent
            fileModel={file}
            reviewModel={this.state.reviewModel}
            disabled={this.state.pendingRequest}
            handler={this}></FileContentMonacoComponent>
        </td>
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

  renderFileRow(file: FileModel) {
    const path = file.getFullPath();
    let expanded = this.state.displayedDiffs.indexOf(path) !== -1;
    return (
      <>
        <tr className="file-list-row" onClick={this.handleDiffClicked.bind(this, path)}>
          <td className="viewed">
            <input type="checkbox"></input>
          </td>
          <td className="diff-file-name">
            <Link href={router.getReviewUrl(this.props.owner, this.props.repo, +this.props.pull, path)}>{path}</Link>
          </td>
          <td>{file.getCommentCount()}</td>
          <td>{expanded ? "Hide" : "Diff"}</td>
          <td>{file.getAdditions() + file.getDeletions()}</td>
          <td>{this.renderDiffBar(file.getAdditions(), file.getDeletions(), 0)}</td>
        </tr>
        {expanded && this.renderFileDiffs(file)}
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

  renderAnalysisResults(statuses: readonly github.ActionStatus[]) {
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

  getPrStatusClass(r?: ReviewModel) {
    if (!r) {
      return "pending";
    }
    if (r.isSubmitted()) {
      return "submitted";
    } else if (r.isMergeable()) {
      return "approved";
    } else {
      return "pending";
    }
  }

  getPrStatusString(r: ReviewModel) {
    if (r.isSubmitted()) {
      return "Merged";
    } else if (r.isMergeable()) {
      return "Ready to merge";
    } else {
      return "Pending";
    }
  }

  startReviewReply(approveAndSubmitNow: boolean) {
    if (!this.state.reviewModel) {
      return;
    }
    if (!approveAndSubmitNow) {
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
    if (!this.state.reviewModel) {
      return;
    }
    this.setState({ pendingRequest: true });
    const req = new github.SendGithubPullRequestReviewRequest({
      reviewId: this.state.reviewModel.isReviewSavedToGithub() ? this.state.reviewModel.getDraftReviewId() : "",
      pullRequestId: this.state.reviewModel.getPullId(),
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

  submitPr() {
    if (!this.state.reviewModel) {
      return;
    }
    this.setState({ pendingRequest: true });
    const req = new github.MergeGithubPullRequest({
      owner: this.state.reviewModel.getOwner(),
      repo: this.state.reviewModel.getRepo(),
      pullNumber: Long.fromNumber(this.state.reviewModel.getPullNumber()),
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

  replyNotReady() {
    return (this.replyBodyTextRef.current?.value ?? "").length === 0 && !this.state.reviewModel?.hasAnyDraftComments();
  }

  userIsPrAuthor() {
    const viewerLogin = this.state.reviewModel?.getViewerLogin();
    return Boolean(viewerLogin && viewerLogin === this.state.reviewModel?.getAuthor());
  }

  handleReplyTextChange(e: React.ChangeEvent<HTMLTextAreaElement>) {
    this.setState({ draftReplyText: e.target.value });
  }

  renderReplyModal() {
    const reviewModel = this.state.reviewModel;
    if (!reviewModel) {
      return undefined;
    }
    const userIsPrAuthor = this.userIsPrAuthor();
    const draftComments = reviewModel.getDraftReviewComments();
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
                  threadId={c.getThreadId()}
                  reviewId={reviewModel.getDraftReviewId()}
                  viewerLogin={reviewModel.getViewerLogin()}
                  comments={[]}
                  draftComment={c}
                  disabled={Boolean(this.state.pendingRequest)}
                  updating={!c.isSubmittedToGithub()}
                  editing={reviewModel.isCommentInProgress(c.getId())}
                  saving={/* TODO(jdhollen */ false}
                  handler={this}
                  activeUsername={reviewModel.getViewerLogin()}></ReviewThreadComponent>
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

  renderSingleFileView(file: FileModel): JSX.Element | undefined {
    if (!this.state.reviewModel) {
      return undefined;
    }
    return (
      <>
        <div className="single-file-header">
          <div className="single-file-name">{file.getFullPath()}</div>
          <div>
            <Link
              href={router.getReviewUrl(
                this.state.reviewModel.getOwner(),
                this.state.reviewModel.getRepo(),
                this.state.reviewModel.getPullNumber()
              )}>
              BACK
            </Link>
          </div>
        </div>
        <FileContentMonacoComponent
          fileModel={file}
          reviewModel={this.state.reviewModel}
          disabled={this.state.pendingRequest}
          handler={this}></FileContentMonacoComponent>
      </>
    );
  }

  renderReviewLandingPage(model: ReviewModel): JSX.Element {
    return (
      <>
        <div className="summary-section">
          <div className="review-cell">
            <div className="attr-grid">
              <div className="attr-label">Reviewers</div>
              <div>{this.renderReviewers(model.getReviewers())}</div>
              <div className="attr-label">Issues</div>
              <div></div>
              <div className="attr-label">Mentions</div>
              <div></div>
              <div></div>
            </div>
          </div>
          <div className="review-cell">
            <div className="description">
              {model.getTitle()}
              <br />
              <br />
              {model.getBody()}
            </div>
          </div>
          <div className="review-cell">
            <div className="attr-grid">
              <div className="attr-label">Created</div>
              <div>{format.formatTimestampUsec(model.getCreatedAtUsec())}</div>
              <div className="attr-label">Modified</div>
              <div>{format.formatTimestampUsec(model.getUpdatedAtUsec())}</div>
              <div className="attr-label">Branch</div>
              <div>{model.getBranch()}</div>
            </div>
          </div>
          <div className="review-cell">
            <div className="attr-grid">
              <div className="attr-label">Status</div>
              <div>{this.getPrStatusString(model)}</div>
              <div className="attr-label">Analysis</div>
              <div>{this.renderAnalysisResults(model.getActionStatuses())}</div>
            </div>
          </div>
          <div className="review-cell header">Files</div>
          <div className="review-cell header"></div>
        </div>
        <div className="file-section">
          <table>
            {this.renderFileHeader()}
            {model.getFiles().map((f) => this.renderFileRow(f))}
          </table>
        </div>
        {this.renderReplyModal()}
      </>
    );
  }

  renderPageContent(): JSX.Element | undefined {
    if (this.state.reviewModel === undefined) {
      return undefined;
    }
    const pathParts = this.props.path.split("/");
    let filePath: string | undefined = undefined;

    if (pathParts.length > 5) {
      filePath = pathParts.slice(5).join("/");
      const file = this.state.reviewModel.getFile(filePath);
      if (file !== undefined) {
        return this.renderSingleFileView(file);
      } else {
        error_service.handleError("File not found.");
        // Fall through to landing page below.
      }
    }

    return this.renderReviewLandingPage(this.state.reviewModel);
  }

  render() {
    const pageContent = this.renderPageContent();
    if (!this.state.reviewModel) {
      // TODO(jdhollen): Error state.
      return <></>;
    }

    return (
      <div className={"pr-view " + this.getPrStatusClass(this.state.reviewModel)}>
        <PullRequestHeaderComponent
          reviewModel={this.state.reviewModel}
          path={this.props.path}
          controller={this}></PullRequestHeaderComponent>
        {pageContent}
      </div>
    );
  }
}
