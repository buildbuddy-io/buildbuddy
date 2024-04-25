import React from "react";
import moment from "moment";
import { CommentModel } from "./review_model";
import { ReviewController } from "./review_controller";

interface ReviewThreadComponentProps {
  threadId: string;
  reviewId: string;
  viewerLogin: string;
  comments: readonly CommentModel[];
  draftComment?: CommentModel;
  updating: boolean;
  disabled: boolean;
  editing: boolean;
  saving: boolean;
  handler: ReviewController;
  activeUsername: string;
}

export default class ReviewThreadComponent extends React.Component<ReviewThreadComponentProps> {
  commentTextRef: React.RefObject<HTMLTextAreaElement> = React.createRef();

  handleCreate(commentBody: string) {
    commentBody = commentBody.trim();
    if (commentBody === "") {
      return;
    }
    let commentToSubmit: CommentModel;
    if (this.props.draftComment) {
      commentToSubmit = this.props.draftComment.updateBody(commentBody);
    } else {
      commentToSubmit = this.props.comments[0]
        .createReply(this.props.reviewId, this.props.viewerLogin)
        .updateBody(commentBody);
    }
    this.props.handler.handleCreateComment(commentToSubmit);
  }

  handleUpdate() {
    if (!this.props.draftComment || !this.commentTextRef.current?.value) {
      return;
    }
    this.props.handler.handleUpdateComment(this.props.draftComment.getId(), this.commentTextRef.current.value);
  }

  handleDelete() {
    if (!this.props.draftComment?.getId()) {
      return;
    }
    this.props.handler.handleDeleteComment(this.props.draftComment.getId());
  }

  handleCancel() {
    if (!this.props.draftComment?.getId()) {
      return;
    }
    this.props.handler.handleCancelComment(this.props.draftComment.getId());
  }

  handleStartReply() {
    this.props.handler.handleStartReply(this.props.threadId);
  }

  render() {
    const comments = [...this.props.comments];
    const draft = this.props.draftComment;
    const hasDraft = draft !== undefined;

    if (draft !== undefined && !this.props.editing) {
      comments.push(draft);
    }

    if (comments.length == 0 && !draft) {
      // Shouldn't happen, but fine.
      return <></>;
    }

    const isBot = Boolean(comments.length === 1 && comments[0].isBot());

    return (
      <div className="thread">
        {comments.map((c) => {
          return (
            <>
              <div className="thread-comment">
                <div className="comment-author">
                  <div className="comment-author-text">{c.getCommenter()}</div>
                  <div className="comment-timestamp">
                    {moment(+c.getCreatedAtUsec() / 1000).format("HH:mm, MMM DD")}
                  </div>
                </div>
                <div className="comment-body">{c.getBody()}</div>
              </div>
              <div className="comment-divider"></div>
            </>
          );
        })}
        {this.props.editing && draft && (
          <div className="thread-reply">
            <div className="thread-comment">
              <div className="comment-author">
                <div className="comment-author-text">{this.props.activeUsername}</div>
                <div className="comment-timestamp">Draft</div>
              </div>
              {/* Careful! This ID is used to find and focus comment-input. */}
              <div className="comment-body" id={draft.getId()}>
                <textarea
                  disabled={this.props.disabled}
                  autoFocus
                  ref={this.commentTextRef}
                  className="comment-input"
                  defaultValue={draft.getBody()}></textarea>
              </div>
            </div>
          </div>
        )}
        <div className="reply-bar">
          {this.props.editing && (
            <>
              <span
                className="reply-fake-link"
                onClick={() =>
                  this.props.updating
                    ? this.handleUpdate()
                    : this.handleCreate(this.commentTextRef.current?.value ?? "")
                }>
                Save draft
              </span>
              <span className="reply-fake-link" onClick={() => this.handleCancel()}>
                Cancel
              </span>
            </>
          )}
          {!this.props.editing && (
            <>
              <span className="resolution-pill-box">
                <span className={isBot ? "resolution-pill resolved" : "resolution-pill unresolved"}>
                  {isBot ? "Automated" : hasDraft ? "Draft" : "Unresolved"}
                </span>
              </span>
              {isBot && (
                <>
                  <span className="reply-fake-link" onClick={() => this.handleStartReply()}>
                    {hasDraft ? "Edit" : "Reply"}
                  </span>
                  {!hasDraft && (
                    <span className="reply-fake-link" onClick={() => this.handleCreate("Please fix.")}>
                      Please fix
                    </span>
                  )}
                </>
              )}
              {!isBot && (
                <>
                  <span className="reply-fake-link" onClick={() => this.handleStartReply()}>
                    {hasDraft ? "Edit" : "Reply"}
                  </span>
                  {!hasDraft && (
                    <>
                      <span className="reply-fake-link" onClick={() => this.handleCreate("Done.")}>
                        Done
                      </span>
                      <span className="reply-fake-link" onClick={() => this.handleCreate("Acknowledged.")}>
                        Ack
                      </span>
                    </>
                  )}
                  {hasDraft && (
                    <>
                      <span className="reply-fake-link" onClick={() => this.handleDelete()}>
                        Discard
                      </span>
                    </>
                  )}
                </>
              )}
            </>
          )}
        </div>
      </div>
    );
  }
}
