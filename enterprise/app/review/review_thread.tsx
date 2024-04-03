import React from "react";
import { github } from "../../../proto/github_ts_proto";
import moment from "moment";

interface CommentEditHandler {
  handleCreateComment: (comment: github.Comment) => void;
  handleUpdateComment: (commentId: string, newBody: string) => void;
  handleDeleteComment: (commentId: string) => void;
  handleStartReply: (threadId: string) => void;
  handleCancelComment: (commentId: string) => void;
}

interface ReviewThreadComponentProps {
  threadId: string;
  comments: github.Comment[];
  draftComment?: github.Comment;
  updating: boolean;
  disabled: boolean;
  editing: boolean;
  saving: boolean;
  handler: CommentEditHandler;
  activeUsername: string;
}

export default class ReviewThreadComponent extends React.Component<ReviewThreadComponentProps> {
  commentTextRef: React.RefObject<HTMLTextAreaElement> = React.createRef();

  handleCreate(commentBody: string) {
    commentBody = commentBody.trim();
    if (commentBody === "") {
      return;
    }
    // TODO(jdhollen): This is probably a little janky if Github cares about the replyTo fields etc.
    const commentToSubmit = new github.Comment(this.props.draftComment ?? this.props.comments[0]);
    if (!this.props.draftComment) {
      // commentToSubmit.position = undefined;
      commentToSubmit.id = "";
    }
    commentToSubmit.body = commentBody;
    this.props.handler.handleCreateComment(commentToSubmit);
  }

  handleUpdate() {
    if (!this.props.draftComment?.id || !this.commentTextRef.current?.value) {
      return;
    }
    this.props.handler.handleUpdateComment(this.props.draftComment.id, this.commentTextRef.current.value);
  }

  handleDelete() {
    if (!this.props.draftComment?.id) {
      return;
    }
    this.props.handler.handleDeleteComment(this.props.draftComment.id);
  }

  handleCancel() {
    if (!this.props.draftComment?.id) {
      return;
    }
    this.props.handler.handleCancelComment(this.props.draftComment.id);
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

    const isBot = Boolean(comments.length === 1 && comments[0].commenter?.bot);

    return (
      <div className="thread">
        {comments.map((c) => {
          return (
            <>
              <div className="thread-comment">
                <div className="comment-author">
                  <div className="comment-author-text">{c.commenter?.login}</div>
                  <div className="comment-timestamp">{moment(+c.createdAtUsec / 1000).format("HH:mm, MMM DD")}</div>
                </div>
                <div className="comment-body">{c.body}</div>
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
              <div className="comment-body">
                <textarea
                  disabled={this.props.disabled}
                  autoFocus
                  ref={this.commentTextRef}
                  className="comment-input"
                  defaultValue={draft.body}></textarea>
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
