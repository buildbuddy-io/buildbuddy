import Long from "long";
import { github } from "../../../proto/github_ts_proto";

const FAKE_ID_PREFIX = "bb-tmp/";
let fakeReviewIdCounter = 0;
function newFakeId(): string {
  fakeReviewIdCounter++;
  return FAKE_ID_PREFIX + fakeReviewIdCounter;
}

export class CommentModel {
  private comment: github.Comment;

  private constructor(comment: github.Comment) {
    this.comment = comment;
  }

  getId(): string {
    return this.comment.id;
  }

  getReviewId(): string {
    return this.comment.reviewId;
  }

  getPath(): string {
    return this.comment.path;
  }

  getCommitSha(): string {
    return this.comment.commitSha;
  }

  getThreadId(): string {
    return this.comment.threadId;
  }

  getBody(): string {
    return this.comment.body;
  }

  getCreatedAtUsec(): number {
    return +this.comment.createdAtUsec;
  }

  getCommenter(): string {
    return this.comment.commenter?.login || "unknown commenter";
  }

  isBot(): boolean {
    return Boolean(this.comment.commenter?.bot);
  }

  getLine(): number {
    return +(this.comment.position?.startLine || this.comment.position?.endLine || 0);
  }

  getSide(): github.CommentSide {
    return this.comment.position?.side ?? github.CommentSide.RIGHT_SIDE;
  }

  isThreadSavedToGithub(): boolean {
    return Boolean(this.comment.threadId && !this.comment.threadId.startsWith(FAKE_ID_PREFIX));
  }

  isSubmittedToGithub(): boolean {
    return this.comment.id !== "" && !this.comment.id.startsWith(FAKE_ID_PREFIX);
  }

  // Careful! This is only a shallow copy, so if you are mucking with nested fields you need
  // to actually set them (or else you'll mess with other people holding a ref to this proto)
  // TODO(jdhollen): Maybe, someday, make this actually a deep copy.
  private toProtoComment(): github.Comment {
    return new github.Comment(this.comment);
  }

  public updateBody(newBody: string): CommentModel {
    const proto = this.toProtoComment();
    proto.body = newBody;
    return new CommentModel(proto);
  }

  createReply(reviewId: string, author: string): CommentModel {
    const newComment = this.toProtoComment();
    newComment.id = newFakeId();
    newComment.reviewId = reviewId;
    newComment.body = "";
    newComment.commenter = new github.ReviewUser({ login: author || "you" });
    newComment.createdAtUsec = Long.fromNumber(Date.now() * 1000);
    newComment.isResolved = false;

    return CommentModel.fromComment(newComment);
  }

  static fromComment(c: github.Comment) {
    return new CommentModel(new github.Comment(c));
  }

  static newComment(
    reviewId: string,
    path: string,
    commitSha: string,
    lineNumber: number,
    side: github.CommentSide
  ): CommentModel {
    return CommentModel.fromComment(
      new github.Comment({
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
      })
    );
  }
}

export class FileModel {
  private file: github.FileSummary;

  private constructor(f: github.FileSummary) {
    this.file = f;
  }

  getFullPath(): string {
    return this.file.name;
  }

  getPatch(): string {
    return this.file.patch;
  }

  getAdditions(): number {
    return +this.file.additions;
  }

  getDeletions(): number {
    return +this.file.deletions;
  }

  getCommentCount(): number {
    return +this.file.comments;
  }

  getCommitSha(): string {
    return this.file.commitSha;
  }

  static fromFileSummary(f: github.FileSummary) {
    return new FileModel(new github.FileSummary(f));
  }
}

interface ThreadAndDraft {
  threadId: string;
  comments: CommentModel[];
  draft?: CommentModel;
}

export class ThreadModel {
  private threadId: string;
  private comments: readonly CommentModel[];
  private draft?: CommentModel;

  private constructor({ threadId, comments, draft }: ThreadAndDraft) {
    this.threadId = threadId;
    this.comments = [...comments];
    this.draft = draft;
  }

  getComments(): readonly CommentModel[] {
    return this.comments;
  }

  hasDraft(): boolean {
    return this.draft !== undefined;
  }

  getDraft(): CommentModel | undefined {
    return this.draft;
  }

  getId(): string {
    return this.threadId;
  }

  static threadsFromComments(comments: readonly CommentModel[], draftReviewId?: string): Map<string, ThreadModel> {
    const threads: Map<string, ThreadAndDraft> = new Map();
    comments.forEach((c) => {
      const thread = c.getThreadId();
      let threadAndDraft = threads.get(thread);
      if (!threadAndDraft) {
        threadAndDraft = {
          threadId: thread,
          comments: [],
        };
        threads.set(thread, threadAndDraft);
      }
      if (draftReviewId && c.getReviewId() === draftReviewId) {
        threadAndDraft.draft = c;
      } else {
        threadAndDraft.comments.push(c);
      }
    });

    const result = new Map<string, ThreadModel>();
    threads.forEach((v, k) => result.set(k, new ThreadModel(v)));

    return result;
  }
}

// This little nightmare makes it easier to modify the otherwise-immutable
// state of a model: we can just make a copy and replace the necessary fields
// instead of exposing a modifiable structure to callers.
interface State {
  draftReviewId: string;
  title: string;
  body: string;
  owner: string;
  repo: string;
  pullNumber: number;
  pullId: string;
  branch: string;
  githubUrl: string;
  viewerLogin: string;
  author: string;
  submitted: boolean;
  mergeable: boolean;
  createdAtUsec: number;
  updatedAtUsec: number;
  files: readonly FileModel[];
  comments: readonly CommentModel[];
  actionStatuses: readonly github.ActionStatus[];
  reviewers: readonly github.Reviewer[];
  inProgressComments: Set<string>;
}

export class ReviewModel {
  private state: State;

  private constructor(state: State) {
    this.state = state;
  }

  getDraftReviewId(): string {
    return this.state.draftReviewId;
  }

  isCommentInProgress(id?: string): boolean {
    return Boolean(id && this.state.inProgressComments.has(id));
  }

  isReviewSavedToGithub(): boolean {
    return this.state.draftReviewId !== "" && !this.state.draftReviewId.startsWith(FAKE_ID_PREFIX);
  }
  getOwner(): string {
    return this.state.owner;
  }

  getRepo(): string {
    return this.state.repo;
  }

  getPullNumber(): number {
    return this.state.pullNumber;
  }

  getPullId(): string {
    return this.state.pullId;
  }

  getGithubUrl(): string {
    return this.state.githubUrl;
  }

  getCreatedAtUsec(): number {
    return this.state.createdAtUsec;
  }

  getUpdatedAtUsec(): number {
    return this.state.updatedAtUsec;
  }

  getTitle(): string {
    return this.state.title;
  }

  getBody(): string {
    return this.state.body;
  }

  getBranch(): string {
    return this.state.branch;
  }

  isSubmitted(): boolean {
    return this.state.submitted;
  }

  isMergeable(): boolean {
    return this.state.mergeable;
  }

  getDraftReviewComments(): readonly CommentModel[] {
    return !this.state.draftReviewId
      ? []
      : this.state.comments.filter((v) => v.getReviewId() === this.state.draftReviewId);
  }

  hasAnyDraftComments(): boolean {
    return (
      this.state.draftReviewId !== "" &&
      this.state.comments.filter((v) => v.getReviewId() === this.state.draftReviewId).length > 0
    );
  }

  getComment(commentId: string): CommentModel | undefined {
    return this.state.comments.find((c) => c.getId() === commentId);
  }

  getCommentsForFile(path: string, commitSha: string): readonly CommentModel[] {
    // TODO(jdhollen): filter on commitSha.
    return this.state.comments.filter((v) => v.getPath() === path);
  }

  getCommentsForThread(threadId: string): readonly CommentModel[] {
    return this.state.comments.filter((v) => v.getThreadId() === threadId);
  }

  getViewerLogin(): string {
    return this.state.viewerLogin;
  }

  getAuthor(): string {
    return this.state.author;
  }

  getFiles(): readonly FileModel[] {
    return this.state.files;
  }

  getFile(path: string): FileModel | undefined {
    return this.state.files.find((f) => f.getFullPath() === path);
  }

  // TODO(jdhollen): This needs to be fully locked down / unmodifiable.
  getReviewers(): readonly github.Reviewer[] {
    return this.state.reviewers;
  }

  // TODO(jdhollen): This needs to be fully locked down / unmodifiable.
  getActionStatuses(): readonly github.ActionStatus[] {
    return this.state.actionStatuses;
  }

  viewerIsPrAuthor(): boolean {
    return this.state.author === this.state.viewerLogin;
  }

  private stateCopy(): State {
    // TODO(jdhollen): Do a better deep copy once all fields are unmodifiable.
    const copy = { ...this.state };
    copy.inProgressComments = new Set(copy.inProgressComments);
    return copy;
  }

  addComment(newComment: CommentModel): ReviewModel {
    const newState = this.stateCopy();
    const newComments = [...newState.comments];
    newComments.push(newComment);
    newState.comments = newComments;
    return new ReviewModel(newState);
  }

  updateComment(commentId: string, newComment: CommentModel): ReviewModel {
    const newState = this.stateCopy();
    const newComments = [...newState.comments];
    const oldCommentIndex = newComments.findIndex((c) => c.getId() === commentId) ?? -1;
    if (oldCommentIndex === -1) {
      newComments.push(newComment);
    } else {
      newComments.splice(oldCommentIndex, 1, newComment);
    }
    newState.comments = newComments;

    return new ReviewModel(newState);
  }

  deleteComment(commentId: string): ReviewModel {
    const newState = this.stateCopy();
    const newComments = [...newState.comments];
    const oldCommentIndex = newComments.findIndex((c) => c.getId() === commentId) ?? -1;
    if (oldCommentIndex !== -1) {
      newComments.splice(oldCommentIndex, 1);
    }
    newState.comments = newComments;
    return new ReviewModel(newState);
  }

  setCommentToPending(commentId: string): ReviewModel {
    if (this.isCommentInProgress(commentId)) {
      return this;
    }
    const newState = this.stateCopy();
    newState.inProgressComments.add(commentId);
    return new ReviewModel(newState);
  }

  removeCommentFromPending(commentId: string) {
    if (!this.isCommentInProgress(commentId)) {
      return this;
    }
    const newState = this.stateCopy();
    newState.inProgressComments.delete(commentId);
    return new ReviewModel(newState);
  }

  setDraftReviewId(draftReviewId: string) {
    if (this.state.draftReviewId === draftReviewId) {
      return this;
    }
    const newState = this.stateCopy();
    newState.draftReviewId = draftReviewId;
    return new ReviewModel(newState);
  }

  static fromResponse(response: github.GetGithubPullRequestDetailsResponse): ReviewModel {
    const reviewers = [...response.reviewers];
    reviewers.sort((a, b) => (a.login.toLowerCase() < b.login.toLowerCase() ? -1 : 1));
    return new ReviewModel({
      draftReviewId: response.draftReviewId ? response.draftReviewId : newFakeId(),
      title: response.title,
      body: response.body,
      owner: response.owner,
      repo: response.repo,
      pullNumber: +response.pull,
      pullId: response.pullId,
      branch: response.branch,
      submitted: response.submitted,
      mergeable: response.mergeable,
      createdAtUsec: +response.createdAtUsec,
      updatedAtUsec: +response.updatedAtUsec,
      reviewers,
      author: response.author,
      comments: response.comments.map(CommentModel.fromComment),
      files: response.files.map(FileModel.fromFileSummary),
      githubUrl: response.githubUrl,
      viewerLogin: response.viewerLogin,
      actionStatuses: [...response.actionStatuses],
      inProgressComments: new Set<string>(),
    });
  }
}
