import { github } from "../../../proto/github_ts_proto";
import { CommentModel } from "./review_model";

export interface ReviewController {
  startComment: (side: github.CommentSide, path: string, commitSha: string, lineNumber: number) => void;
  startReviewReply: (approveAndSubmitNow: boolean) => void;
  submitPr: () => void;
  handleCreateComment: (comment: CommentModel) => void;
  handleUpdateComment: (commentId: string, newBody: string) => void;
  handleDeleteComment: (commentId: string) => void;
  handleStartReply: (threadId: string) => void;
  handleCancelComment: (commentId: string) => void;
}
