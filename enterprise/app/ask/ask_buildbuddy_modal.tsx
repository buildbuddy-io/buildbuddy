import React from "react";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";
import { RepoURL } from "../../../app/util/git";

export interface AskBuildBuddyRepository {
  repoUrl: string;
  commitSha: string;
  branch: string;
}

export interface AskBuildBuddyRequest {
  question: string;
  invocationIds: string[];
  repository?: AskBuildBuddyRepository;
}

interface Props {
  isOpen: boolean;
  invocationIds: string[];
  onRequestClose: () => void;
  onIncludeRepository: (invocationIds: string[], promptToLink: boolean) => Promise<AskBuildBuddyRepository | undefined>;
  onSubmit: (request: AskBuildBuddyRequest) => void;
}

interface State {
  question: string;
  includedInvocationIds: Set<string>;
  repository?: AskBuildBuddyRepository;
  repositoryLoading: boolean;
}

const genericSuggestions = ["How does remote execution work?", "How does BuildBuddy caching work?"];

const invocationSuggestions = ["Fix this build.", "Why is this build slow?", "Why did this build fail?"];

export default class AskBuildBuddyModal extends React.Component<Props, State> {
  state: State = {
    question: "",
    includedInvocationIds: new Set(),
    repositoryLoading: false,
  };

  private questionRef = React.createRef<HTMLTextAreaElement>();

  componentDidUpdate(prevProps: Props) {
    if (this.props.isOpen && !prevProps.isOpen) {
      this.setState(
        {
          question: "",
          includedInvocationIds: new Set(this.props.invocationIds),
          repository: undefined,
          repositoryLoading: false,
        },
        () => {
          this.includeRepositoryByDefault();
        }
      );
    }
  }

  private afterOpen() {
    this.focusQuestion();
    this.includeRepositoryByDefault();
  }

  private includeRepositoryByDefault() {
    if (!this.props.invocationIds.length || this.state.repository || this.state.repositoryLoading) {
      return;
    }
    this.includeRepository(false);
  }

  private focusQuestion() {
    this.questionRef.current?.focus();
  }

  private setQuestion(question: string) {
    this.setState({ question }, () => this.focusQuestion());
  }

  private removeInvocation(invocationId: string) {
    const includedInvocationIds = new Set(this.state.includedInvocationIds);
    includedInvocationIds.delete(invocationId);
    this.setState({ includedInvocationIds });
  }

  private includeCurrentInvocations() {
    this.setState({ includedInvocationIds: new Set(this.props.invocationIds) });
  }

  private async includeRepository(promptToLink: boolean) {
    this.setState({ repositoryLoading: true });
    try {
      const repository = await this.props.onIncludeRepository(this.props.invocationIds, promptToLink);
      if (repository) {
        this.setState({ repository });
      }
    } finally {
      this.setState({ repositoryLoading: false });
    }
  }

  private submit() {
    const question = this.state.question.trim();
    if (!question) return;
    this.props.onSubmit({
      question,
      invocationIds: this.props.invocationIds.filter((id) => this.state.includedInvocationIds.has(id)),
      repository: this.state.repository,
    });
  }

  private handleQuestionKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      this.submit();
    }
  }

  render() {
    const suggestions = this.props.invocationIds.length ? invocationSuggestions : genericSuggestions;
    const includedInvocationIds = this.props.invocationIds.filter((id) => this.state.includedInvocationIds.has(id));

    return (
      <Modal
        isOpen={this.props.isOpen}
        onAfterOpen={this.afterOpen.bind(this)}
        onRequestClose={this.props.onRequestClose}>
        <Dialog className="ask-buildbuddy-dialog">
          <DialogHeader>
            <DialogTitle>Ask BuildBuddy</DialogTitle>
          </DialogHeader>
          <DialogBody>
            <p className="ask-buildbuddy-description">Ask a question about this build, BuildBuddy, or Bazel.</p>
            <textarea
              ref={this.questionRef}
              className="ask-buildbuddy-question"
              placeholder="What would you like to know?"
              value={this.state.question}
              onChange={(event) => this.setState({ question: event.target.value })}
              onKeyDown={this.handleQuestionKeyDown.bind(this)}
            />
            <div className="ask-buildbuddy-suggestions">
              {suggestions.map((suggestion) => (
                <button
                  key={suggestion}
                  className="ask-buildbuddy-suggestion"
                  onClick={() => this.setQuestion(suggestion)}>
                  {suggestion}
                </button>
              ))}
            </div>
            {this.props.invocationIds.length > 0 && (
              <div className="ask-buildbuddy-context">
                <div className="ask-buildbuddy-context-label">Context</div>
                <div className="ask-buildbuddy-context-items">
                  {includedInvocationIds.map((invocationId) => (
                    <button
                      key={invocationId}
                      className="ask-buildbuddy-context-chip"
                      title={invocationId}
                      onClick={() => this.removeInvocation(invocationId)}>
                      Invocation {invocationId} <span aria-hidden="true">×</span>
                    </button>
                  ))}
                  {includedInvocationIds.length < this.props.invocationIds.length && (
                    <button
                      className="ask-buildbuddy-include-context"
                      onClick={this.includeCurrentInvocations.bind(this)}>
                      + Include current {this.props.invocationIds.length > 1 ? "invocations" : "invocation"}
                    </button>
                  )}
                  {this.state.repository ? (
                    <button
                      className="ask-buildbuddy-context-chip"
                      title={this.state.repository.repoUrl}
                      onClick={() => this.setState({ repository: undefined })}>
                      Repository {repositoryName(this.state.repository.repoUrl)} <span aria-hidden="true">×</span>
                    </button>
                  ) : (
                    <button
                      className="ask-buildbuddy-include-context"
                      disabled={this.state.repositoryLoading}
                      onClick={() => this.includeRepository(true)}>
                      {this.state.repositoryLoading ? "Including repository…" : "+ Include repository"}
                    </button>
                  )}
                </div>
              </div>
            )}
          </DialogBody>
          <DialogFooter>
            <div className="ask-buildbuddy-submit-hint">⌘/Ctrl + Enter</div>
            <DialogFooterButtons>
              <OutlinedButton onClick={this.props.onRequestClose}>Cancel</OutlinedButton>
              <Button disabled={!this.state.question.trim()} onClick={this.submit.bind(this)}>
                Ask
              </Button>
            </DialogFooterButtons>
          </DialogFooter>
        </Dialog>
      </Modal>
    );
  }
}

function repositoryName(repoUrl: string): string {
  return RepoURL.parse(repoUrl)?.repo || repoUrl;
}
