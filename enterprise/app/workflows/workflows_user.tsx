import React from "react";
import { User } from "../../../app/auth/auth_service";
import { OutlinedLinkButton } from "../../../app/components/button/link_button";
import router from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { workflow } from "../../../proto/workflow_ts_proto";
import WorkflowsZeroStateAnimation from "./zero_state";
import { GitMerge } from "lucide-react";
import capabilities from "../../../app/capabilities/capabilities";
import error_service from "../../../app/errors/error_service";
import { normalizeRepoURL } from "../../../app/util/git";
import { Link } from "../../../app/components/link/link";
import ActionListComponent from "./action_list";

export type WorkflowsUserProps = {
  user: User;
};

type State = {
  workflowHistoryLoading: boolean;
  workflowHistoryResponse: workflow.GetWorkflowHistoryResponse | null;
};

export default class WorkflowsUserComponent extends React.Component<WorkflowsUserProps> {
  state: State = {
    workflowHistoryLoading: false,
    workflowHistoryResponse: null,
  };
  private fetchWorkflowHistoryRPC?: CancelablePromise;

  componentDidMount() {
    document.title = "Workflows | BuildBuddy";
    this.fetch();
  }

  componentDidUpdate(prevProps: WorkflowsUserProps) {
    if (this.props.user !== prevProps.user) {
      this.fetch();
    }
  }

  private fetch() {
    this.fetchWorkflowHistoryRPC?.cancel();
    this.fetchWorkflowHistoryRPC = undefined;
    this.setState({ workflowHistoryLoading: false, workflowHistoryResponse: null });

    if (!capabilities.config.workflowHistoryEnabled) {
      return;
    }
    this.setState({ workflowHistoryLoading: true });
    this.fetchWorkflowHistoryRPC = rpcService.service
      .getWorkflowHistory(new workflow.GetWorkflowHistoryRequest({}))
      .then((response) => this.setState({ workflowHistoryResponse: response }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ workflowHistoryLoading: false }));
  }

  renderActionList(history: workflow.GetWorkflowHistoryResponse.WorkflowHistory): JSX.Element | null {
    if (history.actionHistory.length > 0) {
      return <ActionListComponent repoUrl={history.repoUrl} history={history.actionHistory}></ActionListComponent>;
    } else {
      return null;
    }
  }

  render() {
    if (this.state.workflowHistoryLoading) {
      return <div className="loading" />;
    }
    return (
      <div className="workflows-page">
        <div className="shelf">
          <div className="container">
            <div>
              <div className="breadcrumbs">
                {this.props.user && <span>{this.props.user?.selectedGroupName()}</span>}
                <span>Workflows</span>
              </div>
              <div className="title">Workflows</div>
            </div>
            {Boolean(this.state.workflowHistoryResponse?.workflowHistory?.length) && (
              <div className="buttons create-new-container">
                <OutlinedLinkButton href="https://docs.buildbuddy.io/docs/workflows-setup" target="_blank">
                  Learn more
                </OutlinedLinkButton>
              </div>
            )}
          </div>
        </div>
        <div className="content">
          {!Boolean(this.state.workflowHistoryResponse?.workflowHistory?.length) && (
            <div className="no-workflows-container">
              <div className="no-workflows-card">
                <WorkflowsZeroStateAnimation />
                <div className="details">
                  <div>
                    Workflows automatically build and test your code with BuildBuddy when commits are pushed to your
                    repo. Contact your organization's admin about enabling workflows.
                  </div>
                  <div className="buttons">
                    <OutlinedLinkButton href="https://docs.buildbuddy.io/docs/workflows-setup" target="_blank">
                      Learn more
                    </OutlinedLinkButton>
                  </div>
                </div>
              </div>
            </div>
          )}
          {Boolean(this.state.workflowHistoryResponse?.workflowHistory?.length) && (
            <div className="workflows-list">
              {this.state.workflowHistoryResponse?.workflowHistory.map((history) => (
                <>
                  <div className="workflow-item container">
                    <div className="workflow-item-column">
                      <div className="workflow-item-row">
                        <GitMerge />
                        <div>
                          <Link
                            href={router.getWorkflowHistoryUrl(normalizeRepoURL(history.repoUrl))}
                            className="repo-url">
                            {formatURL(history.repoUrl)}
                          </Link>
                        </div>
                      </div>
                    </div>
                  </div>
                  {this.renderActionList(history)}
                </>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  }
}

function formatURL(url: string) {
  return normalizeRepoURL(url).replace(/^https:\/\//, "");
}
