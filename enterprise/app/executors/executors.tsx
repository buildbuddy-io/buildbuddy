import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { User } from "../../../app/auth/auth_service";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import ExecutorCardComponent from "./executor_card";
import { Subscription } from "rxjs";

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State {
  nodes: scheduler.ExecutionNode[];
  loading: boolean;
}

export default class ExecutorsComponent extends React.Component<Props> {
  state: State = {
    nodes: [],
    loading: true,
  };

  subscription: Subscription;

  componentWillMount() {
    document.title = `Executors | BuildBuddy`;
    this.fetch();

    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetch(),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.hash !== prevProps.hash || this.props.search != prevProps.search) {
      this.fetch();
    }
  }

  fetch() {
    if (!this.props.user || !this.props.user.selectedGroup) return

    try {
      let request = new scheduler.GetExecutionNodesRequest();
      request.groupId = this.props.user.selectedGroup.id

      this.setState({ ...this.state, loading: true });
      rpcService.service.getExecutionNodes(request).then((response) => {
        let nodeList = response.executionNode;
        this.setState({
          ...this.state,
          nodes: nodeList,
          loading: false,
        });
      });
    } catch (error) {
      this.setState({ error: BuildBuddyError.parse(error) });
    }
  }

  render() {
    return (
      <div className="executors-page">
        <div className="shelf">
          <div className="container">
            <div className="title">Executors</div>
          </div>
        </div>
        {this.state.nodes.length == 0 && (
          <div className="container narrow">
            <div className="empty-state history">
              <h2>No remote execution nodes found!</h2>
              <p>
                Check out our documentation on deploying remote executors:
                <br />
                <br />
                <a className="button" href="https://docs.buildbuddy.io/docs/enterprise-rbe">
                  Click here for more information
                </a>
              </p>
            </div>
          </div>
        )}
        {this.state.nodes.length > 0 && (
          <div className="executor-cards">
            {this.state.nodes.map((node) => (
              <ExecutorCardComponent node={node} />
            ))}
          </div>
        )}
        {this.state.loading && <div className="loading"></div>}
      </div>
    );
  }
}
