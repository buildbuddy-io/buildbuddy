import React from "react";

import InvocationModel from "./invocation_model";
import TargetsCardComponent from "./invocation_targets_card";
import { XCircle, CheckCircle, HelpCircle, Clock, SkipForward } from "lucide-react";
import { api as api_common } from "../../proto/api/v1/common_ts_proto";
import { target } from "../../proto/target_ts_proto";
import TargetGroupCard from "./invocation_target_group_card";
import rpc_service, { CancelablePromise } from "../service/rpc_service";

interface Props {
  model: InvocationModel;
  pageSize: number;
  filter: string;
  mode: "passing" | "failing";
}

interface State {
  searchLoading: boolean;
  searchResponse?: target.GetTargetResponse;
}

const Status = api_common.v1.Status;

const STATUS_ORDERING = [
  Status.FAILED,
  Status.FAILED_TO_BUILD,
  Status.TIMED_OUT,
  Status.FLAKY,
  Status.PASSED,
  Status.BUILT,
  Status.SKIPPED,
  // TODO: Render the following status types somewhere?
  // INCOMPLETE, BUILDING, TESTING, TOOL_FAILED, CANCELLED
];

const OK_STATUSES = new Set([Status.PASSED, Status.BUILT, Status.SKIPPED]);

export default class TargetsComponent extends React.Component<Props, State> {
  state: State = {
    searchLoading: false,
  };

  componentDidMount() {
    if (this.props.filter) {
      this.search();
    }
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.filter !== prevProps.filter) {
      this.search();
    }
  }

  private searchRPC?: CancelablePromise;

  private search() {
    this.searchRPC?.cancel();
    this.setState({ searchLoading: true });
    if (!this.props.filter) {
      this.setState({ searchLoading: false, searchResponse: undefined });
      return;
    }
    this.searchRPC = rpc_service.service
      .getTarget({
        invocationId: this.props.model.getInvocationId(),
        filter: this.props.filter,
      })
      .then((response) => this.setState({ searchResponse: response }))
      .finally(() => this.setState({ searchLoading: false }));
  }

  private renderTargetGroup(group: target.TargetGroup) {
    return (
      <TargetGroupCard invocationId={this.props.model.getInvocationId()} group={group} filter={this.props.filter} />
    );
  }

  private isGroupVisible(group: target.TargetGroup): boolean {
    if (!group.targets.length || !STATUS_ORDERING.includes(group.status)) {
      return false;
    }

    const ok = OK_STATUSES.has(group.status);
    return this.props.mode === "passing" ? ok : !ok;
  }

  render() {
    if (this.state.searchLoading) {
      return <div className="loading" />;
    }

    if (this.props.model.invocation.targetGroups.length) {
      // Paginated invocation; render target groups.
      const targetGroups = this.state.searchResponse?.targetGroups || this.props.model.invocation.targetGroups;
      return targetGroups
        .filter((group) => this.isGroupVisible(group))
        .sort((a, b) => STATUS_ORDERING.indexOf(a.status) - STATUS_ORDERING.indexOf(b.status))
        .map((group) => this.renderTargetGroup(group));
    }

    return (
      <div>
        {!!this.props.model.failedTest.length && this.props.mode == "failing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.failedTest}
            className="card-failure"
            icon={<XCircle className="icon red" />}
            presentVerb={`failing ${this.props.model.failedTest.length == 1 ? "test" : "tests"}`}
            pastVerb={`${this.props.model.failedTest.length == 1 ? "test" : "tests"} failed`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}

        {!!this.props.model.failed.length && this.props.mode == "failing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.failed}
            className="card-failure"
            icon={<XCircle className="icon red" />}
            presentVerb={`failing ${this.props.model.failed.length == 1 ? "target" : "targets"}`}
            pastVerb={`${this.props.model.failed.length == 1 ? "target" : "targets"} failed to build`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}

        {!!this.props.model.brokenTest.length && this.props.mode == "failing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.brokenTest}
            className="card-failure"
            icon={<XCircle className="icon red" />}
            presentVerb={`broken ${this.props.model.brokenTest.length == 1 ? "test" : "tests"}`}
            pastVerb={`${this.props.model.brokenTest.length == 1 ? "test" : "tests"} broken`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}

        {!!this.props.model.timeoutTest.length && this.props.mode == "failing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.timeoutTest}
            className="card-timeout"
            icon={<Clock className="icon" />}
            presentVerb={`timed out ${this.props.model.timeoutTest.length == 1 ? "test" : "tests"}`}
            pastVerb={`${this.props.model.timeoutTest.length == 1 ? "test" : "tests"} timed out`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}

        {!!this.props.model.flakyTest.length && this.props.mode == "failing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.flakyTest}
            className="card-flaky"
            icon={<HelpCircle className="icon orange" />}
            presentVerb={`flaky ${this.props.model.flakyTest.length == 1 ? "test" : "tests"}`}
            pastVerb={`flaky ${this.props.model.flakyTest.length == 1 ? "test" : "tests"}`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}

        {!!this.props.model.succeededTest.length && this.props.mode == "passing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.succeededTest}
            className="card-success"
            icon={<CheckCircle className="icon green" />}
            presentVerb={`passing ${this.props.model.succeededTest.length == 1 ? "test" : "tests"}`}
            pastVerb={`${this.props.model.succeededTest.length == 1 ? "test" : "tests"} passed`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}

        {!!this.props.model.succeeded.length && this.props.mode == "passing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.succeeded}
            className="card-success"
            icon={<CheckCircle className="icon green" />}
            presentVerb={`${this.props.model.succeeded.length == 1 ? "target" : "targets"}`}
            pastVerb={`${this.props.model.succeeded.length == 1 ? "target" : "targets"} built successfully`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}

        {!!this.props.model.skipped.length && this.props.mode == "passing" && (
          <TargetsCardComponent
            buildEvents={this.props.model.skipped}
            className="card-skipped"
            icon={<SkipForward className="icon purple" />}
            presentVerb={`${this.props.model.skipped.length == 1 ? "target" : "targets"}`}
            pastVerb={`${this.props.model.skipped.length == 1 ? "target" : "targets"} skipped`}
            model={this.props.model}
            filter={this.props.filter}
            pageSize={this.props.pageSize}
          />
        )}
      </div>
    );
  }
}
