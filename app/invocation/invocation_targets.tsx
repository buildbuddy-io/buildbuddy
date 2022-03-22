import React from "react";

import InvocationModel from "./invocation_model";
import TargetsCardComponent from "./invocation_targets_card";
import { XCircle, CheckCircle, HelpCircle, Clock, SkipForward } from "lucide-react";

interface Props {
  model: InvocationModel;
  pageSize: number;
  filter: string;
  mode: string;
}

export default class TargetsComponent extends React.Component<Props> {
  render() {
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
