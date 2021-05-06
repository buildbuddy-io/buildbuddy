import React from "react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import InvocationModel from "./invocation_model";
import WorkflowCommandsCard from "./workflow_commands_card";

export type WorkflowCommandsProps = {
  model: InvocationModel;
};

export type BazelCommandResult = {
  invocation: build_event_stream.WorkflowConfigured.IInvocationMetadata;
  durationMillis?: number;
};

export default class WorkflowCommands extends React.Component<WorkflowCommandsProps> {
  render() {
    const configuredEvent = this.props.model.workflowConfigured!;
    const completedEventsById = this.props.model.workflowCommandCompletedByInvocationId;

    const completed: BazelCommandResult[] = [];
    const failed: BazelCommandResult[] = [];
    const notRun: BazelCommandResult[] = [];

    let prevTimeMillis = Number(this.props.model.started.startTimeMillis) || 0;
    for (const invocation of configuredEvent.invocation) {
      const completedEvent = completedEventsById.get(invocation.invocationId);
      if (!completedEvent) {
        notRun.push({
          invocation: {
            ...invocation,
            // Clear the invocation ID so that we don't render links for
            // not run actions.
            invocationId: "",
          },
        });
        continue;
      }
      const curTimeMillis = Number(completedEvent.finishTimeMillis) || 0;
      const durationMillis = curTimeMillis - prevTimeMillis;
      prevTimeMillis = curTimeMillis;

      const result = { invocation, durationMillis };
      if (completedEvent.exitCode === 0) {
        completed.push(result);
      } else {
        failed.push(result);
      }
    }

    return (
      <>
        {Boolean(failed.length) && (
          <WorkflowCommandsCard
            status="failed"
            results={failed}
            className="card-failure"
            iconPath="/image/x-circle.svg"
          />
        )}
        {Boolean(completed.length) && (
          <WorkflowCommandsCard
            status="succeeded"
            results={completed}
            className="card-success"
            iconPath="/image/check-circle.svg"
          />
        )}
        {Boolean(notRun.length) && (
          <WorkflowCommandsCard
            status="not run"
            results={notRun}
            className="card-neutral"
            iconPath="/image/skipped-circle.svg"
          />
        )}
      </>
    );
  }
}
