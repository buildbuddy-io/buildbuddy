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

    const isInProgress = !this.props.model.finished;

    const failed: BazelCommandResult[] = [];
    const succeeded: BazelCommandResult[] = [];
    const notRun: BazelCommandResult[] = [];
    const inProgress: BazelCommandResult[] = [];
    const queued: BazelCommandResult[] = [];

    for (const invocation of configuredEvent.invocation) {
      const completedEvent = completedEventsById.get(invocation.invocationId);
      if (!completedEvent) {
        if (isInProgress) {
          if (inProgress.length === 0) {
            inProgress.push({ invocation });
          } else {
            queued.push({ invocation });
          }
        } else {
          notRun.push({ invocation });
        }
        continue;
      }
      const durationMillis = Number(completedEvent.durationMillis);

      const result = { invocation, durationMillis };
      if (completedEvent.exitCode === 0) {
        succeeded.push(result);
      } else {
        failed.push(result);
      }
    }

    return (
      <>
        {failed.length > 0 && (
          <WorkflowCommandsCard
            status="failed"
            results={failed}
            className="card-failure"
            iconPath="/image/x-circle.svg"
          />
        )}
        {succeeded.length > 0 && (
          <WorkflowCommandsCard
            status="succeeded"
            results={succeeded}
            className="card-success"
            iconPath="/image/check-circle.svg"
          />
        )}
        {notRun.length > 0 && (
          <WorkflowCommandsCard
            status="not run"
            results={notRun}
            className="card-neutral"
            iconPath="/image/skipped-circle.svg"
            linksDisabled={true}
          />
        )}
        {inProgress.length > 0 && (
          <WorkflowCommandsCard
            status="in progress"
            results={inProgress}
            className="card-in-progress"
            iconPath="/image/play-circle.svg"
          />
        )}
        {queued.length > 0 && (
          <WorkflowCommandsCard
            status="queued"
            results={queued}
            className="card-neutral"
            iconPath="/image/play-circle.svg"
            linksDisabled={true}
          />
        )}
      </>
    );
  }
}
