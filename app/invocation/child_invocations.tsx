import React from "react";
import InvocationModel from "./invocation_model";
import ChildInvocationCard from "./child_invocation_card";
import { CheckCircle, PlayCircle, XCircle } from "lucide-react";
import { BazelCommandResult } from "./child_invocation_card";
import { durationToMillisWithFallback } from "../util/proto";

export type ChildInvocationProps = {
  model: InvocationModel;
};

export default class ChildInvocations extends React.Component<ChildInvocationProps> {
  render() {
    const workflowConfiguredEvent = this.props.model.workflowConfigured!;
    const childInvocationsConfiguredEvent = this.props.model.childInvocationsConfigured;
    const invocations = childInvocationsConfiguredEvent
      ? childInvocationsConfiguredEvent.invocation
      : workflowConfiguredEvent.invocation;
    const completedEventsById = this.props.model.childInvocationCompletedByInvocationId;

    const isInProgress = !this.props.model.finished;

    const failed: BazelCommandResult[] = [];
    const succeeded: BazelCommandResult[] = [];
    const notRun: BazelCommandResult[] = [];
    const inProgress: BazelCommandResult[] = [];
    const queued: BazelCommandResult[] = [];

    for (const invocation of invocations) {
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
      const durationMillis = durationToMillisWithFallback(
        completedEvent?.duration,
        +(completedEvent?.durationMillis ?? 0)
      );

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
          <ChildInvocationCard
            status="failed"
            results={failed}
            className="card-failure"
            icon={<XCircle className="icon red" />}
          />
        )}
        {inProgress.length > 0 && (
          <ChildInvocationCard
            status="in progress"
            results={inProgress}
            className="card-in-progress"
            icon={<PlayCircle className="icon blue" />}
          />
        )}
        {queued.length > 0 && (
          <ChildInvocationCard
            status="queued"
            results={queued}
            className="card-neutral"
            icon={<PlayCircle className="icon blue" />}
            linksDisabled={true}
          />
        )}
        {notRun.length > 0 && (
          <ChildInvocationCard
            status="not run"
            results={notRun}
            className="card-neutral"
            icon={<XCircle className="icon" />}
            linksDisabled={true}
          />
        )}
        {succeeded.length > 0 && (
          <ChildInvocationCard
            status="succeeded"
            results={succeeded}
            className="card-success"
            icon={<CheckCircle className="icon green" />}
          />
        )}
      </>
    );
  }
}
