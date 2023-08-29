import React from "react";
import InvocationModel from "./invocation_model";
import ChildInvocationCard, { CommandStatus, InvocationMetadata } from "./child_invocation_card";
import { BazelCommandResult } from "./child_invocation_card";
import { durationToMillisWithFallback } from "../util/proto";

export type ChildInvocationProps = {
  model: InvocationModel;
};

export default class ChildInvocations extends React.Component<ChildInvocationProps> {
  private getDurationMillis(invocation: InvocationMetadata): number | undefined {
    const completedEvent = this.props.model.childInvocationCompletedByInvocationId.get(invocation.invocationId ?? "");
    if (!completedEvent) return undefined;
    return durationToMillisWithFallback(completedEvent.duration, +(completedEvent?.durationMillis ?? 0));
  }

  render() {
    const workflowConfiguredEvent = this.props.model.workflowConfigured;
    const childInvocationsConfiguredEvent = this.props.model.childInvocationsConfigured;
    const invocations = childInvocationsConfiguredEvent
      ? childInvocationsConfiguredEvent.invocation
      : workflowConfiguredEvent
      ? workflowConfiguredEvent.invocation
      : [];

    const results: BazelCommandResult[] = [];
    let inProgressCount = 0;
    const getStatus = (invocation: InvocationMetadata): CommandStatus => {
      const completedEvent = this.props.model.childInvocationCompletedByInvocationId.get(invocation.invocationId ?? "");
      if (completedEvent) {
        return completedEvent.exitCode === 0 ? "succeeded" : "failed";
      } else if (this.props.model.finished) {
        return "not-run";
      } else if (inProgressCount === 0) {
        // Only one command should be marked in progress; the rest should be
        // marked queued.
        inProgressCount++;
        return "in-progress";
      } else {
        return "queued";
      }
    };

    for (const invocation of invocations) {
      results.push({ invocation, status: getStatus(invocation), durationMillis: this.getDurationMillis(invocation) });
    }

    if (!results.length) return null;

    return (
      <div className="child-invocations-section">
        <h2>Bazel commands</h2>
        <div className="subtitle">Click a command to see results.</div>
        <div className="child-invocations-list">
          {results.map((result) => (
            <ChildInvocationCard key={result.invocation.invocationId} result={result} />
          ))}
        </div>
      </div>
    );
  }
}
