import React from "react";
import ChildInvocationCard from "./child_invocation_card";
import { invocation } from "../../proto/invocation_ts_proto";

export type ChildInvocationProps = {
  childInvocations: invocation.Invocation[];
};

export default class ChildInvocations extends React.Component<ChildInvocationProps> {
  render() {
    if (!this.props.childInvocations.length) return null;

    return (
      <div className="child-invocations-section">
        <h2>Bazel commands</h2>
        <div className="subtitle">Click a command to see results.</div>
        <div className="child-invocations-list">
          {this.props.childInvocations.map((result) => (
            <ChildInvocationCard key={result.invocationId} invocation={result} />
          ))}
        </div>
      </div>
    );
  }
}
