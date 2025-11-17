import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import ChildInvocationCard from "./child_invocation_card";

export type ChildInvocationProps = {
  childInvocations: invocation.Invocation[];
};

export default class ChildInvocations extends React.Component<ChildInvocationProps> {
  render(): JSX.Element | null {
    if (!this.props.childInvocations.length) return null;

    return (
      <div className="child-invocations-section">
        <h2>Builds from this run</h2>
        <div className="subtitle">Click a command to see detailed build and test information.</div>
        <div className="child-invocations-list">
          {this.props.childInvocations.map((result) => (
            <ChildInvocationCard key={result.invocationId} invocation={result} />
          ))}
        </div>
      </div>
    );
  }
}
