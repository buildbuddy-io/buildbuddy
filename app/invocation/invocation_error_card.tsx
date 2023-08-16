import { AlertCircle } from "lucide-react";
import React from "react";
import InvocationModel from "./invocation_model";
import { failure_details } from "../../proto/failure_details_ts_proto";

interface Props {
  model: InvocationModel;
}

/**
 * Displays a card containing the main reason that an invocation failed.
 */
export default class ErrorCardComponent extends React.Component<Props> {
  render() {
    let title = "";
    let description = "";
    if (this.props.model.failedAction?.action?.failureDetail?.message) {
      title = "Build action failed";
      description = this.props.model.failedAction.action.failureDetail.message;
      const spawnCode = this.props.model.failedAction?.action?.failureDetail?.spawn?.code;
      if (spawnCode) {
        title += ` (code: ${failure_details.Spawn.Code[spawnCode]})`;
      }
    } else if (this.props.model.aborted?.aborted?.description) {
      title = "Build aborted";
      description = this.props.model.aborted.aborted.description;
    } else {
      return null;
    }

    return (
      <div className="invocation-error-card card card-failure">
        <AlertCircle className="icon red" />
        <div className="content">
          <div className="title">{title}</div>
          <div className="details">
            <pre className="error-contents">{description}</pre>
          </div>
        </div>
      </div>
    );
  }
}
