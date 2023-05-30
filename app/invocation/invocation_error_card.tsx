import { AlertCircle } from "lucide-react";
import React from "react";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
}

export default class ErrorCardComponent extends React.Component<Props> {
  render() {
    return (
      <div className="card card-failure">
        <AlertCircle className="icon" />
        <div className="content">
          <div className="title">Error</div>
          <div className="details">
            {this.props.model.aborted?.aborted?.description || "No description was found for this error."}
          </div>
        </div>
      </div>
    );
  }
}
