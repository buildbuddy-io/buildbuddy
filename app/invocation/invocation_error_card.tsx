import { AlertCircle } from "lucide-react";
import React from "react";
import InvocationModel from "./invocation_model";
import { failure_details } from "../../proto/failure_details_ts_proto";
import TerminalComponent from "../terminal/terminal";
import rpc_service from "../service/rpc_service";

interface Props {
  model: InvocationModel;
}

interface State {
  stdErr: string;
}

/**
 * Displays a card containing the main reason that an invocation failed.
 */
export default class ErrorCardComponent extends React.Component<Props, State> {
  state: State = {
    stdErr: "",
  };

  componentDidMount() {
    if (this.props.model.failedAction?.action?.stderr?.uri) {
      rpc_service
        .fetchBytestreamFile(
          this.props.model.failedAction?.action.stderr?.uri,
          this.props.model.getInvocationId(),
          "text"
        )
        .then((resp) => this.setState({ stdErr: resp }));
    }
  }

  render() {
    let title = "";
    let description = "";
    if (this.props.model.failedAction?.action?.failureDetail?.message) {
      title = `${this.props.model.failedAction?.action?.type} action failed with exit code ${this.props.model.failedAction?.action?.exitCode}`;
      description = this.props.model.failedAction.action.failureDetail.message;
      const spawnCode = this.props.model.failedAction?.action?.failureDetail?.spawn?.code;
      if (spawnCode) {
        title += ` (${failure_details.Spawn.Code[spawnCode]})`;
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
          <div className="subtitle">{this.props.model.failedAction?.action?.label}</div>
          <div className="details">
            <pre className="error-contents">{description}</pre>
            {this.state.stdErr && <TerminalComponent value={this.state.stdErr} lightTheme />}
          </div>
        </div>
      </div>
    );
  }
}
