import { AlertCircle } from "lucide-react";
import React from "react";
import InvocationModel from "./invocation_model";
import { failure_details } from "../../proto/failure_details_ts_proto";
import TerminalComponent from "../terminal/terminal";
import rpc_service from "../service/rpc_service";

const debugMessage =
  "Use --sandbox_debug to see verbose messages from the sandbox and retain the sandbox build root for debugging";
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

  prettyPrintedStdErr() {
    if (!this.state.stdErr) {
      return "";
    }
    // Style file names like "/path/to/file.go:10:20" in bold+underline
    return "\n\n" + this.state.stdErr.replaceAll(/([^\s]*:\d+:\d+)/g, "\x1b[1;4m$1\x1b[0m");
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

    // De-emphasize the "--sandbox_debug" message.
    description = description.replaceAll(debugMessage, `\x1b[90m${debugMessage}\x1b[0m\n \n`);

    return (
      <div className="invocation-error-card card card-failure">
        <AlertCircle className="icon red" />
        <div className="content">
          <div className="title">{title}</div>
          <div className="subtitle">{this.props.model.failedAction?.action?.label}</div>
          <div className="details">
            <TerminalComponent
              value={"\x1b[1;31m" + description + "\x1b[0m" + this.prettyPrintedStdErr()}
              lightTheme
              scrollTop
              bottomControls
              defaultWrapped
            />
          </div>
        </div>
      </div>
    );
  }
}
