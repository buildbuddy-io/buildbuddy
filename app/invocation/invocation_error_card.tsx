import { AlertCircle } from "lucide-react";
import React from "react";
import InvocationModel from "./invocation_model";
import { failure_details } from "../../proto/failure_details_ts_proto";
import TerminalComponent from "../terminal/terminal";
import rpc_service, { CancelablePromise } from "../service/rpc_service";
import { exitCode } from "../util/exit_codes";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";

const debugMessage =
  "Use --sandbox_debug to see verbose messages from the sandbox and retain the sandbox build root for debugging";

interface Props {
  model: InvocationModel;
}

interface State {
  model?: CardModel;
}

type CardModel = {
  errors: ErrorData[];
};

type ErrorData = {
  action?: build_event_stream.ActionExecuted;
  actionStderr?: string;
  actionStdout?: string;

  aborted?: build_event_stream.BuildEvent;

  finished?: build_event_stream.BuildFinished;
};

/**
 * Displays a card containing the main reason that an invocation failed.
 */
export default class ErrorCardComponent extends React.Component<Props, State> {
  state: State = {};

  componentDidMount() {
    this.fetchModelData(getModel(this.props));
  }

  componentDidUpdate(prevProps: Props): void {
    const model = getModel(this.props);
    if (!modelsEqual(model, getModel(prevProps))) {
      this.fetchModelData(model);
    }
  }

  private inFlightFetch: CancelablePromise | null = null;
  fetchModelData(model: CardModel) {
    this.inFlightFetch?.cancel();

    const promises: Promise<any>[] = [];
    for (const error of model.errors) {
      if (error.action?.stderr?.uri) {
        promises.push(
          rpc_service
            .fetchBytestreamFile(error.action?.stderr?.uri, this.props.model.getInvocationId(), "text")
            .then((stderr) => {
              error.actionStderr = stderr;
            })
            .catch((e) => console.error("Failed to fetch failed action stderr:", e))
        );
      }
      if (error.action?.stdout?.uri) {
        promises.push(
          rpc_service
            .fetchBytestreamFile(error.action?.stdout?.uri, this.props.model.getInvocationId(), "text")
            .then((stdout) => {
              error.actionStdout = stdout;
            })
            .catch((e) => console.error("Failed to fetch failed action stdout:", e))
        );
      }
    }

    this.inFlightFetch = new CancelablePromise(Promise.all(promises));
    this.inFlightFetch.then(() => this.setState({ model }));
  }

  getTitle(model: CardModel) {
    // If there was a failed action with a non-zero exit code, use that as the title.
    for (const error of model.errors) {
      if ((error.action?.exitCode ?? 0) !== 0) {
        return `${this.props.model.failedAction?.action?.type ?? "Build"} action failed with exit code ${
          this.props.model.failedAction?.action?.exitCode
        }`;
      }
    }
    // If the finished event exists and there's a non-zero exit code, use that as the card title.
    for (const error of model.errors) {
      if ((error.finished?.exitCode ?? 0) !== 0) {
        return exitCode(error.finished?.exitCode?.name ?? "");
      }
    }
    // If there was an aborted event, use a generic "aborted" title.
    for (const error of model.errors) {
      if (error.aborted) return "Build aborted";
    }
    return "Build failed";
  }

  getBodyText(model: CardModel) {
    const lines: string[] = [];
    const errorPrefix = "\x1b[1;91mERROR:\x1b[m ";
    for (const error of model.errors) {
      if (error.aborted?.aborted?.reason) {
        lines.push(
          errorPrefix +
            joinNonEmpty(
              [
                error.aborted.id?.configuredLabel?.label ?? "",
                error.aborted.id?.targetConfigured?.label ?? "",
                naiveFormatEnum(build_event_stream.Aborted.AbortReason[error.aborted?.aborted?.reason] ?? ""),
                error.aborted?.aborted?.description,
              ],
              ": "
            )
        );
      }
      if (error.action) {
        if (error.action.failureDetail) {
          lines.push(errorPrefix + formatFailureDescription(error.action.failureDetail));
        }
        if (error.actionStderr) {
          lines.push("\x1b[1m" + error.actionStderr + "\x1b[m");
        }
        if (error.actionStdout) {
          lines.push("\x1b[1m" + error.actionStdout + "\x1b[m");
        }
      }
      if (error.finished) {
        if (error.finished.failureDetail) {
          lines.push(errorPrefix + formatFailureDescription(error.finished.failureDetail));
        }
      }
    }
    let text = lines.join("\n");
    text = deemphasizeSandboxDebug(text);
    text = underlineFileNames(text);
    return text;
  }

  render() {
    if (!this.state.model?.errors?.length) {
      return null;
    }

    return (
      <div className="invocation-error-card card card-failure">
        <AlertCircle className="icon red" />
        <div className="content">
          <div className="title">{this.getTitle(this.state.model)}</div>
          <div className="subtitle">{this.props.model.failedAction?.action?.label}</div>
          <div className="details">
            <TerminalComponent
              value={this.getBodyText(this.state.model)}
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

function getModel(props: Props): CardModel {
  const model: CardModel = { errors: [] };
  if (props.model.failedAction?.action?.failureDetail?.message) {
    model.errors.push({ action: props.model.failedAction.action });
  }
  for (const event of props.model.aborted) {
    if (!event.aborted) continue;
    model.errors.push({ aborted: event });
  }
  if (props.model.finished?.failureDetail?.message) {
    model.errors.push({ finished: props.model.finished });
  }
  return model;
}

function formatFailureDescription(failureDetail: failure_details.IFailureDetail): string {
  let message = failureDetail.message;
  let code = "";
  if (failureDetail.spawn) {
    code = failure_details.Spawn.Code[failureDetail.spawn.code];
  } else if (failureDetail.execution) {
    code = failure_details.Execution.Code[failureDetail.execution.code];
  } else if (failureDetail.targetPatterns) {
    code = failure_details.TargetPatterns.Code[failureDetail.targetPatterns.code];
  }
  // TODO: handle other FailureDetail fields

  return joinNonEmpty([naiveFormatEnum(code), message ?? ""], ": ");
}

function joinNonEmpty(parts: string[], join: string) {
  return parts.filter((x) => x).join(join);
}

function modelsEqual(a: CardModel, b: CardModel): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * Styles file names like "/path/to/file.go:10:20" in bold+underline
 */
function underlineFileNames(text: string): string {
  return text.replaceAll(/([^\s]*:\d+:\d+)/g, "\x1b[1;4m$1\x1b[0m");
}

/**
 * De-emphasizes the "--sandbox_debug" message.
 */
function deemphasizeSandboxDebug(text: string): string {
  return text.replaceAll(debugMessage, `\x1b[90m${debugMessage}\x1b[0m\n \n`);
}

/**
 * Naive enum formatter which just makes the enum lowercase
 * and replaces underscores with spaces.
 */
function naiveFormatEnum(text: string): string {
  return text.toLowerCase().replaceAll("_", " ");
}
