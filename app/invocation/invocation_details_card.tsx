import React from "react";
import InvocationModel from "./invocation_model";
import { Copy, Info } from "lucide-react";
import { copyToClipboard } from "../util/clipboard";
import alert_service from "../alert/alert_service";
import { command_line } from "../../proto/command_line_ts_proto";
import shlex from "shlex";

interface Props {
  model: InvocationModel;
  limitResults: boolean;
}

interface State {
  limit: number;
}

const defaultPageSize = 1;

export default class ArtifactsCardComponent extends React.Component<Props, State> {
  state: State = {
    limit: defaultPageSize,
  };

  handleMoreInvocationClicked() {
    this.setState({
      ...this.state,
      limit: this.state.limit ? undefined : defaultPageSize,
    });
  }

  handleCopyClicked(label: string) {
    copyToClipboard(label);
    alert_service.success("Command line copied to clipboard!");
  }

  // Wraps arguments containing spaces in the provided command-line in
  // quotation marks so they work when copied and pasted. The input
  // command-line is passed in as an array with one entry per piece. For
  // example, this command:
  //   "bazel build --output_filter='argument with spaces' //..."
  // is passed into this function as:
  //   ["bazel", "build", "--output_filter=argument with spaces"," "//..."],
  // and this will be returned:
  //   ["bazel", "build", "--output_filter='argument with spaces'"," "//..."],
  quote(pieces: string[]) {
    return pieces
      .map((value) => {
        if (value.includes("=")) {
          // shlex.quote everything after the first '=' so that arguments like:
          // --flag="  = = = '' \"" are properly quoted.
          let parts: string[] = value.split("=");
          return parts[0] + "=" + shlex.quote(parts.slice(1).join("="));
        }
        return value;
      })
      .join(" ");
  }

  bazelCommandAndPatternWithOptions(options: string[]) {
    return this.quote(
      [
        "bazel",
        this.props.model.started?.command,
        ...(this.props.model.expanded?.id?.pattern?.pattern || []),
        ...(options || []),
      ].filter((value) => value)
    );
  }

  explicitCommandLine() {
    // We allow overriding EXPLICIT_COMMAND_LINE to enable tools that wrap bazel
    // to append bazel args but still preserve the appearance of the original
    // command line. The effective command line can still be used to see the
    // effective configuration used by bazel.
    const overrideJSON = this.props.model.buildMetadataMap.get("EXPLICIT_COMMAND_LINE");
    if (overrideJSON) {
      try {
        return this.quote(JSON.parse(overrideJSON));
      } catch (_) {
        // Invalid JSON; fall back to showing BES event.
      }
    }

    return this.bazelCommandAndPatternWithOptions(this.props.model.optionsParsed?.explicitCmdLine);
  }

  render() {
    const isBazelInvocation = this.props.model.isBazelInvocation();

    return (
      <div className="card">
        <Info className="icon purple" />
        <div className="content">
          <div className="title">Invocation details</div>
          <div className="details">
            {this.props.model.workflowConfigured && (
              <>
                <div className="invocation-section">
                  <div className="invocation-section-title">Workflow action</div>
                  <div>{this.props.model.workflowConfigured.actionName}</div>
                </div>
                {this.props.model.workflowConfigured.actionTriggerEvent && (
                  <div className="invocation-section">
                    <div className="invocation-section-title">Trigger event</div>
                    <div>{this.props.model.workflowConfigured.actionTriggerEvent}</div>
                  </div>
                )}
                <div className="invocation-section">
                  <div className="invocation-section-title">Pushed branch</div>
                  <div>{this.props.model.workflowConfigured.pushedBranch}</div>
                </div>
                <div className="invocation-section">
                  <div className="invocation-section-title">Target branch</div>
                  <div>{this.props.model.workflowConfigured.targetBranch}</div>
                </div>
              </>
            )}

            <div className="invocation-section">
              <div className="invocation-section-title">Status</div>
              <div>{this.props.model.getStatus()}</div>
            </div>
            <div className="invocation-section">
              <div className="invocation-section-title">Run date</div>
              <div>{this.props.model.getFormattedStartedDate()}</div>
            </div>
            <div className="invocation-section">
              <div className="invocation-section-title">Elapsed time</div>
              <div>{this.props.model.getTiming()}</div>
            </div>
            <div className="invocation-section">
              <div className="invocation-section-title">User</div>
              <div>{this.props.model.getUser(false)}</div>
            </div>
            <div className="invocation-section">
              <div className="invocation-section-title">Host name</div>
              <div>{this.props.model.getHost()}</div>
            </div>
            <div className="invocation-section">
              <div className="invocation-section-title">Tool</div>
              <div>{this.props.model.getTool()}</div>
            </div>
            {isBazelInvocation && (
              <>
                <div className="invocation-section">
                  <div className="invocation-section-title">Pattern</div>
                  <div title={this.props.model.getAllPatterns()}>{this.props.model.getPattern()}</div>
                </div>
                <div className="invocation-section">
                  <div className="invocation-section-title">CPU</div>
                  <div>{this.props.model.getCPU()}</div>
                </div>
                <div className="invocation-section">
                  <div className="invocation-section-title">Mode</div>
                  <div>{this.props.model.getMode()}</div>
                </div>
                <div className="invocation-section">
                  <div className="invocation-section-title">Targets</div>
                  <div>
                    {this.props.model.targets.length} {this.props.model.targets.length == 1 ? "target" : "targets"}
                    {!!this.props.model.buildMetrics?.targetMetrics.targetsConfigured && (
                      <span> ({this.props.model.buildMetrics?.targetMetrics.targetsConfigured} configured)</span>
                    )}
                  </div>
                </div>
                <div className="invocation-section">
                  <div className="invocation-section-title">Actions</div>
                  <div>
                    {this.props.model.buildMetrics?.actionSummary.actionsExecuted} actions
                    {!!this.props.model.buildMetrics?.actionSummary.actionsCreated && (
                      <span> ({this.props.model.buildMetrics?.actionSummary.actionsCreated} created)</span>
                    )}
                  </div>
                </div>
                <div className="invocation-section">
                  <div className="invocation-section-title">Packages</div>
                  <div>{this.props.model.buildMetrics?.packageMetrics.packagesLoaded} packages</div>
                </div>
              </>
            )}

            {this.props.model.getGithubUser() && (
              <div className="invocation-section">
                <div className="invocation-section-title">GitHub user</div>
                <div>
                  <a href={`${this.props.model.getGithubUser()}`}>{this.props.model.getGithubUser()}</a>
                </div>
              </div>
            )}

            {this.props.model.getGithubRepo() && (
              <div className="invocation-section">
                <div className="invocation-section-title">GitHub repo</div>
                <div>
                  <a href={`${this.props.model.getGithubRepo()}`}>{this.props.model.getGithubRepo()}</a>
                </div>
              </div>
            )}

            {this.props.model.getGithubBranch() && (
              <div className="invocation-section">
                <div className="invocation-section-title">GitHub branch</div>
                <div>
                  <a href={`${this.props.model.getGithubRepo()}/tree/${this.props.model.getGithubBranch()}`}>
                    {this.props.model.getGithubBranch()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGithubSHA() && (
              <div className="invocation-section">
                <div className="invocation-section-title">GitHub commit</div>
                <div>
                  <a
                    href={`${this.props.model
                      .getGithubRepo()
                      .replace(/\.git$/, "")}/commit/${this.props.model.getGithubSHA()}`}>
                    {this.props.model.getGithubSHA()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGithubRun() && (
              <div className="invocation-section">
                <div className="invocation-section-title">GitHub run</div>
                <div>
                  <a href={`${this.props.model.getGithubRepo()}/actions/runs/${this.props.model.getGithubRun()}`}>
                    {this.props.model.getGithubRun()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGKEProject() && (
              <div className="invocation-section">
                <div className="invocation-section-title">GKE project</div>
                <div>
                  <a
                    href={`http://console.cloud.google.com/home/dashboard?project=${this.props.model.getGKEProject()}`}>
                    {this.props.model.getGKEProject()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGKECluster() && (
              <div className="invocation-section">
                <div className="invocation-section-title">GKE cluster</div>
                <div>
                  <a
                    href={`https://console.cloud.google.com/kubernetes/list?project=${this.props.model.getGKEProject()}&filter=name:${this.props.model.getGKECluster()}`}>
                    {this.props.model.getGKECluster()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.isBazelInvocation() && (
              <>
                <div className="invocation-command-line">
                  <div className="invocation-command-line-title">
                    explicit command line{" "}
                    <Copy
                      className="copy-icon"
                      onClick={this.handleCopyClicked.bind(this, this.explicitCommandLine())}
                    />
                  </div>
                  <div className="invocation-section">
                    <code className="wrap">{this.explicitCommandLine()}</code>
                  </div>
                </div>

                <div className="invocation-command-line">
                  <div className="invocation-command-line-title">
                    effective command line{" "}
                    <Copy
                      className="copy-icon"
                      onClick={this.handleCopyClicked.bind(
                        this,
                        `${this.bazelCommandAndPatternWithOptions(this.props.model.optionsParsed?.cmdLine)}`
                      )}
                    />
                  </div>
                  <div className="invocation-section">
                    <code className="wrap">
                      {this.bazelCommandAndPatternWithOptions(this.props.model.optionsParsed?.cmdLine)}
                    </code>
                  </div>
                </div>
              </>
            )}
            {this.props.model.structuredCommandLine
              .filter((commandLine) => commandLine.commandLineLabel && commandLine.commandLineLabel.length)
              .sort((a, b) => {
                return a.commandLineLabel < b.commandLineLabel ? -1 : 1;
              })
              .slice(0, this.props.limitResults && this.state.limit ? this.state.limit : undefined)
              .map((commandLine) => (
                <div className="invocation-command-line">
                  <div className="invocation-command-line-title">{commandLine.commandLineLabel} command line</div>
                  {commandLine.sections.flatMap((section) => (
                    <div className="invocation-section">
                      <div className="invocation-section-title">{section.sectionLabel}</div>
                      <div>
                        {section.chunkList?.chunk.map((chunk) => <div className="invocation-chunk">{chunk}</div>) || []}
                        {/* Bazel sometimes sends empty options in the command line event; filter these out.
                            Also, Bazel sometimes sends options with only a combined form.
                            Attempt to split these into optionName/optionValue so we can render them nicely. */}
                        {section.optionList?.option
                          .filter((option) => option.optionName || option.combinedForm)
                          .map((option) => ensureNameAndValue(option))
                          .map((option) => (
                            <div>
                              <span className="invocation-option-dash">--</span>
                              {/*
                                For custom flags like --@repo//foo:bar=true or --//foo:bar=true,
                                render as plain text. For other flags, link to Bazel docs.
                              */}
                              {option.optionName.startsWith("//") || option.optionName.startsWith("@") ? (
                                <span className="invocation-option-name">{option.optionName}</span>
                              ) : (
                                <a
                                  className="invocation-option-name"
                                  href={`https://bazel.build/reference/command-line-reference#flag--${option.optionName}`}
                                  target="_blank">
                                  {option.optionName}
                                </a>
                              )}

                              {option.optionValue !== undefined && (
                                <>
                                  <span className="invocation-option-equal">=</span>
                                  <span className="invocation-option-value">{option.optionValue}</span>
                                </>
                              )}
                            </div>
                          )) || []}
                      </div>
                    </div>
                  ))}
                </div>
              ))}
          </div>
          {this.props.limitResults && !!this.state.limit && (
            <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>
              See more invocation details
            </div>
          )}
          {this.props.limitResults && !this.state.limit && (
            <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>
              See less invocation details
            </div>
          )}
        </div>
      </div>
    );
  }
}

function ensureNameAndValue(option: command_line.IOption): command_line.IOption {
  if (option.optionName) return option;
  if (!option.combinedForm.startsWith("--")) return option;

  if (!option.combinedForm.includes("=")) {
    return { ...option, optionName: option.combinedForm.substring(2), optionValue: undefined };
  }

  const [optionName, optionValue] = option.combinedForm.substring(2).split("=");
  return { ...option, optionName, optionValue };
}
