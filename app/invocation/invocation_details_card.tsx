import React from "react";
import InvocationModel, { CI_RUNNER_ROLE } from "./invocation_model";

interface Props {
  model: InvocationModel;
  limitResults: boolean;
}

interface State {
  limit: number;
}

const defaultPageSize = 1;

export default class ArtifactsCardComponent extends React.Component {
  props: Props;

  state: State = {
    limit: defaultPageSize,
  };

  handleMoreInvocationClicked() {
    this.setState({
      ...this.state,
      limit: this.state.limit ? undefined : defaultPageSize,
    });
  }

  render() {
    const isBazelInvocation = this.props.model.isBazelInvocation();

    return (
      <div className="card">
        <img className="icon" src="/image/info.svg" />
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
              <div>
                {this.props.model.getStartDate()} at {this.props.model.getStartTime()}
              </div>
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
                      <span>
                        {" "}
                        ({this.props.model.buildMetrics?.targetMetrics.targetsConfigured} configured /{" "}
                        {this.props.model.buildMetrics?.targetMetrics.targetsLoaded} loaded)
                      </span>
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
                <div className="invocation-section-title">Github user</div>
                <div>
                  <a href={`https://github.com/${this.props.model.getGithubUser()}`}>
                    {this.props.model.getGithubUser()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGithubRepo() && (
              <div className="invocation-section">
                <div className="invocation-section-title">Github repo</div>
                <div>
                  <a href={`https://github.com/${this.props.model.getGithubRepo()}`}>
                    {this.props.model.getGithubRepo()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGithubBranch() && (
              <div className="invocation-section">
                <div className="invocation-section-title">Github branch</div>
                <div>
                  <a
                    href={`https://github.com/${this.props.model.getGithubRepo()}/tree/${this.props.model.getGithubBranch()}`}>
                    {this.props.model.getGithubBranch()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGithubSHA() && (
              <div className="invocation-section">
                <div className="invocation-section-title">Github commit</div>
                <div>
                  <a
                    href={`https://github.com/${this.props.model.getGithubRepo()}/commit/${this.props.model.getGithubSHA()}`}>
                    {this.props.model.getGithubSHA()}
                  </a>
                </div>
              </div>
            )}

            {this.props.model.getGithubRun() && (
              <div className="invocation-section">
                <div className="invocation-section-title">Github run</div>
                <div>
                  <a
                    href={`https://github.com/${this.props.model.getGithubRepo()}/actions/runs/${this.props.model.getGithubRun()}`}>
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
                        {section.optionList?.option.map((option) => (
                          <div>
                            <span className="invocation-option-dash">--</span>
                            <span className="invocation-option-name">{option.optionName}</span>
                            <span className="invocation-option-equal">=</span>
                            <span className="invocation-option-value">{option.optionValue}</span>
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
