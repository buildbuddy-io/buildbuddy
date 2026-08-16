import { ChevronDown, ChevronRight } from "lucide-react";
import React from "react";
import alert_service from "../../../app/alert/alert_service";
import { FilledButton, OutlinedButton } from "../../../app/components/button/button";
import TextInput from "../../../app/components/input/input";
import Popup from "../../../app/components/popup/popup";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { copyToClipboard } from "../../../app/util/clipboard";
import { git } from "../../../proto/git_ts_proto";
import { runner } from "../../../proto/runner_ts_proto";
import { WORKFLOW_PRESETS, WorkflowPreset } from "./workflow_presets";

export interface ChecksDropdownProps {
  repoUrl: string;
}

interface ChecksDropdownState {
  isOpen: boolean;
  // True while a check is being kicked off on a remote runner.
  isRunning: boolean;
  // Which check is expanded in the popup. Only one is expanded at a time.
  expandedId: string;
  // Branch entered in the "Run now" input. Empty until the user types a branch;
  // "Run now" is disabled while it's blank.
  branch: string;
}

// ChecksDropdown renders the "Checks" dropdown: a single row button that opens an
// accordion of pre-configured workflows (WORKFLOW_PRESETS).
export default class ChecksDropdown extends React.Component<ChecksDropdownProps, ChecksDropdownState> {
  state: ChecksDropdownState = {
    isOpen: false,
    isRunning: false,
    expandedId: WORKFLOW_PRESETS[0]?.id ?? "",
    branch: "",
  };

  private onToggleOpen() {
    this.setState({ isOpen: !this.state.isOpen });
  }

  private onClose() {
    this.setState({ isOpen: false });
  }

  private onToggleExpanded(id: string) {
    this.setState({ expandedId: this.state.expandedId === id ? "" : id });
  }

  private onClickCopyYaml(preset: WorkflowPreset) {
    copyToClipboard(preset.yaml);
    alert_service.success("Copied buildbuddy.yaml snippet to clipboard!");
  }

  private onClickCopyCli(preset: WorkflowPreset) {
    copyToClipboard(preset.cliCommand);
    alert_service.success("Copied CLI command to clipboard!");
  }

  private runNow(preset: WorkflowPreset) {
    const branch = this.state.branch.trim();
    this.setState({ isRunning: true, isOpen: false });

    rpcService.service
      .run(
        new runner.RunRequest({
          gitRepo: new git.GitRepo({ repoUrl: this.props.repoUrl }),
          repoState: new git.RepoState({ branch: branch }),
          steps: [new runner.Step({ run: preset.cliCommand })],
          name: preset.actionName,
          async: true,
        })
      )
      .then((response) => {
        window.open(`/invocation/${response.invocationId}?queued=true`, "_blank");
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isRunning: false }));
  }

  private renderPreset(preset: WorkflowPreset) {
    const expanded = this.state.expandedId === preset.id;
    const Icon = preset.icon;
    return (
      <div className={`check-item ${expanded ? "expanded" : ""}`} key={preset.id}>
        <button className="check-item-header" onClick={() => this.onToggleExpanded(preset.id)}>
          <div className="check-item-titles">
            <div className="check-item-label">
              <Icon className="icon" />
              <span>{preset.label}</span>
            </div>
            <div className="check-item-description">{preset.description}</div>
          </div>
          {expanded ? <ChevronDown className="icon chevron" /> : <ChevronRight className="icon chevron" />}
        </button>
        {expanded && (
          <div className="check-item-body">
            <div className="check-run-row">
              <TextInput
                value={this.state.branch}
                placeholder="Branch to run on"
                aria-label="Branch to run on"
                onChange={(e) => this.setState({ branch: e.target.value })}
              />
              <FilledButton
                onClick={() => this.runNow(preset)}
                disabled={this.state.isRunning || !this.state.branch.trim()}>
                Run now
              </FilledButton>
            </div>
            <OutlinedButton className="check-copy-button" onClick={() => this.onClickCopyYaml(preset)}>
              <span>Copy buildbuddy.yaml</span>
              {preset.yamlHint && <span className="check-copy-hint">{preset.yamlHint}</span>}
            </OutlinedButton>
            <OutlinedButton className="check-copy-button" onClick={() => this.onClickCopyCli(preset)}>
              <span>Copy CLI command</span>
            </OutlinedButton>
          </div>
        )}
      </div>
    );
  }

  render() {
    return (
      <div className="workflow-button-container">
        <OutlinedButton
          className="checks-button"
          onClick={this.onToggleOpen.bind(this)}
          disabled={this.state.isRunning}>
          <span>Checks</span>
          {this.state.isRunning ? <Spinner /> : <ChevronDown className="icon chevron" />}
        </OutlinedButton>
        <Popup isOpen={this.state.isOpen} onRequestClose={this.onClose.bind(this)} className="checks-popup">
          <div className="checks-list">{WORKFLOW_PRESETS.map((preset) => this.renderPreset(preset))}</div>
        </Popup>
      </div>
    );
  }
}
