import { Bot, Check, ChevronDown, Copy, Info } from "lucide-react";
import React from "react";
import { DEFAULT_REMOTE_RUNNER_AGENT, REMOTE_RUNNER_AGENTS, RemoteRunnerAgent } from "../../util/remote_runner";
import { OutlinedButton } from "./button";
import { OutlinedButtonGroup } from "./button_group";
import Menu, { MenuItem } from "../menu/menu";
import Popup, { PopupContainer } from "../popup/popup";
import Spinner from "../spinner/spinner";

interface Props {
  label: string;
  loadingLabel?: string;
  onClick: (agent: RemoteRunnerAgent) => void | Promise<void>;
  onCopyCommand: (agent: RemoteRunnerAgent) => void;
  onInfoClick: () => void;
}

interface State {
  agent: RemoteRunnerAgent;
  isMenuOpen: boolean;
  isLoading: boolean;
}

export default class AIButton extends React.Component<Props, State> {
  state: State = {
    agent: DEFAULT_REMOTE_RUNNER_AGENT,
    isMenuOpen: false,
    isLoading: false,
  };

  private selectAgent(agent: RemoteRunnerAgent) {
    this.setState({ agent, isMenuOpen: false });
  }

  private copyCommand() {
    this.setState({ isMenuOpen: false });
    this.props.onCopyCommand(this.state.agent);
  }

  private async onClick() {
    this.setState({ isLoading: true });
    try {
      await this.props.onClick(this.state.agent);
    } finally {
      this.setState({ isLoading: false });
    }
  }

  render() {
    return (
      <PopupContainer className="ai-button-actions">
        <OutlinedButtonGroup>
          <OutlinedButton disabled={this.state.isLoading} onClick={this.onClick.bind(this)}>
            {this.state.isLoading ? <Spinner /> : <Bot className="icon" />}
            <span>{this.state.isLoading ? this.props.loadingLabel || "Starting..." : this.props.label}</span>
          </OutlinedButton>
          <OutlinedButton
            disabled={this.state.isLoading}
            className="icon-button"
            aria-label="Choose AI agent or copy command"
            aria-haspopup="menu"
            aria-expanded={this.state.isMenuOpen}
            onClick={() => this.setState({ isMenuOpen: true })}>
            <ChevronDown />
          </OutlinedButton>
          <OutlinedButton
            className="icon-button"
            aria-label="How this AI action works"
            onClick={this.props.onInfoClick}>
            <Info />
          </OutlinedButton>
        </OutlinedButtonGroup>
        <Popup
          isOpen={this.state.isMenuOpen}
          onRequestClose={() => this.setState({ isMenuOpen: false })}
          anchor="right">
          <Menu className="ai-button-menu">
            <li className="ai-button-menu-label" role="presentation">
              Run with
            </li>
            {REMOTE_RUNNER_AGENTS.map((agent) => (
              <MenuItem
                key={agent.id}
                className={this.state.agent === agent.id ? "selected" : ""}
                role="menuitemradio"
                aria-checked={this.state.agent === agent.id}
                onClick={() => this.selectAgent(agent.id)}>
                <Check className="ai-button-menu-icon check" />
                <span>{agent.name}</span>
              </MenuItem>
            ))}
            <li className="ai-button-menu-divider" role="separator" />
            <MenuItem onClick={this.copyCommand.bind(this)}>
              <Copy className="ai-button-menu-icon" />
              <span>Copy command to run locally</span>
            </MenuItem>
          </Menu>
        </Popup>
      </PopupContainer>
    );
  }
}
