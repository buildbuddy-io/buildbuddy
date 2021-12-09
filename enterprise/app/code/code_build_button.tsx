import React from "react";
import { OutlinedButton } from "../../../app/components/button/button";
import { OutlinedButtonGroup } from "../../../app/components/button/button_group";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Popup, { PopupContainer } from "../../../app/components/popup/popup";
import Spinner from "../../../app/components/spinner/spinner";
import { ChevronDown, PlayCircle } from "lucide-react";

const LOCAL_STORAGE_STATE_KEY = "code-button-commands";
const MAX_COMMANDS = 10;

export interface CodeBuildButtonProps {
  onCommandClicked: (args: string) => void;
  isLoading: boolean;
  project: string;
}

type State = {
  isMenuOpen?: boolean;
  commands: string[];
};

export default class CodeBuildButton extends React.Component<CodeBuildButtonProps, State> {
  state: State = this.getSavedState()
    ? (JSON.parse(this.getSavedState()) as State)
    : {
        commands: ["build //...", "test //..."],
      };

  private getSavedState() {
    return localStorage[this.localStorageKey()];
  }

  private saveState() {
    localStorage[this.localStorageKey()] = JSON.stringify(this.state);
  }

  private localStorageKey() {
    return LOCAL_STORAGE_STATE_KEY + this.props.project;
  }

  private onOpenMenu() {
    this.setState({ isMenuOpen: true }, this.saveState.bind(this));
  }
  private onCloseMenu() {
    this.setState({ isMenuOpen: false }, this.saveState.bind(this));
  }

  private handleCommandClicked(args: string) {
    this.onCloseMenu();
    let newCommands = this.state.commands;
    // Remove if it already exists.
    const index = newCommands.indexOf(args);
    if (index > -1) {
      newCommands.splice(index, 1);
    }
    // Place it at the front.
    newCommands.unshift(args);
    // Limit the number of commands.
    newCommands = newCommands.slice(0, MAX_COMMANDS);
    this.setState({ commands: newCommands }, this.saveState.bind(this));
    this.props.onCommandClicked(args);
  }

  private handleCustomClicked() {
    this.onCloseMenu();
    let customArgs = prompt("bazel");
    if (!customArgs) {
      return;
    }
    this.handleCommandClicked(customArgs);
  }

  render() {
    return (
      <PopupContainer>
        <OutlinedButtonGroup>
          <OutlinedButton
            className="workflow-rerun-button"
            onClick={this.handleCommandClicked.bind(this, this.state.commands[0])}>
            {this.props.isLoading ? <Spinner /> : <PlayCircle className="icon green" />}
            <span>{this.state.commands[0]}</span>
          </OutlinedButton>
          <OutlinedButton className="icon-button" onClick={this.onOpenMenu.bind(this)}>
            <ChevronDown />
          </OutlinedButton>
        </OutlinedButtonGroup>
        <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)} anchor="right">
          <Menu>
            {this.state.commands.slice(1).map((command) => (
              <MenuItem onClick={this.handleCommandClicked.bind(this, command)}>{command}</MenuItem>
            ))}
            <MenuItem onClick={this.handleCustomClicked.bind(this, undefined)}>Custom...</MenuItem>
          </Menu>
        </Popup>
      </PopupContainer>
    );
  }
}
