import React from "react";
import { OutlinedButton } from "../../../app/components/button/button";
import { OutlinedButtonGroup } from "../../../app/components/button/button_group";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Popup, { PopupContainer } from "../../../app/components/popup/popup";
import Spinner from "../../../app/components/spinner/spinner";
import { Boxes, ChevronDown } from "lucide-react";

export interface CodeBuildButtonProps {
  onCommandClicked: (args: string) => void;
  onDefaultConfig: (config: string) => void;
  isLoading: boolean;
  project: string;
  commands: string[];
  defaultConfig: string;
}

type State = {
  isMenuOpen: boolean;
};

export default class CodeBuildButton extends React.Component<CodeBuildButtonProps, State> {
  state: State = {
    isMenuOpen: false,
  };

  private onOpenMenu() {
    this.setState({ isMenuOpen: true });
  }
  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
  }

  private handleCommandClicked(args: string) {
    this.onCloseMenu();
    this.props.onCommandClicked(args);
  }

  private handleCustomClicked(defaultValue: string) {
    this.onCloseMenu();
    let customArgs = prompt("bazel", defaultValue);
    if (!customArgs) {
      return;
    }
    if (customArgs.startsWith("bazel ")) {
      customArgs = customArgs.replace("bazel ", "");
    }
    this.handleCommandClicked(customArgs);
  }

  private handleDefaultConfigClicked() {
    this.onCloseMenu();
    this.props.onDefaultConfig(prompt("config") || "");
  }

  private getConfig() {
    return this.props.defaultConfig ? ` --config=${this.props.defaultConfig}` : "";
  }

  render() {
    return (
      <PopupContainer>
        <OutlinedButtonGroup>
          <OutlinedButton
            className="workflow-rerun-button"
            onClick={this.handleCommandClicked.bind(this, this.props.commands[0])}>
            {this.props.isLoading ? <Spinner /> : <Boxes className="icon green" />}
            <span>
              {this.props.commands[0]}
              {this.getConfig()}
            </span>
          </OutlinedButton>
          <OutlinedButton className="icon-button" onClick={this.onOpenMenu.bind(this)}>
            <ChevronDown />
          </OutlinedButton>
        </OutlinedButtonGroup>
        <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)} anchor="right">
          <Menu>
            {this.props.commands.slice(1).map((command) => (
              <MenuItem onClick={this.handleCommandClicked.bind(this, command)}>
                {command}
                {this.getConfig()}
              </MenuItem>
            ))}
            <MenuItem onClick={this.handleDefaultConfigClicked.bind(this, undefined)}>Set default config...</MenuItem>
            <MenuItem onClick={this.handleCustomClicked.bind(this, this.props.commands[0])}>Edit...</MenuItem>
            <MenuItem onClick={this.handleCustomClicked.bind(this, "")}>New...</MenuItem>
          </Menu>
        </Popup>
      </PopupContainer>
    );
  }
}
