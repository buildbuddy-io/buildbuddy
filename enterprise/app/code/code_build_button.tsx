import React from "react";
import { OutlinedButton } from "../../../app/components/button/button";
import { OutlinedButtonGroup } from "../../../app/components/button/button_group";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Popup, { PopupContainer } from "../../../app/components/popup/popup";

export interface CodeBuildButtonProps {
    handleButtonClicked: (args: string) => void;
    isLoading: boolean;
}

type State = {
  isMenuOpen?: boolean;
};

export default class CodeBuildButton extends React.Component<CodeBuildButtonProps, State> {
  state: State = {};

  private onOpenMenu() {
    this.setState({ isMenuOpen: true });
  }
  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
  }

  render() {
    return (
        <PopupContainer>
          <OutlinedButtonGroup>
            <OutlinedButton
              className="workflow-rerun-button"
              onClick={this.props.handleButtonClicked.bind(this, "build //...")}>
              {this.props.isLoading ? <div className="loading"></div> : <img alt="" src="/image/play-circle.svg" />}
              <span>bazel build //...</span>
            </OutlinedButton>
            <OutlinedButton className="icon-button" onClick={this.onOpenMenu.bind(this)}>
              <img alt="" src="/image/chevron-down.svg" />
            </OutlinedButton>
          </OutlinedButtonGroup>
          <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)} anchor="left">
            <Menu>
            <MenuItem onClick={this.props.handleButtonClicked.bind(this, "test //...")}>bazel test //...</MenuItem>
            <MenuItem onClick={this.props.handleButtonClicked.bind(this, undefined)}>Custom...</MenuItem>
            </Menu>
          </Popup>
        </PopupContainer>
      
    );
  }
}
