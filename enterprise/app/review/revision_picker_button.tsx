import React from "react";
import { github } from "../../../proto/github_ts_proto";
import { OutlinedButton } from "../../../app/components/button/button";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Popup from "../../../app/components/popup/popup";

export interface Props {
  baseCommitSha: string;
  prCommits: github.PrCommit[];
}

interface State {
  isDropdownOpen: boolean;
}

export default class RevisionPickerButton extends React.Component<Props, State> {
  state: State = {
    isDropdownOpen: false,
  };

  private onClick() {
    this.setState({ isDropdownOpen: true });
  }

  private onClickRevision(sha: string) {
    console.log(sha);
  }

  private onRequestCloseDropdown() {
    this.setState({ isDropdownOpen: false });
  }

  render() {
    return (
      <div>
        <OutlinedButton className="small-button" onClick={() => this.onClick()}>
          <div>Compare</div>
        </OutlinedButton>
        <Popup isOpen={this.state.isDropdownOpen} onRequestClose={this.onRequestCloseDropdown.bind(this)}>
          <Menu>
            <MenuItem onClick={this.onClickRevision.bind(this, this.props.baseCommitSha)}>PR Base</MenuItem>
            {this.props.prCommits.map((c) => (
              <MenuItem onClick={this.onClickRevision.bind(this, c.sha)}></MenuItem>
            ))}
          </Menu>
        </Popup>
      </div>
    );
  }
}
