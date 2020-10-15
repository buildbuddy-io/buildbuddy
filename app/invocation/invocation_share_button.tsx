import React from "react";
import capabilities from "../capabilities/capabilities";
import InvocationModel from "./invocation_model";
import Modal from "../components/modal/modal";
import Button from "../components/button/button";
import Input from "../components/input/input";
import Dialog, { DialogHeader, DialogBody, DialogTitle, DialogCloseButton } from "../components/dialog/dialog";
import { User } from "../auth/auth_service";

export interface InvocationShareButtonComponentProps {
  user?: User;
  model: InvocationModel;
}

interface State {
  isOpen: boolean;
}

export default class InvocationShareButtonComponent extends React.Component<
  InvocationShareButtonComponentProps,
  State
> {
  state = { isOpen: false };

  private inputRef = React.createRef<HTMLInputElement>();

  private onClick() {
    this.setState({ isOpen: true });
  }

  private onRequestClose() {
    this.setState({ isOpen: false });
  }

  private onInputClick() {
    this.inputRef.current.select();
  }

  render() {
    if (!this.props.user || window.localStorage["invocation_sharing"] !== "true") {
      return <></>;
    }

    return (
      <>
        <Button
          className="invocation-share-button"
          disabled={!capabilities.invocationSharing}
          onClick={this.onClick.bind(this)}>
          {/* TODO: Have the icon represent the current permissions */}
          <img src="/image/share-2-white.svg" alt="" />
          Share
        </Button>
        <Modal isOpen={this.state.isOpen} onRequestClose={this.onRequestClose.bind(this)}>
          <Dialog>
            <DialogHeader>
              <DialogTitle>Get link</DialogTitle>
              <DialogCloseButton onClick={this.onRequestClose.bind(this)} />
            </DialogHeader>
            <DialogBody className="invocation-share-dialog">
              <div className="row">
                <Input
                  ref={this.inputRef}
                  readOnly={true}
                  value={window.location.href}
                  className="link-input"
                  onClick={this.onInputClick.bind(this)}
                />
                <Button>Copy link</Button>
              </div>
              <div>
                <label>
                  <input name="link-sharing-visibility" type="radio" value="owner" />
                  <span>Only you</span>
                </label>
                <label>
                  <input name="link-sharing-visibility" type="radio" value="group" />
                  <span>{this.props.user.selectedGroup.name}</span>
                </label>
                <label>
                  <input name="link-sharing-visibility" type="radio" value="owner" />
                  <span>Anyone with the link</span>
                </label>
              </div>
            </DialogBody>
          </Dialog>
        </Modal>
      </>
    );
  }
}
