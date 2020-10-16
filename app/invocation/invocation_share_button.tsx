import React from "react";
import { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import { FilledButton, OutlinedButton } from "../components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../components/dialog/dialog";
import Input from "../components/input/input";
import Modal from "../components/modal/modal";
import Select, { Option } from "../components/select/select";
import InvocationModel from "./invocation_model";

export interface InvocationShareButtonComponentProps {
  user?: User;
  model: InvocationModel;
}

interface State {
  isOpen: boolean;
  // TODO: Replace with request proto once it's available.
  visibility?: VisibilitySelection;
}

type VisibilitySelection = "owner" | "group" | "public";

export default class InvocationShareButtonComponent extends React.Component<
  InvocationShareButtonComponentProps,
  State
> {
  state: State = { isOpen: false };

  private inputRef = React.createRef<HTMLInputElement>();

  componentDidMount() {
    // TODO: Fetch current invocation permissions
  }

  private onClick() {
    this.setState({ isOpen: true });
  }

  private onRequestClose() {
    this.setState({ isOpen: false });
  }

  private onInputClick() {
    this.inputRef.current.select();
  }

  private onVisibilityChange(e: React.ChangeEvent) {
    const visibility = (e.target as HTMLSelectElement).value as VisibilitySelection;
    this.setState({ visibility });

    // TODO: wire up mutate RPC
  }

  private onCopyLinkButtonClick() {
    this.inputRef.current.select();
    document.execCommand("copy");
  }

  render() {
    if (!this.props.user || window.localStorage["invocation_sharing"] !== "true") {
      return <></>;
    }

    const { visibility } = this.state;

    return (
      <>
        <FilledButton
          className="invocation-share-button"
          disabled={!capabilities.invocationSharing}
          onClick={this.onClick.bind(this)}>
          {/* TODO: Use an icon that signifies the current permissions */}
          <img src="/image/share-2-white.svg" alt="" />
          Share
        </FilledButton>
        <Modal isOpen={this.state.isOpen} onRequestClose={this.onRequestClose.bind(this)}>
          <Dialog>
            <DialogHeader>
              <DialogTitle>Get link</DialogTitle>
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
                <OutlinedButton onClick={this.onCopyLinkButtonClick.bind(this)}>Copy link</OutlinedButton>
              </div>
              <div>
                <div className="visibility-header">Visibility</div>
                <Select onChange={this.onVisibilityChange.bind(this)}>
                  <Option value="owner">Private</Option>
                  <Option value="group">{this.props.user.selectedGroup.name}</Option>
                  <Option value="public">Anyone with the link</Option>
                </Select>
                <div className="visibility-explanation">
                  {(visibility === "owner" || !visibility) && "Only you can view"}
                  {visibility === "group" && `Anyone in this organization with the link can view`}
                  {visibility === "public" && "Anyone on the Internet with this link can view"}
                </div>
              </div>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                <FilledButton onClick={this.onRequestClose.bind(this)}>Done</FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
