import React from "react";
import { OutlinedButton } from "../components/button/button";
import LinkButton from "../components/button/link_button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../components/dialog/dialog";
import Modal from "../components/modal/modal";

export interface LinkGithubRepoModalProps {
  isOpen: boolean;
  onRequestClose: () => any;
}

export default class LinkGithubRepoModal extends React.Component<LinkGithubRepoModalProps> {
  render(): React.ReactNode {
    return (
      <Modal className="link-repo-modal" isOpen={this.props.isOpen} onRequestClose={this.props.onRequestClose}>
        <Dialog>
          <DialogHeader>
            <DialogTitle>GitHub link required</DialogTitle>
          </DialogHeader>
          <DialogBody>
            {/*TODO(Maggie): Link to remote bazel docs*/}
            <p>To use this feature, link this GitHub repository to your BuildBuddy organization.</p>
          </DialogBody>
          <DialogFooter>
            <DialogFooterButtons>
              <OutlinedButton onClick={this.props.onRequestClose}>Cancel</OutlinedButton>
              <LinkButton href="/workflows" target="_blank" onClick={this.props.onRequestClose}>
                Link a Repository
              </LinkButton>
            </DialogFooterButtons>
          </DialogFooter>
        </Dialog>
      </Modal>
    );
  }
}
