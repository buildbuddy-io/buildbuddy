import React from "react";
import Modal from "../modal/modal";
import Dialog, { DialogHeader, DialogTitle, DialogBody, DialogFooter, DialogFooterButtons } from "./dialog";
import Spinner from "../spinner/spinner";
import FilledButton, { OutlinedButton } from "../button/button";

export interface SimpleModalDialogProps {
  children: React.ReactNode;

  title: React.ReactNode;
  isOpen: boolean;
  submitLabel: string;
  onRequestClose: () => any;
  onSubmit: () => any;

  className?: string;
  loading?: boolean;
  submitDisabled?: boolean;
  destructive?: boolean;
}

/**
 * Shows a simple modal dialog with a title, body, cancel button, and submit
 * button.
 *
 * The contents are rendered as a form, so that pressing Enter will submit the
 * form. If form submission triggers an async action such as an RPC, then the
 * `loading` prop should be provided to indicate whether the RPC is in progress.
 */
export default class SimpleModalDialog extends React.Component<SimpleModalDialogProps> {
  render() {
    return (
      <Modal isOpen={this.props.isOpen} onRequestClose={this.props.loading ? undefined : this.props.onRequestClose}>
        <Dialog className={this.props.className || ""}>
          <DialogHeader>
            <DialogTitle>{this.props.title}</DialogTitle>
          </DialogHeader>
          <form className="dialog-form" onSubmit={this.props.onSubmit}>
            <DialogBody>{this.props.children}</DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                {this.props.loading && <Spinner />}
                <OutlinedButton type="button" onClick={this.props.onRequestClose} disabled={this.props.loading}>
                  Cancel
                </OutlinedButton>
                <FilledButton
                  type="submit"
                  onClick={this.props.onSubmit}
                  className={this.props.destructive ? "destructive" : ""}
                  disabled={this.props.submitDisabled || this.props.loading}>
                  {this.props.submitLabel}
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </form>
        </Dialog>
      </Modal>
    );
  }
}
