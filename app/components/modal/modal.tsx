import React from "react";
import ReactModal from "react-modal";

export type ModalProps = ReactModal.Props;

// Skip setting the app element in jasmine node tests (this fails)
if (typeof document !== "undefined") {
  ReactModal.setAppElement("#app");
}

export default class Modal extends React.Component<ModalProps> {
  render() {
    return <ReactModal overlayClassName="modal-overlay" className="modal-content" {...this.props} />;
  }
}
