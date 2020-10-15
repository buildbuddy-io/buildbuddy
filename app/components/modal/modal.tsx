import React from "react";
import ReactModal from "react-modal";

export type ModalProps = ReactModal.Props;

ReactModal.setAppElement("#app");

export default class Modal extends React.Component<ModalProps> {
  render() {
    return <ReactModal overlayClassName="modal-overlay" className="modal-content" {...this.props} />;
  }
}
