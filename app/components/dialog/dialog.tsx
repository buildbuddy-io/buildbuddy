import React from "react";

export type DialogProps = JSX.IntrinsicElements["div"];

export default class DialogComponent extends React.Component<DialogProps> {
  render() {
    const { className, ...props } = this.props;
    return (
      <div className="dialog-wrapper">
        <div className={`dialog-card ${className || ""}`} {...props} />
      </div>
    );
  }
}

export type DialogHeaderProps = JSX.IntrinsicElements["div"];

export class DialogHeader extends React.Component<DialogHeaderProps> {
  render() {
    const { className, ...props } = this.props;
    return <div className={`dialog-header ${className || ""}`} {...props} />;
  }
}

export type DialogTitleProps = JSX.IntrinsicElements["h2"];

export class DialogTitle extends React.Component<DialogTitleProps> {
  render() {
    const { className, ...props } = this.props;
    return <h2 className={`dialog-title ${className || ""}`} {...props} />;
  }
}

export type DialogBodyProps = JSX.IntrinsicElements["div"];

export class DialogBody extends React.Component<DialogBodyProps> {
  render() {
    const { className, ...props } = this.props;
    return <div className={`dialog-body ${className || ""}`} {...props} />;
  }
}

export type DialogFooterProps = JSX.IntrinsicElements["div"];

export class DialogFooter extends React.Component<DialogFooterProps> {
  render() {
    const { className, ...props } = this.props;
    return <div className={`dialog-footer ${className || ""}`} {...props} />;
  }
}

export type DialogFooterButtonsProps = JSX.IntrinsicElements["div"];

export class DialogFooterButtons extends React.Component<DialogFooterButtonsProps> {
  render() {
    const { className, ...props } = this.props;
    return <div className={`dialog-footer-buttons ${className || ""}`} {...props} />;
  }
}
