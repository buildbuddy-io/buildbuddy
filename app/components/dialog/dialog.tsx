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

export type DialogCloseButtonProps = Omit<JSX.IntrinsicElements["button"], "children">;

export class DialogCloseButton extends React.Component<DialogCloseButtonProps> {
  render() {
    const { className, ...props } = this.props;
    return (
      <button className={`dialog-close-button ${className || ""}`} aria-label="Close" {...props}>
        <img src="/image/x.svg" />
      </button>
    );
  }
}

export type DialogBodyProps = JSX.IntrinsicElements["div"];

export class DialogBody extends React.Component<DialogBodyProps> {
  render() {
    const { className, ...props } = this.props;
    return <div className={`dialog-body ${className}`} {...props} />;
  }
}
