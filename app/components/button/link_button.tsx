import React from "react";
import Link from "../link/link";

export type LinkButtonProps = React.AnchorHTMLAttributes<HTMLAnchorElement>;

export const LinkButton = React.forwardRef((props: LinkButtonProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { className, ...rest } = props;
  return (
    <Link role="button" ref={ref} className={`base-button filled-button link-button ${className || ""}`} {...rest} />
  );
});

export const OutlinedLinkButton = React.forwardRef((props: LinkButtonProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { className, ...rest } = props;
  return (
    <Link role="button" ref={ref} className={`base-button outlined-button link-button ${className || ""}`} {...rest} />
  );
});

export default LinkButton;
