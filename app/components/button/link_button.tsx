import React from "react";
import Link from "../link/link";

export type LinkButtonProps = React.AnchorHTMLAttributes<HTMLAnchorElement>;

export const LinkButton: React.ForwardRefExoticComponent<
  LinkButtonProps & React.RefAttributes<HTMLAnchorElement>
> = React.forwardRef<HTMLAnchorElement, LinkButtonProps>((props, ref) => {
  const { className, ...rest } = props;
  return (
    <Link role="button" ref={ref} className={`base-button filled-button link-button ${className || ""}`} {...rest} />
  );
});

export const OutlinedLinkButton: React.ForwardRefExoticComponent<
  LinkButtonProps & React.RefAttributes<HTMLAnchorElement>
> = React.forwardRef<HTMLAnchorElement, LinkButtonProps>((props, ref) => {
  const { className, ...rest } = props;
  return (
    <Link role="button" ref={ref} className={`base-button outlined-button link-button ${className || ""}`} {...rest} />
  );
});

export default LinkButton;
