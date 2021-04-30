import React from "react";

export type LinkButtonProps = JSX.IntrinsicElements["a"];

export const LinkButton = React.forwardRef((props: LinkButtonProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { className, ...rest } = props;
  return <a role="button" ref={ref} className={`base-button filled-button link-button ${className || ""}`} {...rest} />;
});

export const OutlinedLinkButton = React.forwardRef((props: LinkButtonProps, ref: React.Ref<HTMLAnchorElement>) => {
  const { className, ...rest } = props;
  return (
    <a role="button" ref={ref} className={`base-button outlined-button link-button ${className || ""}`} {...rest} />
  );
});

export default LinkButton;
