import React from "react";

export type ButtonProps = JSX.IntrinsicElements["button"];

export const FilledButton = React.forwardRef((props: ButtonProps, ref: React.Ref<HTMLButtonElement>) => {
  const { className, ...rest } = props;
  return <button ref={ref} className={`button filled-button ${className || ""}`} {...rest} />;
});

export const OutlinedButton = React.forwardRef((props: ButtonProps, ref: React.Ref<HTMLButtonElement>) => {
  const { className, ...rest } = props;
  return <button ref={ref} className={`button outlined-button ${className || ""}`} {...rest} />;
});

export default FilledButton;
