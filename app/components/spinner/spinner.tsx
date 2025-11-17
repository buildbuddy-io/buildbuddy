import React from "react";

export type SpinnerProps = JSX.IntrinsicElements["div"];

export default function Spinner({ className, ...rest }: SpinnerProps): JSX.Element {
  return <div className={`spinner ${className || ""}`} {...rest} />;
}
