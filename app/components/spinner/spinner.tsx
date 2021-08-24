import React from "react";

export type SpinnerProps = JSX.IntrinsicElements["div"];

export default function Spinner({ className, ...rest }: SpinnerProps) {
  return <div className={`spinner ${className || ""}`} {...rest} />;
}
