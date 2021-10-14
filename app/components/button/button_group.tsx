import React from "react";

export type OutlinedButtonGroupProps = JSX.IntrinsicElements["div"];

export function OutlinedButtonGroup({ className, ...props }: OutlinedButtonGroupProps) {
  return <div className={`outlined-button-group ${className || ""}`} {...props} />;
}
