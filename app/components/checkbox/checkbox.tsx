import React from "react";

export type CheckboxProps = Omit<JSX.IntrinsicElements["input"], "type">;

export function Checkbox({ className, ...rest }: CheckboxProps): JSX.Element {
  return <input type="checkbox" className={`checkbox-input ${className || ""}`} {...rest} />;
}

export default Checkbox;
