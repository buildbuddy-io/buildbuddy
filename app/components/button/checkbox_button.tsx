import React from "react";
import { OutlinedButton } from "./button";

export default function CheckboxButton({
  children,
  checked,
  onChange,
  className,
  ...props
}: JSX.IntrinsicElements["input"]) {
  return (
    <OutlinedButton className={`checkbox-button ${className || ""}`}>
      <label>
        <span>{children} </span>
        <input type="checkbox" checked={checked} onChange={onChange} {...props} />
      </label>
    </OutlinedButton>
  );
}
