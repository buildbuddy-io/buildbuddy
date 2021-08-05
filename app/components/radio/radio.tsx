import React from "react";

export type RadioProps = Omit<JSX.IntrinsicElements["input"], "type">;

export function Radio({ className, ...rest }: RadioProps) {
  return <input type="radio" className={`radio-input ${className || ""}`} {...rest} />;
}

export default Radio;
