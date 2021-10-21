import React from "react";

export type OutlinedButtonGroupProps = JSX.IntrinsicElements["div"];

/**
 * OutlinedButtonGroup wraps multiple OutlineButtons as direct children,
 * rendering the outline around the whole group and coalescing the outlines
 * between buttons.
 */
export const OutlinedButtonGroup = React.forwardRef(
  ({ className, ...props }: OutlinedButtonGroupProps, ref: React.Ref<HTMLDivElement>) => {
    return <div ref={ref} className={`outlined-button-group ${className || ""}`} {...props} />;
  }
);
