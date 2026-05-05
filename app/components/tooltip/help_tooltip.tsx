import { HelpCircle } from "lucide-react";
import React from "react";
import { Tooltip, pinBottomLeftOffsetFromMouse, type PinPositionFunc } from "./tooltip";

export type HelpTooltipProps = React.HTMLAttributes<HTMLDivElement> & {
  /** Content to show when the help icon is hovered. */
  children: React.ReactNode;
  /** Specifies the pin position of the tooltip. */
  pin?: PinPositionFunc;
};

/** HelpTooltip renders a compact help icon with hoverable explanatory text. */
export default function HelpTooltip({
  "aria-label": ariaLabel,
  children,
  className,
  onClick,
  onKeyDown,
  pin = pinBottomLeftOffsetFromMouse,
  role,
  tabIndex,
  ...props
}: HelpTooltipProps) {
  const isButton = !!onClick;

  const handleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    onKeyDown?.(e);
    if (e.defaultPrevented) {
      return;
    }
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      e.currentTarget.click();
    }
  };

  return (
    <Tooltip
      aria-label={isButton ? (ariaLabel ?? "Help") : ariaLabel}
      className={`help-tooltip ${className || ""}`}
      onClick={onClick}
      onKeyDown={isButton ? handleKeyDown : onKeyDown}
      pin={pin}
      renderContent={() => <div className="help-tooltip-content">{children}</div>}
      role={isButton ? "button" : role}
      tabIndex={isButton ? (tabIndex ?? 0) : tabIndex}
      {...props}>
      <HelpCircle className="icon help-tooltip-icon" />
    </Tooltip>
  );
}
