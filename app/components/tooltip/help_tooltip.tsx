import { HelpCircle } from "lucide-react";
import React from "react";
import { Tooltip, pinBottomLeftOffsetFromMouse } from "./tooltip";

export type HelpTooltipProps = React.HTMLAttributes<HTMLDivElement> & {
  /** Content to show when the help icon is hovered. */
  children: React.ReactNode;
};

/** HelpTooltip renders a compact help icon with hoverable explanatory text. */
export default function HelpTooltip({
  children,
  className,
  onClick,
  onKeyDown,
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
      className={`help-tooltip ${className || ""}`}
      onClick={onClick}
      onKeyDown={isButton ? handleKeyDown : onKeyDown}
      pin={pinBottomLeftOffsetFromMouse}
      renderContent={() => <div className="help-tooltip-content">{children}</div>}
      role={isButton ? "button" : role}
      tabIndex={isButton ? (tabIndex ?? 0) : tabIndex}
      {...props}>
      <HelpCircle className="icon help-tooltip-icon" />
    </Tooltip>
  );
}
