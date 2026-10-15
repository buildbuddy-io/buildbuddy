import { ListChevronsDownUp, ListChevronsUpDown } from "lucide-react";
import React from "react";
import { OutlinedButton } from "../button/button";

export type ViewMode = "summary" | "details";

export interface ViewModeToggleProps {
  viewMode: ViewMode;
  onChange: (viewMode: ViewMode) => void;
  showLabel?: boolean;
}

export default function ViewModeToggle({ viewMode, onChange, showLabel = false }: ViewModeToggleProps) {
  const isSummary = viewMode === "summary";
  const label = isSummary ? "Expand all" : "Collapse all";
  return (
    <div className="view-mode-toggle">
      <OutlinedButton
        className={showLabel ? "small-button" : "icon-button"}
        title={showLabel ? label : isSummary ? "Show details" : "Hide details"}
        onClick={() => onChange(isSummary ? "details" : "summary")}>
        {isSummary ? (
          <ListChevronsUpDown className="icon" aria-hidden="true" />
        ) : (
          <ListChevronsDownUp className="icon" aria-hidden="true" />
        )}
        {showLabel && label}
      </OutlinedButton>
    </div>
  );
}
