import { ListChevronsDownUp, ListChevronsUpDown } from "lucide-react";
import React from "react";
import { OutlinedButton } from "../button/button";

export type ViewMode = "summary" | "details";

export interface ViewModeToggleProps {
  viewMode: ViewMode;
  onChange: (viewMode: ViewMode) => void;
}

export default function ViewModeToggle({ viewMode, onChange }: ViewModeToggleProps) {
  const isSummary = viewMode === "summary";
  const label = isSummary ? "Expand all" : "Collapse all";
  return (
    <div className="view-mode-toggle">
      <OutlinedButton
        className="small-button"
        title={label}
        onClick={() => onChange(isSummary ? "details" : "summary")}>
        {isSummary ? (
          <ListChevronsUpDown className="icon" aria-hidden="true" />
        ) : (
          <ListChevronsDownUp className="icon" aria-hidden="true" />
        )}
        {label}
      </OutlinedButton>
    </div>
  );
}
