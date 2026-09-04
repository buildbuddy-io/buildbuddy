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
  return (
    <div className="view-mode-toggle">
      <OutlinedButton
        className="icon-button"
        title={isSummary ? "Show details" : "Hide details"}
        onClick={() => onChange(isSummary ? "details" : "summary")}>
        {isSummary ? <ListChevronsUpDown /> : <ListChevronsDownUp />}
      </OutlinedButton>
    </div>
  );
}
