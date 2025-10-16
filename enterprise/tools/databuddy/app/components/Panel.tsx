import React from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import Collapsible from "./Collapsible";

export type PanelProps = React.PropsWithChildren<{
  title: string;
  info?: React.ReactNode;
  icon?: React.ReactNode;
  className?: string;
  controls?: React.ReactNode;
}>;

/** Collapsible panel component. */
export default function Panel({ title, icon, className, info, controls, children }: PanelProps) {
  return (
    <Collapsible>
      <div className={`panel ${className ?? ""}`}>
        <div className="panel-header">
          {icon}
          <div className="panel-title">
            <h2>{title}</h2>
          </div>
          {info && <div className="panel-info">{info}</div>}
          <div className="panel-controls">
            {controls}
            <Collapsible.Consumer>
              {(context) => (
                <button className="icon-button" onMouseDown={context?.toggleExpanded}>
                  {context?.expanded ? <ChevronDown /> : <ChevronUp />}
                </button>
              )}
            </Collapsible.Consumer>
          </div>
        </div>
        <Collapsible.Content className="panel-content">{children}</Collapsible.Content>
      </div>
    </Collapsible>
  );
}
